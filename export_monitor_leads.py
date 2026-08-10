"""
export_monitor_leads.py — Monitor de Leads (Outros Monitores).

De hora em hora (cron :10):
  1. Conta leads criados no MySQL camim_leads_production (LEADS_DB_* do .env):
     por hora (últimas 72h), por dia (30d), hoje/ontem e por fonte (hoje);
  2. Grava json_consolidado/monitor_leads.json (consumido por
     /api/monitores/leads na página outros_monitores.html);
  3. Envia e-mail horário aos usuários inscritos (tabela monitor_notificacoes
     do camim_auth.db, gerida pela própria página) — só entre 07h e 22h.

O banco de leads guarda created_at em UTC e o servidor local é America/Sao_
Paulo: o offset é calculado AO VIVO comparando NOW() do MySQL com o relógio
local — nada de -3 fixo (horário de verão ou mudança de TZ do servidor
quebrariam a conta em silêncio).
"""

import os
import sys
import json
import sqlite3
import smtplib
from email.mime.text import MIMEText
from datetime import datetime, date, timedelta

import pymysql
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

OUT_PATH = os.path.join(BASE_DIR, "json_consolidado", "monitor_leads.json")


def _resolver_auth_db() -> str:
    """O serviço web usa AUTH_DB_PATH de /etc/camim-auth.env
    (/var/lib/camim-auth/camim_auth.db) — que NÃO está no .env deste projeto.
    Sem esta resolução o ETL lia outro camim_auth.db e via 'nenhum inscrito'
    com inscrições existindo (medido em 2026-08-10)."""
    candidatos = [
        os.getenv("AUTH_DB_PATH"),
        "/var/lib/camim-auth/camim_auth.db",
        "/opt/relatorio_h_t/camim_auth.db",
    ]
    for c in candidatos:
        if c and os.path.isfile(c):
            return c
    return candidatos[-1]


AUTH_DB = _resolver_auth_db()
LINK_PAGINA = "https://kpi.camim.com.br/outros_monitores.html"

EMAIL_HOST = os.getenv("ALARM_EMAIL_HOST", "smtp.gmail.com")
EMAIL_PORT = int(os.getenv("ALARM_EMAIL_PORT", "465"))
EMAIL_USER = os.getenv("ALARM_EMAIL_USER", "")
EMAIL_PASSWORD = os.getenv("ALARM_EMAIL_PASSWORD", "")
EMAIL_FROM = os.getenv("ALARM_EMAIL_FROM", "") or EMAIL_USER


def log(msg: str) -> None:
    print(f"{datetime.now().isoformat(timespec='seconds')} {msg}", flush=True)


def conn_leads():
    cfg = {
        "host": os.getenv("LEADS_DB_HOST"),
        "port": int(os.getenv("LEADS_DB_PORT", "3306")),
        "user": os.getenv("LEADS_DB_USER"),
        "password": os.getenv("LEADS_DB_PASSWORD"),
        "database": os.getenv("LEADS_DB_NAME"),
        "charset": "utf8mb4",
        "cursorclass": pymysql.cursors.DictCursor,
    }
    if not cfg["host"] or not cfg["database"]:
        log("ERRO: LEADS_DB_* não configurado no .env")
        sys.exit(1)
    return pymysql.connect(**cfg)


def offset_horas(cur) -> int:
    """Horas a SOMAR no created_at para virar hora local (negativo p/ UTC→BRT)."""
    cur.execute("SELECT NOW() AS agora")
    db_now = cur.fetchone()["agora"]
    delta = datetime.now() - db_now
    return round(delta.total_seconds() / 3600)


def coletar() -> dict:
    c = conn_leads()
    try:
        cur = c.cursor()
        off = offset_horas(cur)
        shift = f"created_at + INTERVAL {off} HOUR"
        log(f"offset MySQL→local: {off:+d}h")

        # ATENÇÃO: '%' SIMPLES no DATE_FORMAT. execute() sem params não
        # interpola, e '%%' chegava literal no MySQL → todas as linhas caíam
        # num único balde de rótulo "%Y-%m-%d %H:00" (gráfico com 1 barra e
        # última hora sempre 0 — bug de 2026-08-10).
        cur.execute(
            f"SELECT DATE_FORMAT({shift}, '%Y-%m-%d %H:00') h, COUNT(*) n "
            f"FROM leads WHERE created_at >= NOW() - INTERVAL 75 HOUR "
            f"GROUP BY 1 ORDER BY 1")
        horas = [{"h": r["h"], "n": int(r["n"])} for r in cur.fetchall()]

        cur.execute(
            f"SELECT DATE({shift}) d, COUNT(*) n "
            f"FROM leads WHERE created_at >= NOW() - INTERVAL 32 DAY "
            f"GROUP BY 1 ORDER BY 1")
        dias = [{"d": str(r["d"]), "n": int(r["n"])} for r in cur.fetchall()]

        cur.execute(
            f"SELECT COALESCE(s.title, CONCAT('fonte #', l.leadsource_id), 'sem fonte') fonte, "
            f"       COUNT(*) n "
            f"FROM leads l LEFT JOIN leadsources s ON s.id = l.leadsource_id "
            f"WHERE DATE(l.created_at + INTERVAL {off} HOUR) = DATE(NOW() + INTERVAL {off} HOUR) "
            f"GROUP BY 1 ORDER BY 2 DESC")
        fontes = [{"fonte": str(r["fonte"]), "n": int(r["n"])} for r in cur.fetchall()]
    finally:
        c.close()

    hoje = date.today().isoformat()
    ontem = (date.today() - timedelta(days=1)).isoformat()
    total_hoje = sum(x["n"] for x in dias if x["d"] == hoje)
    total_ontem = sum(x["n"] for x in dias if x["d"] == ontem)

    # última hora FECHADA (ex.: às 12:10 reporta o balde 11:00)
    ult = (datetime.now() - timedelta(hours=1)).strftime("%Y-%m-%d %H:00")
    n_ult = next((x["n"] for x in horas if x["h"] == ult), 0)

    return {
        "gerado_em": datetime.now().astimezone().isoformat(timespec="seconds"),
        "horas": horas[-72:],
        "dias": dias,
        "hoje": total_hoje,
        "ontem": total_ontem,
        "ultima_hora": {"label": ult[-5:], "n": n_ult},
        "fontes_hoje": fontes,
    }


# ---------------------------------------------------------------------------
# Notificação por inscrito, com preferências (engrenagem da página):
#   modo:    sempre | sem_lead (alerta de silêncio) | com_lead
#   dias:    seg_sex / sab / dom (união)
#   horarios: 7_18 / 8_22 / 24h (união das faixas)
# Fora da janela nada é enviado; o PRIMEIRO e-mail da janela seguinte traz o
# resumo das horas não avisadas (gerenciador de pendências). O estado de
# "último e-mail por inscrito" fica em arquivo PRÓPRIO do ETL — este script
# roda como root e NÃO PODE escrever no camim_auth.db (sqlite criaria -wal
# de root e derrubaria a escrita do serviço web; incidente alarmes.db
# 2026-08-10). A leitura das inscrições é mode=ro.
# ---------------------------------------------------------------------------
CFG_PADRAO = {"modo": "sempre", "dias": ["seg_sex", "sab", "dom"],
              "horarios": ["8_22"]}
FAIXAS = {"7_18": range(7, 19), "8_22": range(8, 23), "24h": range(0, 24)}
STATE_PATH = os.path.join(BASE_DIR, "json_consolidado",
                          "monitor_leads_notif_state.json")
DIGEST_MAX_HORAS = 24


def inscritos(monitor_key: str) -> list[tuple[str, dict]]:
    """[(email, config)] dos ativos. Só leitura (mode=ro), conexão fechada."""
    conn = sqlite3.connect(f"file:{AUTH_DB}?mode=ro", uri=True)
    try:
        try:
            rows = conn.execute(
                "SELECT email, config FROM monitor_notificacoes "
                "WHERE monitor_key=? AND ativo=1", (monitor_key,)).fetchall()
        except sqlite3.OperationalError:
            try:  # base antiga sem a coluna config
                rows = [(r[0], None) for r in conn.execute(
                    "SELECT email FROM monitor_notificacoes "
                    "WHERE monitor_key=? AND ativo=1", (monitor_key,)).fetchall()]
            except sqlite3.OperationalError:
                return []
        out = []
        for email, cfg_raw in rows:
            try:
                cfg = json.loads(cfg_raw) if cfg_raw else {}
            except Exception:
                cfg = {}
            out.append((email, {**CFG_PADRAO, **{k: v for k, v in cfg.items() if v}}))
        return out
    finally:
        conn.close()


def _dia_cat(dt: datetime) -> str:
    wd = dt.weekday()
    return "seg_sex" if wd <= 4 else ("sab" if wd == 5 else "dom")


def _horas_permitidas(cfg: dict) -> set[int]:
    horas: set[int] = set()
    for faixa in cfg.get("horarios") or []:
        horas.update(FAIXAS.get(faixa, []))
    return horas or set(FAIXAS["8_22"])


def _load_state() -> dict:
    try:
        with open(STATE_PATH, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_state(state: dict) -> None:
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False)
    os.replace(tmp, STATE_PATH)


def _pendencias(dados: dict, desde_iso: str | None, ate_label: str) -> list[str]:
    """Linhas-resumo das horas fechadas SEM e-mail desde o último envio
    (exclui a hora atual, que vai no corpo principal). Cap de 24h."""
    por_h = {x["h"]: x["n"] for x in dados.get("horas") or []}
    agora = datetime.now()
    try:
        desde = datetime.fromisoformat(desde_iso) if desde_iso else None
    except Exception:
        desde = None
    inicio = agora - timedelta(hours=DIGEST_MAX_HORAS)
    if desde and desde > inicio:
        inicio = desde
    linhas = []
    # Começa no PRÓPRIO balde do último e-mail: o e-mail enviado às 10h14
    # reportou o balde das 9h — o das 10h ainda não tinha fechado e ficaria
    # órfão se partíssemos de last+1h.
    h = inicio.replace(minute=0, second=0, microsecond=0)
    fim = agora.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
    while h <= fim:
        chave = h.strftime("%Y-%m-%d %H:00")
        if chave[-5:] != ate_label:  # hora atual fica fora do resumo
            linhas.append(f"  {h.strftime('%d/%m %Hh')}: {por_h.get(chave, 0)} lead(s)")
        h += timedelta(hours=1)
    return linhas


def notificar(dados: dict) -> None:
    lista = inscritos("leads")
    if not lista:
        log("nenhum inscrito para notificação de leads")
        return
    if not EMAIL_USER or not EMAIL_PASSWORD:
        log("ALARM_EMAIL_* ausente — notificações não enviadas")
        return

    agora = datetime.now()
    u = dados["ultima_hora"]
    top_fontes = " · ".join(f"{f['fonte']}: {f['n']}" for f in dados["fontes_hoje"][:4])
    state = _load_state()
    mudou = False

    try:
        smtp = smtplib.SMTP_SSL(EMAIL_HOST, EMAIL_PORT, timeout=30)
        smtp.login(EMAIL_USER, EMAIL_PASSWORD)
    except Exception as e:
        log(f"SMTP FALHOU: {e}")
        return
    try:
        for dest, cfg in lista:
            if _dia_cat(agora) not in (cfg.get("dias") or []):
                log(f"{dest}: dia não habilitado ({_dia_cat(agora)}) — pulado")
                continue
            if agora.hour not in _horas_permitidas(cfg):
                log(f"{dest}: fora do horário escolhido ({agora.hour}h) — pulado")
                continue
            modo = cfg.get("modo") or "sempre"
            if modo == "com_lead" and u["n"] == 0:
                log(f"{dest}: modo só-com-lead e última hora=0 — pulado")
                continue
            if modo == "sem_lead" and u["n"] > 0:
                log(f"{dest}: modo só-sem-lead e última hora={u['n']} — pulado")
                continue

            pend = _pendencias(dados, state.get(dest), u["label"])
            corpo = (
                f"Leads criados na última hora ({u['label']}): {u['n']}\n"
                f"Hoje até agora: {dados['hoje']}  (ontem o dia todo: {dados['ontem']})\n"
                + (f"Fontes de hoje: {top_fontes}\n" if top_fontes else "")
            )
            if pend:
                corpo += ("\nHoras sem aviso desde o seu último e-mail:\n"
                          + "\n".join(pend) + "\n")
            corpo += (f"\nPainel: {LINK_PAGINA}\n\n"
                      "— Monitor de Leads (ajuste modo, dias e horário na "
                      "engrenagem ao lado do sino na página).")
            assunto = f"Leads {u['label']}h: {u['n']} na última hora · {dados['hoje']} hoje"
            try:
                msg = MIMEText(corpo, "plain", "utf-8")
                msg["Subject"] = assunto
                msg["From"] = EMAIL_FROM
                msg["To"] = dest
                smtp.sendmail(EMAIL_FROM, [dest], msg.as_string())
                log(f"notificação enviada: {dest}"
                    + (f" (+{len(pend)} hora(s) resumida(s))" if pend else ""))
                state[dest] = agora.isoformat(timespec="seconds")
                mudou = True
            except Exception as e:
                log(f"EMAIL FALHOU para {dest}: {e}")
    finally:
        smtp.quit()
    if mudou:
        _save_state(state)


def main() -> int:
    log("=== export_monitor_leads: início ===")
    dados = coletar()
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    tmp = OUT_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(dados, f, ensure_ascii=False)
    os.replace(tmp, OUT_PATH)
    log(f"JSON salvo: {OUT_PATH} — última hora {dados['ultima_hora']['label']} = "
        f"{dados['ultima_hora']['n']} · hoje {dados['hoje']}")
    notificar(dados)
    log("=== export_monitor_leads: fim ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
