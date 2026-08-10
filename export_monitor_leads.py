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
AUTH_DB = os.getenv("AUTH_DB_PATH", "/opt/relatorio_h_t/camim_auth.db")
LINK_PAGINA = "https://kpi.camim.com.br/outros_monitores.html"

EMAIL_HOST = os.getenv("ALARM_EMAIL_HOST", "smtp.gmail.com")
EMAIL_PORT = int(os.getenv("ALARM_EMAIL_PORT", "465"))
EMAIL_USER = os.getenv("ALARM_EMAIL_USER", "")
EMAIL_PASSWORD = os.getenv("ALARM_EMAIL_PASSWORD", "")
EMAIL_FROM = os.getenv("ALARM_EMAIL_FROM", "") or EMAIL_USER
HORA_MIN_NOTIF, HORA_MAX_NOTIF = 7, 22   # não acordar ninguém de madrugada


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

        cur.execute(
            f"SELECT DATE_FORMAT({shift}, '%%Y-%%m-%%d %%H:00') h, COUNT(*) n "
            f"FROM leads WHERE created_at >= NOW() - INTERVAL 75 HOUR "
            f"GROUP BY 1 ORDER BY 1")
        horas = [{"h": r["h"], "n": int(r["n"])} for r in cur.fetchall()]

        cur.execute(
            f"SELECT DATE({shift}) d, COUNT(*) n "
            f"FROM leads WHERE created_at >= NOW() - INTERVAL 32 DAY "
            f"GROUP BY 1 ORDER BY 1")
        dias = [{"d": str(r["d"]), "n": int(r["n"])} for r in cur.fetchall()]

        cur.execute(
            f"SELECT COALESCE(s.name, CONCAT('fonte #', l.leadsource_id), 'sem fonte') fonte, "
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


def inscritos(monitor_key: str) -> list[str]:
    """E-mails inscritos. Tabela criada pelo blueprint; aqui só leitura.
    Conexão FECHADA explicitamente (Py3.14 não fecha no GC — ver CLAUDE.md)."""
    conn = sqlite3.connect(AUTH_DB)
    try:
        try:
            rows = conn.execute(
                "SELECT email FROM monitor_notificacoes "
                "WHERE monitor_key=? AND ativo=1", (monitor_key,)).fetchall()
        except sqlite3.OperationalError:
            return []   # tabela ainda não existe = ninguém inscrito
        return [r[0] for r in rows]
    finally:
        conn.close()


def notificar(dados: dict) -> None:
    hora = datetime.now().hour
    if not (HORA_MIN_NOTIF <= hora <= HORA_MAX_NOTIF):
        log(f"fora da janela de notificação ({hora}h) — e-mails suprimidos")
        return
    lista = inscritos("leads")
    if not lista:
        log("nenhum inscrito para notificação de leads")
        return
    if not EMAIL_USER or not EMAIL_PASSWORD:
        log("ALARM_EMAIL_* ausente — notificações não enviadas")
        return
    u = dados["ultima_hora"]
    top_fontes = " · ".join(f"{f['fonte']}: {f['n']}" for f in dados["fontes_hoje"][:4])
    corpo = (
        f"Leads criados na última hora ({u['label']}): {u['n']}\n"
        f"Hoje até agora: {dados['hoje']}  (ontem o dia todo: {dados['ontem']})\n"
        + (f"Fontes de hoje: {top_fontes}\n" if top_fontes else "")
        + f"\nPainel: {LINK_PAGINA}\n\n"
        f"— Monitor de Leads (para parar de receber, desligue o sino na página)."
    )
    assunto = f"Leads {u['label']}h: {u['n']} na última hora · {dados['hoje']} hoje"
    try:
        with smtplib.SMTP_SSL(EMAIL_HOST, EMAIL_PORT, timeout=30) as s:
            s.login(EMAIL_USER, EMAIL_PASSWORD)
            for dest in lista:
                msg = MIMEText(corpo, "plain", "utf-8")
                msg["Subject"] = assunto
                msg["From"] = EMAIL_FROM
                msg["To"] = dest
                s.sendmail(EMAIL_FROM, [dest], msg.as_string())
                log(f"notificação enviada: {dest}")
    except Exception as e:
        log(f"EMAIL FALHOU: {e}")


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
