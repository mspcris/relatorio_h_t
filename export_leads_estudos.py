"""
export_leads_estudos.py — Estudos de Leads & Vendas (Outros Monitores).

Complementa o export_monitor_leads.py (volume horário) com ANÁLISES de retorno,
sem tocar naquele robô nem no JSON dele. Gera json_consolidado/leads_estudos.json,
consumido por /api/monitores/leads/estudos na página outros_monitores.html.

O que responde (pedido do Cristiano em 19/09/2026):
  - MELHOR RETORNO: taxa de conversão por canal/fonte (matrículas ÷ leads),
    não só volume — o canal que enche a lista raramente é o que fecha.
  - QUE HORAS MAIS ENTRA LEAD: distribuição por hora do dia (0-23h) e por dia
    da semana, com a conversão de cada faixa.
  - VENDAS: conversão por posto, ranking de vendedores (quem fecha matrícula),
    e tendência mensal de 24 meses (períodos maiores).

CONVERSÃO = finish_lead_signup=1 (virou matrícula). ATENÇÃO: associate_date
NÃO é conversão — está preenchido em ~99% dos leads (medido 19/09/2026:
12.612 de 12.702 em 90d). Quem confundir os dois vai reportar 99% de fechamento.

Timezone: created_at é UTC e o servidor é America/Sao_Paulo. O offset é
calculado AO VIVO (NOW() do MySQL × relógio local) — mesma regra do
export_monitor_leads.py, nada de -3 fixo.
"""

import os
import sys
import json
from datetime import datetime

import pymysql
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

OUT_PATH = os.path.join(BASE_DIR, "json_consolidado", "leads_estudos.json")

# Janelas de análise (dias). "Períodos maiores" pedidos — inclui 12 meses.
JANELAS = [30, 90, 365]
MESES_TENDENCIA = 24

# Os 13 postos de atendimento, na mesma ordem do export_monitor_leads.py.
POSTOS_REDE = ["A", "N", "I", "X", "G", "Y", "B", "R", "M", "C", "D", "J", "P"]
POSTO_SQL = "COALESCE(NULLIF(TRIM(filialCode), ''), '?')"

# Fonte com pouquíssimo volume dá taxa-ruído (1/1 = 100%). O corte fica no
# frontend (que mostra o volume ao lado), mas guardo o mínimo sugerido no JSON.
MIN_VOL_TAXA = 20


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
    """Horas a SOMAR no created_at para virar hora local (negativo em UTC→BRT)."""
    cur.execute("SELECT NOW() AS agora")
    db_now = cur.fetchone()["agora"]
    delta = datetime.now() - db_now
    return round(delta.total_seconds() / 3600)


def _linhas(cur, sql):
    cur.execute(sql)
    return cur.fetchall()


def coletar_janela(cur, shift: str, dias: int) -> dict:
    """Todos os estudos de uma janela de N dias."""
    filtro = f"created_at >= NOW() - INTERVAL {dias} DAY"

    # total + conversão
    tot = _linhas(cur, f"SELECT COUNT(*) n, SUM(finish_lead_signup=1) c "
                       f"FROM leads WHERE {filtro}")[0]
    total = int(tot["n"] or 0)
    conv = int(tot["c"] or 0)

    # por fonte (canal) — o "melhor retorno"
    fontes = [
        {"fonte": str(r["fonte"]), "n": int(r["n"]), "c": int(r["c"] or 0)}
        for r in _linhas(cur,
            f"SELECT COALESCE(s.title, CONCAT('fonte #', l.leadsource_id), 'sem fonte') fonte, "
            f"COUNT(*) n, SUM(l.finish_lead_signup=1) c "
            f"FROM leads l LEFT JOIN leadsources s ON s.id = l.leadsource_id "
            f"WHERE l.{filtro} GROUP BY 1 HAVING n > 0 ORDER BY n DESC")
    ]

    # por posto
    postos = [
        {"p": str(r["p"]), "n": int(r["n"]), "c": int(r["c"] or 0)}
        for r in _linhas(cur,
            f"SELECT {POSTO_SQL} p, COUNT(*) n, SUM(finish_lead_signup=1) c "
            f"FROM leads WHERE {filtro} GROUP BY 1 ORDER BY n DESC")
    ]

    # por hora do dia (0-23, hora local)
    hmap = {int(r["h"]): {"n": int(r["n"]), "c": int(r["c"] or 0)}
            for r in _linhas(cur,
                f"SELECT HOUR({shift}) h, COUNT(*) n, SUM(finish_lead_signup=1) c "
                f"FROM leads WHERE {filtro} GROUP BY 1")}
    hora = [{"h": h, "n": hmap.get(h, {}).get("n", 0), "c": hmap.get(h, {}).get("c", 0)}
            for h in range(24)]

    # por dia da semana (MySQL DAYOFWEEK: 1=dom..7=sáb → 0=dom..6=sáb)
    dmap = {int(r["d"]) - 1: {"n": int(r["n"]), "c": int(r["c"] or 0)}
            for r in _linhas(cur,
                f"SELECT DAYOFWEEK({shift}) d, COUNT(*) n, SUM(finish_lead_signup=1) c "
                f"FROM leads WHERE {filtro} GROUP BY 1")}
    dow = [{"d": d, "n": dmap.get(d, {}).get("n", 0), "c": dmap.get(d, {}).get("c", 0)}
           for d in range(7)]

    # ranking de vendedores — quem FECHA matrícula (finish_lead_user_id)
    vendedores = [
        {"nome": (r["nome"] or f"#{r['uid']}").strip(), "c": int(r["c"])}
        for r in _linhas(cur,
            f"SELECT l.finish_lead_user_id uid, u.name nome, COUNT(*) c "
            f"FROM leads l LEFT JOIN users u ON u.id = l.finish_lead_user_id "
            f"WHERE l.{filtro} AND l.finish_lead_signup=1 AND l.finish_lead_user_id IS NOT NULL "
            f"GROUP BY 1,2 ORDER BY c DESC LIMIT 20")
    ]

    return {
        "total": total, "conv": conv,
        "taxa": round(100 * conv / total, 2) if total else 0,
        "fontes": fontes, "postos": postos,
        "hora": hora, "dow": dow, "vendedores": vendedores,
    }


def coletar() -> dict:
    c = conn_leads()
    try:
        cur = c.cursor()
        off = offset_horas(cur)
        shift = f"created_at + INTERVAL {off} HOUR"
        log(f"offset MySQL→local: {off:+d}h")

        janelas = {str(d): coletar_janela(cur, shift, d) for d in JANELAS}

        # tendência mensal (24 meses) — período longo, volume × conversão
        mensal = [
            {"m": str(r["m"]), "n": int(r["n"]), "c": int(r["c"] or 0),
             "taxa": round(100 * int(r["c"] or 0) / int(r["n"]), 2) if r["n"] else 0}
            for r in _linhas(cur,
                f"SELECT DATE_FORMAT({shift}, '%Y-%m') m, COUNT(*) n, "
                f"SUM(finish_lead_signup=1) c FROM leads "
                f"WHERE created_at >= NOW() - INTERVAL {MESES_TENDENCIA} MONTH "
                f"GROUP BY 1 ORDER BY 1")
        ]
    finally:
        c.close()

    return {
        "gerado_em": datetime.now().astimezone().isoformat(timespec="seconds"),
        "janelas": janelas,
        "mensal": mensal,
        "min_vol_taxa": MIN_VOL_TAXA,
        "postos_ordem": POSTOS_REDE,
    }


def main() -> int:
    log("=== export_leads_estudos: início ===")
    dry = "--dry-run" in sys.argv
    dados = coletar()
    j90 = dados["janelas"].get("90", {})
    log(f"90d: {j90.get('total')} leads · {j90.get('conv')} matrículas · "
        f"{j90.get('taxa')}% · {len(dados['mensal'])} meses")
    if dry:
        log("--dry-run: JSON NÃO gravado")
        print(json.dumps(dados, ensure_ascii=False)[:1500])
        return 0
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    tmp = OUT_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(dados, f, ensure_ascii=False)
    os.replace(tmp, OUT_PATH)
    log(f"JSON salvo: {OUT_PATH}")
    log("=== export_leads_estudos: fim ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
