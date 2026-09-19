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


def _pp_slot(store: dict, p: str) -> dict:
    """Slot vazio de um posto dentro de um bloco (janela ou mês)."""
    if p not in store:
        store[p] = {"total": 0, "conv": 0, "fontes": [],
                    "hora": [{"h": h, "n": 0, "c": 0} for h in range(24)],
                    "dow": [{"d": d, "n": 0, "c": 0} for d in range(7)],
                    "vendedores": []}
    return store[p]


def coletar_meses(cur, shift_l: str) -> dict:
    """Um bloco por mês (YYYY-MM), últimos MESES_TENDENCIA meses, QUEBRADO POR
    POSTO — o frontend soma só os postos do filtro do topo. Feito em 5 queries
    agrupadas por mês+posto (não uma varredura por mês/posto) porque a tabela
    leads não tem índice em created_at. `shift_l` já vem qualificado com o alias
    l. O mês usa o fuso LOCAL, igual à tendência mensal — 'agosto' no seletor é o
    mesmo agosto do gráfico de tendência."""
    janela = f"l.created_at >= NOW() - INTERVAL {MESES_TENDENCIA} MONTH"
    mes = f"DATE_FORMAT({shift_l}, '%Y-%m')"
    meses: dict = {}

    def slot(m: str, p: str) -> dict:
        return _pp_slot(meses.setdefault(m, {"por_posto": {}})["por_posto"], p)

    # total + conversão
    for r in _linhas(cur, f"SELECT {mes} m, {POSTO_SQL} p, COUNT(*) n, SUM(l.finish_lead_signup=1) c "
                          f"FROM leads l WHERE {janela} GROUP BY 1,2"):
        s = slot(str(r["m"]), str(r["p"]))
        s["total"] = int(r["n"] or 0); s["conv"] = int(r["c"] or 0)

    # por fonte (canal)
    for r in _linhas(cur,
            f"SELECT {mes} m, {POSTO_SQL} p, COALESCE(s.title, CONCAT('fonte #', l.leadsource_id), 'sem fonte') fonte, "
            f"COUNT(*) n, SUM(l.finish_lead_signup=1) c "
            f"FROM leads l LEFT JOIN leadsources s ON s.id = l.leadsource_id "
            f"WHERE {janela} GROUP BY 1,2,3 ORDER BY n DESC"):
        slot(str(r["m"]), str(r["p"]))["fontes"].append(
            {"fonte": str(r["fonte"]), "n": int(r["n"]), "c": int(r["c"] or 0)})

    # por hora do dia
    for r in _linhas(cur,
            f"SELECT {mes} m, {POSTO_SQL} p, HOUR({shift_l}) h, COUNT(*) n, SUM(l.finish_lead_signup=1) c "
            f"FROM leads l WHERE {janela} GROUP BY 1,2,3"):
        slot(str(r["m"]), str(r["p"]))["hora"][int(r["h"])] = {
            "h": int(r["h"]), "n": int(r["n"]), "c": int(r["c"] or 0)}

    # por dia da semana (1=dom..7=sáb → 0=dom..6=sáb)
    for r in _linhas(cur,
            f"SELECT {mes} m, {POSTO_SQL} p, DAYOFWEEK({shift_l}) d, COUNT(*) n, SUM(l.finish_lead_signup=1) c "
            f"FROM leads l WHERE {janela} GROUP BY 1,2,3"):
        slot(str(r["m"]), str(r["p"]))["dow"][int(r["d"]) - 1] = {
            "d": int(r["d"]) - 1, "n": int(r["n"]), "c": int(r["c"] or 0)}

    # ranking de vendedores — top 20 por mês+posto (ordena tudo e corta no Python)
    vtmp: dict = {}
    for r in _linhas(cur,
            f"SELECT {mes} m, {POSTO_SQL} p, l.finish_lead_user_id uid, u.name nome, COUNT(*) c "
            f"FROM leads l LEFT JOIN users u ON u.id = l.finish_lead_user_id "
            f"WHERE {janela} AND l.finish_lead_signup=1 AND l.finish_lead_user_id IS NOT NULL "
            f"GROUP BY 1,2,3,4 ORDER BY c DESC"):
        vtmp.setdefault((str(r["m"]), str(r["p"])), []).append(
            {"nome": (r["nome"] or f"#{r['uid']}").strip(), "c": int(r["c"])})
    for (m, p), lst in vtmp.items():
        slot(m, p)["vendedores"] = lst[:20]

    return meses


def coletar_janela(cur, shift_l: str, dias: int) -> dict:
    """Uma janela de N dias, QUEBRADA POR POSTO — o frontend soma só os postos
    do filtro do topo. `shift_l` e o filtro vêm qualificados com o alias l."""
    filtro = f"l.created_at >= NOW() - INTERVAL {dias} DAY"
    pp: dict = {}

    # total + conversão por posto
    for r in _linhas(cur, f"SELECT {POSTO_SQL} p, COUNT(*) n, SUM(l.finish_lead_signup=1) c "
                          f"FROM leads l WHERE {filtro} GROUP BY 1"):
        s = _pp_slot(pp, str(r["p"]))
        s["total"] = int(r["n"] or 0); s["conv"] = int(r["c"] or 0)

    # por fonte (canal)
    for r in _linhas(cur,
            f"SELECT {POSTO_SQL} p, COALESCE(s.title, CONCAT('fonte #', l.leadsource_id), 'sem fonte') fonte, "
            f"COUNT(*) n, SUM(l.finish_lead_signup=1) c "
            f"FROM leads l LEFT JOIN leadsources s ON s.id = l.leadsource_id "
            f"WHERE {filtro} GROUP BY 1,2 ORDER BY n DESC"):
        _pp_slot(pp, str(r["p"]))["fontes"].append(
            {"fonte": str(r["fonte"]), "n": int(r["n"]), "c": int(r["c"] or 0)})

    # por hora do dia (0-23, hora local)
    for r in _linhas(cur,
            f"SELECT {POSTO_SQL} p, HOUR({shift_l}) h, COUNT(*) n, SUM(l.finish_lead_signup=1) c "
            f"FROM leads l WHERE {filtro} GROUP BY 1,2"):
        _pp_slot(pp, str(r["p"]))["hora"][int(r["h"])] = {
            "h": int(r["h"]), "n": int(r["n"]), "c": int(r["c"] or 0)}

    # por dia da semana (1=dom..7=sáb → 0=dom..6=sáb)
    for r in _linhas(cur,
            f"SELECT {POSTO_SQL} p, DAYOFWEEK({shift_l}) d, COUNT(*) n, SUM(l.finish_lead_signup=1) c "
            f"FROM leads l WHERE {filtro} GROUP BY 1,2"):
        _pp_slot(pp, str(r["p"]))["dow"][int(r["d"]) - 1] = {
            "d": int(r["d"]) - 1, "n": int(r["n"]), "c": int(r["c"] or 0)}

    # ranking de vendedores — top 20 por posto (ordena tudo e corta no Python)
    vtmp: dict = {}
    for r in _linhas(cur,
            f"SELECT {POSTO_SQL} p, l.finish_lead_user_id uid, u.name nome, COUNT(*) c "
            f"FROM leads l LEFT JOIN users u ON u.id = l.finish_lead_user_id "
            f"WHERE {filtro} AND l.finish_lead_signup=1 AND l.finish_lead_user_id IS NOT NULL "
            f"GROUP BY 1,2,3 ORDER BY c DESC"):
        vtmp.setdefault(str(r["p"]), []).append(
            {"nome": (r["nome"] or f"#{r['uid']}").strip(), "c": int(r["c"])})
    for p, lst in vtmp.items():
        _pp_slot(pp, p)["vendedores"] = lst[:20]

    return {"por_posto": pp}


def coletar() -> dict:
    c = conn_leads()
    try:
        cur = c.cursor()
        off = offset_horas(cur)
        shift = f"created_at + INTERVAL {off} HOUR"
        shift_l = f"l.created_at + INTERVAL {off} HOUR"   # mesmo shift, qualificado
        log(f"offset MySQL→local: {off:+d}h")

        janelas = {str(d): coletar_janela(cur, shift_l, d) for d in JANELAS}
        meses = coletar_meses(cur, shift_l)

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
        "meses": meses,
        "mensal": mensal,
        "min_vol_taxa": MIN_VOL_TAXA,
        "postos_ordem": POSTOS_REDE,
    }


def main() -> int:
    log("=== export_leads_estudos: início ===")
    dry = "--dry-run" in sys.argv
    dados = coletar()
    pp90 = dados["janelas"].get("90", {}).get("por_posto", {})
    t90 = sum(b["total"] for b in pp90.values())
    c90 = sum(b["conv"] for b in pp90.values())
    log(f"90d: {t90} leads · {c90} matrículas · "
        f"{round(100*c90/t90,2) if t90 else 0}% · {len(pp90)} postos · "
        f"{len(dados.get('meses', {}))} meses")
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
