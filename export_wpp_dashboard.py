#!/usr/bin/env python3
# export_wpp_dashboard.py
# ETL do Dashboard WhatsApp Cobrança (Meta).
#
# Objetivo:
#   - Ler envios accepted de campanhas com enviar_meta=1 em /opt/camim-auth/whatsapp_cobranca.db
#   - Cruzar com fin_receita de cada posto (SQL Server) via idreceita
#   - Calcular conversão (enviou → pagou em até 30d) com múltiplos cortes
#   - Gerar json_consolidado/wpp_dashboard.json consumido por wpp_dashboard.html
#
# Regras (confirmadas com o usuário 2026-04-09):
#   - Só conta envio que foi para Meta (campanhas.enviar_meta=1 AND envios.status='accepted')
#   - Janela de atribuição: 30 dias após o envio
#   - Pagou no mesmo dia ou depois do envio (DataPagamento >= enviado_em.date)
#   - Envio sem idreceita (ex: "Indique um amigo") entra no bloco de custo/marketing,
#     NÃO no bloco de conversão
#   - Lembrança = campanhas.modo_envio='pre_vencimento'
#   - Cobrança  = campanhas.modo_envio='atraso'
#
# Cron sugerido (a cada 30 min):
#   */30 * * * * /opt/relatorio_h_t/.venv/bin/python /opt/relatorio_h_t/export_wpp_dashboard.py >> /opt/relatorio_h_t/logs/wpp_dashboard.log 2>&1

import json
import os
import sqlite3
import sys
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from urllib.parse import quote_plus

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from etl_meta import ETLMeta


# =============================================================================
# Configuração
# =============================================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JSON_DIR = os.path.join(BASE_DIR, "json_consolidado")
OUT_PATH = os.path.join(JSON_DIR, "wpp_dashboard.json")

WAPP_DB_PATH = os.getenv("WAPP_CTRL_DB", "/opt/camim-auth/whatsapp_cobranca.db")
ODBC_DRIVER  = os.getenv("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")
POSTOS_FALLBACK = list("ANXYBRPCDGIMJ")

# Histórico analisado (dias). Envios mais antigos são ignorados.
HIST_DIAS = int(os.getenv("WPP_DASH_HIST_DIAS", "180"))
# Janela de atribuição (dias) para contar "pagou após envio".
JANELA_DIAS = int(os.getenv("WPP_DASH_JANELA_DIAS", "30"))

# Faixas de dias de atraso (cobrança)
FAIXA_1_MIN, FAIXA_1_MAX = 1, 15     # "1 a 15"
FAIXA_2_MIN              = 16        # "16+"


# =============================================================================
# Utilitários
# =============================================================================
def env(k: str, default: str = "") -> str:
    v = os.getenv(k, default)
    return v.strip() if isinstance(v, str) else v


def build_conn_str(host, base, user, pwd, port="1433"):
    return (
        f"DRIVER={{{ODBC_DRIVER}}};"
        f"SERVER=tcp:{host},{port};DATABASE={base};"
        f"Encrypt=yes;TrustServerCertificate=yes;"
        + (f"UID={user};PWD={pwd}" if user else "Trusted_Connection=yes")
    )


def make_engine(odbc_str):
    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_str)}",
        future=True,
        pool_pre_ping=True,
    )


def build_conns_from_env(postos=None) -> dict:
    postos = postos or POSTOS_FALLBACK
    conns = {}
    for p in postos:
        host = env(f"DB_HOST_{p}")
        base = env(f"DB_BASE_{p}")
        if not host or not base:
            continue
        user = env(f"DB_USER_{p}")
        pwd  = env(f"DB_PASSWORD_{p}")
        port = env(f"DB_PORT_{p}", "1433")
        conns[p] = build_conn_str(host, base, user, pwd, port)
    return conns


def chunked(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def parse_dt(s):
    """Parse ISO ou 'YYYY-MM-DD HH:MM:SS' vindo do SQLite."""
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00").replace(" ", "T"))
    except Exception:
        try:
            return datetime.strptime(str(s)[:19], "%Y-%m-%d %H:%M:%S")
        except Exception:
            return None


def atomic_write_json(path: str, payload) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, default=str)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


# =============================================================================
# 1) Leitura dos envios (SQLite)
# =============================================================================
def ler_envios_meta() -> pd.DataFrame:
    """Retorna envios accepted de campanhas com enviar_meta=1, até HIST_DIAS atrás."""
    if not os.path.isfile(WAPP_DB_PATH):
        raise FileNotFoundError(f"Base WhatsApp não encontrada: {WAPP_DB_PATH}")

    corte = (datetime.now() - timedelta(days=HIST_DIAS)).strftime("%Y-%m-%d")
    sql = """
        SELECT
            e.id                AS envio_id,
            e.campanha_id,
            e.posto,
            e.idreceita,
            e.matricula,
            e.nome,
            e.telefone,
            e.template          AS envio_template,
            e.enviado_em,
            e.dias_atraso,
            e.valor,
            e.ref,
            e.wamid,
            c.nome              AS campanha_nome,
            c.modo_envio,
            c.template          AS campanha_template,
            c.enviar_meta
        FROM envios e
        JOIN campanhas c ON c.id = e.campanha_id
        WHERE c.enviar_meta = 1
          AND e.status = 'accepted'
          AND date(e.enviado_em) >= date(?)
    """
    with sqlite3.connect(f"file:{WAPP_DB_PATH}?mode=ro", uri=True) as conn:
        df = pd.read_sql_query(sql, conn, params=(corte,))

    # Normalizações
    df["posto"]      = df["posto"].fillna("").str.strip().str.upper()
    df["idreceita"]  = df["idreceita"].fillna("").astype(str).str.strip()
    df["modo_envio"] = df["modo_envio"].fillna("atraso").str.strip().str.lower()
    df["dias_atraso"] = pd.to_numeric(df["dias_atraso"], errors="coerce").fillna(0).astype(int)
    df["enviado_em_dt"] = df["enviado_em"].apply(parse_dt)
    df = df.dropna(subset=["enviado_em_dt"])
    df["enviado_data"] = df["enviado_em_dt"].dt.date

    return df


# =============================================================================
# 2) Consulta de pagamentos no SQL Server (por posto)
# =============================================================================
def buscar_pagamentos(posto: str, engine, idreceitas: list[str]) -> dict:
    """Dado um posto e uma lista de idreceitas (strings), retorna:
       { idreceita_str: {'data_pagamento': date, 'valor_pago': float} }
    """
    result = {}
    # idreceita em fin_receita é INT
    ids_int = []
    for i in idreceitas:
        s = str(i).strip()
        if s.isdigit():
            ids_int.append(int(s))
    if not ids_int:
        return result

    for chunk in chunked(ids_int, 1000):
        placeholders = ",".join(str(n) for n in chunk)
        sql = f"""
            SELECT idreceita, DataPagamento, ValorPago
            FROM fin_receita
            WHERE idreceita IN ({placeholders})
              AND DataPagamento IS NOT NULL
              AND dataabono IS NULL
              AND idcontaTipo = 5
        """
        with engine.connect() as con:
            df = pd.read_sql_query(text(sql), con)
        for _, row in df.iterrows():
            idr = str(int(row["idreceita"]))
            dt  = pd.to_datetime(row["DataPagamento"], errors="coerce")
            if pd.isna(dt):
                continue
            vp  = float(row["ValorPago"] or 0.0)
            # Fica o primeiro pagamento encontrado por idreceita
            if idr not in result:
                result[idr] = {
                    "data_pagamento": dt.date(),
                    "valor_pago": vp,
                }
    return result


# =============================================================================
# 3) Agregação e métricas
# =============================================================================
def agregar_por_idreceita(df_cob: pd.DataFrame) -> pd.DataFrame:
    """Consolida múltiplos envios da mesma idreceita num único registro."""
    if df_cob.empty:
        return df_cob

    agg = df_cob.groupby(["posto", "idreceita"], as_index=False).agg(
        first_send=("enviado_em_dt", "min"),
        last_send=("enviado_em_dt", "max"),
        n_sends=("envio_id", "count"),
        has_prevenc=("modo_envio", lambda x: bool((x == "pre_vencimento").any())),
        has_atraso=("modo_envio", lambda x: bool((x == "atraso").any())),
        max_dias_atraso=("dias_atraso", "max"),
        nome=("nome", "first"),
        matricula=("matricula", "first"),
        campanha_nome=("campanha_nome", "first"),
    )
    agg["first_send_date"] = agg["first_send"].dt.date
    return agg


def metricas_bloco(df: pd.DataFrame) -> dict:
    """Métricas básicas de um bloco (enviou / pagou / dias)."""
    total = len(df)
    pagos_df = df[df["pago"] == True]
    pagos = len(pagos_df)
    dias = pagos_df["dias_ate_pagar"].dropna().tolist()
    valor_pago = float(pagos_df["valor_pago"].sum()) if pagos else 0.0
    return {
        "enviados":   int(total),
        "pagos":      int(pagos),
        "conversao":  round((pagos / total * 100), 2) if total else 0.0,
        "dias_medio": round(float(sum(dias) / len(dias)), 2) if dias else None,
        "dias_mediana": round(float(pd.Series(dias).median()), 2) if dias else None,
        "dias_p75":   round(float(pd.Series(dias).quantile(0.75)), 2) if dias else None,
        "valor_pago_total": round(valor_pago, 2),
    }


def curva_sobrevivencia(df: pd.DataFrame) -> list[dict]:
    """% pago em até 1, 3, 7, 15, 30 dias."""
    total = len(df)
    if not total:
        return []
    out = []
    for limite in (1, 3, 7, 15, 30):
        n = int((df["dias_ate_pagar"].notna() & (df["dias_ate_pagar"] <= limite)).sum())
        out.append({
            "dias": limite,
            "pagos": n,
            "pct": round(n / total * 100, 2),
        })
    return out


# =============================================================================
# 4) Build do JSON final
# =============================================================================
def build_dashboard(df_envios: pd.DataFrame, pagamentos_por_posto: dict) -> dict:
    gerado_em = datetime.now().astimezone()

    # ── Divide envios ──────────────────────────────────────────────────────
    df_com_rec = df_envios[df_envios["idreceita"] != ""].copy()
    df_sem_rec = df_envios[df_envios["idreceita"] == ""].copy()

    # ── Agrega por idreceita (cobrança/lembrança) ──────────────────────────
    agg = agregar_por_idreceita(df_com_rec)

    # ── Marca pago (janela de 30d a partir do first_send) ──────────────────
    agg["pago"] = False
    agg["data_pagamento"] = None
    agg["dias_ate_pagar"] = None
    agg["valor_pago"] = 0.0

    for idx, row in agg.iterrows():
        pag = pagamentos_por_posto.get(row["posto"], {}).get(row["idreceita"])
        if not pag:
            continue
        first_send_date = row["first_send_date"]
        dt_pag = pag["data_pagamento"]
        if dt_pag < first_send_date:
            continue
        dias = (dt_pag - first_send_date).days
        if dias > JANELA_DIAS:
            continue
        agg.at[idx, "pago"] = True
        agg.at[idx, "data_pagamento"] = dt_pag.isoformat()
        agg.at[idx, "dias_ate_pagar"] = dias
        agg.at[idx, "valor_pago"] = pag["valor_pago"]

    # =========================================================================
    # SEÇÃO 1 — Envios & Custo (TODOS os envios Meta)
    # =========================================================================
    total_envios_meta = len(df_envios)
    envios_com_rec    = len(df_com_rec)
    envios_sem_rec    = len(df_sem_rec)

    por_template = (
        df_envios.groupby("envio_template")
        .size()
        .reset_index(name="n")
        .sort_values("n", ascending=False)
    )
    por_template_list = [
        {"template": r["envio_template"] or "(sem template)", "envios": int(r["n"])}
        for _, r in por_template.iterrows()
    ]

    por_campanha = (
        df_envios.groupby(["campanha_id", "campanha_nome", "modo_envio"])
        .size()
        .reset_index(name="n")
        .sort_values("n", ascending=False)
    )
    por_campanha_list = [
        {
            "campanha_id":   int(r["campanha_id"]),
            "campanha_nome": r["campanha_nome"] or "?",
            "modo_envio":    r["modo_envio"],
            "envios":        int(r["n"]),
        }
        for _, r in por_campanha.iterrows()
    ]

    por_posto_envio = (
        df_envios.groupby("posto")
        .size()
        .reset_index(name="n")
        .sort_values("n", ascending=False)
    )
    por_posto_envio_list = [
        {"posto": r["posto"], "envios": int(r["n"])}
        for _, r in por_posto_envio.iterrows() if r["posto"]
    ]

    # Série temporal diária (todos os envios)
    df_envios["data"] = df_envios["enviado_em_dt"].dt.strftime("%Y-%m-%d")
    serie_diaria = (
        df_envios.groupby("data")
        .size()
        .reset_index(name="envios")
        .sort_values("data")
    )
    serie_diaria_list = [
        {"data": r["data"], "envios": int(r["envios"])}
        for _, r in serie_diaria.iterrows()
    ]

    # =========================================================================
    # SEÇÃO 2 — Conversão GERAL (todas as cobranças com idreceita)
    # =========================================================================
    geral = metricas_bloco(agg)

    # =========================================================================
    # SEÇÃO 3 — Por faixa de atraso
    # =========================================================================
    agg_atraso = agg[agg["has_atraso"] == True].copy()

    faixa_1_15 = agg_atraso[
        (agg_atraso["max_dias_atraso"] >= FAIXA_1_MIN) &
        (agg_atraso["max_dias_atraso"] <= FAIXA_1_MAX)
    ]
    faixa_16   = agg_atraso[agg_atraso["max_dias_atraso"] >= FAIXA_2_MIN]

    # =========================================================================
    # SEÇÃO 4 — Lembrança (pré-vencimento) que NÃO precisou de cobrança
    # =========================================================================
    agg_prev = agg[agg["has_prevenc"] == True].copy()
    # sucesso: recebeu lembrança, pagou dentro da janela, e não teve envio de atraso
    lembranca_sucesso = agg_prev[(agg_prev["pago"] == True) & (agg_prev["has_atraso"] == False)]

    lembranca = {
        "total_lembrancas": int(len(agg_prev)),
        "pagou_sem_cobrar": int(len(lembranca_sucesso)),
        "pagou_mas_precisou_cobrar": int(((agg_prev["pago"] == True) & (agg_prev["has_atraso"] == True)).sum()),
        "nao_pagou": int((agg_prev["pago"] == False).sum()),
        "pct_sucesso": round(
            len(lembranca_sucesso) / len(agg_prev) * 100, 2
        ) if len(agg_prev) else 0.0,
        "dias_medio_pagamento": round(
            float(lembranca_sucesso["dias_ate_pagar"].mean()), 2
        ) if len(lembranca_sucesso) else None,
    }

    # =========================================================================
    # Breakdown extras
    # =========================================================================
    # Conversão por posto
    conv_por_posto = []
    for posto, grp in agg.groupby("posto"):
        m = metricas_bloco(grp)
        m["posto"] = posto
        conv_por_posto.append(m)
    conv_por_posto.sort(key=lambda x: x["conversao"], reverse=True)

    # Conversão por campanha
    conv_por_campanha = []
    for (cid, cnome), grp in df_com_rec.groupby(["campanha_id", "campanha_nome"]):
        # precisamos usar o agg por receita; filtra pelo conjunto de idreceitas desta campanha
        ids_camp = set(grp["idreceita"].unique())
        sub = agg[agg["idreceita"].isin(ids_camp)]
        if sub.empty:
            continue
        m = metricas_bloco(sub)
        m["campanha_id"]   = int(cid)
        m["campanha_nome"] = cnome or "?"
        conv_por_campanha.append(m)
    conv_por_campanha.sort(key=lambda x: x["conversao"], reverse=True)

    # Conversão por faixa de dias de atraso detalhada
    def bucket(d):
        if d <= 0:  return "pré-venc"
        if d <= 3:  return "1-3"
        if d <= 7:  return "4-7"
        if d <= 15: return "8-15"
        if d <= 30: return "16-30"
        return "30+"
    agg["faixa"] = agg["max_dias_atraso"].apply(bucket)
    conv_por_faixa = []
    for f, grp in agg.groupby("faixa"):
        m = metricas_bloco(grp)
        m["faixa"] = f
        conv_por_faixa.append(m)
    ordem = ["pré-venc", "1-3", "4-7", "8-15", "16-30", "30+"]
    conv_por_faixa.sort(key=lambda x: ordem.index(x["faixa"]) if x["faixa"] in ordem else 99)

    # Conversão por hora do envio
    df_com_rec["hora"] = df_com_rec["enviado_em_dt"].dt.hour
    # para conversão por hora, usamos o first_send; mas mapeando por envio mesmo dá sinal suficiente
    hora_map = df_com_rec.groupby("idreceita")["hora"].first().to_dict()
    agg["hora_envio"] = agg["idreceita"].map(hora_map)
    conv_por_hora = []
    for h, grp in agg.groupby("hora_envio"):
        if pd.isna(h):
            continue
        m = metricas_bloco(grp)
        m["hora"] = int(h)
        conv_por_hora.append(m)
    conv_por_hora.sort(key=lambda x: x["hora"])

    # Conversão por dia da semana (0=seg..6=dom)
    df_com_rec["dow"] = df_com_rec["enviado_em_dt"].dt.weekday
    dow_map = df_com_rec.groupby("idreceita")["dow"].first().to_dict()
    agg["dow_envio"] = agg["idreceita"].map(dow_map)
    dow_labels = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"]
    conv_por_dow = []
    for d, grp in agg.groupby("dow_envio"):
        if pd.isna(d):
            continue
        m = metricas_bloco(grp)
        m["dow"] = int(d)
        m["dow_label"] = dow_labels[int(d)]
        conv_por_dow.append(m)
    conv_por_dow.sort(key=lambda x: x["dow"])

    # Curva de sobrevivência (todo o bloco agg)
    curva = curva_sobrevivencia(agg)

    # =========================================================================
    # Payload final
    # =========================================================================
    return {
        "meta": {
            "gerado_em":       gerado_em.isoformat(timespec="seconds"),
            "gerado_em_br":    gerado_em.strftime("%d/%m/%Y %H:%M"),
            "historico_dias":  HIST_DIAS,
            "janela_atribuicao_dias": JANELA_DIAS,
            "fonte_envios":    "whatsapp_cobranca.db (enviar_meta=1, status=accepted)",
            "fonte_pagamentos": "fin_receita (idcontaTipo=5, DataPagamento IS NOT NULL)",
        },
        "envios_custo": {
            "total_meta":      int(total_envios_meta),
            "com_idreceita":   int(envios_com_rec),
            "sem_idreceita":   int(envios_sem_rec),
            "por_template":    por_template_list,
            "por_campanha":    por_campanha_list,
            "por_posto":       por_posto_envio_list,
            "serie_diaria":    serie_diaria_list,
        },
        "conversao_geral": geral,
        "conversao_faixa": {
            "faixa_1_15":   metricas_bloco(faixa_1_15),
            "faixa_16_mais": metricas_bloco(faixa_16),
        },
        "lembranca": lembranca,
        "breakdowns": {
            "por_posto":    conv_por_posto,
            "por_campanha": conv_por_campanha,
            "por_faixa":    conv_por_faixa,
            "por_hora":     conv_por_hora,
            "por_dow":      conv_por_dow,
            "curva_sobrevivencia": curva,
        },
    }


# =============================================================================
# Main
# =============================================================================
def main():
    t0 = time.time()
    print(f"=== WPP Dashboard ETL — {datetime.now().isoformat()} ===")
    print(f"  WAPP DB:       {WAPP_DB_PATH}")
    print(f"  Out:           {OUT_PATH}")
    print(f"  Histórico:     {HIST_DIAS} dias")
    print(f"  Janela atrib.: {JANELA_DIAS} dias")

    os.makedirs(JSON_DIR, exist_ok=True)
    load_dotenv(os.path.join(BASE_DIR, ".env"))

    meta = ETLMeta("export_wpp_dashboard", "json_consolidado")

    # 1) Envios
    try:
        df_envios = ler_envios_meta()
        print(f"  Envios Meta (últimos {HIST_DIAS}d): {len(df_envios)}")
    except Exception as e:
        print(f"ERRO lendo envios: {e}")
        meta.error("sqlite", str(e))
        meta.save()
        return

    if df_envios.empty:
        print("Nenhum envio encontrado — gerando JSON vazio.")
        payload = build_dashboard(df_envios, {})
        atomic_write_json(OUT_PATH, payload)
        meta.ok("sqlite")
        meta.save()
        return

    # 2) Pagamentos por posto
    conns = build_conns_from_env()
    if not conns:
        print("AVISO: nenhuma conexão por posto configurada — dashboard sem pagamentos.")
    pagamentos_por_posto = defaultdict(dict)

    ids_por_posto = (
        df_envios[df_envios["idreceita"] != ""]
        .groupby("posto")["idreceita"]
        .unique()
    )

    for posto, ids in ids_por_posto.items():
        if not posto or posto not in conns:
            meta.error(posto or "?", "sem conexão")
            continue
        try:
            eng = make_engine(conns[posto])
            pag = buscar_pagamentos(posto, eng, list(ids))
            pagamentos_por_posto[posto] = pag
            print(f"  [{posto}] {len(ids)} idreceitas → {len(pag)} pagas")
            meta.ok(posto, envios=int(len(ids)), pagos=int(len(pag)))
        except Exception as e:
            print(f"  [{posto}] ERRO: {e}")
            meta.error(posto, str(e))

    # 3) Build e salva
    payload = build_dashboard(df_envios, pagamentos_por_posto)
    atomic_write_json(OUT_PATH, payload)
    meta.save()

    print(f"  Total envios:     {payload['envios_custo']['total_meta']}")
    print(f"  Conversão geral:  {payload['conversao_geral']['conversao']}%")
    print(f"  Tempo total:      {time.time() - t0:.1f}s")
    print(f"  JSON salvo em:    {OUT_PATH}")


if __name__ == "__main__":
    main()
