# export_vagas.py
# Processo oficial de exportação de vagas via stored procedure sp_rel_vagas
# Regras:
#   - Executar de 2020-01 até o mês atual (inclusive)
#   - Processar mês a mês por posto
#   - PULAR meses onde todos os CSVs existem
#   - SEMPRE reprocessar mês atual e mês anterior
#   - Gera CSVs em /dados_vagas e JSONs em /json_vagas
#   - Totalmente integrado ao modelo do export_governanca.py

import os, sys, json
from datetime import date, datetime
from urllib.parse import quote_plus
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# -------------------------------------------
# CONFIG
# -------------------------------------------
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
DADOS_DIR  = os.path.join(BASE_DIR, "dados_vagas")
JSON_DIR   = os.path.join(BASE_DIR, "json_vagas")

POSTOS = list("ANXYBRPCDGIMJ")
ODBC_DRIVER = os.getenv("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")
EARLIEST = date(2020, 1, 1)

def ensure_dir(p):
    os.makedirs(p, exist_ok=True)

# -------------------------------------------
# DATE HELPERS
# -------------------------------------------
def month_bounds(dt: date):
    ini = date(dt.year, dt.month, 1)
    nxt = date(dt.year + (dt.month == 12), (dt.month % 12) + 1, 1)
    ym_str = f"{ini.year:04d}-{ini.month:02d}"
    return ini, nxt, ym_str

def month_iter(start: date, end: date):
    y, m = start.year, start.month
    while True:
        d = date(y, m, 1)
        if d >= end:
            break
        yield d
        m = 1 if m == 12 else m + 1
        y = y + 1 if m == 1 else y

# -------------------------------------------
# CONNECTION
# -------------------------------------------
def env(key, default=""):
    v = os.getenv(key, default)
    return v.strip() if isinstance(v, str) else v

def build_conn_str(host, base, user, pwd, port):
    return (
        f"DRIVER={{{ODBC_DRIVER}}};"
        f"SERVER=tcp:{host},{port};"
        f"DATABASE={base};"
        "Encrypt=yes;TrustServerCertificate=yes;"
        f"UID={user};PWD={pwd}"
    )

def make_engine(conn_str):
    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={quote_plus(conn_str)}",
        pool_pre_ping=True,
        future=True,
    )

def load_postos_conn():
    load_dotenv(os.path.join(BASE_DIR, ".env"))
    conns = {}
    for p in POSTOS:
        host = env(f"DB_HOST_{p}")
        base = env(f"DB_BASE_{p}")
        if not host or not base:
            continue
        user = env(f"DB_USER_{p}")
        pwd  = env(f"DB_PASSWORD_{p}")
        port = env(f"DB_PORT_{p}", "1433")
        conns[p] = build_conn_str(host, base, user, pwd, port)
    return conns

# -------------------------------------------
# EXECUTE STORED PROCEDURE
# -------------------------------------------
def run_sp(engine, dataini: date, datafim: date):
    sql = text("EXEC sp_rel_vagas :dataini, :datafim;")
    with engine.connect() as con:
        df = pd.read_sql_query(sql, con, params={"dataini": dataini, "datafim": datafim})
    return df

# -------------------------------------------
# FILE HELPERS
# -------------------------------------------
def csv_path(posto, ym):
    return os.path.join(DADOS_DIR, f"{posto}_{ym}_vagas.csv")

# -------------------------------------------
# JSON BUILDERS
# -------------------------------------------
def build_json_global():
    out = {}
    for fn in os.listdir(DADOS_DIR):
        if not fn.endswith("_vagas.csv"):
            continue

        parts = fn.split("_")
        posto = parts[0]
        ym    = parts[1]

        df = pd.read_csv(os.path.join(DADOS_DIR, fn))

        out.setdefault(ym, {}).setdefault(posto, {})
        out[ym][posto] = {
            "total_vagas": int(df["Total de Vagas"].sum()),
            "indisp_falta": int(df["Indisponíveis Por Falta Médica"].sum()),
            "total_disponiveis": int(df["Total de Vagas Disponiveis"].sum()),
            "reserva": int(df["Disponíveis Em Reserva de Vaga"].sum()),
            "ocupadas": int(df["Ocupadas Por Agendamento de Cliente"].sum())
        }

    ensure_dir(JSON_DIR)
    path = os.path.join(JSON_DIR, "consolidado_global_vagas.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)

    print(f"[JSON] Global -> {path}")

def build_json_por_posto():
    out = {}

    for fn in os.listdir(DADOS_DIR):
        if not fn.endswith("_vagas.csv"):
            continue

        parts = fn.split("_")
        posto = parts[0]
        ym    = parts[1]

        df = pd.read_csv(os.path.join(DADOS_DIR, fn))

        out.setdefault(posto, {})
        out[posto][ym] = {
            "linhas": len(df),
            "total_vagas": int(df["Total de Vagas"].sum()),
            "indisp_falta": int(df["Indisponíveis Por Falta Médica"].sum()),
            "total_disponiveis": int(df["Total de Vagas Disponiveis"].sum()),
            "reserva": int(df["Disponíveis Em Reserva de Vaga"].sum()),
            "ocupadas": int(df["Ocupadas Por Agendamento de Cliente"].sum())
        }

    ensure_dir(JSON_DIR)
    path = os.path.join(JSON_DIR, "vagas_por_posto.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)

    print(f"[JSON] Por posto -> {path}")

# -------------------------------------------
# MAIN PROCESS
# -------------------------------------------
def run():
    print("========== EXPORT VAGAS ==========")

    ensure_dir(DADOS_DIR)
    ensure_dir(JSON_DIR)

    conns = load_postos_conn()
    if not conns:
        print("ERRO: Nenhum posto configurado no .env.")
        sys.exit(1)

    hoje = date.today()
    _, _, ym_current = month_bounds(hoje)
    _, _, ym_prev    = month_bounds(date(hoje.year, hoje.month-1 if hoje.month>1 else 12,
                                         1 if hoje.month>1 else hoje.year-1))

    forced_months = {ym_current, ym_prev}

    print(f"Postos: {list(conns.keys())}")
    print(f"Mês atual: {ym_current}")
    print(f"Mês anterior: {ym_prev}")
    print("Reprocessar sempre:", forced_months)

    start = EARLIEST
    end   = month_bounds(hoje)[1]   # primeiro dia do mês seguinte

    for dt_month in month_iter(start, end):
        ini, fim, ym = month_bounds(dt_month)
        print(f"\n-- Mês {ym} ----------------------------")

        for posto, conn_str in conns.items():
            out_file = csv_path(posto, ym)

            if ym not in forced_months and os.path.exists(out_file):
                print(f"[{posto}] SKIP {ym} (já existe)")
                continue

            print(f"[{posto}] Executando SP para {ym}...")
            engine = make_engine(conn_str)
            try:
                df = run_sp(engine, ini, fim)
            except Exception as e:
                print(f"[{posto}] ERRO ao executar SP: {e}")
                continue

            try:
                df.to_csv(out_file, index=False, encoding="utf-8-sig")
                print(f"[{posto}] CSV salvo -> {out_file}  ({len(df)} linhas)")
            except Exception as e:
                print(f"[{posto}] ERRO ao salvar CSV: {e}")

    print("\nGerando JSONs...")
    build_json_global()
    build_json_por_posto()

    print("\n✅ Finalizado export_vagas.py")

# -------------------------------------------
if __name__ == "__main__":
    run()
