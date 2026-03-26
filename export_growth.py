# export_growth.py
# Gera json_consolidado/growth_dashboard.json com MRR e CAC por posto/mes.
# Usa o mesmo padrao de conexao do export_governanca.py.

import os, re, json, sys
from datetime import date, datetime, timezone
from urllib.parse import quote_plus
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SQL_DIR = os.path.join(BASE_DIR, "sql_growth")
JSON_DIR = os.path.join(BASE_DIR, "json_consolidado")

POSTOS = list("ANXYBRPCDGIMJ")
ODBC_DRIVER = os.getenv("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")

# ---------- helpers ----------

def env(key, default=""):
    v = os.getenv(key, default)
    return v.strip() if isinstance(v, str) else v

def build_conn_str(host, base, user, pwd, port, encrypt, trust_cert, timeout):
    server = f"tcp:{host},{port or '1433'}"
    common = (
        f"DRIVER={{{ODBC_DRIVER}}};"
        f"SERVER={server};DATABASE={base};"
        f"Encrypt={encrypt};TrustServerCertificate={trust_cert};"
        f"Connection Timeout={timeout or '5'};"
    )
    if user:
        return common + f"UID={user};PWD={pwd}"
    return common + "Trusted_Connection=yes"

def make_engine(odbc_conn_str):
    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_conn_str)}",
        pool_pre_ping=True,
        future=True,
    )

def build_conns():
    load_dotenv(os.path.join(BASE_DIR, ".env"))
    encrypt    = env("DB_ENCRYPT", "yes")
    trust_cert = env("DB_TRUST_CERT", "yes")
    timeout    = env("DB_TIMEOUT", "20")
    conns = {}
    for p in POSTOS:
        host = env(f"DB_HOST_{p}")
        base = env(f"DB_BASE_{p}")
        if not host or not base:
            continue
        user = env(f"DB_USER_{p}")
        pwd  = env(f"DB_PASSWORD_{p}")
        port = env(f"DB_PORT_{p}", "1433")
        conns[p] = build_conn_str(host, base, user, pwd, port, encrypt, trust_cert, timeout)
    return conns

def month_bounds(dt):
    ini = date(dt.year, dt.month, 1)
    nxt = date(dt.year + (dt.month == 12), (dt.month % 12) + 1, 1)
    return ini, nxt, f"{ini.year:04d}-{ini.month:02d}"

def month_iter(start, end_exclusive):
    y, m = start.year, start.month
    while True:
        ini = date(y, m, 1)
        if ini >= end_exclusive:
            break
        yield ini
        m = 1 if m == 12 else m + 1
        y = y + 1 if m == 1 else y

def run_query(engine, sql_txt, ini, fim):
    body = sql_txt if sql_txt.lstrip().upper().startswith("SET NOCOUNT ON") else "SET NOCOUNT ON;\n" + sql_txt
    with engine.connect() as con:
        return pd.read_sql_query(text(body), con, params={"ini": str(ini), "fim": str(fim)})

def load_sql(name):
    path = os.path.join(SQL_DIR, name)
    return open(path, "r", encoding="utf-8").read().strip()

# ---------- main ----------

def main():
    os.makedirs(JSON_DIR, exist_ok=True)
    conns = build_conns()
    if not conns:
        print("Nenhuma conexao de posto configurada (.env)")
        sys.exit(1)

    sql_mrr = load_sql("mrr.sql")
    sql_cac = load_sql("cac_despesas_vendas.sql")

    # Periodo: de 2024-01 ate mes atual
    today = date.today()
    start = date(2024, 1, 1)
    _, end_exc, _ = month_bounds(today)

    dados = {}
    all_postos = set()
    all_meses = set()

    for dt in month_iter(start, end_exc):
        ini, fim, ym = month_bounds(dt)
        all_meses.add(ym)
        print(f"[{ym}] ", end="", flush=True)

        for posto, odbc_str in conns.items():
            all_postos.add(posto)
            engine = make_engine(odbc_str)
            rec = {}

            # MRR
            try:
                df = run_query(engine, sql_mrr, ini, fim)
                if not df.empty:
                    rec["mrr_count"] = int(df.iloc[0].get("mrr_count", 0) or 0)
                    rec["mrr_valor"] = round(float(df.iloc[0].get("mrr_valor", 0) or 0), 2)
            except Exception as e:
                print(f"\n  WARN MRR {posto}/{ym}: {e}")

            # CAC despesas vendas
            try:
                df = run_query(engine, sql_cac, ini, fim)
                if not df.empty:
                    rec["cac_despesas_vendas"] = round(float(df.iloc[0].get("cac_despesas_vendas", 0) or 0), 2)
            except Exception as e:
                print(f"\n  WARN CAC {posto}/{ym}: {e}")

            if rec:
                dados.setdefault(ym, {})[posto] = rec

        print(f"{len(conns)} postos OK")

    out = {
        "meta": {
            "gerado_em": datetime.now().astimezone().isoformat(timespec="seconds"),
            "arquivo": "growth_dashboard.json",
            "origem": "export_growth.py"
        },
        "meses": sorted(all_meses),
        "postos": sorted(all_postos),
        "dados": dados
    }

    out_path = os.path.join(JSON_DIR, "growth_dashboard.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"\nJSON -> {out_path}")

if __name__ == "__main__":
    main()
