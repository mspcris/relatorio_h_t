# ctrlq_export.py
# Objetivo:
#   - Ler SQL/ctrlq.sql
#   - Executar para cada posto configurado no .env
#   - Normalizar dados (Timestamp, NaT, Decimal, etc.)
#   - Salvar JSON no formato:  A_YYYY-MM-DD_HHMM.json
#
# Dependências: pandas, sqlalchemy, pyodbc, python-dotenv

import os, json
from datetime import datetime, timezone
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ---------------------------------------------
# Configurações
# ---------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SQL_DIR  = os.path.join(BASE_DIR, "SQL")
JSON_DIR = os.path.join(BASE_DIR, "json")

ODBC_DRIVER = os.getenv("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")
POSTOS = list("ANXYBRPCDGIMJ")   # ordem corporativa padrão


# ---------------------------------------------
# Utilitários
# ---------------------------------------------
def ensure_dir(d): 
    os.makedirs(d, exist_ok=True)

def env(key, default=""):
    v = os.getenv(key, default)
    return v.strip() if isinstance(v, str) else v


# ---------------------------------------------
# Carregar SQL
# ---------------------------------------------
def load_sql():
    path = os.path.join(SQL_DIR, "ctrlq.sql")
    if not os.path.isfile(path):
        raise FileNotFoundError("SQL/ctrlq.sql não encontrado.")

    txt = open(path, "r", encoding="utf-8", errors="ignore").read().strip()
    if not txt:
        raise ValueError("ctrlq.sql está vazio.")

    return txt


# ---------------------------------------------
# Conexão ODBC
# ---------------------------------------------
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
        pool_pre_ping=True
    )


def build_conns():
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


# ---------------------------------------------
# SELECT
# ---------------------------------------------
def run_select(engine, sql):
    with engine.connect() as con:
        df = pd.read_sql_query(text(sql), con)
    return df


# ---------------------------------------------
# Normalização dos dados (fundamental)
# ---------------------------------------------
def normalize_value(v):
    """Garante compatibilidade JSON para todos os tipos."""
    if isinstance(v, (pd.Timestamp, datetime)):
        return v.isoformat()

    if pd.isna(v):
        return None

    return v


def normalize_record(row: dict):
    return {k: normalize_value(v) for k, v in row.items()}


# ---------------------------------------------
# Nome do arquivo
# ---------------------------------------------
def generate_filename(posto):
    now = datetime.now(timezone.utc).astimezone()
    return f"{posto}_{now.strftime('%Y-%m-%d')}_{now.strftime('%H%M')}.json"


# ---------------------------------------------
# Main
# ---------------------------------------------
def main():
    print("=== CTRLQ Exporter ===")
    ensure_dir(JSON_DIR)

    sql = load_sql()
    conns = build_conns()

    if not conns:
        print("Nenhum posto configurado no .env")
        return

    for posto, odbc in conns.items():
        print(f"\n[{posto}] Conectando...")

        # --------------------------
        # Engine
        # --------------------------
        try:
            engine = make_engine(odbc)
        except Exception as e:
            print(f"[{posto}] ERRO engine: {e}")
            continue

        # --------------------------
        # Exec SELECT
        # --------------------------
        print(f"[{posto}] Executando SELECT...")
        try:
            df = run_select(engine, sql)
        except Exception as e:
            print(f"[{posto}] ERRO ao executar SQL: {e}")
            continue

        # --------------------------
        # Normalização (crucial)
        # --------------------------
        registros = []
        for row in df.to_dict(orient="records"):
            registros.append(normalize_record(row))

        # --------------------------
        # Salvar JSON
        # --------------------------
        filename = generate_filename(posto)
        path_out = os.path.join(JSON_DIR, filename)

        try:
            with open(path_out, "w", encoding="utf-8") as f:
                json.dump(registros, f, ensure_ascii=False, indent=2)

            print(f"[{posto}] OK -> {path_out}  ({len(registros)} registros)")
        except Exception as e:
            print(f"[{posto}] ERRO salvando JSON: {e}")

    print("\n=== Concluído ===")


if __name__ == "__main__":
    main()
