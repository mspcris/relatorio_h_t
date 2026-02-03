# export_consultas_mensal_json.py
# Requisitos: pandas, sqlalchemy>=2,<3, pyodbc, python-dotenv
# Escopo: conectar via .env (mesmo padrão do export_governanca.py),
#         executar SQL com params :ini/:fim e gerar JSON.
# Rodagem diária: atualiza SOMENTE mês atual e mês imediatamente anterior.

import os
import json
from datetime import date
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv


# =========================
# Constantes / Paths
# =========================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

SQL_PATH = os.path.join(BASE_DIR, "sql_consultas_mensal", "sql.sql")
OUT_DIR  = os.path.join(BASE_DIR, "json_consultas_mensal")
os.makedirs(OUT_DIR, exist_ok=True)

# Mesmo driver default do governança
ODBC_DRIVER = os.getenv("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")


# =========================
# Helpers de .env (iguais ao governança)
# =========================
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

def build_conn_from_env(posto: str):
    """
    Lê do .env no padrão:
      DB_HOST_{P}
      DB_BASE_{P}
      DB_USER_{P}
      DB_PASSWORD_{P}
      DB_PORT_{P} (opcional, default 1433)

    Defaults globais (mesmo governança):
      DB_ENCRYPT (default yes)
      DB_TRUST_CERT (default yes)
      DB_TIMEOUT (default 20)
    """
    load_dotenv(os.path.join(BASE_DIR, ".env"))

    encrypt    = env("DB_ENCRYPT", "yes")
    trust_cert = env("DB_TRUST_CERT", "yes")
    timeout    = env("DB_TIMEOUT", "20")

    posto = (posto or "").strip().upper()
    if not posto:
        raise RuntimeError("POSTO vazio. Ex: A, N, X...")

    host = env(f"DB_HOST_{posto}")
    base = env(f"DB_BASE_{posto}")
    if not host or not base:
        raise RuntimeError(f".env sem DB_HOST_{posto} e/ou DB_BASE_{posto}")

    user = env(f"DB_USER_{posto}")
    pwd  = env(f"DB_PASSWORD_{posto}")
    port = env(f"DB_PORT_{posto}", "1433")

    return build_conn_str(host, base, user, pwd, port, encrypt, trust_cert, timeout)


# =========================
# Datas
# =========================
def month_bounds_from_dt(dt: date):
    ini = date(dt.year, dt.month, 1)
    nxt = date(dt.year + (dt.month == 12), (dt.month % 12) + 1, 1)
    return ini, nxt, f"{ini.year:04d}_{ini.month:02d}"

def previous_month_dt(dt: date):
    if dt.month == 1:
        return date(dt.year - 1, 12, 1)
    return date(dt.year, dt.month - 1, 1)


# =========================
# SQL Exec
# =========================
def _ensure_nocount(sql_text: str) -> str:
    return sql_text if sql_text.lstrip().upper().startswith("SET NOCOUNT ON") else "SET NOCOUNT ON;\n" + sql_text

def load_sql():
    if not os.path.exists(SQL_PATH):
        raise FileNotFoundError(f"SQL não encontrado: {SQL_PATH}")
    with open(SQL_PATH, "r", encoding="utf-8", errors="ignore") as f:
        return f.read().strip()

def run_one_month(engine, sql_txt: str, ini: date, fim: date):
    body = _ensure_nocount(sql_txt)
    with engine.connect() as con:
        return pd.read_sql_query(text(body), con, params={"ini": ini, "fim": fim})


# =========================
# JSON
# =========================
def write_json(ano: int, mes: int, ini: date, fim: date, df: pd.DataFrame):
    payload = {
        "periodo": {
            "ano": int(ano),
            "mes": int(mes),
            "ini": ini.isoformat(),
            "fim": fim.isoformat(),
        },
        "linhas": df.to_dict(orient="records"),
    }

    out_path = os.path.join(OUT_DIR, f"consultas_{ano}_{mes:02d}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"OK -> {out_path} ({len(df)} registros)")


# =========================
# Orquestração (diária)
# =========================
def run_daily_refresh(posto: str):
    """
    Atualiza SOMENTE:
      - mês anterior
      - mês atual
    """
    odbc_str = build_conn_from_env(posto)
    engine = make_engine(odbc_str)

    sql_txt = load_sql()

    today = date.today()

    # mês anterior
    dt_prev = previous_month_dt(today)
    ini_prev, fim_prev, _ = month_bounds_from_dt(dt_prev)
    df_prev = run_one_month(engine, sql_txt, ini_prev, fim_prev)
    write_json(dt_prev.year, dt_prev.month, ini_prev, fim_prev, df_prev)

    # mês atual
    ini_cur, fim_cur, _ = month_bounds_from_dt(today)
    df_cur = run_one_month(engine, sql_txt, ini_cur, fim_cur)
    write_json(today.year, today.month, ini_cur, fim_cur, df_cur)


if __name__ == "__main__":
    # Mantém padrão simples: escolha o posto via env.
    # Exemplo no .env (ou export no shell):
    #   POSTO_CONSULTAS=A
    posto = os.getenv("POSTO_CONSULTAS", "").strip().upper()
    if not posto:
        raise RuntimeError("Defina POSTO_CONSULTAS no .env (ex: POSTO_CONSULTAS=A).")

    run_daily_refresh(posto)
