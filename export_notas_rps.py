# export_notas_rps.py
# Requisitos: pandas, sqlalchemy>=2,<3, pyodbc, python-dotenv
# Objetivo: por posto e por mês, gerar JSON com:
# - notas_emitidas (agregado)
# - rps_pendentes (agregado)
#
# Política de atualização:
# - backfill incremental desde 2020-01: se JSON do mês existe -> skip
# - sempre reprocessa mês atual e mês anterior

import os
import re
import json
import argparse
from datetime import date, datetime, timezone
from urllib.parse import quote_plus
import time

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv


# =========================
# Paths / Defaults
# =========================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

SQL_DIR = os.path.join(BASE_DIR, "sql_notas_rps")
SQL_NOTAS_PATH = os.path.join(SQL_DIR, "notas_emitidas.sql")
SQL_RPS_PATH   = os.path.join(SQL_DIR, "rps_pendentes.sql")

OUT_JSON_DIR = os.path.join(BASE_DIR, "json_notas_rps")
os.makedirs(OUT_JSON_DIR, exist_ok=True)

POSTOS_DEFAULT = list("ANXYBRPCDGIMJP")
ODBC_DRIVER = os.getenv("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")

EARLIEST_ALLOWED = date(2026, 1, 1)


# =========================
# Utils
# =========================
def env(key, default=""):
    v = os.getenv(key, default)
    return v.strip() if isinstance(v, str) else v

def _set_mtime(path: str) -> None:
    ts = datetime.now(timezone.utc).astimezone().timestamp()
    os.utime(path, (ts, ts))

def load_sql_strip_go(path: str) -> str:
    txt = open(path, "r", encoding="utf-8", errors="ignore").read()
    txt = re.sub(r"(?im)^\s*go\s*$", "", txt)
    return txt.strip()

def _ensure_nocount(sql_text: str) -> str:
    return sql_text if sql_text.lstrip().upper().startswith("SET NOCOUNT ON") else "SET NOCOUNT ON;\n" + sql_text

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

def make_engine(odbc_conn_str: str):
    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_conn_str)}",
        pool_pre_ping=True,
        pool_recycle=300,
        future=True,
    )

def _sleep_backoff(attempt: int):
    time.sleep(min(0.5 * (2 ** (attempt - 1)), 5.0))

def build_conns_from_env(postos=None):
    load_dotenv(os.path.join(BASE_DIR, ".env"))

    encrypt    = env("DB_ENCRYPT", "yes")
    trust_cert = env("DB_TRUST_CERT", "yes")
    timeout    = env("DB_TIMEOUT", "20")

    conns = {}
    base_postos = postos or POSTOS_DEFAULT

    for p in base_postos:
        host = env(f"DB_HOST_{p}") or env(f"DB_HOST_{p.lower()}")
        base = env(f"DB_BASE_{p}") or env(f"DB_BASE_{p.lower()}")
        if not host or not base:
            continue

        user = env(f"DB_USER_{p}") or env(f"DB_USER_{p.lower()}")
        pwd  = env(f"DB_PASSWORD_{p}") or env(f"DB_PASSWORD_{p.lower()}")
        port = env(f"DB_PORT_{p}", "1433") or env(f"DB_PORT_{p.lower()}", "1433")

        conns[p] = build_conn_str(host, base, user, pwd, port, encrypt, trust_cert, timeout)

    return conns


# =========================
# Datas / iteração mensal
# =========================
def month_bounds(dt: date):
    ini = date(dt.year, dt.month, 1)
    nxt = date(dt.year + (dt.month == 12), (dt.month % 12) + 1, 1)
    return ini, nxt, f"{ini.year:04d}-{ini.month:02d}"

def previous_month_bounds(dt: date):
    m = 12 if dt.month == 1 else dt.month - 1
    y = dt.year - 1 if dt.month == 1 else dt.year
    ini = date(y, m, 1)
    nxt = date(dt.year, dt.month, 1)
    return ini, nxt, f"{y:04d}-{m:02d}"

def month_iter(start: date, end_exclusive: date):
    y, m = start.year, start.month
    while True:
        ini = date(y, m, 1)
        if ini >= end_exclusive:
            break
        yield ini
        m = 1 if m == 12 else m + 1
        y = y + 1 if m == 1 else y


# =========================
# IO
# =========================
def json_path(posto: str, ym: str) -> str:
    return os.path.join(OUT_JSON_DIR, f"{posto}_notas_rps_{ym}.json")

def should_write(out_path: str, force: bool) -> bool:
    if force:
        return True
    return not os.path.exists(out_path)


# =========================
# Execução
# =========================
def try_build_engine(odbc_str: str, retries: int = 4):
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            eng = make_engine(odbc_str)
            with eng.connect() as con:
                con.execute(text("SELECT 1"))
            return eng
        except Exception as e:
            last_err = e
            print(f"[WARN] tentativa {attempt}/{retries} falhou (engine/connect): {e}")
            if attempt < retries:
                _sleep_backoff(attempt)
    raise last_err

def run_query(engine, sql_txt: str, ini: date, fim: date, retries: int = 4) -> pd.DataFrame:
    body = _ensure_nocount(sql_txt)
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            with engine.connect() as con:
                return pd.read_sql_query(text(body), con, params={"ini": ini, "fim": fim})
        except Exception as e:
            last_err = e
            print(f"[WARN] tentativa {attempt}/{retries} falhou (query): {e}")
            if attempt < retries:
                _sleep_backoff(attempt)
    raise last_err

def write_outputs(posto: str, ym: str, ini: date, fim: date,
                  df_notas: pd.DataFrame, df_rps: pd.DataFrame):
    payload = {
        "posto": posto,
        "periodo": {"ym": ym, "ini": ini.isoformat(), "fim": fim.isoformat()},
        "notas_emitidas": df_notas.to_dict(orient="records"),
        "rps_pendentes": df_rps.to_dict(orient="records"),
        "gerado_em": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
    }

    out_json = json_path(posto, ym)
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    _set_mtime(out_json)

    print(f"[{posto}] OK {ym} notas={len(df_notas)} rps={len(df_rps)} -> {os.path.relpath(out_json, BASE_DIR)}")


def run_incremental_all_postos(postos=None, force_months=None):
    if not os.path.exists(SQL_NOTAS_PATH):
        raise FileNotFoundError(f"SQL não encontrado: {SQL_NOTAS_PATH}")
    if not os.path.exists(SQL_RPS_PATH):
        raise FileNotFoundError(f"SQL não encontrado: {SQL_RPS_PATH}")

    sql_notas = load_sql_strip_go(SQL_NOTAS_PATH)
    sql_rps   = load_sql_strip_go(SQL_RPS_PATH)
    if not sql_notas:
        raise RuntimeError(f"SQL vazio: {SQL_NOTAS_PATH}")
    if not sql_rps:
        raise RuntimeError(f"SQL vazio: {SQL_RPS_PATH}")

    conns = build_conns_from_env(postos=postos)
    if not conns:
        raise RuntimeError("Nenhum posto encontrado no .env. Precisa de DB_HOST_{P} e DB_BASE_{P}.")

    today = date.today()
    ini_cur, fim_cur, ym_cur = month_bounds(today)
    _, _, ym_prev = previous_month_bounds(today)

    forced = set(force_months or set())
    forced.update({ym_cur, ym_prev})

    start = EARLIEST_ALLOWED
    end_exclusive = fim_cur  # início do próximo mês (exclusive)

    for month_start in month_iter(start, end_exclusive):
        ini, fim, ym = month_bounds(month_start)

        for posto, odbc_str in conns.items():
            out_json = json_path(posto, ym)
            force = (ym in forced)

            if not should_write(out_json, force=force):
                continue

            try:
                engine = try_build_engine(odbc_str, retries=4)
            except Exception as e:
                print(f"[{posto}] ERRO conexão {ym}: {e} (pulando)")
                continue

            try:
                df_notas = run_query(engine, sql_notas, ini, fim, retries=4)
            except Exception as e:
                print(f"[{posto}] ERRO notas {ym}: {e} (pulando)")
                continue

            try:
                df_rps = run_query(engine, sql_rps, ini, fim, retries=4)
            except Exception as e:
                print(f"[{posto}] ERRO rps {ym}: {e} (pulando)")
                continue

            try:
                write_outputs(posto, ym, ini, fim, df_notas, df_rps)
            except Exception as e:
                print(f"[{posto}] ERRO salvar {ym}: {e} (pulando)")
                continue


def parse_args():
    p = argparse.ArgumentParser(description="Export Notas Emitidas + RPS Pendentes (JSON) incremental desde 2026-01; força mês atual e anterior.")
    p.add_argument("--postos", default="", help="Opcional: subset de postos. Ex: ANX. Se vazio, usa lista padrão.")
    return p.parse_args()

def main():
    args = parse_args()
    postos = [c for c in (args.postos or "") if c.isalpha()]
    run_incremental_all_postos(postos=postos if postos else None, force_months=None)

if __name__ == "__main__":
    main()