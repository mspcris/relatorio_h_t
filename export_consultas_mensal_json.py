# export_consultas_mensal_json.py
# Requisitos: pandas, sqlalchemy>=2,<3, pyodbc, python-dotenv
# Escopo:
# - Lê .env multi-posto (padrão export_governanca.py)
# - Garante histórico desde 2020-01 (backfill incremental):
#     * se JSON do mês NÃO existe -> roda e cria
#     * se JSON do mês JÁ existe -> não roda (skip)
# - Rotina diária: sempre força (recria) mês anterior e mês atual (como governança)
# - Opcional: salva CSV também (mesma lógica de skip/force)
# - Sem inteligência além disso.

import os
import re
import json
import glob
import argparse
from datetime import date, datetime, timezone
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import time


# =========================
# Paths / Defaults
# =========================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

SQL_PATH = os.path.join(BASE_DIR, "sql_consultas_mensal", "sql.sql")

OUT_JSON_DIR = os.path.join(BASE_DIR, "json_consultas_mensal")
OUT_CSV_DIR  = os.path.join(BASE_DIR, "dados_consultas_mensal")

os.makedirs(OUT_JSON_DIR, exist_ok=True)
os.makedirs(OUT_CSV_DIR, exist_ok=True)

POSTOS_DEFAULT = list("ANXYBRPCDGIMJ")
ODBC_DRIVER = os.getenv("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")

EARLIEST_ALLOWED = date(2020, 1, 1)


# =========================
# Utilitários (governança-like)
# =========================
def env(key, default=""):
    v = os.getenv(key, default)
    return v.strip() if isinstance(v, str) else v

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

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
    # pool_recycle ajuda a evitar conexões "podres" em exec longa
    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_conn_str)}",
        pool_pre_ping=True,
        pool_recycle=300,
        future=True,
    )

def _sleep_backoff(attempt: int):
    # 0.5s, 1s, 2s, 4s... cap em 5s
    time.sleep(min(0.5 * (2 ** (attempt - 1)), 5.0))

def _with_timeout_seconds(seconds: int):
    """
    Timeout global simples por tentativa:
    - Para Windows pode não funcionar via SIGALRM.
    - Como alternativa robusta cross-platform, dependeríamos de threads/subprocess.
    Aqui, usamos timeouts de conexão + read_sql e retries, que já resolve 99% dos travamentos.
    """
    return seconds

def build_conns_from_env(postos=None):
    load_dotenv(os.path.join(BASE_DIR, ".env"))

    encrypt    = env("DB_ENCRYPT", "yes")
    trust_cert = env("DB_TRUST_CERT", "yes")
    timeout    = env("DB_TIMEOUT", "20")

    conns = {}
    base_postos = postos or POSTOS_DEFAULT

    for p in base_postos:
        host = env(f"DB_HOST_{p}")
        base = env(f"DB_BASE_{p}")
        if not host or not base:
            continue

        user = env(f"DB_USER_{p}")
        pwd  = env(f"DB_PASSWORD_{p}")
        port = env(f"DB_PORT_{p}", "1433")

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
# IO / naming
# =========================
def json_path(posto: str, ym: str) -> str:
    return os.path.join(OUT_JSON_DIR, f"{posto}_consultas_{ym}.json")

def csv_path(posto: str, ym: str) -> str:
    return os.path.join(OUT_CSV_DIR, f"{posto}_consultas_{ym}.csv")

def should_write(out_path: str, force: bool) -> bool:
    if force:
        return True
    return not os.path.exists(out_path)


# =========================
# Execução
# =========================
def run_query(engine, sql_txt: str, ini: date, fim: date, retries: int = 3) -> pd.DataFrame:
    """
    retries: número de tentativas antes de desistir e seguir para o próximo.
    Cada tentativa respeita timeouts do driver + pool_pre_ping.
    """
    body = _ensure_nocount(sql_txt)

    last_err = None
    for attempt in range(1, retries + 1):
        try:
            with engine.connect() as con:
                # opcional: força um statement timeout no SQL Server via SET LOCK_TIMEOUT (ms)
                # e/ou SET QUERY_GOVERNOR_COST_LIMIT. Aqui deixei conservador.
                return pd.read_sql_query(text(body), con, params={"ini": ini, "fim": fim})
        except Exception as e:
            last_err = e
            print(f"[WARN] tentativa {attempt}/{retries} falhou (query): {e}")
            if attempt < retries:
                _sleep_backoff(attempt)

    raise last_err


def try_build_engine(odbc_str: str, retries: int = 3):
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            eng = make_engine(odbc_str)
            # valida conexão rápida (evita ficar preso esperando a primeira query)
            with eng.connect() as con:
                con.execute(text("SELECT 1"))
            return eng
        except Exception as e:
            last_err = e
            print(f"[WARN] tentativa {attempt}/{retries} falhou (engine/connect): {e}")
            if attempt < retries:
                _sleep_backoff(attempt)
    raise last_err


def write_outputs(posto: str, ym: str, ini: date, fim: date, df: pd.DataFrame, do_csv: bool):
    # JSON
    payload = {
        "posto": posto,
        "periodo": {
            "ym": ym,
            "ini": ini.isoformat(),
            "fim": fim.isoformat(),
        },
        "linhas": df.to_dict(orient="records"),
    }
    out_json = json_path(posto, ym)
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    _set_mtime(out_json)

    # CSV (opcional)
    if do_csv:
        out_csv = csv_path(posto, ym)
        df.to_csv(out_csv, index=False, encoding="utf-8-sig")
        _set_mtime(out_csv)

    print(f"[{posto}] OK {ym} linhas={len(df)} -> {os.path.relpath(out_json, BASE_DIR)}" + ("" if not do_csv else f" + {os.path.relpath(csv_path(posto, ym), BASE_DIR)}"))


# =========================
# Orquestração (mesma lógica do outro back)
# =========================
def run_incremental_all_postos(postos=None, do_csv=False, force_months=None):
    """
    - Backfill desde 2020-01 até mês atual (exclusive do próximo mês):
        * se arquivo existe -> skip
        * se não existe -> roda e cria
    - force_months: set de 'YYYY-MM' que sempre recria (mês atual e anterior)
    - Robustez:
        * se falhar conexão/query: tenta por ~10s (via retries+backoff) e segue pro próximo
    """
    ensure_dir(OUT_JSON_DIR)
    ensure_dir(OUT_CSV_DIR)

    if not os.path.exists(SQL_PATH):
        raise FileNotFoundError(f"SQL não encontrado: {SQL_PATH}")

    sql_txt = load_sql_strip_go(SQL_PATH)
    if not sql_txt:
        raise RuntimeError(f"SQL vazio: {SQL_PATH}")

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

    # ~10s total na pior hipótese: 4 tentativas com backoff 0.5 + 1 + 2 + 4 = 7.5s (+ overhead)
    ENGINE_RETRIES = 4
    QUERY_RETRIES = 4

    for month_start in month_iter(start, end_exclusive):
        ini, fim, ym = month_bounds(month_start)

        for posto, odbc_str in conns.items():
            out_json = json_path(posto, ym)
            out_csv  = csv_path(posto, ym)

            force = (ym in forced)

            need_json = should_write(out_json, force=force)
            need_csv  = (do_csv and should_write(out_csv, force=force))

            if not need_json and not need_csv:
                continue

            # 1) engine/connect com retry
            try:
                engine = try_build_engine(odbc_str, retries=ENGINE_RETRIES)
            except Exception as e:
                print(f"[{posto}] ERRO conexão {ym}: {e} (pulando)")
                continue

            # 2) query com retry
            try:
                df = run_query(engine, sql_txt, ini, fim, retries=QUERY_RETRIES)
            except Exception as e:
                print(f"[{posto}] ERRO exec {ym}: {e} (pulando)")
                continue

            # 3) salvar (se falhar, também não para)
            try:
                write_outputs(posto, ym, ini, fim, df, do_csv=do_csv)
            except Exception as e:
                print(f"[{posto}] ERRO salvar {ym}: {e} (pulando)")
                continue

def parse_args():
    p = argparse.ArgumentParser(description="Export consultas mensais (JSON; opcional CSV) incremental desde 2020-01, forçando mês atual e anterior.")
    p.add_argument("--postos", default="", help="Opcional: subset de postos. Ex: ANX. Se vazio, usa lista padrão.")
    p.add_argument("--csv", action="store_true", help="Também grava CSV (mesma lógica de skip/force).")
    return p.parse_args()


def main():
    args = parse_args()
    postos = [c for c in (args.postos or "") if c.isalpha()]
    run_incremental_all_postos(
        postos=postos if postos else None,
        do_csv=args.csv,
        force_months=None
    )


if __name__ == "__main__":
    main()
