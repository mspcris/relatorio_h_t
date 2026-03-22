#!/usr/bin/env python3
"""
sync_tef.py — Sincroniza vw_Sis_TefRecorrenteHistorico → camim_kpi.db (SQLite local)

Usa as mesmas credenciais do .env (DB_HOST_*/DB_BASE_*) por posto.

Uso:
  python sync_tef.py              # todos os postos, últimos 60 dias
  python sync_tef.py --postos A P # só postos A e P
  python sync_tef.py --dias 30    # últimos 30 dias
  python sync_tef.py --check      # status do último sync por posto
"""

import argparse
import os
import sqlite3
import sys
from datetime import date, datetime, timedelta

import pyodbc
from dotenv import load_dotenv

BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
POSTOS      = list("ANXYBRPCDGIMJ")
ODBC_DRIVER = os.getenv("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")
KPI_DB      = os.getenv("KPI_DB_PATH", "/opt/relatorio_h_t/camim_kpi.db")
DIAS_JANELA = int(os.getenv("TEF_SYNC_DIAS", "60"))

load_dotenv(os.path.join(BASE_DIR, ".env"))


# ── Conexões ──────────────────────────────────────────────────────────────────

def _env(key, default=""):
    v = os.getenv(key, default)
    return v.strip() if isinstance(v, str) else v


def _build_conn_str(posto: str) -> str | None:
    host = _env(f"DB_HOST_{posto}")
    base = _env(f"DB_BASE_{posto}")
    if not host or not base:
        return None
    user    = _env(f"DB_USER_{posto}")
    pwd     = _env(f"DB_PASSWORD_{posto}")
    port    = _env(f"DB_PORT_{posto}", "1433")
    encrypt = _env("DB_ENCRYPT",    "yes")
    trust   = _env("DB_TRUST_CERT", "yes")
    timeout = _env("DB_TIMEOUT",    "20")
    base_str = (
        f"DRIVER={{{ODBC_DRIVER}}};"
        f"SERVER=tcp:{host},{port};DATABASE={base};"
        f"Encrypt={encrypt};TrustServerCertificate={trust};"
        f"Connection Timeout={timeout};"
    )
    if user:
        return base_str + f"UID={user};PWD={pwd}"
    return base_str + "Trusted_Connection=yes"


def build_conns(postos: list[str]) -> dict[str, str]:
    return {p: cs for p in postos if (cs := _build_conn_str(p))}


# ── SQLite schema ─────────────────────────────────────────────────────────────

DDL = """
CREATE TABLE IF NOT EXISTS ind_tef (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    posto          TEXT NOT NULL,
    datahora       TEXT,
    matricula      TEXT,
    resposta_cielo TEXT,
    erro           TEXT,
    valor          REAL,
    aprovado       INTEGER DEFAULT 0,
    synced_at      TEXT
);
CREATE INDEX IF NOT EXISTS idx_tef_posto    ON ind_tef(posto);
CREATE INDEX IF NOT EXISTS idx_tef_datahora ON ind_tef(datahora);
CREATE INDEX IF NOT EXISTS idx_tef_aprovado ON ind_tef(aprovado);
"""


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(DDL)
    conn.commit()


# ── Status da transação ───────────────────────────────────────────────────────

def is_aprovado(erro: str) -> int:
    """1 = aprovada, 0 = negada/erro."""
    if not erro:
        return 1
    e = erro.lower()
    if any(k in e for k in ("autoriza", "sucesso", "aprovad")):
        return 1
    return 0


# ── ETL por posto ─────────────────────────────────────────────────────────────

def sync_posto(posto: str, odbc_str: str, kpi: sqlite3.Connection,
               ini_dt: datetime, ini: str, agora: str) -> int:
    srv    = pyodbc.connect(odbc_str, timeout=30)
    cursor = srv.cursor()
    cursor.execute("""
                SELECT DataHora, Matricula, RespostaCielo, Erro, ValorTotal
        FROM   vw_Sis_TefRecorrenteHistorico
        WHERE  Desativado = 0
          AND  DataHora >= ?
    """, ini_dt)
    rows = cursor.fetchall()
    srv.close()

    kpi.execute(
        "DELETE FROM ind_tef WHERE posto = ? AND datahora >= ?",
        (posto, ini)
    )

    registros = [
        (
            posto,
            str(dh  or "")[:19],
            str(mat or ""),
            str(resp or ""),
            str(erro or ""),
            float(valor) if valor is not None else None,
            is_aprovado(str(erro or "")),
            agora,
        )
        for dh, mat, resp, erro, valor in rows
    ]

    kpi.executemany("""
        INSERT INTO ind_tef
            (posto, datahora, matricula, resposta_cielo, erro, valor, aprovado, synced_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, registros)

    return len(registros)


def sync(postos: list[str], dias: int) -> None:
    agora  = datetime.now().isoformat(timespec="seconds")
    ini_dt = datetime.combine(date.today() - timedelta(days=dias), datetime.min.time())
    ini    = ini_dt.strftime("%Y-%m-%d")

    print(f"[sync_tef] {agora} | janela: {dias} dias (desde {ini})")
    print(f"[sync_tef] postos: {postos}")

    conns = build_conns(postos)
    if not conns:
        print("ERRO: nenhum DB_HOST_*/DB_BASE_* encontrado no .env.")
        sys.exit(1)

    kpi = sqlite3.connect(KPI_DB)
    init_db(kpi)

    totais = {}
    for posto, odbc_str in conns.items():
        print(f"  [{posto}] sincronizando...", end=" ", flush=True)
        try:
            n = sync_posto(posto, odbc_str, kpi, ini_dt, ini, agora)
            kpi.execute("""
                INSERT INTO ind_sync_log (indicador, posto, synced_at, total_records, status)
                VALUES ('tef', ?, ?, ?, 'ok')
            """, (posto, agora, n))
            kpi.commit()
            totais[posto] = n
            print(f"{n} registros.")
        except Exception as exc:
            kpi.execute("""
                INSERT INTO ind_sync_log (indicador, posto, synced_at, total_records, status, mensagem)
                VALUES ('tef', ?, ?, 0, 'erro', ?)
            """, (posto, agora, str(exc)))
            kpi.commit()
            print(f"ERRO: {exc}")

    kpi.close()
    total_geral = sum(totais.values())
    print(f"[sync_tef] concluído — {total_geral} registros em {len(totais)} posto(s).")


def check() -> None:
    kpi = sqlite3.connect(KPI_DB)
    rows = kpi.execute("""
        SELECT posto, synced_at, total_records, status, mensagem
        FROM   ind_sync_log
        WHERE  indicador = 'tef'
          AND  id IN (
              SELECT MAX(id) FROM ind_sync_log
              WHERE indicador = 'tef'
              GROUP BY posto
          )
        ORDER BY posto
    """).fetchall()
    kpi.close()

    if not rows:
        print("Nenhum sync registrado ainda.")
        return
    for posto, synced_at, total, status, msg in rows:
        linha = f"  [{posto}] {synced_at}  {total:>6} registros  {status}"
        if msg:
            linha += f"  — {msg}"
        print(linha)


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync vw_Sis_TefRecorrenteHistorico → camim_kpi.db")
    parser.add_argument("--postos", nargs="+", default=POSTOS)
    parser.add_argument("--dias",   type=int, default=DIAS_JANELA)
    parser.add_argument("--check",  action="store_true")
    args = parser.parse_args()

    if args.check:
        check()
    else:
        postos = [p.upper() for p in args.postos if p.strip()]
        sync(postos, args.dias)
