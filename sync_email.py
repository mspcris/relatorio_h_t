#!/usr/bin/env python3
"""
sync_email.py — Sincroniza vw_cad_email de TODOS os postos → camim_kpi.db (SQLite local)

Lê credenciais do .env exatamente como export_governanca.py:
  DB_HOST_A, DB_BASE_A, DB_USER_A, DB_PASSWORD_A, DB_PORT_A  (para cada posto)

Uso:
  python sync_email.py              # todos os postos, últimos 90 dias
  python sync_email.py --postos A P # só postos A e P
  python sync_email.py --dias 30    # últimos 30 dias
  python sync_email.py --check      # status do último sync por posto
"""

import argparse
import os
import re
import sqlite3
import sys
from datetime import date, datetime, timedelta

import pyodbc
from dotenv import load_dotenv

BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
POSTOS      = list("ANXYBRPCDGIMJ")
ODBC_DRIVER = os.getenv("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")
KPI_DB      = os.getenv("KPI_DB_PATH", "/opt/relatorio_h_t/camim_kpi.db")
DIAS_JANELA = int(os.getenv("EMAIL_SYNC_DIAS", "90"))

load_dotenv(os.path.join(BASE_DIR, ".env"))


# ── Conexões (mesmo padrão de export_governanca.py) ──────────────────────────

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
    """Retorna {letra_posto: odbc_conn_str} para postos com credenciais no .env."""
    return {p: cs for p in postos if (cs := _build_conn_str(p))}


# ── SQLite schema ─────────────────────────────────────────────────────────────

DDL = """
CREATE TABLE IF NOT EXISTS ind_email (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    posto            TEXT NOT NULL,
    titulo_original  TEXT,
    titulo_categoria TEXT,
    datahora         TEXT,
    matricula        TEXT,
    status           TEXT,
    synced_at        TEXT
);
CREATE INDEX IF NOT EXISTS idx_em_posto    ON ind_email(posto);
CREATE INDEX IF NOT EXISTS idx_em_datahora ON ind_email(datahora);
CREATE INDEX IF NOT EXISTS idx_em_cat      ON ind_email(titulo_categoria);

CREATE TABLE IF NOT EXISTS ind_sync_log (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    indicador     TEXT NOT NULL,
    posto         TEXT,
    synced_at     TEXT NOT NULL,
    total_records INTEGER DEFAULT 0,
    status        TEXT,
    mensagem      TEXT
);
"""


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(DDL)
    conn.commit()


# ── Normalização de título ────────────────────────────────────────────────────

_RE_PREFIXO      = re.compile(r'^Camim\s*[-–]\s*', re.I)
_RE_CODIGO_PREF  = re.compile(r'^\d[\w\d]*\s*[-–]\s*')   # ex: "101147P - " ou "0Y - "
_RE_MATRICULA    = re.compile(r'Matr[íi]cula\s+[\w]+\.?\s*', re.I)
_RE_MES_ANO      = re.compile(
    r'\b(?:janeiro|fevereiro|mar[çc]o|abril|maio|junho|julho|agosto|setembro|'
    r'outubro|novembro|dezembro|jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez)'
    r'\w*/\d{4}\b', re.I
)
_RE_DATA         = re.compile(r'\b\d{2}/\d{2}/\d{4}\b')
_RE_DINHEIRO     = re.compile(r'R\$\s*[\d.,]+')
_RE_NUMEROS      = re.compile(r'\b\d[\d.,]*\b')
_RE_LIXO_FIM     = re.compile(r'[-–:,;]\s*$')
_RE_ESPACOS      = re.compile(r'\s{2,}')


def normalizar_titulo(titulo: str) -> str:
    t = titulo or ""
    t = _RE_PREFIXO.sub("", t)
    t = _RE_CODIGO_PREF.sub("", t)   # remove código alfanumérico inicial tipo "101147P - "
    t = _RE_MATRICULA.sub("", t)
    t = _RE_MES_ANO.sub("", t)
    t = _RE_DATA.sub("", t)
    t = _RE_DINHEIRO.sub("", t)
    t = _RE_NUMEROS.sub("", t)
    t = _RE_LIXO_FIM.sub("", t.strip())
    t = _RE_ESPACOS.sub(" ", t).strip(" .-–")
    t = t[:120] if t else (titulo or "")[:120]
    return categorizar(t)


# ── Categorização canônica ────────────────────────────────────────────────────

_CAT_RULES = [
    (re.compile(r'\bboleto\b',           re.I), "Boleto"),
    (re.compile(r'\bfalta\b',            re.I), "Falta do Médico"),
    (re.compile(r'\bcancelamento\b',     re.I), "Cancelamento"),
    (re.compile(r'\bnota\s*fiscal\b',    re.I), "Nota Fiscal"),
]


def categorizar(titulo: str) -> str:
    """Agrupa variações num nome canônico de categoria."""
    for regex, nome in _CAT_RULES:
        if regex.search(titulo):
            return nome
    return titulo


# ── ETL por posto ─────────────────────────────────────────────────────────────

def sync_posto(posto: str, odbc_str: str, kpi: sqlite3.Connection,
               ini_dt: datetime, ini: str, agora: str) -> int:
    """Sincroniza um posto. Retorna número de registros. Levanta em caso de erro."""
    srv    = pyodbc.connect(odbc_str, timeout=30)
    cursor = srv.cursor()
    cursor.execute("""
        SELECT Titulo, Datahora, Matricula, ProgramaOrigem
        FROM   vw_cad_email
        WHERE  Desativado = 0
          AND  Datahora >= ?
    """, ini_dt)
    rows = cursor.fetchall()
    srv.close()

    # Apaga e re-insere o período (evita duplicatas)
    kpi.execute(
        "DELETE FROM ind_email WHERE posto = ? AND datahora >= ?",
        (posto, ini)
    )

    registros = [
        (
            posto,
            str(titulo  or ""),
            normalizar_titulo(str(titulo or "")),
            str(dh      or "")[:19],
            str(mat     or ""),
            str(origem  or ""),
            agora,
        )
        for titulo, dh, mat, origem in rows
    ]

    kpi.executemany("""
        INSERT INTO ind_email
            (posto, titulo_original, titulo_categoria, datahora, matricula, status, synced_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, registros)

    return len(registros)


def sync(postos: list[str], dias: int) -> None:
    agora  = datetime.now().isoformat(timespec="seconds")
    ini_dt = datetime.combine(date.today() - timedelta(days=dias), datetime.min.time())
    ini    = ini_dt.strftime("%Y-%m-%d")

    print(f"[sync_email] {agora} | janela: {dias} dias (desde {ini})")
    print(f"[sync_email] postos: {postos}")

    conns = build_conns(postos)
    if not conns:
        print("ERRO: nenhum DB_HOST_*/DB_BASE_* encontrado no .env para os postos informados.")
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
                VALUES ('email', ?, ?, ?, 'ok')
            """, (posto, agora, n))
            kpi.commit()
            totais[posto] = n
            print(f"{n} registros.")
        except Exception as exc:
            kpi.execute("""
                INSERT INTO ind_sync_log (indicador, posto, synced_at, total_records, status, mensagem)
                VALUES ('email', ?, ?, 0, 'erro', ?)
            """, (posto, agora, str(exc)))
            kpi.commit()
            print(f"ERRO: {exc}")

    kpi.close()
    total_geral = sum(totais.values())
    print(f"[sync_email] concluído — {total_geral} registros em {len(totais)} posto(s).")


def check() -> None:
    kpi = sqlite3.connect(KPI_DB)
    rows = kpi.execute("""
        SELECT posto, synced_at, total_records, status, mensagem
        FROM   ind_sync_log
        WHERE  indicador = 'email'
          AND  id IN (
              SELECT MAX(id) FROM ind_sync_log
              WHERE indicador = 'email'
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
    parser = argparse.ArgumentParser(description="Sync vw_cad_email → camim_kpi.db")
    parser.add_argument("--postos", nargs="+", default=POSTOS,
                        help=f"Postos a sincronizar (padrão: todos — {POSTOS})")
    parser.add_argument("--dias",   type=int, default=DIAS_JANELA,
                        help=f"Janela em dias (padrão: {DIAS_JANELA})")
    parser.add_argument("--check",  action="store_true",
                        help="Mostra status do último sync por posto")
    args = parser.parse_args()

    if args.check:
        check()
    else:
        postos = [p.upper() for p in args.postos if p.strip()]
        sync(postos, args.dias)
