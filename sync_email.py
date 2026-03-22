#!/usr/bin/env python3
"""
sync_email.py — Sincroniza vw_cad_email (Anchieta SQL Server) → camim_kpi.db (SQLite local)

Execução: 1x ao dia via cron, de madrugada.
Registra data/hora de atualização em ind_sync_log.

Uso:
  python sync_email.py            # sincroniza últimos 90 dias (padrão)
  python sync_email.py --dias 30  # sincroniza últimos 30 dias
  python sync_email.py --check    # só mostra status do último sync
"""

import argparse
import os
import re
import sqlite3
import sys
from datetime import date, datetime, timedelta

import pyodbc
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

# ── Configuração ──────────────────────────────────────────────────────────────

ODBC_DRIVER = os.getenv("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")

KPI_DB = os.getenv("KPI_DB_PATH", "/opt/relatorio_h_t/camim_kpi.db")

DIAS_JANELA = int(os.getenv("EMAIL_SYNC_DIAS", "90"))


def _anchieta_conn_str() -> str:
    host      = os.getenv("ANCHIETA_HOST", "")
    db        = os.getenv("ANCHIETA_DB",   "Anchieta")
    user      = os.getenv("ANCHIETA_USER", "")
    pwd       = os.getenv("ANCHIETA_PWD",  "")
    port      = os.getenv("ANCHIETA_PORT", "1433")
    encrypt   = os.getenv("DB_ENCRYPT",    "yes")
    trust     = os.getenv("DB_TRUST_CERT", "yes")
    timeout   = os.getenv("DB_TIMEOUT",    "20")
    base = (
        f"DRIVER={{{ODBC_DRIVER}}};"
        f"SERVER=tcp:{host},{port};DATABASE={db};"
        f"Encrypt={encrypt};TrustServerCertificate={trust};"
        f"Connection Timeout={timeout};"
    )
    if user:
        return base + f"UID={user};PWD={pwd}"
    return base + "Trusted_Connection=yes"


# ── Schema SQLite ─────────────────────────────────────────────────────────────

DDL = """
CREATE TABLE IF NOT EXISTS ind_email (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    titulo_original    TEXT,
    titulo_categoria   TEXT,     -- titulo normalizado (sem variáveis)
    datahora           TEXT,     -- ISO: "2026-03-21 14:31:00"
    posto              TEXT,     -- letra extraída de matricula_completa
    matricula          TEXT,     -- matricula_completa original
    status             TEXT,     -- ENVIADO, CANCELADO, etc.
    synced_at          TEXT      -- quando foi inserido no SQLite
);

CREATE INDEX IF NOT EXISTS idx_em_datahora ON ind_email(datahora);
CREATE INDEX IF NOT EXISTS idx_em_posto    ON ind_email(posto);
CREATE INDEX IF NOT EXISTS idx_em_cat      ON ind_email(titulo_categoria);

CREATE TABLE IF NOT EXISTS ind_sync_log (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    indicador     TEXT    NOT NULL,
    synced_at     TEXT    NOT NULL,
    total_records INTEGER DEFAULT 0,
    status        TEXT,            -- 'ok' | 'erro'
    mensagem      TEXT
);
"""


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(DDL)
    conn.commit()


# ── Normalização de título ────────────────────────────────────────────────────
# Remove variáveis (matrícula, datas, valores) para obter o template da campanha.
# Ex.: "Camim - Matrícula 45739A. Pagamento Janeiro/2014 com vencimento: 23/01/2014 - Valor R$ 43,00"
#   →  "Pagamento com vencimento"

_RE_PREFIXO  = re.compile(r'^Camim\s*[-–]\s*', re.I)
_RE_MATRICULA = re.compile(r'Matr[íi]cula\s+[\w]+\.?\s*', re.I)
_RE_MES_ANO  = re.compile(
    r'\b(?:janeiro|fevereiro|mar[çc]o|abril|maio|junho|julho|agosto|setembro|'
    r'outubro|novembro|dezembro|jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez)'
    r'\w*/\d{4}\b', re.I
)
_RE_DATA     = re.compile(r'\b\d{2}/\d{2}/\d{4}\b')
_RE_DINHEIRO = re.compile(r'R\$\s*[\d.,]+')
_RE_NUMEROS  = re.compile(r'\b\d[\d.,]*\b')
_RE_LIXO     = re.compile(r'[-–:,;]\s*$')
_RE_ESPACOS  = re.compile(r'\s{2,}')


def normalizar_titulo(titulo: str) -> str:
    t = titulo or ""
    t = _RE_PREFIXO.sub("", t)
    t = _RE_MATRICULA.sub("", t)
    t = _RE_MES_ANO.sub("", t)
    t = _RE_DATA.sub("", t)
    t = _RE_DINHEIRO.sub("", t)
    t = _RE_NUMEROS.sub("", t)
    t = _RE_LIXO.sub("", t.strip())
    t = _RE_ESPACOS.sub(" ", t).strip(" .-–")
    return t[:120] if t else titulo[:120]


def extrair_posto(matricula: str) -> str:
    """'45739A' → 'A'. Retorna '' se não encontrar letra."""
    m = re.search(r'([A-Z])\s*$', str(matricula or "").strip().upper())
    return m.group(1) if m else ""


# ── ETL principal ─────────────────────────────────────────────────────────────

def sync(dias: int = DIAS_JANELA) -> None:
    agora = datetime.now().isoformat(timespec="seconds")
    ini   = (date.today() - timedelta(days=dias)).strftime("%Y-%m-%d")
    print(f"[sync_email] {agora} | janela: últimos {dias} dias (desde {ini})")

    kpi = sqlite3.connect(KPI_DB)
    init_db(kpi)

    try:
        srv    = pyodbc.connect(_anchieta_conn_str(), timeout=30)
        cursor = srv.cursor()

        cursor.execute("""
            SELECT
                Titulo,
                Datahora,
                Matricula_completa,
                Status
            FROM vw_cad_email
            WHERE Desativado = 0
              AND Datahora >= ?
        """, ini)

        rows = cursor.fetchall()
        srv.close()

        # Apaga o período que vai ser re-inserido (evita duplicatas)
        kpi.execute("DELETE FROM ind_email WHERE datahora >= ?", (ini,))

        registros = []
        for titulo_orig, datahora, matricula, status in rows:
            registros.append((
                str(titulo_orig  or ""),
                normalizar_titulo(str(titulo_orig or "")),
                str(datahora     or "")[:19],   # corta milissegundos
                extrair_posto(str(matricula or "")),
                str(matricula    or ""),
                str(status       or ""),
                agora,
            ))

        kpi.executemany("""
            INSERT INTO ind_email
                (titulo_original, titulo_categoria, datahora, posto, matricula, status, synced_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, registros)

        kpi.execute("""
            INSERT INTO ind_sync_log (indicador, synced_at, total_records, status, mensagem)
            VALUES ('email', ?, ?, 'ok', '')
        """, (agora, len(registros)))

        kpi.commit()
        print(f"[sync_email] OK — {len(registros)} registros.")

    except Exception as exc:
        kpi.execute("""
            INSERT INTO ind_sync_log (indicador, synced_at, total_records, status, mensagem)
            VALUES ('email', ?, 0, 'erro', ?)
        """, (agora, str(exc)))
        kpi.commit()
        print(f"[sync_email] ERRO: {exc}", file=sys.stderr)
        raise

    finally:
        kpi.close()


def check() -> None:
    kpi = sqlite3.connect(KPI_DB)
    row = kpi.execute("""
        SELECT indicador, synced_at, total_records, status, mensagem
        FROM ind_sync_log
        WHERE indicador = 'email'
        ORDER BY id DESC LIMIT 1
    """).fetchone()
    kpi.close()
    if row:
        print(f"Último sync: {row[1]}  |  registros: {row[2]}  |  status: {row[3]}")
        if row[4]:
            print(f"Mensagem: {row[4]}")
    else:
        print("Nenhum sync registrado ainda.")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dias",  type=int, default=DIAS_JANELA, help="Janela de dias para sincronizar")
    parser.add_argument("--check", action="store_true", help="Mostra status do último sync")
    args = parser.parse_args()

    if args.check:
        check()
    else:
        sync(args.dias)
