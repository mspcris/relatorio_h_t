# ctrlq_export_relatorio.py
# Objetivo:
#   - Ler SQL em: sql_ctrlq_relatorio/sql_ctrlq_relatorio.sql
#   - Executar em LOOP por posto (DB_HOST_A/DB_BASE_A ...)
#   - Normalizar dados (Timestamp, NaT, Decimal, numpy types, etc.)
#   - Salvar:
#       1) 1 JSON por posto (NOME FIXO): json_ctrlq_relatorio/CTRLQ_RELATORIO_<POSTO>.json
#       2) 1 JSON consolidado (NOME FIXO): json_ctrlq_relatorio/CTRLQ_RELATORIO_CONSOLIDADO.json
#   - Antes de salvar: limpar os .json antigos da pasta json_ctrlq_relatorio
#
# Dependências: pandas, sqlalchemy, pyodbc, python-dotenv

import os
import json
import decimal
from datetime import datetime, date, timezone
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv


# ---------------------------------------------
# Configurações de pastas/arquivos
# ---------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

SQL_FOLDER = "sql_ctrlq_relatorio"
SQL_FILE = "sql_ctrlq_relatorio.sql"
SQL_PATH = os.path.join(BASE_DIR, SQL_FOLDER, SQL_FILE)

JSON_DIR = os.path.join(BASE_DIR, "json_ctrlq_relatorio")

ODBC_DRIVER = os.getenv("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")
POSTOS_FALLBACK = list("ANXYBRPCDGIMJ")  # fallback caso não exista nada no .env


# ---------------------------------------------
# Utilitários
# ---------------------------------------------
def ensure_dir(d: str):
    os.makedirs(d, exist_ok=True)

def env(key: str, default=""):
    v = os.getenv(key, default)
    return v.strip() if isinstance(v, str) else v

def atomic_write_json(path_out: str, payload):
    tmp = path_out + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path_out)

def cleanup_json_dir(json_dir: str):
    """Remove apenas *.json dentro do diretório alvo."""
    ensure_dir(json_dir)
    removed = 0
    for name in os.listdir(json_dir):
        if name.lower().endswith(".json"):
            try:
                os.remove(os.path.join(json_dir, name))
                removed += 1
            except Exception:
                pass
    print(f"[CLEANUP] Removidos {removed} arquivos .json em {json_dir}")


# ---------------------------------------------
# Carregar SQL
# ---------------------------------------------
def load_sql():
    if not os.path.isfile(SQL_PATH):
        raise FileNotFoundError(f"SQL não encontrado em: {SQL_PATH}")

    txt = open(SQL_PATH, "r", encoding="utf-8", errors="ignore").read().strip()
    if not txt:
        raise ValueError(f"O arquivo SQL está vazio: {SQL_PATH}")

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

def build_conns_from_env(postos=None):
    """
    Monta dict { 'A': '<odbc_str>', 'B': '<odbc_str>', ... }
    A partir de DB_HOST_<POSTO> / DB_BASE_<POSTO> / DB_USER_<POSTO> / DB_PASSWORD_<POSTO> / DB_PORT_<POSTO>
    """
    postos = postos or POSTOS_FALLBACK
    conns = {}

    for p in postos:
        host = env(f"DB_HOST_{p}")
        base = env(f"DB_BASE_{p}")
        if not host or not base:
            continue
        user = env(f"DB_USER_{p}")
        pwd = env(f"DB_PASSWORD_{p}")
        port = env(f"DB_PORT_{p}", "1433")
        conns[p] = build_conn_str(host, base, user, pwd, port)

    # Compat: se existir DB_HOST/DB_BASE (sem sufixo), trata como posto "SINGLE"
    host0 = env("DB_HOST")
    base0 = env("DB_BASE")
    if host0 and base0 and not conns:
        user0 = env("DB_USER")
        pwd0 = env("DB_PASSWORD")
        port0 = env("DB_PORT", "1433")
        conns["SINGLE"] = build_conn_str(host0, base0, user0, pwd0, port0)

    return conns


# ---------------------------------------------
# SELECT
# ---------------------------------------------
def run_select(engine, sql):
    with engine.connect() as con:
        df = pd.read_sql_query(text(sql), con)
    return df


# ---------------------------------------------
# Normalização (JSON-safe)
# ---------------------------------------------
def normalize_value(v):
    if isinstance(v, (pd.Timestamp, datetime, date)):
        return v.isoformat()

    if isinstance(v, decimal.Decimal):
        return format(v, "f")

    if pd.isna(v):
        return None

    try:
        import numpy as np
        if isinstance(v, np.generic):
            return v.item()
    except Exception:
        pass

    if isinstance(v, (bytes, bytearray)):
        return v.decode("utf-8", errors="ignore")

    return v

def normalize_record(row: dict):
    return {k: normalize_value(v) for k, v in row.items()}

def drop_ignored_fields(rec: dict):
    rec.pop("temporario", None)
    return rec


# ---------------------------------------------
# Nome do arquivo (FIXO)
# ---------------------------------------------
def filename_posto(posto: str):
    return f"CTRLQ_RELATORIO_{posto}.json"

def filename_consolidado():
    return "CTRLQ_RELATORIO_CONSOLIDADO.json"


# ---------------------------------------------
# Consolidação (mes -> posto -> linhas)
# ---------------------------------------------
def month_key_from_record(rec: dict):
    v = rec.get("DataFechamentoMes") or rec.get("datafechamentomes")
    if isinstance(v, str) and len(v) >= 7:
        return v[:7]
    v = rec.get("DataHoraInclusao") or rec.get("datahorainclusao")
    if isinstance(v, str) and len(v) >= 7:
        return v[:7]
    return "UNKNOWN"

def build_consolidated_json(all_rows_by_posto: dict):
    agora_br = datetime.now().strftime("%d/%m/%Y, %H:%M")
    agora_iso = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")

    dados = {}
    meses_set = set()

    for posto, rows in all_rows_by_posto.items():
        for r in rows:
            mes = month_key_from_record(r)
            meses_set.add(mes)
            dados.setdefault(mes, {}).setdefault(posto, {"linhas": []})["linhas"].append(r)

    meses = sorted([m for m in meses_set if m != "UNKNOWN"]) + (["UNKNOWN"] if "UNKNOWN" in meses_set else [])
    postos = sorted(all_rows_by_posto.keys())

    return {
        "meta": {
            "dados_gerados_em": agora_br,
            "export_timestamp": agora_iso,
            "origem": "ctrlq_relatorio",
        },
        "meses": meses,
        "postos": postos,
        "dados": dados,
    }


# ---------------------------------------------
# Main
# ---------------------------------------------
def main():
    print("=== CTRLQ Relatório Exporter (por posto) ===")
    ensure_dir(JSON_DIR)

    # .env no mesmo diretório do script
    load_dotenv(os.path.join(BASE_DIR, ".env"))

    # SQL
    try:
        sql = load_sql()
    except Exception as e:
        print(f"ERRO carregando SQL: {e}")
        return

    # Conexões (loop postos)
    conns = build_conns_from_env()
    if not conns:
        print("ERRO: nenhuma conexão encontrada no .env. Configure DB_HOST_A/DB_BASE_A etc. (ou DB_HOST/DB_BASE sem sufixo).")
        return

    print(f"Postos detectados: {list(conns.keys())}")

    # 1) limpa outputs antigos (antes de gerar novos)
    cleanup_json_dir(JSON_DIR)

    all_rows_by_posto = {}

    # Executar por posto
    for posto, odbc in conns.items():
        print(f"\n[{posto}] Criando engine...")
        try:
            engine = make_engine(odbc)
        except Exception as e:
            print(f"[{posto}] ERRO criando engine: {e}")
            continue

        print(f"[{posto}] Executando SELECT...")
        try:
            df = run_select(engine, sql)
        except Exception as e:
            print(f"[{posto}] ERRO ao executar SQL: {e}")
            continue

        rows = []
        for r in df.to_dict(orient="records"):
            rec = normalize_record(r)
            rec = drop_ignored_fields(rec)
            rec["posto"] = posto
            rows.append(rec)

        all_rows_by_posto[posto] = rows

        # Salvar JSON por posto (NOME FIXO)
        path_out = os.path.join(JSON_DIR, filename_posto(posto))
        try:
            atomic_write_json(path_out, rows)
            print(f"[{posto}] OK -> {path_out}  ({len(rows)} registros)")
        except Exception as e:
            print(f"[{posto}] ERRO salvando JSON: {e}")

    if not all_rows_by_posto:
        print("\nERRO: nenhuma exportação bem-sucedida.")
        return

    # Salvar consolidado (NOME FIXO)
    consolidado = build_consolidated_json(all_rows_by_posto)
    path_cons = os.path.join(JSON_DIR, filename_consolidado())
    try:
        atomic_write_json(path_cons, consolidado)
        total = sum(len(v) for v in all_rows_by_posto.values())
        print(f"\n[CONSOLIDADO] OK -> {path_cons}  (total registros={total})")
    except Exception as e:
        print(f"\n[CONSOLIDADO] ERRO salvando JSON: {e}")

    print("\n=== Concluído ===")


if __name__ == "__main__":
    main()
