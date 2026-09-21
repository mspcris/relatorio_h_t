# ctrlq_export_relatorio.py
# Objetivo:
#   - Ler SQL em: sql_ctrlq_relatorio/sql_ctrlq_relatorio.sql
#   - Executar em LOOP por posto (DB_HOST_A/DB_BASE_A ...)
#   - Normalizar dados (Timestamp, NaT, Decimal, numpy types, etc.)
#   - Salvar:
#       1) 1 JSON por posto (NOME FIXO): json_ctrlq_relatorio/CTRLQ_RELATORIO_<POSTO>.json
#       2) 1 JSON consolidado (NOME FIXO): json_ctrlq_relatorio/CTRLQ_RELATORIO_CONSOLIDADO.json
#   - Posto que falha MANTÉM o JSON da última leitura boa e sai marcado em
#     meta.postos_desatualizados — falha de posto não pode parecer posto sem médico.
#     (Até 2026-09-21 a pasta era apagada no início de cada rodada: posto com link
#     fora sumia do consolidado até a rodada seguinte.)
#
# Dependências: pandas, sqlalchemy, pyodbc, python-dotenv

import os
import json
import time
import decimal
from datetime import datetime, date, timezone
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text
from etl_meta import ETLMeta
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

def cleanup_json_dir(json_dir: str, manter: set):
    """Remove os *.json que NÃO são desta rodada (posto que saiu do .env, nome antigo).
    Roda no FIM: apagar antes deixava a pasta sem consolidado durante a rodada inteira
    e sem o posto que falhasse."""
    ensure_dir(json_dir)
    removed = 0
    for name in os.listdir(json_dir):
        if name.lower().endswith(".json") and name not in manter and not name.startswith("_etl_meta"):
            try:
                os.remove(os.path.join(json_dir, name))
                removed += 1
            except Exception:
                pass
    print(f"[CLEANUP] Removidos {removed} arquivos .json órfãos em {json_dir}")

def carregar_anterior(path_json: str):
    """Linhas da última leitura boa do posto + quando foi. None se não houver."""
    try:
        with open(path_json, "r", encoding="utf-8") as f:
            rows = json.load(f)
        if not isinstance(rows, list):
            return None
        quando = datetime.fromtimestamp(os.path.getmtime(path_json)).strftime("%d/%m/%Y, %H:%M")
        return rows, quando
    except Exception:
        return None


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
TENTATIVAS = 3
ESPERA_S = (3, 8)
# Queda de link, não erro de SQL: vale tentar de novo. Erro de sintaxe/coluna não entra.
_MARCAS_CONEXAO = ("connection is closed", "08s01", "08001", "hyt00", "hyt01", "tcp provider",
                   "login timeout", "communication link", "connection reset", "timeout expired")

def _erro_de_conexao(e: BaseException) -> bool:
    while e is not None:
        if any(m in str(e).lower() for m in _MARCAS_CONEXAO):
            return True
        e = e.__cause__ or e.__context__
    return False

def run_select(engine, sql):
    """SELECT com nova tentativa. O link de alguns postos cai no meio da consulta
    ("This Connection is closed", 08S01, login timeout) e volta em segundos. É só
    leitura, repetir é seguro; o dispose() descarta a conexão morta do pool."""
    for n in range(1, TENTATIVAS + 1):
        try:
            with engine.connect() as con:
                return pd.read_sql_query(text(sql), con)
        except Exception as e:
            if n == TENTATIVAS or not _erro_de_conexao(e):
                raise
            print(f"    tentativa {n}/{TENTATIVAS} falhou ({str(e)[:140]}); nova tentativa em {ESPERA_S[n - 1]}s")
            engine.dispose()
            time.sleep(ESPERA_S[n - 1])


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

def build_consolidated_json(all_rows_by_posto: dict, desatualizados: dict = None):
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
            # posto -> quando foi a última leitura boa (o dado dele aqui é dessa hora)
            "postos_desatualizados": desatualizados or {},
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

    meta = ETLMeta('ctrlq_export_relatorio', 'json_ctrlq_relatorio')
    all_rows_by_posto = {}
    desatualizados = {}

    def manter_anterior(posto):
        ant = carregar_anterior(os.path.join(JSON_DIR, filename_posto(posto)))
        if ant:
            all_rows_by_posto[posto], desatualizados[posto] = ant
            print(f"[{posto}] mantida a leitura de {ant[1]} ({len(ant[0])} registros)")
        else:
            print(f"[{posto}] sem leitura anterior para manter — posto fica fora do consolidado")

    # Executar por posto
    for posto, odbc in conns.items():
        print(f"\n[{posto}] Criando engine...")
        try:
            engine = make_engine(odbc)
        except Exception as e:
            print(f"[{posto}] ERRO criando engine: {e}")
            meta.error(posto, str(e))
            manter_anterior(posto)
            continue

        print(f"[{posto}] Executando SELECT...")
        try:
            df = run_select(engine, sql)
        except Exception as e:
            print(f"[{posto}] ERRO ao executar SQL: {e}")
            meta.error(posto, str(e))
            manter_anterior(posto)
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
            meta.ok(posto)
        except Exception as e:
            print(f"[{posto}] ERRO salvando JSON: {e}")
            meta.error(posto, str(e))

    if not all_rows_by_posto:
        print("\nERRO: nenhuma exportação bem-sucedida.")
        return

    # Salvar consolidado (NOME FIXO)
    consolidado = build_consolidated_json(all_rows_by_posto, desatualizados)
    path_cons = os.path.join(JSON_DIR, filename_consolidado())
    try:
        atomic_write_json(path_cons, consolidado)
        total = sum(len(v) for v in all_rows_by_posto.values())
        print(f"\n[CONSOLIDADO] OK -> {path_cons}  (total registros={total})")
    except Exception as e:
        print(f"\n[CONSOLIDADO] ERRO salvando JSON: {e}")

    cleanup_json_dir(JSON_DIR, {filename_consolidado()} | {filename_posto(p) for p in conns})

    meta.save()
    print("\n=== Concluído ===")


if __name__ == "__main__":
    main()
