# ctrlq_desbloqueio.py
# Exporta registros de desbloqueio de agenda (cad_especialidade com DataFimExibicao)
# por posto para json_ctrlq_desbloqueio/.
#
# Saídas:
#   json_ctrlq_desbloqueio/CTRLQ_DESBLOQUEIO_<POSTO>.json  — por posto
#   json_ctrlq_desbloqueio/CTRLQ_DESBLOQUEIO_CONSOLIDADO.json — todos os postos

import os
import json
import decimal
from datetime import datetime, date, time as time_type, timezone
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
SQL_PATH    = os.path.join(BASE_DIR, "sql_ctrlq_desbloqueio", "sql_ctrlq_desbloqueio.sql")
SQL_AUD_PATH = os.path.join(BASE_DIR, "sql_ctrlq_desbloqueio", "sql_ctrlq_desbloqueio_aud.sql")
JSON_DIR    = os.path.join(BASE_DIR, "json_ctrlq_desbloqueio")

ODBC_DRIVER     = os.getenv("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")
POSTOS_FALLBACK = list("ANXYBRPCDGIMJ")


# ── utilidades ────────────────────────────────────────────────────────────────

def ensure_dir(d):
    os.makedirs(d, exist_ok=True)

def env(key, default=""):
    v = os.getenv(key, default)
    return v.strip() if isinstance(v, str) else v

def atomic_write(path, payload):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.flush(); os.fsync(f.fileno())
    os.replace(tmp, path)

def cleanup_json_dir(d):
    ensure_dir(d)
    n = 0
    for name in os.listdir(d):
        if name.lower().endswith(".json"):
            try: os.remove(os.path.join(d, name)); n += 1
            except Exception: pass
    print(f"[CLEANUP] removidos {n} .json em {d}")

def normalize(v):
    if isinstance(v, time_type):
        return v.strftime('%H:%M:%S')
    if isinstance(v, (pd.Timestamp, datetime, date)):
        return v.isoformat()
    if isinstance(v, decimal.Decimal):
        return float(v)
    if pd.isna(v) if not isinstance(v, (list, dict)) else False:
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

def normalize_row(row):
    return {k: normalize(v) for k, v in row.items()}


# ── conexão ───────────────────────────────────────────────────────────────────

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
        future=True, pool_pre_ping=True
    )

def build_conns_from_env(postos=None):
    postos = postos or POSTOS_FALLBACK
    conns = {}
    for p in postos:
        host = env(f"DB_HOST_{p}"); base = env(f"DB_BASE_{p}")
        if not host or not base: continue
        conns[p] = build_conn_str(host, base, env(f"DB_USER_{p}"),
                                  env(f"DB_PASSWORD_{p}"), env(f"DB_PORT_{p}", "1433"))
    if not conns:
        host0 = env("DB_HOST"); base0 = env("DB_BASE")
        if host0 and base0:
            conns["SINGLE"] = build_conn_str(host0, base0, env("DB_USER"),
                                             env("DB_PASSWORD"), env("DB_PORT", "1433"))
    return conns


# ── auditoria (vw_Sis_Historico) ─────────────────────────────────────────────

def fetch_audit(engine, aud_sql):
    """Retorna dict {idEspecialidade(int): [lista de registros]} ou {} se indisponível."""
    try:
        with engine.connect() as con:
            df = pd.read_sql_query(text(aud_sql), con)
        result = {}
        for r in df.to_dict(orient="records"):
            ide = r.get("idEspecialidade")
            if ide is not None:
                entry = {
                    "aud_idHistorico": normalize(r.get("aud_idHistorico")),
                    "aud_data":        normalize(r.get("aud_data")),
                    "aud_usuario":     normalize(r.get("aud_usuario")),
                    "aud_detalhe":     normalize(r.get("aud_detalhe")),
                    "aud_comando":     normalize(r.get("aud_comando")),
                    "aud_descricao":   normalize(r.get("aud_descricao")),
                    "aud_computador":  normalize(r.get("aud_computador")),
                    "aud_fallback":    bool(r.get("aud_fallback", 0)),
                }
                result.setdefault(int(ide), []).append(entry)
        return result
    except Exception as e:
        print(f"(auditoria indisponível: {type(e).__name__})", end=" ")
        return {}

def merge_audit(rows, audit_map):
    empty_scalar = {"aud_idHistorico": None, "aud_data": None,
                    "aud_usuario": None, "aud_detalhe": None}
    for r in rows:
        ide = r.get("idEspecialidade")
        aud_list = audit_map.get(int(ide)) if ide is not None else None
        r["aud_historico"] = aud_list if aud_list else []
        # Campos escalares: entrada que alterou DataFimExibicao, ou a mais recente
        if aud_list:
            principal = next(
                (e for e in reversed(aud_list)
                 if e.get("aud_detalhe") and "DataFimExibicao" in str(e["aud_detalhe"])),
                aud_list[-1]
            )
            r["aud_idHistorico"] = principal["aud_idHistorico"]
            r["aud_data"]        = principal["aud_data"]
            r["aud_usuario"]     = principal["aud_usuario"]
            r["aud_detalhe"]     = principal["aud_detalhe"]
        else:
            r.update(empty_scalar)
    return rows


# ── exportação ────────────────────────────────────────────────────────────────

def main():
    print("=== CTRLQ Desbloqueio Exporter ===")
    load_dotenv(os.path.join(BASE_DIR, ".env"))
    ensure_dir(JSON_DIR)

    if not os.path.isfile(SQL_PATH):
        print(f"ERRO: SQL não encontrado em {SQL_PATH}"); return
    sql = open(SQL_PATH, encoding="utf-8").read().strip()
    if not sql:
        print("ERRO: SQL vazio"); return

    aud_sql = ""
    if os.path.isfile(SQL_AUD_PATH):
        aud_sql = open(SQL_AUD_PATH, encoding="utf-8").read().strip()

    conns = build_conns_from_env()
    if not conns:
        print("ERRO: nenhuma conexão no .env"); return

    print(f"Postos: {list(conns.keys())}")
    cleanup_json_dir(JSON_DIR)

    por_posto = {}

    for posto, odbc in conns.items():
        print(f"[{posto}] executando...", end=" ")
        try:
            engine = make_engine(odbc)
            with engine.connect() as con:
                df = pd.read_sql_query(text(sql), con)
            rows = [normalize_row(r) for r in df.to_dict(orient="records")]
            if aud_sql:
                audit_map = fetch_audit(engine, aud_sql)
                rows = merge_audit(rows, audit_map)
            por_posto[posto] = rows
            out = os.path.join(JSON_DIR, f"CTRLQ_DESBLOQUEIO_{posto}.json")
            atomic_write(out, rows)
            print(f"OK ({len(rows)} registros)")
        except Exception as e:
            print(f"ERRO: {e}")

    if not por_posto:
        print("Nenhum posto exportado."); return

    agora = datetime.now(timezone.utc).astimezone()
    consolidado = {
        "meta": {
            "gerado_em":        agora.isoformat(timespec="seconds"),
            "gerado_em_br":     agora.strftime("%d/%m/%Y, %H:%M"),
            "postos":           sorted(por_posto.keys()),
        },
        "postos": por_posto,
    }
    cons_path = os.path.join(JSON_DIR, "CTRLQ_DESBLOQUEIO_CONSOLIDADO.json")
    atomic_write(cons_path, consolidado)
    total = sum(len(v) for v in por_posto.values())
    print(f"[CONSOLIDADO] {cons_path}  (total={total})")
    print("=== Concluído ===")


if __name__ == "__main__":
    main()
