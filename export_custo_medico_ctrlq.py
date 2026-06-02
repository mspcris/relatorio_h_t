#!/usr/bin/env python3
# export_custo_medico_ctrlq.py
# Custo médico por dia da semana, de TODOS os médicos de TODOS os postos.
# Alimenta o botão "Custo Médico no Ctrl-Q" da página KPI Custo Médico, que
# busca por nome / CRM / CPF (cliente-side) e mostra todos os postos.
#
# Atualização noturna (cron 23:30). Read-only nos SQL Servers dos postos.
# Saída: json_custo_medico_ctrlq/CONSOLIDADO.json  (lista achatada + meta)

import os
import json
import decimal
import math
from datetime import datetime, timezone
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SQL_PATH = os.path.join(BASE_DIR, "sql_custo_medico_ctrlq", "sql.sql")
JSON_DIR = os.path.join(BASE_DIR, "json_custo_medico_ctrlq")
OUT_PATH = os.path.join(JSON_DIR, "CONSOLIDADO.json")

ODBC_DRIVER = os.getenv("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")
POSTOS      = list("ANXYBRPCDGIMJ")

# Colunas de custo na ordem Seg→Dom (chaves no JSON de saída).
DIAS = [
    ("ValorCustoSegunda", "segunda"),
    ("ValorCustoTerca",   "terca"),
    ("ValorCustoQuarta",  "quarta"),
    ("ValorCustoQuinta",  "quinta"),
    ("ValorCustoSexta",   "sexta"),
    ("ValorCustoSabado",  "sabado"),
    ("ValorCustoDomingo", "domingo"),
]


def _env(key, default=""):
    v = os.getenv(key, default)
    return v.strip() if isinstance(v, str) else v


def _num(v):
    """Decimal/NaN/None → float|None."""
    if v is None:
        return None
    if isinstance(v, decimal.Decimal):
        return float(v)
    try:
        if isinstance(v, float) and math.isnan(v):
            return None
    except Exception:
        pass
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _str(v):
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _conn_str(host, base, user, pwd, port="1433"):
    return (
        f"DRIVER={{{ODBC_DRIVER}}};"
        f"SERVER=tcp:{host},{port};DATABASE={base};"
        f"Encrypt=yes;TrustServerCertificate=yes;"
        + (f"UID={user};PWD={pwd}" if user else "Trusted_Connection=yes")
    )


def _build_conns():
    conns = {}
    for p in POSTOS:
        host = _env(f"DB_HOST_{p}"); base = _env(f"DB_BASE_{p}")
        if not host or not base:
            continue
        conns[p] = _conn_str(host, base, _env(f"DB_USER_{p}"),
                             _env(f"DB_PASSWORD_{p}"), _env(f"DB_PORT_{p}", "1433"))
    return conns


def _atomic_write(path, payload):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.flush(); os.fsync(f.fileno())
    os.replace(tmp, path)


def main():
    print("=== Custo Médico Ctrl-Q Exporter ===")
    load_dotenv(os.path.join(BASE_DIR, ".env"))

    sql = open(SQL_PATH, encoding="utf-8").read().strip()
    conns = _build_conns()
    if not conns:
        print("ERRO: nenhuma conexão de posto no .env (DB_HOST_<P>)."); return
    print(f"Postos: {list(conns.keys())}")

    registros = []
    postos_ok = []
    for posto, odbc in conns.items():
        print(f"[{posto}] executando...", end=" ", flush=True)
        try:
            engine = create_engine(
                f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc)}",
                future=True, pool_pre_ping=True,
            )
            with engine.connect() as con:
                df = pd.read_sql_query(text(sql), con)
            for r in df.to_dict(orient="records"):
                item = {
                    "posto":         posto,
                    "medico":        _str(r.get("medico")),
                    "conselho":      _str(r.get("conselho")),
                    "crm":           _str(r.get("crm")),
                    "cpf":           _str(r.get("cpf")),
                    "especialidade": _str(r.get("especialidade")),
                }
                for col, key in DIAS:
                    item[key] = _num(r.get(col))
                registros.append(item)
            postos_ok.append(posto)
            print(f"OK ({len(df)} linhas)")
        except Exception as e:
            print(f"ERRO: {type(e).__name__}: {str(e)[:160]}")

    if not postos_ok:
        print("Nenhum posto exportado — JSON não regenerado."); return

    agora = datetime.now(timezone.utc).astimezone()
    payload = {
        "meta": {
            "gerado_em":    agora.isoformat(timespec="seconds"),
            "gerado_em_br": agora.strftime("%d/%m/%Y, %H:%M"),
            "postos":       sorted(postos_ok),
            "total":        len(registros),
            "dias":         [k for _, k in DIAS],
        },
        "registros": registros,
    }
    _atomic_write(OUT_PATH, payload)
    print(f"[CONSOLIDADO] {OUT_PATH}  (postos={len(postos_ok)}, registros={len(registros)})")
    print("=== Concluído ===")


if __name__ == "__main__":
    main()
