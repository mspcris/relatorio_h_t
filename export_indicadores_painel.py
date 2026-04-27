#!/usr/bin/env python3
"""
export_indicadores_painel.py

Pré-agrega os 4 indicadores do painel monitorarrobos.html em um único JSON
estático, eliminando a agregação ao vivo no Flask (que estava estourando o
timeout do gunicorn por causa da fila MAX/COUNT em ind_email — 73s para
268k linhas — e arrastando todos os outros endpoints).

Saída:
    /opt/relatorio_h_t/json_consolidado/indicadores_painel.json
    /opt/relatorio_h_t/json_consolidado/_etl_meta_indicadores_painel.json

Cron: */5 * * * *  (definido em cron/relatorio_ht)

Estrutura do JSON (`ultimo_envio` é string vinda do banco; o endpoint Flask
calcula `dias` em runtime para refletir o momento da request, não o do ETL):

    {
      "generated_at": "2026-04-26T19:35:00",
      "indicadores": {
        "push":  {"A": {"ultimo_envio": "...", "total_dia": 12}, ...},
        "email": {
          "data": {"A|Boleto": {"posto":"A","categoria":"Boleto",
                                 "ultimo_envio":"...","total":5}, ...},
          "sync": {"synced_at":"...","total_records":N,
                    "status":"ok","mensagem":""}
        },
        "tef":   {"data": {"A": {...}, ...}, "sync": {...}},
        "wpp":   [{"id":1,"nome":"Cobrança",
                    "postos":{"A":{"ultimo_envio":"..."}, ...}}, ...]
      },
      "erros": {}
    }
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
import traceback
from datetime import datetime

OUT_DIR   = "/opt/relatorio_h_t/json_consolidado"
OUT_FILE  = os.path.join(OUT_DIR, "indicadores_painel.json")
META_FILE = os.path.join(OUT_DIR, "_etl_meta_indicadores_painel.json")

KPI_DB  = os.environ.get("KPI_DB_PATH",  "/opt/relatorio_h_t/camim_kpi.db")
PUSH_DB = os.environ.get("PUSH_LOG_DB",  "/opt/push_clientes/push_log.db")
WPP_DB  = os.environ.get("WAPP_CTRL_DB", "/opt/camim-auth/whatsapp_cobranca.db")


def _connect_ro(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=30)
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def _coletar_email() -> dict:
    conn = _connect_ro(KPI_DB)
    rows = conn.execute("""
        SELECT posto,
               titulo_categoria,
               MAX(datahora)                                                    AS ultimo_envio,
               COUNT(CASE WHEN DATE(datahora)=DATE('now','localtime') THEN 1 END) AS total
        FROM ind_email
        WHERE titulo_categoria = 'Boleto'
        GROUP BY posto, titulo_categoria
    """).fetchall()
    sync = conn.execute("""
        SELECT synced_at, total_records, status, mensagem
        FROM   ind_sync_log
        WHERE  indicador='email'
        ORDER BY id DESC LIMIT 1
    """).fetchone()
    conn.close()

    data = {}
    for posto, _cat, ultimo, total in rows:
        posto = (posto or "").strip().upper()
        if not posto:
            continue
        data[f"{posto}|Boleto"] = {
            "posto":        posto,
            "categoria":    "Boleto",
            "ultimo_envio": ultimo,
            "total":        total,
        }
    return {
        "data": data,
        "sync": {
            "synced_at":     sync[0] if sync else None,
            "total_records": sync[1] if sync else 0,
            "status":        sync[2] if sync else None,
            "mensagem":      sync[3] if sync else None,
        },
    }


def _coletar_tef() -> dict:
    conn = _connect_ro(KPI_DB)
    rows = conn.execute("""
        SELECT posto, MAX(datahora) AS ultimo, COUNT(*) AS total
        FROM   ind_tef
        GROUP BY posto
    """).fetchall()
    sync = conn.execute("""
        SELECT synced_at, total_records, status, mensagem
        FROM   ind_sync_log
        WHERE  indicador='tef'
        ORDER BY id DESC LIMIT 1
    """).fetchone()
    conn.close()

    data = {}
    for posto, ultimo, total in rows:
        posto = (posto or "").strip().upper()
        if not posto:
            continue
        data[posto] = {
            "posto":      posto,
            "ultimo_tef": ultimo,
            "total":      total,
        }
    return {
        "data": data,
        "sync": {
            "synced_at":     sync[0] if sync else None,
            "total_records": sync[1] if sync else 0,
            "status":        sync[2] if sync else None,
            "mensagem":      sync[3] if sync else None,
        },
    }


def _coletar_push() -> dict:
    conn = _connect_ro(PUSH_DB)
    tables = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()]
    tabela = next(
        (t for t in tables if "push" in t.lower() or "log" in t.lower()),
        tables[0] if tables else None,
    )
    if not tabela:
        conn.close()
        return {}

    cols = [r[1] for r in conn.execute(f"PRAGMA table_info({tabela})").fetchall()]
    col_data  = next((c for c in cols if any(k in c.lower()
                       for k in ("data", "hora", "created", "time", "sent"))), None)
    col_posto = next((c for c in cols if "posto" in c.lower()), None)
    col_modo  = next((c for c in cols if "modo"  in c.lower()), None)
    if not col_data or not col_posto:
        conn.close()
        return {}

    where = f"WHERE {col_modo}='producao'" if col_modo else ""
    rows = conn.execute(f"""
        SELECT {col_posto} AS posto,
               MAX({col_data}) AS ultimo,
               COUNT(*) AS total
        FROM   {tabela}
        {where}
        GROUP BY {col_posto}
    """).fetchall()
    conn.close()

    data = {}
    for posto_raw, ultimo, total in rows:
        posto = str(posto_raw).strip().upper() if posto_raw else None
        if not posto:
            continue
        data[posto] = {"ultimo_envio": ultimo, "total_dia": total}
    return data


def _coletar_wpp() -> list:
    conn = _connect_ro(WPP_DB)
    conn.row_factory = sqlite3.Row
    campanhas = conn.execute(
        "SELECT id, nome, postos FROM campanhas WHERE ativa=1"
    ).fetchall()

    result = []
    for c in campanhas:
        try:
            postos_lista = json.loads(c["postos"] or "[]")
        except Exception:
            postos_lista = []
        postos_dados = {}
        for posto in postos_lista:
            row = conn.execute(
                "SELECT MAX(enviado_em) AS ultimo FROM envios "
                "WHERE campanha_id=? AND posto=? AND status='accepted'",
                (c["id"], posto),
            ).fetchone()
            postos_dados[posto] = {
                "ultimo_envio": row["ultimo"] if row and row["ultimo"] else None
            }
        result.append({"id": c["id"], "nome": c["nome"], "postos": postos_dados})
    conn.close()
    return result


def _atomic_write(path: str, payload) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def main() -> int:
    started_at = datetime.now()
    os.makedirs(OUT_DIR, exist_ok=True)

    indicadores = {}
    erros = {}
    for nome, fn in (
        ("push",  _coletar_push),
        ("email", _coletar_email),
        ("tef",   _coletar_tef),
        ("wpp",   _coletar_wpp),
    ):
        try:
            indicadores[nome] = fn()
        except Exception as exc:
            erros[nome] = f"{type(exc).__name__}: {exc}"
            traceback.print_exc(file=sys.stderr)

    _atomic_write(OUT_FILE, {
        "generated_at": started_at.isoformat(timespec="seconds"),
        "indicadores":  indicadores,
        "erros":        erros,
    })

    finished_at = datetime.now()
    _atomic_write(META_FILE, {
        "script":            "export_indicadores_painel",
        "started_at":        started_at.isoformat(timespec="seconds"),
        "finished_at":       finished_at.isoformat(timespec="seconds"),
        "duracao_segundos":  round((finished_at - started_at).total_seconds(), 2),
        "indicadores_ok":    [k for k in indicadores if k not in erros],
        "erros":             erros,
    })

    print(f"[OK] {OUT_FILE} "
          f"({(finished_at - started_at).total_seconds():.2f}s, "
          f"ok={[k for k in indicadores if k not in erros]}, "
          f"erros={list(erros)})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
