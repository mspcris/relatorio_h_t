#!/usr/bin/env python3
"""
export_fin_receita_pg.py

ETL incremental: vw_Fin_Receita (SQL Server, 13 postos) -> Postgres RDS AWS (relatorio_h_t.fin_receita)

Espelha export_fin_despesa_pg.py:
- Incremental: pega so idReceita > MAX(id_receita) ja em relatorio_h_t.fin_receita (por posto)
- Filtros fixos: [Valor pago] IS NOT NULL, [Data de pagamento] >= 01/01/2024
  (janela de 24m rolante; antes disso o plano de contas mudou de nomes/IDs)
- Cron: a cada 2 horas
- Meta: json_consolidado/_etl_meta_export_fin_receita_pg.json (widget ETL v2)
- Credenciais: PG_RDS_* no /opt/relatorio_h_t/.env (nao commitadas)

DDL: sql_camim_fin/fin_receita_pg_ddl.sql (aplicar manualmente uma vez antes
do primeiro run).
"""
import os
import sys
import time
import traceback
from typing import List, Tuple

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

import psycopg2
from psycopg2.extras import execute_values
from sqlalchemy import text

from export_governanca import build_conns_from_env, make_engine
from etl_meta import ETLMeta


# (coluna SQL Server, coluna Postgres)
COLUMNS: List[Tuple[str, str]] = [
    ("idReceita",                 "id_receita"),
    ("idLancamento",              "id_lancamento"),
    ("idPlano",                   "id_plano"),
    ("idConta",                   "id_conta"),
    ("idContaTipo",               "id_conta_tipo"),
    ("idCliente",                 "id_cliente"),
    ("[Valor devido]",            "valor_devido"),
    ("[Valor pago]",              "valor_pago"),
    ("[Data de vencimento]",      "data_vencimento"),
    ("[Data de pagamento]",       "data_pagamento"),
    ("[Data de cancelamento]",    "data_cancelamento"),
    ("[Data prestaçao]",          "data_prestacao"),
    ("[Descrição]",               "descricao"),
    ("Comentario",                "comentario"),
    ("Plano",                     "plano"),
    ("Tipo",                      "tipo"),
    ("Conta",                     "conta"),
    ("Classe",                    "classe"),
    ("Cliente",                   "cliente"),
    ("[Matrícula]",               "matricula"),
    ("[Responsável]",             "responsavel"),
    ("[Situação]",                "situacao"),
    ("Cobrador",                  "cobrador"),
    ("Corretor",                  "corretor"),
    ("Usuario",                   "usuario"),
    ("Forma",                     "forma"),
    ("FormaTipo",                 "forma_tipo"),
    ("FormaNumero",               "forma_numero"),
    ("[Talão]",                   "talao"),
    ("NossoNumero",               "nosso_numero"),
    ("[Endereço]",                "endereco"),
    ("Bairro",                    "bairro"),
    ("Cep",                       "cep"),
    ("Rota",                      "rota"),
    ("Parcela",                   "parcela"),
]

SQL_SELECT_COLS = ",\n  ".join(c[0] for c in COLUMNS)
PG_INSERT_COLS  = ["posto"] + [c[1] for c in COLUMNS]

# Janela de 24m. Antes disso o plano de contas mudou (nomes E IDs).
# Data em DD/MM/YYYY (SET DATEFORMAT dmy das views da CAMIM).
SQL_FROM_WHERE = """
FROM vw_Fin_Receita
WHERE [Valor pago] IS NOT NULL
  AND [Data de pagamento] >= '01/01/2024'
  AND idReceita > :wm
"""

BATCH = 5000
POSTOS_ORDER = ["N", "X", "Y", "M", "P", "D", "B", "I", "G", "R", "J", "C", "A"]


def pg_conn():
    return psycopg2.connect(
        host=os.environ["PG_RDS_HOST"],
        port=int(os.environ.get("PG_RDS_PORT", "9432")),
        dbname=os.environ.get("PG_RDS_DB", "relatorio_h_t"),
        user=os.environ["PG_RDS_USER"],
        password=os.environ["PG_RDS_PASSWORD"],
        sslmode=os.environ.get("PG_RDS_SSLMODE", "require"),
        connect_timeout=15,
    )


def get_watermark(pg, posto: str) -> int:
    with pg.cursor() as c:
        c.execute("SELECT COALESCE(MAX(id_receita), 0) FROM fin_receita WHERE posto = %s", (posto,))
        return int(c.fetchone()[0])


def sync_posto(pg, posto: str, engine) -> int:
    wm = get_watermark(pg, posto)
    sql = f"SELECT\n  {SQL_SELECT_COLS}\n{SQL_FROM_WHERE}\nORDER BY idReceita"
    inserted = 0

    placeholders = "(" + ", ".join(["%s"] * len(PG_INSERT_COLS)) + ")"
    cols_str = ", ".join(PG_INSERT_COLS)
    insert_sql = (
        f"INSERT INTO fin_receita ({cols_str}) VALUES %s "
        "ON CONFLICT (posto, id_receita) DO NOTHING"
    )

    with engine.connect().execution_options(stream_results=True) as sc:
        result = sc.execute(text(sql), {"wm": wm})
        while True:
            rows = result.fetchmany(BATCH)
            if not rows:
                break
            batch = [(posto, *row) for row in rows]
            with pg.cursor() as pc:
                execute_values(pc, insert_sql, batch, template=placeholders, page_size=BATCH)
            pg.commit()
            inserted += len(batch)
            last_id = batch[-1][1]
            print(f"[{posto}] +{len(batch):>6}  acum={inserted:>8}  last_id_receita={last_id}", flush=True)

    return inserted


def main() -> int:
    meta = ETLMeta("export_fin_receita_pg", "json_consolidado")
    conns = build_conns_from_env()
    rc = 0

    pg = pg_conn()
    try:
        for p in POSTOS_ORDER:
            if p not in conns:
                print(f"[{p}] sem conn no .env — pulando", flush=True)
                continue
            t0 = time.time()
            try:
                engine = make_engine(conns[p])
                n = sync_posto(pg, p, engine)
                dt = time.time() - t0
                print(f"[{p}] OK  inseridos={n}  elapsed={dt:.1f}s", flush=True)
                meta.ok(p)
            except Exception as e:
                rc = 2
                traceback.print_exc()
                meta.error(p, str(e))
    finally:
        pg.close()
        meta.save()

    return rc


if __name__ == "__main__":
    sys.exit(main())
