#!/usr/bin/env python3
"""
migrate_custos_ti.py — Cria as tabelas ti_* no Postgres RDS e semeia os centros.

Idempotente: pode rodar quantas vezes quiser. `create_all` só cria o que falta e
`seed_centros` pula centro que já existe.

Uso:
    python migrate_custos_ti.py            # cria tabelas + centros iniciais
    python migrate_custos_ti.py --dry-run  # só mostra o que faria
    python migrate_custos_ti.py --sem-seed # cria as tabelas, sem centros

Na VM:
    cd /opt/relatorio_h_t && .venv/bin/python migrate_custos_ti.py
"""
from __future__ import annotations

import argparse
import sys

from dotenv import load_dotenv

load_dotenv()
load_dotenv("/opt/relatorio_h_t/.env")


# Colunas acrescentadas depois da criação original da tabela.
# (tabela, coluna, DDL, expressão de backfill ou None)
COLUNAS_NOVAS = [
    ("ti_centro_custo", "integracao", "VARCHAR(20)",
     # o centro de Comunicação é quem tem a integração da Meta
     "integracao = 'meta' WHERE key = 'comunicacao'"),
    ("ti_centro_custo", "forma_pagamento_id",
     "INTEGER REFERENCES ti_forma_pagamento(id) ON DELETE SET NULL", None),
    ("ti_lancamento", "valor_usd", "NUMERIC(14,4) NOT NULL DEFAULT 0",
     # Recalcula o dólar do que já existe: USD é o próprio valor; o resto
     # divide pela cotação que ficou congelada na linha.
     "valor_usd = CASE WHEN moeda = 'USD' THEN valor "
     "WHEN COALESCE(cotacao, 0) > 0 THEN ROUND(valor / cotacao, 4) ELSE 0 END"),
]


def _adiciona_colunas(db, insp) -> None:
    from sqlalchemy import text
    for tabela, coluna, ddl, backfill in COLUNAS_NOVAS:
        if tabela not in insp.get_table_names():
            continue
        existentes = {c["name"] for c in insp.get_columns(tabela)}
        if coluna in existentes:
            print(f"  coluna {tabela}.{coluna}: já existe")
            continue
        with db.pg_engine.begin() as con:
            con.execute(text(f"ALTER TABLE {tabela} ADD COLUMN {coluna} {ddl}"))
            if backfill:
                r = con.execute(text(f"UPDATE {tabela} SET {backfill}"))
                print(f"  coluna {tabela}.{coluna}: criada e {r.rowcount} linha(s) recalculada(s)")
            else:
                print(f"  coluna {tabela}.{coluna}: criada")


def main() -> int:
    ap = argparse.ArgumentParser(description="Migration do módulo Custos de TI")
    ap.add_argument("--dry-run", action="store_true",
                    help="não altera nada; só lista tabelas e centros que faltam")
    ap.add_argument("--sem-seed", action="store_true",
                    help="cria as tabelas mas não semeia os centros de custo")
    args = ap.parse_args()

    import custos_ti
    import custos_ti_db as db
    from sqlalchemy import inspect

    insp = inspect(db.pg_engine)
    existentes = set(insp.get_table_names())
    alvo = [t.name for t in db.TiBase.metadata.sorted_tables]
    faltando = [t for t in alvo if t not in existentes]

    print(f"Banco: {db.pg_engine.url.database} @ {db.pg_engine.url.host}")
    print(f"Tabelas do módulo: {', '.join(alvo)}")
    print(f"Já existem:        {', '.join(t for t in alvo if t in existentes) or '—'}")
    print(f"Serão criadas:     {', '.join(faltando) or '— nenhuma —'}")

    if args.dry_run:
        if not faltando:
            sess = db.TiSession()
            try:
                atuais = {c.key for c in custos_ti.listar_centros(sess, True)}
            finally:
                sess.close()
            novos = [c["key"] for c in custos_ti.CENTROS_SEED if c["key"] not in atuais]
            print(f"Centros a semear:  {', '.join(novos) or '— nenhum —'}")
        else:
            print("Centros a semear:  "
                  + ", ".join(c["key"] for c in custos_ti.CENTROS_SEED))
        print("\n--dry-run: nada foi gravado.")
        return 0

    db.init_ti_db()
    print("Tabelas criadas/verificadas.")

    # create_all() não adiciona coluna em tabela que já existe — colunas novas
    # entram aqui, uma a uma, de forma idempotente.
    _adiciona_colunas(db, insp)

    if args.sem_seed:
        print("--sem-seed: centros não foram semeados.")
        return 0

    sess = db.TiSession()
    try:
        criados = custos_ti.seed_centros(sess)
        print(f"Centros criados:   {', '.join(criados) or '— nenhum (já existiam) —'}")
        for c in custos_ti.listar_centros(sess, incluir_inativos=True):
            print(f"  [{c.ordem:>3}] {c.key:<14} {c.nome:<22} fonte={c.fonte:<6} {c.url}")
    finally:
        sess.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
