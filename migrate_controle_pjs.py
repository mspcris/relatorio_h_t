#!/usr/bin/env python3
"""
migrate_controle_pjs.py — Cria as tabelas pj_* no Postgres RDS.

Idempotente: `create_all` só cria o que falta. Sem seed — empresa é cadastrada
pela própria página.

Uso:
    python migrate_controle_pjs.py            # cria as tabelas que faltam
    python migrate_controle_pjs.py --dry-run  # só mostra o que faria

Na VM:
    cd /opt/relatorio_h_t && .venv/bin/python migrate_controle_pjs.py
"""
from __future__ import annotations

import argparse
import sys

from dotenv import load_dotenv

load_dotenv()
load_dotenv("/opt/relatorio_h_t/.env")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="não grava nada")
    args = ap.parse_args()

    import controle_pjs_db as pjdb

    if pjdb.pj_engine is None:
        print("ERRO: PG_RDS_* não configurado no ambiente/.env")
        return 2

    from sqlalchemy import inspect
    insp = inspect(pjdb.pj_engine)
    existentes = set(insp.get_table_names())
    alvo = [t.name for t in pjdb.Base.metadata.sorted_tables]
    faltam = [t for t in alvo if t not in existentes]

    print(f"Banco: {pjdb.pj_engine.url.host}/{pjdb.pj_engine.url.database}")
    print(f"Tabelas do módulo: {', '.join(alvo)}")
    if not faltam:
        print("Nada a fazer — todas já existem.")
        return 0
    print(f"A criar: {', '.join(faltam)}")
    if args.dry_run:
        print("(dry-run: nada foi gravado)")
        return 0
    pjdb.init_pj_schema()
    print("OK — tabelas criadas.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
