"""
migrate_medico_custo_hist.py — cria as tabelas mc_* do histórico de agendas.

Idempotente: `create_all` só cria o que falta. Rodar de novo não apaga nada.

    # local, sem gravar nada
    python migrate_medico_custo_hist.py --dry-run

    # na VM
    cd /opt/relatorio_h_t && .venv/bin/python migrate_medico_custo_hist.py
"""
from __future__ import annotations

import os
import sys

from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def main() -> int:
    load_dotenv(os.path.join(BASE_DIR, ".env"))
    load_dotenv("/opt/relatorio_h_t/.env")

    dry = "--dry-run" in sys.argv

    import medico_custo_hist_db as h

    if h.pg_engine is None:
        print("ERRO: sem conexão com o RDS — confira PG_RDS_* no .env")
        return 1

    from sqlalchemy import inspect
    insp = inspect(h.pg_engine)
    existentes = set(insp.get_table_names())
    alvo = ["mc_execucao", "mc_agenda_versao", "mc_mudanca"]

    print("=== Histórico do Custo Efetivo Nominal ===")
    for t in alvo:
        print(f"  {t:<20} {'já existe' if t in existentes else 'SERÁ CRIADA'}")

    if dry:
        faltam = [t for t in alvo if t not in existentes]
        print(f"\n--dry-run: nada foi gravado. Criaria {len(faltam)} tabela(s).")
        return 0

    h.criar_tabelas()
    depois = set(inspect(h.pg_engine).get_table_names())
    criadas = [t for t in alvo if t in depois and t not in existentes]
    print(f"\nOK. Criadas agora: {', '.join(criadas) if criadas else 'nenhuma (já existiam)'}")

    # Contagem, para dar para ver na hora se a coleta já rodou alguma vez.
    with h.pg_engine.connect() as con:
        from sqlalchemy import text
        for t in alvo:
            n = con.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar()
            print(f"  {t:<20} {n:>7} linha(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
