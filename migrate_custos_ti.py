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
    # Fila de auditoria: o PDF da conta e o que o robô leu dele. Sem backfill —
    # linha antiga não tem PDF guardado e fica com o campo vazio, que é a
    # verdade (o e-mail dela entrou antes de existir a leitura de anexo).
    ("ti_email_auditoria", "anexo_nome", "VARCHAR(260)", None),
    ("ti_email_auditoria", "anexo_tipo", "VARCHAR(80)", None),
    ("ti_email_auditoria", "anexo_bytes", "BYTEA", None),
    ("ti_email_auditoria", "texto_extraido", "TEXT", None),
    ("ti_email_auditoria", "extraido_como", "VARCHAR(10)", None),
    ("ti_email_auditoria", "valor_sugerido", "NUMERIC(14,4)", None),
    ("ti_email_auditoria", "moeda_sugerida", "VARCHAR(3)", None),
    ("ti_email_auditoria", "trecho_valor", "VARCHAR(300)", None),
    # Fatura agregada (status='anexado'): de qual fornecedor e de qual mês ela é.
    # Sem backfill — item antigo nunca foi anexado, o vazio é a verdade.
    ("ti_email_auditoria", "rateio_fornecedor", "VARCHAR(120)", None),
    ("ti_email_auditoria", "rateio_competencia", "VARCHAR(7)", None),
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


def _checks_do_codigo(db):
    """Os CheckConstraint de vocabulário fechado (`coluna IN (...)`) do modelo.

    Devolve (tabela, nome, coluna, {valores}, sqltext). Constraint que não é
    lista de valores (faixa de dia_vencimento, por exemplo) fica de fora — ali
    não existe "valor novo" para sincronizar.
    """
    import re
    from sqlalchemy import CheckConstraint

    achados = []
    for t in db.TiBase.metadata.sorted_tables:
        for c in t.constraints:
            if not isinstance(c, CheckConstraint) or not c.name:
                continue
            txt = str(c.sqltext).strip()
            m = re.fullmatch(r"(\w+)\s+IN\s+\((.+)\)", txt, re.S)
            if not m:
                continue
            achados.append((t.name, c.name, m.group(1),
                            set(re.findall(r"'([^']*)'", m.group(2))), txt))
    return achados


def _sincroniza_checks(db, insp, dry_run: bool) -> list[str]:
    """Acerta no banco as listas de valores aceitos que mudaram no código.

    `create_all()` não toca em tabela existente: acrescentar um valor à tupla
    (ORIGENS_LANC, STATUS_LANC, MOEDAS...) muda o modelo mas deixa a trava
    antiga em produção, e a gravação morre com CheckViolation só lá. Foi o que
    aconteceu com a origem 'email' em 2026-08-06 — a fila de auditoria inteira
    ficou impossível de confirmar.

    Compara CONJUNTO de valores, não o texto: o Postgres reescreve
    `origem IN ('a','b')` como `origem::text = ANY (ARRAY['a'::varchar,...])`,
    então comparar string acusaria diferença em toda execução.

    Só amplia ou reescreve a lista — nunca apaga linha. Se um valor sair do
    código com linha antiga usando ele, o ADD CONSTRAINT falha e a migration
    para com o erro do banco, que é o comportamento certo: alguém precisa
    decidir o que fazer com as linhas órfãs.
    """
    import re
    from sqlalchemy import text

    mudou = []
    tabelas = set(insp.get_table_names())
    for tabela, nome, coluna, valores, _txt in _checks_do_codigo(db):
        if tabela not in tabelas:
            continue  # tabela nova: create_all já cria a trava certa
        with db.pg_engine.connect() as con:
            atual = con.execute(text(
                "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
                # CAST, não `:t::regclass` — o text() do SQLAlchemy lê o `::`
                # como bind param e estoura com "syntax error at or near :".
                "WHERE conname = :n AND conrelid = CAST(:t AS regclass)"
            ), {"n": nome, "t": tabela}).scalar()
        no_banco = set(re.findall(r"'([^']*)'::", atual or "")) if atual else None
        if no_banco == valores:
            continue
        faltam = sorted(valores - (no_banco or set()))
        sobram = sorted((no_banco or set()) - valores)
        desc = (f"  trava {tabela}.{coluna}: "
                + (f"faltando {', '.join(faltam)}" if faltam else "")
                + ("; " if faltam and sobram else "")
                + (f"sobrando {', '.join(sobram)}" if sobram else ""))
        mudou.append(desc)
        if dry_run:
            continue
        lista = ", ".join(f"'{v}'" for v in sorted(valores))
        with db.pg_engine.begin() as con:
            con.execute(text(f"ALTER TABLE {tabela} DROP CONSTRAINT IF EXISTS {nome}"))
            con.execute(text(f"ALTER TABLE {tabela} ADD CONSTRAINT {nome} "
                             f"CHECK ({coluna} IN ({lista}))"))
        print(desc + " → atualizada")
    return mudou


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
        pendentes = _sincroniza_checks(db, insp, dry_run=True)
        print("Travas a sincronizar:\n" + "\n".join(pendentes)
              if pendentes else "Travas a sincronizar: — nenhuma —")
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

    # …nem atualiza a lista de valores aceitos de uma coluna que já existe.
    if not _sincroniza_checks(db, insp, dry_run=False):
        print("Travas de valores: já estavam iguais ao código.")

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
