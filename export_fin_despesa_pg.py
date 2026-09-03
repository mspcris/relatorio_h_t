#!/usr/bin/env python3
"""
export_fin_despesa_pg.py

ETL incremental: vw_Fin_Despesa (SQL Server, 13 postos) -> Postgres RDS AWS (relatorio_h_t.fin_despesa)

- Incremental: pega so idDespesa > MAX(id_despesa) ja em relatorio_h_t.fin_despesa (por posto)
- REFRESH (2026-09-03): alem do incremental, reenvia com UPSERT tudo que tem
  DataPagamentoAuto nos ultimos REFRESH_DIAS (45). Antes era so INSERT ... DO
  NOTHING: despesa editada/cancelada depois de carregada nunca chegava ao RDS.
- data_pagamento_auto (2026-09-03): vem de Fin_Despesa (a view nao expoe). E a
  data que o kpi_receita_despesa usa como MES (sql_full/*.sql filtra por
  DataPagamentoAuto). Difere de DataPagamento no mes em ~1,6% das linhas
  (G: 135/8.250; A: 297/29.568 em 12m) — sem ela o drill-down da pagina nao
  fecha com o total do mes. `--backfill-auto` preenche o historico.
- Filtros fixos: [Valor pago] IS NOT NULL, idContaTipo <> 11, [Data de pagamento] >= 01/01/2020
- Cron: a cada 2 horas
- Flags: --dry-run (so conta, nao escreve) · --backfill-auto (preenche
  data_pagamento_auto em todo o historico, uma vez) · --refresh-dias N · --postos G,A
- Meta: json_consolidado/_etl_meta_export_fin_despesa_pg.json (widget ETL v2)
- Credenciais: PG_RDS_* no /opt/relatorio_h_t/.env (nao commitadas)
"""
import os
import sys
import time
import traceback
from datetime import date
from typing import List, Tuple

from dotenv import load_dotenv
_BASE = os.path.dirname(os.path.abspath(__file__))
# .env do proprio diretorio; rodando de outro lugar (dry-run em /tmp na VM)
# cai no .env e nos modulos de producao.
for _env_path in (os.path.join(_BASE, ".env"), "/opt/relatorio_h_t/.env"):
    if os.path.isfile(_env_path):
        load_dotenv(_env_path); break
sys.path.insert(0, _BASE)
sys.path.insert(0, "/opt/relatorio_h_t")

import psycopg2
from psycopg2.extras import execute_values
from sqlalchemy import text

from export_governanca import build_conns_from_env, make_engine
from etl_meta import ETLMeta


# (coluna SQL Server, coluna Postgres)
# Ordem importa: mesma ordem do SELECT e do INSERT.
COLUMNS: List[Tuple[str, str]] = [
    ("idDespesa",                  "id_despesa"),
    ("[Valor devido]",             "valor_devido"),
    ("[Valor pago]",               "valor_pago"),
    ("[Data de vencimento]",       "data_vencimento"),
    ("[Data de pagamento]",        "data_pagamento"),
    ("[Data de cancelamento]",     "data_cancelamento"),
    ("[Descrição]",                "descricao"),
    ("[Comentário]",               "comentario"),
    ("Tipo",                       "tipo"),
    ("Plano",                      "plano"),
    ("Corretor",                   "corretor"),
    ("Cobrador",                   "cobrador"),
    ("[Médico]",                   "medico"),
    ("Conta",                      "conta"),
    ("[Situação]",                 "situacao"),
    ("[Talão]",                    "talao"),
    ("[Valor fatura]",             "valor_fatura"),
    ("Cliente",                    "cliente"),
    ("Matricula",                  "matricula"),
    ("[Funcionário]",              "funcionario"),
    ("Confere",                    "confere"),
    ("Pessoa",                     "pessoa"),
    ("idConta",                    "id_conta"),
    ("[Data prestaçao]",           "data_prestacao"),
    ("idContaTipo",                "id_conta_tipo"),
    ("idLancamento",               "id_lancamento"),
    ("Usuario",                    "usuario"),
    ("Paciente",                   "paciente"),
    ("OrdemPagamento",             "ordem_pagamento"),
    ("Contabilizado",              "contabilizado"),
    ("PlanoPrincipal",             "plano_principal"),
    ("Fornecedor",                 "fornecedor"),
    ("Endereco",                   "endereco"),
    ("idFuncionario",              "id_funcionario"),
    ("idMedico",                   "id_medico"),
    ("UsuarioInclusao",            "usuario_inclusao"),
    ("Marcado",                    "marcado"),
    ("ValorRateio",                "valor_rateio"),
    ("PossuiRateio",               "possui_rateio"),
    ("idForma",                    "id_forma"),
    ("forma",                      "forma"),
    ("DigitouTalao",               "digitou_talao"),
    ("valormedicoctrlq",           "valor_medico_ctrlq"),
    ("[Data atendimento]",         "data_atendimento"),
    ("SistemaMedicoInclusao",      "sistema_medico_inclusao"),
    ("QuantidadeAnexo",            "quantidade_anexo"),
    ("PossuiAnexo",                "possui_anexo"),
    ("carteiradomedico",           "carteira_do_medico"),
    ("LinkBoletoMedico",           "link_boleto_medico"),
    ("Cargo",                      "cargo"),
    ("Atendido",                   "atendido"),
    ("SubCorretor",                "sub_corretor"),
    ("[Data Agendamento]",         "data_agendamento"),
    ("ContaBancaria",              "conta_bancaria"),
    ("PagamentoOnline",            "pagamento_online"),
    ("[Data Agendamento médico]",  "data_agendamento_medico"),
    ("t.DataPagamentoAuto",        "data_pagamento_auto"),   # da TABELA (join), nao da view
]

def _qual(col: str) -> str:
    # colunas da view ganham alias v.; as da tabela ja vem com t.
    return col if col.startswith("t.") else f"v.{col}"

SQL_SELECT_COLS = ",\n  ".join(_qual(c[0]) for c in COLUMNS)
PG_INSERT_COLS  = ["posto"] + [c[1] for c in COLUMNS]

# Filtro: desde Jan/2020, ignora idContaTipo = 11, so pagos.
# Data em DD/MM/YYYY (SET DATEFORMAT dmy das views da CAMIM).
SQL_FROM = """
FROM vw_Fin_Despesa v
JOIN Fin_Despesa t ON t.idDespesa = v.idDespesa
"""
SQL_WHERE_BASE = """
WHERE v.[Valor pago] IS NOT NULL
  AND v.idContaTipo <> 11
  AND v.[Data de pagamento] >= '01/01/2020'
"""
SQL_FROM_WHERE = SQL_FROM + SQL_WHERE_BASE + "  AND v.idDespesa > :wm\n"
# Janela de refresh: DataPagamentoAuto e a data do LANCAMENTO do pagamento, entao
# tudo que foi mexido recentemente cai aqui (inclusive despesa de julho paga e
# lancada em agosto). O parametro ref e datetime (bind), nunca string.
SQL_FROM_WHERE_REFRESH = SQL_FROM + SQL_WHERE_BASE + "  AND t.DataPagamentoAuto >= :ref\n"
REFRESH_DIAS = int(os.environ.get("FIN_DESPESA_REFRESH_DIAS", "45") or 45)

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


def ensure_schema(pg) -> None:
    """Idempotente: coluna + indice novos (2026-09-03)."""
    with pg.cursor() as c:
        c.execute("ALTER TABLE fin_despesa ADD COLUMN IF NOT EXISTS data_pagamento_auto timestamp")
        c.execute("CREATE INDEX IF NOT EXISTS ix_fin_despesa_data_pagamento_auto ON fin_despesa (data_pagamento_auto)")
    pg.commit()


def _upsert_sql() -> str:
    cols_str = ", ".join(PG_INSERT_COLS)
    upd = ", ".join(f"{c} = EXCLUDED.{c}" for c in PG_INSERT_COLS if c not in ("posto", "id_despesa"))
    return (f"INSERT INTO fin_despesa ({cols_str}) VALUES %s "
            f"ON CONFLICT (posto, id_despesa) DO UPDATE SET {upd}, imported_at = NOW()")


def refresh_posto(pg, posto: str, engine, dias: int, dry_run: bool) -> int:
    """Reenvia (UPSERT) tudo com DataPagamentoAuto nos ultimos `dias`."""
    from datetime import datetime, timedelta
    ref = datetime.now() - timedelta(days=dias)
    sql = f"SELECT\n  {SQL_SELECT_COLS}\n{SQL_FROM_WHERE_REFRESH}\nORDER BY v.idDespesa"
    placeholders = "(" + ", ".join(["%s"] * len(PG_INSERT_COLS)) + ")"
    n = 0
    with engine.connect().execution_options(stream_results=True) as sc:
        result = sc.execute(text(sql), {"ref": ref})
        while True:
            rows = result.fetchmany(BATCH)
            if not rows:
                break
            batch = [(posto, *row) for row in rows]
            n += len(batch)
            if dry_run:
                continue
            with pg.cursor() as pc:
                execute_values(pc, _upsert_sql(), batch, template=placeholders, page_size=BATCH)
            pg.commit()
    return n


def backfill_auto(pg, posto: str, engine, dry_run: bool) -> int:
    """Uma vez: preenche data_pagamento_auto nas linhas antigas (so onde esta NULL)."""
    with pg.cursor() as c:
        c.execute("SELECT 1 FROM information_schema.columns WHERE table_name='fin_despesa' AND column_name='data_pagamento_auto'")
        if not c.fetchone():
            print(f"[{posto}] backfill: coluna data_pagamento_auto ainda nao existe (dry-run sem ensure_schema)", flush=True)
            return 0
        c.execute("SELECT COUNT(*) FROM fin_despesa WHERE posto = %s AND data_pagamento_auto IS NULL", (posto,))
        faltam = int(c.fetchone()[0])
    if not faltam:
        return 0
    sql = ("SELECT idDespesa, DataPagamentoAuto FROM Fin_Despesa "
           "WHERE idContaTipo <> 11 AND DataPagamento >= '20200101' AND DataPagamento IS NOT NULL ORDER BY idDespesa")
    n = 0
    with engine.connect().execution_options(stream_results=True) as sc:
        result = sc.execute(text(sql))
        while True:
            rows = result.fetchmany(20000)
            if not rows:
                break
            n += len(rows)
            if dry_run:
                continue
            with pg.cursor() as pc:
                pc.execute("CREATE TEMP TABLE tmp_dpa (id_despesa bigint, dpa timestamp) ON COMMIT DROP")
                execute_values(pc, "INSERT INTO tmp_dpa (id_despesa, dpa) VALUES %s",
                               [(int(r[0]), r[1]) for r in rows], page_size=20000)
                pc.execute("UPDATE fin_despesa f SET data_pagamento_auto = x.dpa FROM tmp_dpa x "
                           "WHERE f.posto = %s AND f.id_despesa = x.id_despesa AND f.data_pagamento_auto IS NULL", (posto,))
            pg.commit()
    return n


def get_watermark(pg, posto: str) -> int:
    with pg.cursor() as c:
        c.execute("SELECT COALESCE(MAX(id_despesa), 0) FROM fin_despesa WHERE posto = %s", (posto,))
        return int(c.fetchone()[0])


def sync_posto(pg, posto: str, engine, dry_run: bool = False) -> int:
    wm = get_watermark(pg, posto)
    sql = f"SELECT\n  {SQL_SELECT_COLS}\n{SQL_FROM_WHERE}\nORDER BY v.idDespesa"
    inserted = 0

    placeholders = "(" + ", ".join(["%s"] * len(PG_INSERT_COLS)) + ")"
    cols_str = ", ".join(PG_INSERT_COLS)
    insert_sql = (
        f"INSERT INTO fin_despesa ({cols_str}) VALUES %s "
        "ON CONFLICT (posto, id_despesa) DO NOTHING"
    )

    with engine.connect().execution_options(stream_results=True) as sc:
        result = sc.execute(text(sql), {"wm": wm})
        while True:
            rows = result.fetchmany(BATCH)
            if not rows:
                break
            batch = [(posto, *row) for row in rows]
            inserted += len(batch)
            if dry_run:
                continue
            with pg.cursor() as pc:
                execute_values(pc, insert_sql, batch, template=placeholders, page_size=BATCH)
            pg.commit()
            last_id = batch[-1][1]
            print(f"[{posto}] +{len(batch):>6}  acum={inserted:>8}  last_id_despesa={last_id}", flush=True)

    return inserted


def main() -> int:
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="le e conta; nao escreve no RDS nem grava meta")
    ap.add_argument("--backfill-auto", action="store_true", help="preenche data_pagamento_auto no historico (uma vez)")
    ap.add_argument("--refresh-dias", type=int, default=REFRESH_DIAS)
    ap.add_argument("--postos", help="ex.: G,A (padrao: todos)")
    args = ap.parse_args()
    postos = [x.strip().upper() for x in args.postos.split(",")] if args.postos else POSTOS_ORDER

    meta = ETLMeta("export_fin_despesa_pg", "json_consolidado")
    conns = build_conns_from_env()
    rc = 0

    pg = pg_conn()
    try:
        if not args.dry_run:
            ensure_schema(pg)
        for p in postos:
            if p not in conns:
                print(f"[{p}] sem conn no .env — pulando", flush=True)
                continue
            t0 = time.time()
            try:
                engine = make_engine(conns[p])
                n = sync_posto(pg, p, engine, dry_run=args.dry_run)
                r = refresh_posto(pg, p, engine, args.refresh_dias, args.dry_run)
                b = backfill_auto(pg, p, engine, args.dry_run) if args.backfill_auto else 0
                dt = time.time() - t0
                print(f"[{p}] OK  inseridos={n}  refresh({args.refresh_dias}d)={r}  backfill_auto={b}  elapsed={dt:.1f}s"
                      f"{'  [DRY-RUN]' if args.dry_run else ''}", flush=True)
                meta.ok(p, inseridos=n, refresh=r)
            except Exception as e:
                rc = 2
                traceback.print_exc()
                meta.error(p, str(e))
                try:
                    pg.rollback()   # senao o proximo posto morre em "transaction is aborted"
                except Exception:
                    pass
    finally:
        pg.close()
        if not args.dry_run:
            meta.save()

    return rc


if __name__ == "__main__":
    sys.exit(main())
