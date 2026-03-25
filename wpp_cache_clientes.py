#!/usr/bin/env python3
"""
wpp_cache_clientes.py — ETL diário: SQL Server → SQLite (cache_clientes).

Fluxo por posto:
  1. Busca todos os idcliente distintos e ativos do SQL Server (query leve)
  2. Compara com idcliente já no SQLite → só processa os pendentes
  3. Busca os dados completos em batches de 2000 idclientes (IN clause)
  4. Insere no SQLite com INSERT OR REPLACE
  5. Dorme 10s entre batches para não sobrecarregar o banco de produção

Uso:
  python3 wpp_cache_clientes.py                # incremental (só novos idcliente)
  python3 wpp_cache_clientes.py --full         # apaga cache do posto e recarrega tudo
  python3 wpp_cache_clientes.py --posto A      # só um posto específico
  python3 wpp_cache_clientes.py --full --posto A
"""

import argparse
import logging
import os
import sqlite3
import time
from datetime import datetime

import pyodbc
from dotenv import load_dotenv

from wpp_cache_clientes_schema import criar_schema

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

DB_PATH      = os.getenv("WAPP_CTRL_DB", "/opt/camim-auth/whatsapp_cobranca.db")
ODBC_DRIVER  = os.getenv("ODBC_DRIVER",  "ODBC Driver 17 for SQL Server")
BATCH_SIZE   = 500
SLEEP_SECS   = 3


# ---------------------------------------------------------------------------
# Query: todos os idcliente ativos do posto (rápida, só 1 coluna)
# ---------------------------------------------------------------------------
_SQL_IDS = """
SELECT DISTINCT f5.idcliente
FROM vw_Cad_PacienteView f5
LEFT JOIN sis_empresa empresa ON empresa.idEndereco = f5.idEndereco
WHERE f5.Desativado = 0
  AND f5.idEndereco = empresa.idEndereco
ORDER BY f5.idcliente
"""

# ---------------------------------------------------------------------------
# Query: dados completos para um batch de idcliente (placeholders dinâmicos)
# ---------------------------------------------------------------------------
_SQL_FETCH = """
SELECT
      f5.idcliente
    , f5.id                                     AS row_id
    , f5.idEndereco                             AS id_endereco
    , f5.sexo
    , ISNULL(p.ClubeBeneficio,    0)            AS clubebeneficio
    , ISNULL(p.ClubeBeneficioJoy, 0)            AS clubebeneficiojoy
    , ISNULL(p.PlanoPremium,      0)            AS planopremium
    , f5.Matricula                              AS matricula
    , f5.nome                                   AS nomecadastro
    , f5.Tipo                                   AS titular_dependente
    , f5.plano
    , f5.idade
    , CONVERT(VARCHAR(10), f5.DataAdmissao, 120) AS dataadmissao
    , f5.cobrador                               AS cobradornome
    , f5.corretor
    , f5.situação                               AS situacao
    , f5.[Dia cobrança]                         AS diacobranca
    , CONVERT(VARCHAR(10), f5.Nascimento, 120)  AS nascimento
    , f5.Bairro                                 AS bairro
    , f5.origem
    , f5.TelefoneWhatsApp                       AS telefone_whatsapp
    , vcc.canceladoans                          AS canceladoans
    , f5.tipo_FJ                                AS tipo_fj
    , f5.Responsavel                            AS responsavel
    , cc.responsaveltelefonewhatsApp            AS responsavel_tel_wpp
    , vcc.SituaçãoClube                         AS situacaoclube
    -- Classificação pré-calculada
    , CASE
          WHEN f5.idplano IS NULL AND f5.Matricula > 0         THEN 'edige'
          WHEN f5.Matricula = 0                                 THEN 'particular'
          WHEN f5.Matricula > 999999 AND f5.idplano IS NOT NULL THEN 'clube'
          WHEN f5.Matricula BETWEEN 1 AND 999999
               AND f5.idplano IS NOT NULL                      THEN 'camim'
          ELSE 'outro'
      END                                       AS tipo_cliente
    , CASE
          WHEN vcc.canceladoans = 1                                 THEN 'Cancelado'
          WHEN f5.Matricula > 999999 AND f5.idplano IS NOT NULL     THEN vcc.SituaçãoClube
          ELSE f5.situação
      END                                       AS situacao_efetiva
    , COALESCE(
          NULLIF(LTRIM(RTRIM(ISNULL(f5.TelefoneWhatsApp, ''))), ''),
          cc.responsaveltelefonewhatsApp
      )                                         AS telefone_efetivo
FROM vw_Cad_PacienteView f5
LEFT JOIN sis_empresa empresa ON empresa.idEndereco = f5.idEndereco
JOIN  cad_cliente cc          ON cc.idcliente       = f5.idCliente
JOIN  vw_cad_cliente vcc      ON vcc.idcliente      = f5.idCliente
LEFT JOIN cad_plano p         ON p.idplano          = f5.idPlano
WHERE f5.Desativado = 0
  AND f5.idEndereco = empresa.idEndereco
  AND f5.idcliente IN ({placeholders})
ORDER BY f5.idcliente
"""

# ---------------------------------------------------------------------------
# Query: idclientes que pagaram atrasado em mais da metade das últimas 12 mensalidades
# ---------------------------------------------------------------------------
_SQL_PAGADORES_ATRASADOS = """
WITH ranked AS (
    SELECT
        idCliente,
        DataVencimento,
        DataPagamento,
        ROW_NUMBER() OVER (
            PARTITION BY idCliente
            ORDER BY DataMensalidade DESC
        ) AS rn
    FROM fin_receita
    WHERE DataPagamento IS NOT NULL
      AND DataCancelamento IS NULL
)
SELECT idCliente
FROM ranked
WHERE rn <= 12
GROUP BY idCliente
HAVING SUM(CASE WHEN DataPagamento > DataVencimento THEN 1 ELSE 0 END)
       > COUNT(*) / 2.0
"""

_INSERT_SQL = """
INSERT OR REPLACE INTO cache_clientes (
    idcliente, row_id, posto, id_endereco,
    sexo, clubebeneficio, clubebeneficiojoy, planopremium,
    matricula, nomecadastro, titular_dependente,
    plano, idade, dataadmissao,
    cobradornome, corretor, situacao,
    diacobranca, nascimento, bairro,
    origem, telefone_whatsapp, canceladoans,
    tipo_fj, responsavel, responsavel_tel_wpp,
    situacaoclube, tipo_cliente, situacao_efetiva,
    telefone_efetivo, pagador_atrasado, carregado_em
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
"""


def _get_conn_posto(posto: str):
    host = os.getenv(f"DB_HOST_{posto}", "").strip()
    base = os.getenv(f"DB_BASE_{posto}", "").strip()
    if not host or not base:
        return None
    user = os.getenv(f"DB_USER_{posto}",     "").strip()
    pwd  = os.getenv(f"DB_PASSWORD_{posto}", "").strip()
    port = os.getenv(f"DB_PORT_{posto}",     "1433").strip()
    conn_str = (
        f"DRIVER={{{ODBC_DRIVER}}};"
        f"SERVER=tcp:{host},{port};"
        f"DATABASE={base};"
        f"Encrypt=yes;TrustServerCertificate=yes;"
        f"Connection Timeout=30;"
    )
    conn_str += f"UID={user};PWD={pwd}" if user else "Trusted_Connection=yes"
    try:
        conn = pyodbc.connect(conn_str)
        conn.timeout = 300  # 5 min por query
        return conn
    except Exception as e:
        log.error("Posto %s: erro ao conectar: %s", posto, e)
        return None


def _get_sqlite() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _postos_configurados() -> list:
    postos = []
    for key in os.environ:
        if key.startswith("DB_HOST_") and os.environ[key].strip():
            postos.append(key[len("DB_HOST_"):])
    return sorted(postos)


def _carregar_posto(posto: str, full: bool = False) -> dict:
    sql_conn = _get_conn_posto(posto)
    if not sql_conn:
        log.warning("Posto %s: sem conexão configurada, pulando.", posto)
        return {"inseridos": 0, "pulados": 0, "erro": True}

    sqlite_conn = _get_sqlite()
    inseridos = 0

    try:
        if full:
            log.info("Posto %s: --full → apagando registros existentes.", posto)
            sqlite_conn.execute("DELETE FROM cache_clientes WHERE posto = ?", (posto,))
            sqlite_conn.commit()

        # 1. Todos os idcliente ativos no SQL Server
        log.info("Posto %s: buscando idclientes do SQL Server...", posto)
        cur = sql_conn.cursor()
        cur.execute(_SQL_IDS)
        todos_ids = [row[0] for row in cur.fetchall()]
        log.info("Posto %s: %d idclientes ativos no SQL Server.", posto, len(todos_ids))

        # 1b. Pagadores atrasados (>50% das últimas 12 mensalidades pagas com atraso)
        log.info("Posto %s: calculando pagadores atrasados...", posto)
        cur.execute(_SQL_PAGADORES_ATRASADOS)
        atrasados_set = {row[0] for row in cur.fetchall()}
        log.info("Posto %s: %d idclientes classificados como pagadores atrasados.", posto, len(atrasados_set))

        # 2. Quais já estão no SQLite?
        if not full:
            ja_cache = {
                row[0]
                for row in sqlite_conn.execute(
                    "SELECT DISTINCT idcliente FROM cache_clientes WHERE posto = ?", (posto,)
                ).fetchall()
            }
            pendentes = [i for i in todos_ids if i not in ja_cache]
            log.info(
                "Posto %s: %d já em cache, %d pendentes.",
                posto, len(ja_cache), len(pendentes),
            )
        else:
            pendentes = todos_ids

        if not pendentes:
            log.info("Posto %s: nenhum cliente novo para inserir.", posto)
        else:
            # 3. Batches de BATCH_SIZE idclientes
            total_batches = (len(pendentes) + BATCH_SIZE - 1) // BATCH_SIZE
            for batch_num, i in enumerate(range(0, len(pendentes), BATCH_SIZE), start=1):
                batch_ids = pendentes[i: i + BATCH_SIZE]
                log.info(
                    "Posto %s: batch %d/%d — %d idclientes...",
                    posto, batch_num, total_batches, len(batch_ids),
                )

                ph  = ",".join("?" * len(batch_ids))
                sql = _SQL_FETCH.format(placeholders=ph)
                cur.execute(sql, batch_ids)
                rows = cur.fetchall()

                agora = datetime.now().isoformat(timespec="seconds")
                registros = [
                    (
                        r[0],  r[1],  posto, r[2],
                        r[3],
                        int(r[4] or 0), int(r[5] or 0), int(r[6] or 0),
                        r[7],  r[8],  r[9],
                        r[10], r[11], r[12],
                        r[13], r[14], r[15],
                        r[16], r[17], r[18],
                        r[19], r[20], int(r[21] or 0),
                        r[22], r[23], r[24],
                        r[25], r[26], r[27],
                        r[28], 1 if r[0] in atrasados_set else 0,
                        agora,
                    )
                    for r in rows
                ]

                sqlite_conn.executemany(_INSERT_SQL, registros)
                sqlite_conn.commit()
                inseridos += len(registros)
                log.info("Posto %s: batch %d concluído — %d linhas inseridas.", posto, batch_num, len(registros))

                if i + BATCH_SIZE < len(pendentes):
                    log.info("Aguardando %ds antes do próximo batch...", SLEEP_SECS)
                    time.sleep(SLEEP_SECS)

        # 4. Atualiza pagador_atrasado para TODOS os idcliente do posto
        #    (incluindo os já em cache de execuções anteriores)
        log.info("Posto %s: atualizando pagador_atrasado no cache...", posto)
        sqlite_conn.execute("UPDATE cache_clientes SET pagador_atrasado = 0 WHERE posto = ?", (posto,))
        if atrasados_set:
            atrasados_list = list(atrasados_set)
            update_batch = 5000
            for i in range(0, len(atrasados_list), update_batch):
                batch = atrasados_list[i: i + update_batch]
                ph = ",".join("?" * len(batch))
                sqlite_conn.execute(
                    f"UPDATE cache_clientes SET pagador_atrasado = 1 "
                    f"WHERE posto = ? AND idcliente IN ({ph})",
                    [posto] + batch,
                )
        sqlite_conn.commit()
        log.info("Posto %s: pagador_atrasado atualizado.", posto)

    finally:
        sql_conn.close()
        sqlite_conn.close()

    pulados = len(todos_ids) - len(pendentes) if not full else 0
    log.info("Posto %s: concluído. inseridos=%d pulados=%d", posto, inseridos, pulados)
    return {"inseridos": inseridos, "pulados": pulados, "erro": False}


def main():
    parser = argparse.ArgumentParser(
        description="ETL: carrega cache de clientes do SQL Server para SQLite"
    )
    parser.add_argument(
        "--full", action="store_true",
        help="Recarga total (apaga cache do posto e recarrega tudo)",
    )
    parser.add_argument(
        "--posto", metavar="POSTO",
        help="Processa só um posto (ex: A). Padrão: todos os postos configurados.",
    )
    args = parser.parse_args()

    # Garante que o schema existe antes de qualquer coisa
    criar_schema(DB_PATH)

    postos = [args.posto.upper()] if args.posto else _postos_configurados()
    if not postos:
        log.error("Nenhum posto configurado (DB_HOST_X não encontrado no .env).")
        return

    log.info("=== wpp_cache_clientes: início | postos=%s full=%s ===", postos, args.full)
    for posto in postos:
        result = _carregar_posto(posto, full=args.full)
        log.info("Posto %s resultado: %s", posto, result)
    log.info("=== wpp_cache_clientes: fim ===")


if __name__ == "__main__":
    main()
