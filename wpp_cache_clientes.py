#!/usr/bin/env python3
"""
wpp_cache_clientes.py — ETL diário: SQL Server → SQLite (cache_clientes).

Fluxo por posto:
  1. Busca todos os idcliente distintos e ativos do SQL Server (query leve)
  2. Compara com idcliente já no SQLite → só processa os pendentes
  3. Busca os dados completos em batches de BATCH_SIZE idclientes (IN clause)
  4. Insere no SQLite com INSERT OR REPLACE
  5. Dorme SLEEP_SECS entre batches para não sobrecarregar o banco de produção

Uso:
  python3 wpp_cache_clientes.py                # incremental (só novos idcliente)
  python3 wpp_cache_clientes.py --full         # apaga cache do posto e recarrega tudo
  python3 wpp_cache_clientes.py --posto A      # só um posto específico
  python3 wpp_cache_clientes.py --full --posto A

Log: wpp_cache_clientes.log (mesmo diretório do script)
"""

import argparse
import logging
import os
import sqlite3
import threading
import time
from datetime import datetime

import pyodbc
from dotenv import load_dotenv

from wpp_cache_clientes_schema import criar_schema

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

# ---------------------------------------------------------------------------
# Logging: console + arquivo
# ---------------------------------------------------------------------------
LOG_FILE = os.path.join(BASE_DIR, "wpp_cache_clientes.log")

_fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
_stream_h = logging.StreamHandler()
_stream_h.setFormatter(_fmt)
_file_h = logging.FileHandler(LOG_FILE, encoding="utf-8")
_file_h.setFormatter(_fmt)

logging.root.setLevel(logging.DEBUG)
logging.root.addHandler(_stream_h)
logging.root.addHandler(_file_h)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------
DB_PATH             = os.getenv("WAPP_CTRL_DB", "/opt/camim-auth/whatsapp_cobranca.db")
ODBC_DRIVER         = os.getenv("ODBC_DRIVER",  "ODBC Driver 17 for SQL Server")
BATCH_SIZE          = 500        # IDs por batch
SLEEP_SECS          = 3          # pausa entre batches (s)
QUERY_TIMEOUT_SECS  = 180        # timeout por query (s) — cur.cancel() é chamado
WATCHDOG_INTERVAL   = 15         # a cada X segundos loga "ainda aguardando..."


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
# Query: pagadores atrasados
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


# ---------------------------------------------------------------------------
# Watchdog: executa query em thread separada e monitora progresso
# ---------------------------------------------------------------------------

def _executar_com_watchdog(cur, sql, params, label: str):
    """
    Executa cur.execute(sql, params) + fetchall() em thread separada.
    A cada WATCHDOG_INTERVAL segundos loga "ainda aguardando...".
    Se QUERY_TIMEOUT_SECS for atingido, chama cur.cancel() e retorna None.

    Retorna (rows, None) em sucesso ou (None, Exception) em falha/timeout.
    """
    result   = {"rows": None}
    error    = {"exc": None}
    finished = threading.Event()

    def _worker():
        try:
            log.debug("%s: enviando SQL ao servidor...", label)
            cur.execute(sql, params)
            log.debug("%s: SQL enviado, aguardando fetchall()...", label)
            result["rows"] = cur.fetchall()
            log.debug("%s: fetchall() concluído — %d linhas.", label, len(result["rows"]))
        except Exception as exc:
            error["exc"] = exc
        finally:
            finished.set()

    t = threading.Thread(target=_worker, daemon=True)
    t_start = time.time()
    t.start()

    while not finished.wait(timeout=WATCHDOG_INTERVAL):
        elapsed = time.time() - t_start
        if elapsed >= QUERY_TIMEOUT_SECS:
            log.warning(
                "%s: TIMEOUT de %.0fs atingido — chamando cur.cancel().",
                label, elapsed,
            )
            try:
                cur.cancel()
            except Exception as ce:
                log.warning("%s: cur.cancel() falhou: %s", label, ce)
            # Aguarda a thread encerrar após o cancel (até 10s)
            t.join(timeout=10)
            return None, TimeoutError(f"{label}: timeout após {elapsed:.0f}s")
        log.info(
            "%s: aguardando resposta do SQL Server... %.0fs decorridos "
            "(timeout em %.0fs).",
            label, elapsed, QUERY_TIMEOUT_SECS,
        )

    elapsed = time.time() - t_start
    if error["exc"]:
        log.warning("%s: query falhou após %.1fs — %s", label, elapsed, error["exc"])
        return None, error["exc"]

    log.debug("%s: query OK em %.1fs.", label, elapsed)
    return result["rows"], None


# ---------------------------------------------------------------------------
# Conexão
# ---------------------------------------------------------------------------

CONNECT_TIMEOUT_SECS = 60   # timeout para tentar conectar/reconectar

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
    log.debug("Posto %s: conectando em %s/%s...", posto, host, base)
    try:
        conn = pyodbc.connect(conn_str)
        conn.timeout = QUERY_TIMEOUT_SECS
        log.debug("Posto %s: conexão estabelecida.", posto)
        return conn
    except Exception as e:
        log.error("Posto %s: erro ao conectar: %s", posto, e)
        return None


def _get_conn_posto_com_timeout(posto: str) -> "pyodbc.Connection | None":
    """Abre conexão com SQL Server dentro de um thread com timeout.
    Evita travar indefinidamente quando o servidor não responde."""
    result   = {"conn": None}
    error    = {"exc": None}
    finished = threading.Event()

    def _worker():
        try:
            result["conn"] = _get_conn_posto(posto)
        except Exception as exc:
            error["exc"] = exc
        finally:
            finished.set()

    t = threading.Thread(target=_worker, daemon=True)
    t.start()
    if not finished.wait(timeout=CONNECT_TIMEOUT_SECS):
        log.warning(
            "Posto %s: reconexão travou após %ds — abandonando reconexão.",
            posto, CONNECT_TIMEOUT_SECS,
        )
        return None
    if error["exc"]:
        log.error("Posto %s: erro na reconexão: %s", posto, error["exc"])
        return None
    return result["conn"]


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


# ---------------------------------------------------------------------------
# Carga por posto
# ---------------------------------------------------------------------------

def _carregar_posto(posto: str, full: bool = False) -> dict:
    t0_posto = time.time()
    log.info("Posto %s: iniciando carga (full=%s).", posto, full)

    sql_conn = _get_conn_posto_com_timeout(posto)
    if not sql_conn:
        log.warning("Posto %s: sem conexão configurada, pulando.", posto)
        return {"inseridos": 0, "pulados": 0, "erro": True}

    sqlite_conn = _get_sqlite()
    inseridos = 0
    todos_ids = []

    try:
        if full:
            log.info("Posto %s: --full → apagando registros existentes.", posto)
            sqlite_conn.execute("DELETE FROM cache_clientes WHERE posto = ?", (posto,))
            sqlite_conn.commit()

        # 1. IDs ativos
        log.info("Posto %s: [1/4] buscando idclientes ativos no SQL Server...", posto)
        cur = sql_conn.cursor()
        t1 = time.time()
        rows_ids, err = _executar_com_watchdog(cur, _SQL_IDS, [], f"Posto {posto} / IDS")
        if err:
            log.error("Posto %s: falha ao buscar IDs — %s. Abortando posto.", posto, err)
            return {"inseridos": 0, "pulados": 0, "erro": True}
        todos_ids = [r[0] for r in rows_ids]
        log.info(
            "Posto %s: %d idclientes ativos (%.1fs).",
            posto, len(todos_ids), time.time() - t1,
        )

        # 2. Pagadores atrasados
        log.info("Posto %s: [2/4] calculando pagadores atrasados...", posto)
        t2 = time.time()
        rows_atr, err = _executar_com_watchdog(
            cur, _SQL_PAGADORES_ATRASADOS, [], f"Posto {posto} / PAGADORES_ATRASADOS"
        )
        if err:
            log.warning(
                "Posto %s: falha ao calcular pagadores atrasados (%s) — "
                "continuando sem esse filtro.", posto, err
            )
            atrasados_set = set()
        else:
            atrasados_set = {r[0] for r in rows_atr}
        log.info(
            "Posto %s: %d pagadores atrasados (%.1fs).",
            posto, len(atrasados_set), time.time() - t2,
        )

        # 3. Pendentes
        log.info("Posto %s: [3/4] verificando cache local...", posto)
        if not full:
            ja_cache = {
                row[0]
                for row in sqlite_conn.execute(
                    "SELECT DISTINCT idcliente FROM cache_clientes WHERE posto = ?", (posto,)
                ).fetchall()
            }
            pendentes = [i for i in todos_ids if i not in ja_cache]
            log.info(
                "Posto %s: %d já em cache, %d pendentes de carga.",
                posto, len(ja_cache), len(pendentes),
            )
        else:
            pendentes = todos_ids
            log.info("Posto %s: modo full — %d pendentes.", posto, len(pendentes))

        if not pendentes:
            log.info("Posto %s: nenhum cliente novo para inserir.", posto)
        else:
            total_batches = (len(pendentes) + BATCH_SIZE - 1) // BATCH_SIZE
            log.info(
                "Posto %s: iniciando %d batches de até %d IDs cada.",
                posto, total_batches, BATCH_SIZE,
            )
            batches_pulados = 0
            abort_posto = False

            for batch_num, i in enumerate(range(0, len(pendentes), BATCH_SIZE), start=1):
                if abort_posto:
                    break
                batch_ids = pendentes[i: i + BATCH_SIZE]
                pct = batch_num / total_batches * 100
                log.info(
                    "Posto %s: [batch %d/%d | %.0f%%] %d IDs — range %d..%d",
                    posto, batch_num, total_batches, pct,
                    len(batch_ids), batch_ids[0], batch_ids[-1],
                )

                ph  = ",".join("?" * len(batch_ids))
                sql = _SQL_FETCH.format(placeholders=ph)
                label = f"Posto {posto} / batch {batch_num}/{total_batches}"

                t_batch = time.time()
                rows, err = _executar_com_watchdog(cur, sql, batch_ids, label)

                if err:
                    batches_pulados += 1
                    log.warning(
                        "Posto %s: batch %d PULADO após %.1fs — %s. Reconectando...",
                        posto, batch_num, time.time() - t_batch, err,
                    )
                    try:
                        sql_conn.close()
                    except Exception:
                        pass
                    sql_conn = _get_conn_posto_com_timeout(posto)
                    if not sql_conn:
                        log.error(
                            "Posto %s: não conseguiu reconectar. Abortando posto.",
                            posto,
                        )
                        abort_posto = True
                        break
                    cur = sql_conn.cursor()
                    time.sleep(SLEEP_SECS)
                    continue

                t_query = time.time() - t_batch
                log.info(
                    "Posto %s: batch %d — %d linhas recebidas em %.1fs. "
                    "Inserindo no SQLite...",
                    posto, batch_num, len(rows), t_query,
                )

                agora = datetime.now().isoformat(timespec="seconds")
                try:
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
                    log.info(
                        "Posto %s: batch %d OK — %d registros inseridos "
                        "(total acumulado: %d | %.0f%% concluído).",
                        posto, batch_num, len(registros), inseridos, pct,
                    )
                except Exception as e_ins:
                    log.warning(
                        "Posto %s: batch %d erro ao inserir no SQLite (%s) — pulando.",
                        posto, batch_num, e_ins,
                    )
                    try:
                        sqlite_conn.rollback()
                    except Exception:
                        pass
                    batches_pulados += 1

                if i + BATCH_SIZE < len(pendentes):
                    log.debug("Posto %s: aguardando %ds antes do próximo batch...", posto, SLEEP_SECS)
                    time.sleep(SLEEP_SECS)

            if batches_pulados:
                log.warning(
                    "Posto %s: %d batches foram pulados por erro/timeout.",
                    posto, batches_pulados,
                )

        # 4. Atualiza pagador_atrasado para todos
        log.info("Posto %s: [4/4] atualizando pagador_atrasado no cache...", posto)
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
        try:
            sql_conn.close()
        except Exception:
            pass
        sqlite_conn.close()

    pulados = len(todos_ids) - len(pendentes) if not full else 0
    elapsed = time.time() - t0_posto
    log.info(
        "Posto %s: CONCLUÍDO em %.0fs — inseridos=%d pulados=%d.",
        posto, elapsed, inseridos, pulados,
    )
    return {"inseridos": inseridos, "pulados": pulados, "erro": False}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

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

    criar_schema(DB_PATH)

    postos = [args.posto.upper()] if args.posto else _postos_configurados()
    if not postos:
        log.error("Nenhum posto configurado (DB_HOST_X não encontrado no .env).")
        return

    log.info(
        "=== wpp_cache_clientes: início | postos=%s full=%s | log=%s ===",
        postos, args.full, LOG_FILE,
    )
    t0 = time.time()
    for posto in postos:
        result = _carregar_posto(posto, full=args.full)
        log.info("Posto %s resultado: %s", posto, result)
    log.info(
        "=== wpp_cache_clientes: fim | tempo total=%.0fs ===",
        time.time() - t0,
    )


if __name__ == "__main__":
    main()
