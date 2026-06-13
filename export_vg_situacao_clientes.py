#!/usr/bin/env python3
"""
export_vg_situacao_clientes.py

KPI Indicadores Vinícius Gama — Indicador 1: Situação dos Clientes.

SQL Server (13 postos) -> Postgres RDS AWS (kpi_vg_situacao_clientes +
kpi_vg_matriculas_anteriores). Dados ISOLADOS deste KPI (decisão do projeto):
nenhuma outra página/ETL compartilha estas tabelas.

Por posto, 1 linha por cliente admitido desde 01/01/2026 (vw_fin_receita2
idcontatipo=5 + vw_cad_cliente), enriquecida com:
  - dependentes_qtd            (cad_clientedependente)
  - consultas_dia_adesao_qtd   (vw_Cad_LancamentoServicos, dia == dataadmissao)
  - consultas_futuras_qtd      (vw_Cad_LancamentoServicos, >= HOJE, não faturado)
  - matriculas_anteriores      (CPF do titular + de cada dependente pesquisado
                                em TODOS os postos como titular/dependente/
                                responsável — mesmas queries do endpoint
                                GET /crm/cpf/{id_endereco} da camila3, rodadas
                                direto no SQL Server pra evitar ~dezenas de
                                milhares de chamadas HTTP por run)

Dois modos (a busca de matrículas anteriores por CPF cruza os 13 bancos e é a
parte cara; o resto é leitura barata single-bank por posto):
  --modo full  (cron 5h): recalcula o posto inteiro — DELETE+INSERT, busca CPF
                de TODOS os clientes. "consultas futuras" é relativa a HOJE.
  --modo light (cron 8-18h, de hora em hora): UPDATE só das colunas leves de
                quem já existe + busca CPF (cara) APENAS dos clientes NOVOS
                (sem linha ainda = admitidos desde o último full). Não toca nas
                matrículas anteriores de quem já estava na tabela.

Leitura pura no SQL Server; escrita só no Postgres do KPI. Sem efeitos colaterais
com custo (sem WhatsApp/SMS/INSERT CAMIM).

DDL: sql_camim_fin/vg_situacao_clientes_pg_ddl.sql (o ETL aplica o CREATE
TABLE IF NOT EXISTS sozinho; o INSERT em `servicos` é manual).

Credenciais: DB_HOST_* / PG_RDS_* no /opt/relatorio_h_t/.env.

Uso:
    python3 export_vg_situacao_clientes.py --modo full      # rebuild completo (5h)
    python3 export_vg_situacao_clientes.py --modo light     # incremental (8-18h)
    python3 export_vg_situacao_clientes.py --modo light --dry-run
    python3 export_vg_situacao_clientes.py --inicio 01/03/2026
"""
import argparse
import os
import sys
import time
import traceback
from collections import defaultdict
from datetime import date, datetime

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

import psycopg2
from psycopg2.extras import execute_values, Json

from export_governanca import build_conns_from_env, make_engine
from etl_meta import ETLMeta

ADMISSAO_INICIO_DEFAULT = "01/01/2026"  # DD/MM/YYYY — views da CAMIM usam SET DATEFORMAT dmy
POSTOS_ORDER = ["N", "X", "Y", "M", "P", "D", "B", "I", "G", "R", "J", "C", "A"]
CHUNK = 500


# ── SQL Server ──────────────────────────────────────────────────────────────
# Datas passadas a VIEWS sempre em DD/MM/YYYY (regra crítica do projeto).

SQL_BASE = """
SELECT r.idcliente,
       c.dataadmissao,
       c.situacao,
       c.[SituaçãoClube],
       c.idade,
       c.sexo,
       c.responsavel,
       c.nome,
       r.[Valor devido],
       r.[Valor pago],
       c.TelefoneWhatsApp,
       c.TelefoneCelular
FROM vw_fin_receita2 r WITH (NOLOCK)
CROSS JOIN sis_empresa emp
LEFT JOIN vw_cad_cliente c ON c.idcliente = r.idcliente
WHERE c.dataadmissao >= ?
  AND r.idendereco = emp.idendereco
  AND r.idcontatipo = 5
"""

SQL_CAD_CLIENTE = """
SELECT idcliente, matricula, cpf
FROM cad_cliente WITH (NOLOCK)
WHERE idcliente IN ({ids})
"""

SQL_DEPENDENTES = """
SELECT d.idcliente, d.iddependente, d.nome, d.cpf, d.desativado
FROM cad_clientedependente d WITH (NOLOCK)
WHERE d.idcliente IN ({ids})
"""

# Consultas futuras (>= hoje, não faturado) — DETALHE (data/hora, médico, etc.).
# A qtd é derivada do len em Python; o detalhe vira JSON na linha (modal do front).
SQL_CONSULTAS_FUTURAS = """
SELECT ls.idcliente, ls.[Data e hora], ls.[Médico], ls.[Especialização], ls.[Servico]
FROM vw_Cad_LancamentoServicos ls
WHERE ls.[Situação] = 'normal'
  AND ls.LancadoNoFaturamento = 'Não'
  AND ls.classe LIKE 'consult%'
  AND ls.[Data e hora] >= ?
  AND ls.idcliente IN ({ids})
ORDER BY ls.[Data e hora]
"""

# Consultas desde o início da janela — DETALHE; filtradas em Python pelo dia da
# admissão de cada cliente. SEM filtro LancadoNoFaturamento: consulta passada já
# faturada continua contando como "usou o plano no dia da adesão".
SQL_CONSULTAS_POR_DIA = """
SELECT ls.idcliente, ls.[Data e hora], ls.[Médico], ls.[Especialização], ls.[Servico]
FROM vw_Cad_LancamentoServicos ls
WHERE ls.[Situação] = 'normal'
  AND ls.classe LIKE 'consult%'
  AND ls.[Data e hora] >= ?
  AND ls.idcliente IN ({ids})
ORDER BY ls.[Data e hora]
"""

# Mensalidades vencidas EM ABERTO de uma matrícula (qualquer posto):
# vencimento já passou, sem pagamento e a própria mensalidade não cancelada.
# CLIENTE CANCELADO: a dívida só vai até a data de cancelamento — mensalidades
# geradas DEPOIS do cancelamento não deveriam existir e não entram na conta
# (ex.: Guilherme cancelou 25/09/2018; conta até a ref. que venceu 02/09, não
# até 2019). Cliente ativo (CanceladoANS=0): sem cap.
# NB: usa CanceladoANS + DataCancelamento* — colunas presentes em TODOS os 13
# bancos. (CancelamentoANS_Calculada só existe no banco A, não dá pra usar.)
# Detalhe das 2 mensalidades vencidas MAIS ANTIGAS (por vencimento asc) de cada
# matrícula — mesmo que haja 200 em aberto, só as 2 mais antigas entram. Devolve
# id_receita, referência (mês da parcela) e valor (pro modal da Dívida ant.);
# qtd/valor são derivados em Python. ROW_NUMBER particiona por cliente; o resto
# dos filtros (PJ fora, cap no cancelamento) fica na subquery.
SQL_MENS_VENCIDAS = """
SELECT t.idCliente, t.id_receita, t.referencia, t.valor
FROM (
  SELECT r.idCliente,
         r.idReceita AS id_receita,
         r.[Data referencia] AS referencia,
         r.[Valor devido] AS valor,
         ROW_NUMBER() OVER (PARTITION BY r.idCliente
                            ORDER BY r.[Data de vencimento] ASC) AS rn
  FROM vw_Fin_Receita r
  JOIN cad_cliente c WITH (NOLOCK) ON c.idCliente = r.idCliente
  WHERE r.idCliente IN ({ids})
    AND r.idContaTipo = 5
    AND r.[Valor pago] IS NULL
    AND r.[Data de cancelamento] IS NULL
    AND r.[Data de vencimento] < ?
    AND (c.tipo IS NULL OR c.tipo <> 'J')   -- plano PJ: dívida é do empregador
    AND (
          ISNULL(c.CanceladoANS, 0) = 0
          OR COALESCE(c.DataCancelamentoANS, c.dataCanceladoANSmanual,
                      c.DataCancelamentoANSabi, c.DataCancelamentoAuto) IS NULL
          OR r.[Data de vencimento] <= COALESCE(c.DataCancelamentoANS, c.dataCanceladoANSmanual,
                      c.DataCancelamentoANSabi, c.DataCancelamentoAuto)
        )
) t
WHERE t.rn <= 2
ORDER BY t.idCliente, t.referencia
"""

# Busca de matrículas por CPF — espelha as 3 queries do endpoint
# GET /crm/cpf/{id_endereco} da api_fin_receita (camila3).
#
# IMPORTANTE: cad_cliente é REPLICADO em todos os bancos (matrícula de
# Anchieta também existe no banco de Campinho). O JOIN sis_empresa filtra
# cada banco só pras matrículas da PRÓPRIA filial (emp.idEndereco =
# c.idendereco) — sem ele, a mesma matrícula aparece 13x, uma por banco.
# Cada query traz também a data de cancelamento (COALESCE das variantes) e o
# `tipo` da MATRÍCULA anterior (F=física, J=jurídica). Em PJ a dívida é do
# empregador, não do beneficiário — zerada na montagem.
_CANCEL_COALESCE = ("COALESCE(c.DataCancelamentoANS, c.dataCanceladoANSmanual, "
                    "c.DataCancelamentoANSabi, c.DataCancelamentoAuto)")

SQL_CPF_TITULAR = """
SELECT c.cpf, c.idcliente, c.matricula, c.nome, c.dataadmissao, c.desativado,
       {cancel} AS data_cancel, c.tipo
FROM cad_cliente c WITH (NOLOCK)
JOIN sis_empresa emp ON emp.idEndereco = c.idendereco
WHERE c.cpf IN ({{cpfs}})
""".format(cancel=_CANCEL_COALESCE)

SQL_CPF_DEPENDENTE = """
SELECT d.cpf, d.idcliente, d.iddependente, c.matricula, d.nome, d.dataadmissao, d.desativado,
       {cancel} AS data_cancel, c.tipo
FROM cad_clientedependente d WITH (NOLOCK)
JOIN cad_cliente c WITH (NOLOCK) ON c.idcliente = d.idcliente
JOIN sis_empresa emp ON emp.idEndereco = c.idendereco
WHERE d.cpf IN ({{cpfs}})
""".format(cancel=_CANCEL_COALESCE)

SQL_CPF_RESPONSAVEL = """
SELECT c.responsavelcpf AS cpf, c.idcliente, c.matricula, c.nome, c.dataadmissao, c.desativado,
       {cancel} AS data_cancel, c.tipo
FROM cad_cliente c WITH (NOLOCK)
JOIN sis_empresa emp ON emp.idEndereco = c.idendereco
WHERE c.responsavelcpf IN ({{cpfs}})
""".format(cancel=_CANCEL_COALESCE)


# ── Postgres ────────────────────────────────────────────────────────────────

DDL = """
CREATE TABLE IF NOT EXISTS kpi_vg_situacao_clientes (
    id                        BIGSERIAL PRIMARY KEY,
    posto                     CHAR(1)       NOT NULL,
    id_cliente                INTEGER       NOT NULL,
    matricula                 VARCHAR(20),
    nome                      VARCHAR(200),
    cpf                       VARCHAR(20),
    data_admissao             DATE,
    situacao                  VARCHAR(80),
    situacao_clube            VARCHAR(80),
    idade                     INTEGER,
    sexo                      VARCHAR(20),
    responsavel               VARCHAR(200),
    telefone_whatsapp         VARCHAR(40),
    telefone_celular          VARCHAR(40),
    valor_devido              NUMERIC(14,2),
    valor_pago                NUMERIC(14,2),
    mensalidade               NUMERIC(14,2),
    receitas_qtd              INTEGER NOT NULL DEFAULT 0,
    dependentes_qtd           INTEGER NOT NULL DEFAULT 0,
    consultas_dia_adesao_qtd  INTEGER NOT NULL DEFAULT 0,
    usou_plano_dia_adesao     BOOLEAN NOT NULL DEFAULT FALSE,
    consultas_futuras_qtd     INTEGER NOT NULL DEFAULT 0,
    tem_consulta_futura       BOOLEAN NOT NULL DEFAULT FALSE,
    matriculas_anteriores_qtd INTEGER NOT NULL DEFAULT 0,
    mat_ant_titular_qtd       INTEGER NOT NULL DEFAULT 0,
    mat_ant_dependente_qtd    INTEGER NOT NULL DEFAULT 0,
    mat_ant_responsavel_qtd   INTEGER NOT NULL DEFAULT 0,
    mat_ant_vencidas_qtd      INTEGER NOT NULL DEFAULT 0,
    mat_ant_vencidas_valor    NUMERIC(14,2) NOT NULL DEFAULT 0,
    atualizado_em             TIMESTAMP NOT NULL DEFAULT now(),
    UNIQUE (posto, id_cliente)
);
ALTER TABLE kpi_vg_situacao_clientes ADD COLUMN IF NOT EXISTS mensalidade NUMERIC(14,2);
ALTER TABLE kpi_vg_situacao_clientes ADD COLUMN IF NOT EXISTS mat_ant_vencidas_qtd INTEGER NOT NULL DEFAULT 0;
ALTER TABLE kpi_vg_situacao_clientes ADD COLUMN IF NOT EXISTS mat_ant_vencidas_valor NUMERIC(14,2) NOT NULL DEFAULT 0;
ALTER TABLE kpi_vg_situacao_clientes ADD COLUMN IF NOT EXISTS consultas_adesao_json  JSONB NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE kpi_vg_situacao_clientes ADD COLUMN IF NOT EXISTS consultas_futuras_json JSONB NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE kpi_vg_situacao_clientes ADD COLUMN IF NOT EXISTS mat_ant_vencidas_json  JSONB NOT NULL DEFAULT '[]'::jsonb;
CREATE INDEX IF NOT EXISTS ix_vg_sit_cli_data_admissao ON kpi_vg_situacao_clientes (data_admissao);
CREATE INDEX IF NOT EXISTS ix_vg_sit_cli_posto ON kpi_vg_situacao_clientes (posto);
CREATE TABLE IF NOT EXISTS kpi_vg_matriculas_anteriores (
    id                       BIGSERIAL PRIMARY KEY,
    posto                    CHAR(1)     NOT NULL,
    id_cliente               INTEGER     NOT NULL,
    cpf                      VARCHAR(20),
    pessoa_nome              VARCHAR(200),
    papel_na_nova            VARCHAR(12) NOT NULL,
    posto_anterior           CHAR(1),
    id_cliente_anterior      INTEGER,
    id_dependente_anterior   INTEGER,
    matricula_anterior       VARCHAR(20),
    papel_anterior           VARCHAR(12),
    nome_anterior            VARCHAR(200),
    data_admissao_anterior   DATE,
    desativado_anterior      BOOLEAN,
    mens_vencidas_qtd        INTEGER NOT NULL DEFAULT 0,
    mens_vencidas_valor      NUMERIC(14,2) NOT NULL DEFAULT 0,
    atualizado_em            TIMESTAMP NOT NULL DEFAULT now()
);
ALTER TABLE kpi_vg_matriculas_anteriores ADD COLUMN IF NOT EXISTS mens_vencidas_qtd INTEGER NOT NULL DEFAULT 0;
ALTER TABLE kpi_vg_matriculas_anteriores ADD COLUMN IF NOT EXISTS mens_vencidas_valor NUMERIC(14,2) NOT NULL DEFAULT 0;
ALTER TABLE kpi_vg_matriculas_anteriores ADD COLUMN IF NOT EXISTS data_cancelamento_anterior DATE;
CREATE INDEX IF NOT EXISTS ix_vg_mat_ant_cliente ON kpi_vg_matriculas_anteriores (posto, id_cliente);
"""

COLS_PRINCIPAL = [
    "posto", "id_cliente", "matricula", "nome", "cpf", "data_admissao",
    "situacao", "situacao_clube", "idade", "sexo", "responsavel",
    "telefone_whatsapp", "telefone_celular", "valor_devido", "valor_pago",
    "mensalidade", "receitas_qtd", "dependentes_qtd",
    "consultas_dia_adesao_qtd", "usou_plano_dia_adesao",
    "consultas_futuras_qtd", "tem_consulta_futura",
    "matriculas_anteriores_qtd", "mat_ant_titular_qtd",
    "mat_ant_dependente_qtd", "mat_ant_responsavel_qtd",
    "mat_ant_vencidas_qtd", "mat_ant_vencidas_valor",
    "consultas_adesao_json", "consultas_futuras_json",
    "mat_ant_vencidas_json",
]

COLS_DETALHE = [
    "posto", "id_cliente", "cpf", "pessoa_nome", "papel_na_nova",
    "posto_anterior", "id_cliente_anterior", "id_dependente_anterior",
    "matricula_anterior", "papel_anterior", "nome_anterior",
    "data_admissao_anterior", "desativado_anterior", "data_cancelamento_anterior",
    "mens_vencidas_qtd", "mens_vencidas_valor",
]

# Colunas derivadas da busca CARA (CPF cross-bank). No run leve só são gravadas
# para clientes NOVOS; nas linhas já existentes ficam intocadas.
HEAVY_COLS = [
    "matriculas_anteriores_qtd", "mat_ant_titular_qtd",
    "mat_ant_dependente_qtd", "mat_ant_responsavel_qtd",
    "mat_ant_vencidas_qtd", "mat_ant_vencidas_valor",
    "mat_ant_vencidas_json",
]

# Colunas atualizadas no run leve (tudo menos a chave e as pesadas acima).
LIGHT_COLS = [c for c in COLS_PRINCIPAL if c not in ("posto", "id_cliente", *HEAVY_COLS)]


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


# ── Helpers ─────────────────────────────────────────────────────────────────

def chunked(seq, size=CHUNK):
    seq = list(seq)
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def to_date(v):
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    return None


def to_bool(v):
    if v is None:
        return None
    try:
        return bool(int(v))
    except (TypeError, ValueError):
        return bool(v)


def clean_str(v, maxlen=None):
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    return s[:maxlen] if maxlen else s


def cpf_pesquisavel(cpf):
    """CPF vale pesquisa global? Evita joins explosivos com lixo de cadastro
    ('', '0', '000.000.000-00' etc.). Mantém o valor BRUTO pra busca (o banco
    guarda como digitado — mesma semântica do endpoint da camila3)."""
    if not cpf:
        return False
    digits = "".join(ch for ch in str(cpf) if ch.isdigit())
    if len(digits) < 8:
        return False
    if len(set(digits)) == 1:  # 000..., 111...
        return False
    return True


def run_query(engine, sql, params=()):
    with engine.connect() as conn:
        cur = conn.exec_driver_sql(sql, params)
        return cur.fetchall()


def _consulta_dict(dh, med, esp, serv):
    """Linha de consulta (adesão ou futura) pro JSON do modal."""
    return {
        "data_hora": dh.strftime("%Y-%m-%d %H:%M") if hasattr(dh, "strftime") else (str(dh) if dh else None),
        "medico": clean_str(med, 200),
        "especializacao": clean_str(esp, 120),
        "servico": clean_str(serv, 200),
    }


# ── Fase 1: base por posto ──────────────────────────────────────────────────

def carregar_posto(engine, posto, adm_inicio_str, hoje_str):
    """Retorna (clientes: dict idcliente -> row dict, cpf_owners: list)."""
    rows = run_query(engine, SQL_BASE, (adm_inicio_str,))
    clientes = {}
    for r in rows:
        (idc, dataadm, situacao, sitclube, idade, sexo, resp, nome,
         vdev, vpago, telwpp, telcel) = r
        if idc is None:
            continue
        idc = int(idc)
        cli = clientes.get(idc)
        if cli is None:
            cli = clientes[idc] = {
                "posto": posto, "id_cliente": idc,
                "matricula": None, "nome": clean_str(nome, 200),
                "cpf": None,
                "data_admissao": to_date(dataadm),
                "situacao": clean_str(situacao, 80),
                "situacao_clube": clean_str(sitclube, 80),
                "idade": int(idade) if idade is not None else None,
                "sexo": clean_str(sexo, 20),
                "responsavel": clean_str(resp, 200),
                "telefone_whatsapp": clean_str(telwpp, 40),
                "telefone_celular": clean_str(telcel, 40),
                "valor_devido": 0, "valor_pago": 0, "mensalidade": 0,
                "receitas_qtd": 0,
                "dependentes_qtd": 0,
                "consultas_dia_adesao_qtd": 0, "consultas_futuras_qtd": 0,
                "consultas_adesao": [], "consultas_futuras": [],
                "mat_ant": {"titular": set(), "dependente": set(), "responsavel": set()},
                "cpfs": [],  # (cpf, papel_na_nova, pessoa_nome)
            }
        # Mais de uma receita idcontatipo=5 pro mesmo cliente (parcelas): soma
        # valores e guarda em `mensalidade` o [Valor devido] de UMA parcela
        # (max — se a 1ª tiver desconto pro-rata, pega a cheia)
        v = float(vdev or 0)
        cli["valor_devido"] += v
        cli["valor_pago"] += float(vpago or 0)
        cli["mensalidade"] = max(cli["mensalidade"], v)
        cli["receitas_qtd"] += 1

    if not clientes:
        return clientes

    ids = sorted(clientes.keys())

    # matrícula + CPF do titular (tabela cad_cliente — fora da view)
    for chunk in chunked(ids):
        ids_sql = ",".join(str(i) for i in chunk)
        for idc, matricula, cpf in run_query(engine, SQL_CAD_CLIENTE.format(ids=ids_sql)):
            cli = clientes.get(int(idc))
            if not cli:
                continue
            cli["matricula"] = clean_str(matricula, 20)
            cli["cpf"] = clean_str(cpf, 20)
            if cpf_pesquisavel(cli["cpf"]):
                cli["cpfs"].append((cli["cpf"], "titular", cli["nome"]))

    # dependentes: qtd + CPFs
    for chunk in chunked(ids):
        ids_sql = ",".join(str(i) for i in chunk)
        for idc, _iddep, dnome, dcpf, _desat in run_query(engine, SQL_DEPENDENTES.format(ids=ids_sql)):
            cli = clientes.get(int(idc))
            if not cli:
                continue
            cli["dependentes_qtd"] += 1
            dcpf = clean_str(dcpf, 20)
            if cpf_pesquisavel(dcpf):
                cli["cpfs"].append((dcpf, "dependente", clean_str(dnome, 200)))

    # consultas futuras (>= hoje, não faturado) — detalhe
    for chunk in chunked(ids):
        ids_sql = ",".join(str(i) for i in chunk)
        for idc, dh, med, esp, serv in run_query(engine, SQL_CONSULTAS_FUTURAS.format(ids=ids_sql), (hoje_str,)):
            cli = clientes.get(int(idc))
            if cli:
                cli["consultas_futuras"].append(_consulta_dict(dh, med, esp, serv))

    # consultas no dia da adesão — detalhe (filtra pelo dia da admissão)
    for chunk in chunked(ids):
        ids_sql = ",".join(str(i) for i in chunk)
        for idc, dh, med, esp, serv in run_query(engine, SQL_CONSULTAS_POR_DIA.format(ids=ids_sql), (adm_inicio_str,)):
            cli = clientes.get(int(idc))
            if cli and cli["data_admissao"] and to_date(dh) == cli["data_admissao"]:
                cli["consultas_adesao"].append(_consulta_dict(dh, med, esp, serv))

    for cli in clientes.values():
        cli["consultas_futuras_qtd"] = len(cli["consultas_futuras"])
        cli["consultas_dia_adesao_qtd"] = len(cli["consultas_adesao"])

    return clientes


# ── Fase 2: matrículas anteriores por CPF, em todos os postos ───────────────

def buscar_cpfs_no_posto(engine, posto_alvo, cpfs):
    """Roda as 3 queries (titular/dependente/responsável) pro conjunto de CPFs.
    Retorna lista de matches: (cpf, papel, idcliente, iddependente, matricula,
    nome, dataadmissao, desativado, posto_alvo, data_cancel, tipo)."""
    matches = []
    for chunk in chunked(sorted(cpfs)):
        marks = ",".join("?" for _ in chunk)
        params = tuple(chunk)
        for cpf, idc, mat, nome, adm, desat, dcanc, tipo in run_query(
                engine, SQL_CPF_TITULAR.format(cpfs=marks), params):
            matches.append((clean_str(cpf, 20), "titular", int(idc), None,
                            clean_str(mat, 20), clean_str(nome, 200),
                            to_date(adm), to_bool(desat), posto_alvo,
                            to_date(dcanc), clean_str(tipo, 4)))
        for cpf, idc, iddep, mat, nome, adm, desat, dcanc, tipo in run_query(
                engine, SQL_CPF_DEPENDENTE.format(cpfs=marks), params):
            matches.append((clean_str(cpf, 20), "dependente", int(idc),
                            int(iddep) if iddep is not None else None,
                            clean_str(mat, 20), clean_str(nome, 200),
                            to_date(adm), to_bool(desat), posto_alvo,
                            to_date(dcanc), clean_str(tipo, 4)))
        for cpf, idc, mat, nome, adm, desat, dcanc, tipo in run_query(
                engine, SQL_CPF_RESPONSAVEL.format(cpfs=marks), params):
            matches.append((clean_str(cpf, 20), "responsavel", int(idc), None,
                            clean_str(mat, 20), clean_str(nome, 200),
                            to_date(adm), to_bool(desat), posto_alvo,
                            to_date(dcanc), clean_str(tipo, 4)))
    return matches


# ── Pipeline por posto ───────────────────────────────────────────────────────
# Cada posto é processado e GRAVADO antes de começar o próximo (pedido do
# usuário 2026-06-12): a página vai sendo atualizada posto a posto, e uma
# falha no meio não atrasa os postos já concluídos.

def buscar_anteriores(engines, p, hoje_str, cpf_set, meta):
    """Pesquisa os CPFs de `cpf_set` em TODOS os bancos (titular/dependente/
    responsável) + mensalidades vencidas das matrículas achadas. É a parte CARA
    do ETL (cross-bank). Retorna (matches_por_cpf, vencidas, rc)."""
    rc = 0
    matches_por_cpf = defaultdict(list)
    vencidas_det = defaultdict(list)  # (posto, idcliente) -> [{id_receita, mes, valor}]
    if not cpf_set:
        return matches_por_cpf, vencidas_det, rc

    t0 = time.time()
    for q in POSTOS_ORDER:
        if q not in engines:
            continue
        try:
            for m in buscar_cpfs_no_posto(engines[q], q, cpf_set):
                matches_por_cpf[m[0]].append(m)
        except Exception as e:
            # matrículas anteriores deste posto ficam INCOMPLETAS (sem as
            # passagens do banco q) — sinaliza e segue
            rc = 2
            traceback.print_exc()
            meta.error(q, f"[{p}] busca CPF no banco {q}: {e}")
    print(f"[{p}] busca CPF OK  cpfs={len(cpf_set)}  elapsed={time.time()-t0:.1f}s", flush=True)

    # mensalidades vencidas em aberto das matrículas anteriores encontradas
    ids_ant_por_posto = defaultdict(set)
    for lst in matches_por_cpf.values():
        for m in lst:
            ids_ant_por_posto[m[8]].add(m[2])
    t0 = time.time()
    for q, ids in sorted(ids_ant_por_posto.items()):
        if q not in engines:
            continue
        try:
            for chunk in chunked(sorted(ids)):
                ids_sql = ",".join(str(i) for i in chunk)
                for idc, idrec, ref, valor in run_query(
                        engines[q], SQL_MENS_VENCIDAS.format(ids=ids_sql), (hoje_str,)):
                    vencidas_det[(q, int(idc))].append({
                        "id_receita": int(idrec) if idrec is not None else None,
                        "mes": ref.strftime("%m/%Y") if hasattr(ref, "strftime") else None,
                        "valor": float(valor or 0),
                    })
        except Exception as e:
            rc = 2
            traceback.print_exc()
            meta.error(q, f"[{p}] mens vencidas no banco {q}: {e}")
    print(f"[{p}] mens vencidas OK  elapsed={time.time()-t0:.1f}s", flush=True)
    return matches_por_cpf, vencidas_det, rc


def montar_rows(clientes_iter, p, matches_por_cpf, vencidas_det):
    """Monta (rows_principal, rows_detalhe) para os clientes dados. Quando
    `matches_por_cpf`/`vencidas_det` vêm vazios (UPDATE leve de quem já existe),
    as colunas de matrícula anterior saem zeradas — e o chamador as descarta.

    `vencidas_det`: (posto, idc) -> lista de {id_receita, mes, valor} (as 2
    parcelas vencidas mais antigas, já sem PJ e capadas no cancelamento)."""
    rows_principal = []
    rows_detalhe = []
    for cli in clientes_iter:
        vistos_detalhe = set()
        info_ant = {}  # (posto_ant, idc_ant) -> (mat_ant, nome_ant) p/ montar o json sem duplicar
        for cpf, papel_novo, pessoa_nome in cli["cpfs"]:
            for (mcpf, papel_ant, idc_ant, iddep_ant, mat_ant, nome_ant,
                 adm_ant, desat_ant, posto_ant, dcanc_ant, tipo_ant) in matches_por_cpf.get(cpf, ()):
                # exclui a própria matrícula nova
                if posto_ant == p and idc_ant == cli["id_cliente"]:
                    continue
                cli["mat_ant"][papel_ant].add((posto_ant, idc_ant))
                info_ant[(posto_ant, idc_ant)] = (mat_ant, nome_ant)
                key = (cpf, papel_ant, posto_ant, idc_ant, iddep_ant)
                if key in vistos_detalhe:
                    continue
                vistos_detalhe.add(key)
                # plano PJ (tipo J): dívida é do empregador — sem parcelas
                ehpj = (tipo_ant or "").strip().upper() == "J"
                det = [] if ehpj else vencidas_det.get((posto_ant, idc_ant), [])
                rows_detalhe.append((
                    p, cli["id_cliente"], cpf, pessoa_nome, papel_novo,
                    posto_ant, idc_ant, iddep_ant, mat_ant, papel_ant,
                    nome_ant, adm_ant, desat_ant, dcanc_ant,
                    len(det), round(sum(d["valor"] for d in det), 2),
                ))

        todas = cli["mat_ant"]["titular"] | cli["mat_ant"]["dependente"] | cli["mat_ant"]["responsavel"]
        # Dívida agregada (parcelas das 2 mais antigas de cada matrícula anterior)
        venc_json = []
        venc_qtd = 0
        venc_val = 0.0
        for t in todas:
            det = vencidas_det.get(t, [])  # PJ não está no dict (query exclui)
            mat_ant, nome_ant = info_ant.get(t, (None, None))
            for d in det:
                venc_json.append({
                    "id_receita": d["id_receita"], "posto": t[0], "matricula": mat_ant,
                    "nome": nome_ant, "mes": d["mes"], "valor": d["valor"],
                })
            venc_qtd += len(det)
            venc_val += sum(d["valor"] for d in det)
        venc_val = round(venc_val, 2)
        rows_principal.append((
            p, cli["id_cliente"], cli["matricula"], cli["nome"], cli["cpf"],
            cli["data_admissao"], cli["situacao"], cli["situacao_clube"],
            cli["idade"], cli["sexo"], cli["responsavel"],
            cli["telefone_whatsapp"], cli["telefone_celular"],
            round(cli["valor_devido"], 2), round(cli["valor_pago"], 2),
            round(cli["mensalidade"], 2),
            cli["receitas_qtd"], cli["dependentes_qtd"],
            cli["consultas_dia_adesao_qtd"], cli["consultas_dia_adesao_qtd"] > 0,
            cli["consultas_futuras_qtd"], cli["consultas_futuras_qtd"] > 0,
            len(todas),
            len(cli["mat_ant"]["titular"]),
            len(cli["mat_ant"]["dependente"]),
            len(cli["mat_ant"]["responsavel"]),
            venc_qtd, venc_val,
            Json(cli["consultas_adesao"]), Json(cli["consultas_futuras"]),
            Json(venc_json),
        ))
    return rows_principal, rows_detalhe


def _upsert_principal(cur, rows, set_cols):
    """INSERT ... ON CONFLICT (posto, id_cliente) DO UPDATE só das `set_cols`.
    Colunas fora de `set_cols` (ex.: as pesadas, no run leve) ficam intocadas."""
    if not rows:
        return
    set_clause = ", ".join(f"{col}=EXCLUDED.{col}" for col in set_cols)
    set_clause += ", atualizado_em=now()"
    execute_values(
        cur,
        f"INSERT INTO kpi_vg_situacao_clientes ({', '.join(COLS_PRINCIPAL)}) VALUES %s "
        f"ON CONFLICT (posto, id_cliente) DO UPDATE SET {set_clause}",
        rows, page_size=1000,
    )


def _ids_existentes(pg, posto):
    with pg.cursor() as c:
        c.execute("SELECT id_cliente FROM kpi_vg_situacao_clientes WHERE posto = %s", (posto,))
        return {int(r[0]) for r in c.fetchall()}


def processar_posto(p, engines, pg, adm_inicio_str, hoje_str, meta, dry_run, modo):
    """modo='full': recalcula o posto inteiro (DELETE+INSERT, busca CPF de todos).
    modo='light': UPDATE só das colunas leves de quem já existe + busca CPF (cara)
    apenas dos clientes NOVOS (sem linha ainda). Retorna (rc, n_clientes)."""
    rc = 0
    clientes = carregar_posto(engines[p], p, adm_inicio_str, hoje_str)
    print(f"[{p}] base OK  clientes={len(clientes)}  modo={modo}", flush=True)

    if modo == "full":
        cpf_set = set()
        for cli in clientes.values():
            for cpf, _papel, _nome in cli["cpfs"]:
                cpf_set.add(cpf)
        matches, vencidas, rc = buscar_anteriores(engines, p, hoje_str, cpf_set, meta)
        rows_principal, rows_detalhe = montar_rows(clientes.values(), p, matches, vencidas)

        if dry_run:
            print(f"[{p}] [dry-run full] principal={len(rows_principal)}  detalhe={len(rows_detalhe)}", flush=True)
            return rc, len(clientes)

        # grava o posto (delete + insert numa transação) e segue pro próximo
        try:
            with pg.cursor() as c:
                c.execute("DELETE FROM kpi_vg_matriculas_anteriores WHERE posto = %s", (p,))
                c.execute("DELETE FROM kpi_vg_situacao_clientes WHERE posto = %s", (p,))
                if rows_principal:
                    execute_values(
                        c,
                        f"INSERT INTO kpi_vg_situacao_clientes ({', '.join(COLS_PRINCIPAL)}) VALUES %s",
                        rows_principal, page_size=1000,
                    )
                if rows_detalhe:
                    execute_values(
                        c,
                        f"INSERT INTO kpi_vg_matriculas_anteriores ({', '.join(COLS_DETALHE)}) VALUES %s",
                        rows_detalhe, page_size=1000,
                    )
            pg.commit()
            print(f"[{p}] GRAVADO (full)  principal={len(rows_principal)}  detalhe={len(rows_detalhe)}", flush=True)
        except Exception:
            pg.rollback()
            raise
        return rc, len(clientes)

    # ── modo light ──────────────────────────────────────────────────────────
    existentes_ids = _ids_existentes(pg, p)
    novos      = [cli for idc, cli in clientes.items() if idc not in existentes_ids]
    existentes = [cli for idc, cli in clientes.items() if idc in existentes_ids]

    # busca CARA só dos CPFs dos clientes NOVOS
    cpf_novos = set()
    for cli in novos:
        for cpf, _papel, _nome in cli["cpfs"]:
            cpf_novos.add(cpf)
    matches, vencidas, rc = buscar_anteriores(engines, p, hoje_str, cpf_novos, meta)

    rows_novos, rows_detalhe_novos = montar_rows(novos, p, matches, vencidas)
    # existentes: colunas pesadas saem zeradas, mas o ON CONFLICT só grava LIGHT_COLS
    rows_existentes, _ = montar_rows(existentes, p, {}, {})

    if dry_run:
        print(f"[{p}] [dry-run light] update_leve={len(rows_existentes)}  "
              f"novos={len(novos)} (busca CPF + insert)  detalhe_novos={len(rows_detalhe_novos)}", flush=True)
        return rc, len(clientes)

    try:
        with pg.cursor() as c:
            # quem já existe: atualiza só as colunas leves (preserva matr. anteriores)
            _upsert_principal(c, rows_existentes, LIGHT_COLS)
            # novos: insere linha completa (leve + pesado)
            _upsert_principal(c, rows_novos, LIGHT_COLS + HEAVY_COLS)
            # detalhe só dos novos (não toca no detalhe de quem já existe)
            if novos:
                ids_novos = [cli["id_cliente"] for cli in novos]
                c.execute(
                    "DELETE FROM kpi_vg_matriculas_anteriores WHERE posto = %s AND id_cliente = ANY(%s)",
                    (p, ids_novos),
                )
            if rows_detalhe_novos:
                execute_values(
                    c,
                    f"INSERT INTO kpi_vg_matriculas_anteriores ({', '.join(COLS_DETALHE)}) VALUES %s",
                    rows_detalhe_novos, page_size=1000,
                )
        pg.commit()
        print(f"[{p}] GRAVADO (light)  update_leve={len(rows_existentes)}  "
              f"novos={len(rows_novos)}  detalhe_novos={len(rows_detalhe_novos)}", flush=True)
    except Exception:
        pg.rollback()
        raise
    return rc, len(clientes)


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="não escreve no Postgres, só loga o que faria")
    ap.add_argument("--inicio", default=ADMISSAO_INICIO_DEFAULT,
                    help="data de admissão inicial em DD/MM/YYYY (default 01/01/2026)")
    ap.add_argument("--modo", choices=["full", "light"], default="full",
                    help="full: rebuild completo (cron 5h). "
                         "light: UPDATE leve + busca CPF só de novos (cron 8-18h)")
    args = ap.parse_args()

    modo = args.modo
    adm_inicio_str = args.inicio
    hoje_str = date.today().strftime("%d/%m/%Y")  # DD/MM/YYYY — view
    print(f"== export_vg_situacao_clientes  modo={modo}  inicio={adm_inicio_str}"
          f"{'  [dry-run]' if args.dry_run else ''} ==", flush=True)

    meta = ETLMeta("export_vg_situacao_clientes", "json_consolidado")
    conns = build_conns_from_env()
    rc = 0

    engines = {}
    for p in POSTOS_ORDER:
        if p in conns:
            engines[p] = make_engine(conns[p])
        else:
            print(f"[{p}] sem conn no .env — pulando", flush=True)

    # light precisa do Postgres pra ler quem já existe (mesmo em dry-run, só leitura).
    pg = None
    if not args.dry_run or modo == "light":
        pg = pg_conn()
        if not args.dry_run:
            with pg.cursor() as c:
                c.execute(DDL)
            pg.commit()

    try:
        for p in POSTOS_ORDER:
            if p not in engines:
                continue
            t0 = time.time()
            try:
                rc_p, n_cli = processar_posto(
                    p, engines, pg, adm_inicio_str, hoje_str, meta, args.dry_run, modo)
                rc = max(rc, rc_p)
                meta.ok(p, clientes=n_cli)
                print(f"[{p}] POSTO CONCLUÍDO  clientes={n_cli}  elapsed={time.time()-t0:.1f}s", flush=True)
            except Exception as e:
                rc = 2
                traceback.print_exc()
                meta.error(p, f"posto {p}: {e}")
    finally:
        if pg is not None:
            pg.close()
        meta.save()

    print("Fim.", flush=True)
    return rc


if __name__ == "__main__":
    sys.exit(main())
