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

Full rebuild: "consultas futuras" é relativo a HOJE, então o ETL recalcula a
janela inteira a cada execução (DELETE por posto + INSERT). Não é incremental.

Cron sugerido: 1x/dia de madrugada. Leitura pura no SQL Server; escrita só no
Postgres do KPI. Sem efeitos colaterais com custo (sem WhatsApp/SMS/INSERT CAMIM).

DDL: sql_camim_fin/vg_situacao_clientes_pg_ddl.sql (o ETL aplica o CREATE
TABLE IF NOT EXISTS sozinho; o INSERT em `servicos` é manual).

Credenciais: DB_HOST_* / PG_RDS_* no /opt/relatorio_h_t/.env.

Uso:
    python3 export_vg_situacao_clientes.py             # run completo
    python3 export_vg_situacao_clientes.py --dry-run   # não escreve no Postgres
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
from psycopg2.extras import execute_values

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

SQL_CONSULTAS_FUTURAS = """
SELECT ls.idcliente, COUNT(*) AS qtd
FROM vw_Cad_LancamentoServicos ls
WHERE ls.[Situação] = 'normal'
  AND ls.LancadoNoFaturamento = 'Não'
  AND ls.classe LIKE 'consult%'
  AND ls.[Data e hora] >= ?
  AND ls.idcliente IN ({ids})
GROUP BY ls.idcliente
"""

# Consultas por dia desde o início da janela — comparadas em Python com a
# dataadmissao de cada cliente. SEM filtro LancadoNoFaturamento: consulta
# passada já faturada continua contando como "usou o plano no dia da adesão".
SQL_CONSULTAS_POR_DIA = """
SELECT ls.idcliente, CONVERT(date, ls.[Data e hora]) AS dia, COUNT(*) AS qtd
FROM vw_Cad_LancamentoServicos ls
WHERE ls.[Situação] = 'normal'
  AND ls.classe LIKE 'consult%'
  AND ls.[Data e hora] >= ?
  AND ls.idcliente IN ({ids})
GROUP BY ls.idcliente, CONVERT(date, ls.[Data e hora])
"""

# Busca de matrículas por CPF — espelha as 3 queries do endpoint
# GET /crm/cpf/{id_endereco} da api_fin_receita (camila3).
SQL_CPF_TITULAR = """
SELECT c.cpf, c.idcliente, c.matricula, c.nome, c.dataadmissao, c.desativado
FROM cad_cliente c WITH (NOLOCK)
WHERE c.cpf IN ({cpfs})
"""

SQL_CPF_DEPENDENTE = """
SELECT d.cpf, d.idcliente, d.iddependente, c.matricula, d.nome, d.dataadmissao, d.desativado
FROM cad_clientedependente d WITH (NOLOCK)
JOIN cad_cliente c WITH (NOLOCK) ON c.idcliente = d.idcliente
WHERE d.cpf IN ({cpfs})
"""

SQL_CPF_RESPONSAVEL = """
SELECT c.responsavelcpf AS cpf, c.idcliente, c.matricula, c.nome, c.dataadmissao, c.desativado
FROM cad_cliente c WITH (NOLOCK)
WHERE c.responsavelcpf IN ({cpfs})
"""


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
    atualizado_em             TIMESTAMP NOT NULL DEFAULT now(),
    UNIQUE (posto, id_cliente)
);
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
    atualizado_em            TIMESTAMP NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_vg_mat_ant_cliente ON kpi_vg_matriculas_anteriores (posto, id_cliente);
"""

COLS_PRINCIPAL = [
    "posto", "id_cliente", "matricula", "nome", "cpf", "data_admissao",
    "situacao", "situacao_clube", "idade", "sexo", "responsavel",
    "telefone_whatsapp", "telefone_celular", "valor_devido", "valor_pago",
    "receitas_qtd", "dependentes_qtd",
    "consultas_dia_adesao_qtd", "usou_plano_dia_adesao",
    "consultas_futuras_qtd", "tem_consulta_futura",
    "matriculas_anteriores_qtd", "mat_ant_titular_qtd",
    "mat_ant_dependente_qtd", "mat_ant_responsavel_qtd",
]

COLS_DETALHE = [
    "posto", "id_cliente", "cpf", "pessoa_nome", "papel_na_nova",
    "posto_anterior", "id_cliente_anterior", "id_dependente_anterior",
    "matricula_anterior", "papel_anterior", "nome_anterior",
    "data_admissao_anterior", "desativado_anterior",
]


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
                "valor_devido": 0, "valor_pago": 0, "receitas_qtd": 0,
                "dependentes_qtd": 0,
                "consultas_dia_adesao_qtd": 0, "consultas_futuras_qtd": 0,
                "mat_ant": {"titular": set(), "dependente": set(), "responsavel": set()},
                "cpfs": [],  # (cpf, papel_na_nova, pessoa_nome)
            }
        # Mais de uma receita idcontatipo=5 pro mesmo cliente: soma valores
        cli["valor_devido"] += float(vdev or 0)
        cli["valor_pago"] += float(vpago or 0)
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

    # consultas futuras (>= hoje, não faturado)
    for chunk in chunked(ids):
        ids_sql = ",".join(str(i) for i in chunk)
        for idc, qtd in run_query(engine, SQL_CONSULTAS_FUTURAS.format(ids=ids_sql), (hoje_str,)):
            cli = clientes.get(int(idc))
            if cli:
                cli["consultas_futuras_qtd"] = int(qtd or 0)

    # consultas no dia da adesão
    for chunk in chunked(ids):
        ids_sql = ",".join(str(i) for i in chunk)
        for idc, dia, qtd in run_query(engine, SQL_CONSULTAS_POR_DIA.format(ids=ids_sql), (adm_inicio_str,)):
            cli = clientes.get(int(idc))
            if cli and cli["data_admissao"] and to_date(dia) == cli["data_admissao"]:
                cli["consultas_dia_adesao_qtd"] += int(qtd or 0)

    return clientes


# ── Fase 2: matrículas anteriores por CPF, em todos os postos ───────────────

def buscar_cpfs_no_posto(engine, posto_alvo, cpfs):
    """Roda as 3 queries (titular/dependente/responsável) pro conjunto de CPFs.
    Retorna lista de matches: (cpf, papel, idcliente, iddependente, matricula,
    nome, dataadmissao, desativado, posto_alvo)."""
    matches = []
    for chunk in chunked(sorted(cpfs)):
        marks = ",".join("?" for _ in chunk)
        params = tuple(chunk)
        for cpf, idc, mat, nome, adm, desat in run_query(
                engine, SQL_CPF_TITULAR.format(cpfs=marks), params):
            matches.append((clean_str(cpf, 20), "titular", int(idc), None,
                            clean_str(mat, 20), clean_str(nome, 200),
                            to_date(adm), to_bool(desat), posto_alvo))
        for cpf, idc, iddep, mat, nome, adm, desat in run_query(
                engine, SQL_CPF_DEPENDENTE.format(cpfs=marks), params):
            matches.append((clean_str(cpf, 20), "dependente", int(idc),
                            int(iddep) if iddep is not None else None,
                            clean_str(mat, 20), clean_str(nome, 200),
                            to_date(adm), to_bool(desat), posto_alvo))
        for cpf, idc, mat, nome, adm, desat in run_query(
                engine, SQL_CPF_RESPONSAVEL.format(cpfs=marks), params):
            matches.append((clean_str(cpf, 20), "responsavel", int(idc), None,
                            clean_str(mat, 20), clean_str(nome, 200),
                            to_date(adm), to_bool(desat), posto_alvo))
    return matches


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="não escreve no Postgres, só loga o que faria")
    ap.add_argument("--inicio", default=ADMISSAO_INICIO_DEFAULT,
                    help="data de admissão inicial em DD/MM/YYYY (default 01/01/2026)")
    args = ap.parse_args()

    adm_inicio_str = args.inicio
    hoje_str = date.today().strftime("%d/%m/%Y")  # DD/MM/YYYY — view

    meta = ETLMeta("export_vg_situacao_clientes", "json_consolidado")
    conns = build_conns_from_env()
    engines = {}
    rc = 0

    # ── Fase 1: base de admissões por posto ──
    dados = {}        # posto -> {idcliente: cli}
    postos_ok = []
    for p in POSTOS_ORDER:
        if p not in conns:
            print(f"[{p}] sem conn no .env — pulando", flush=True)
            continue
        t0 = time.time()
        try:
            engines[p] = make_engine(conns[p])
            dados[p] = carregar_posto(engines[p], p, adm_inicio_str, hoje_str)
            postos_ok.append(p)
            print(f"[{p}] base OK  clientes={len(dados[p])}  elapsed={time.time()-t0:.1f}s", flush=True)
        except Exception as e:
            rc = 2
            traceback.print_exc()
            meta.error(p, f"fase base: {e}")

    # ── Fase 2: busca global de CPFs (matrículas anteriores) ──
    cpf_set = set()
    for p in postos_ok:
        for cli in dados[p].values():
            for cpf, _papel, _nome in cli["cpfs"]:
                cpf_set.add(cpf)
    print(f"CPFs distintos pra pesquisar: {len(cpf_set)}", flush=True)

    matches_por_cpf = defaultdict(list)
    for p in POSTOS_ORDER:
        if p not in engines:
            continue
        t0 = time.time()
        try:
            for m in buscar_cpfs_no_posto(engines[p], p, cpf_set):
                matches_por_cpf[m[0]].append(m)
            print(f"[{p}] busca CPF OK  elapsed={time.time()-t0:.1f}s", flush=True)
        except Exception as e:
            # Contagem de matrículas anteriores fica INCOMPLETA neste run
            # (faltam as passagens deste posto). Sinaliza no meta e segue.
            rc = 2
            traceback.print_exc()
            meta.error(p, f"fase busca CPF: {e}")

    # ── Fase 3: montar linhas ──
    rows_principal = []
    rows_detalhe = []
    for p in postos_ok:
        for cli in dados[p].values():
            vistos_detalhe = set()
            for cpf, papel_novo, pessoa_nome in cli["cpfs"]:
                for (mcpf, papel_ant, idc_ant, iddep_ant, mat_ant, nome_ant,
                     adm_ant, desat_ant, posto_ant) in matches_por_cpf.get(cpf, ()):
                    # exclui a própria matrícula nova
                    if posto_ant == p and idc_ant == cli["id_cliente"]:
                        continue
                    cli["mat_ant"][papel_ant].add((posto_ant, idc_ant))
                    key = (cpf, papel_ant, posto_ant, idc_ant, iddep_ant)
                    if key in vistos_detalhe:
                        continue
                    vistos_detalhe.add(key)
                    rows_detalhe.append((
                        p, cli["id_cliente"], cpf, pessoa_nome, papel_novo,
                        posto_ant, idc_ant, iddep_ant, mat_ant, papel_ant,
                        nome_ant, adm_ant, desat_ant,
                    ))

            todas = cli["mat_ant"]["titular"] | cli["mat_ant"]["dependente"] | cli["mat_ant"]["responsavel"]
            rows_principal.append((
                p, cli["id_cliente"], cli["matricula"], cli["nome"], cli["cpf"],
                cli["data_admissao"], cli["situacao"], cli["situacao_clube"],
                cli["idade"], cli["sexo"], cli["responsavel"],
                cli["telefone_whatsapp"], cli["telefone_celular"],
                round(cli["valor_devido"], 2), round(cli["valor_pago"], 2),
                cli["receitas_qtd"], cli["dependentes_qtd"],
                cli["consultas_dia_adesao_qtd"], cli["consultas_dia_adesao_qtd"] > 0,
                cli["consultas_futuras_qtd"], cli["consultas_futuras_qtd"] > 0,
                len(todas),
                len(cli["mat_ant"]["titular"]),
                len(cli["mat_ant"]["dependente"]),
                len(cli["mat_ant"]["responsavel"]),
            ))

    print(f"Linhas: principal={len(rows_principal)}  detalhe={len(rows_detalhe)}  postos_ok={postos_ok}", flush=True)

    if args.dry_run:
        print("[dry-run] nada gravado no Postgres", flush=True)
        for r in rows_principal[:5]:
            print("[dry-run] exemplo:", r, flush=True)
        meta.save()
        return rc

    if not postos_ok:
        print("nenhum posto processado — não escrevo no Postgres", flush=True)
        meta.save()
        return 2

    # ── Fase 4: gravar (full rebuild só dos postos que deram certo) ──
    pg = pg_conn()
    try:
        with pg.cursor() as c:
            c.execute(DDL)
            c.execute("DELETE FROM kpi_vg_matriculas_anteriores WHERE posto = ANY(%s)", (postos_ok,))
            c.execute("DELETE FROM kpi_vg_situacao_clientes WHERE posto = ANY(%s)", (postos_ok,))
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
        for p in postos_ok:
            meta.ok(p, clientes=len(dados[p]))
        print("Postgres atualizado.", flush=True)
    except Exception as e:
        pg.rollback()
        rc = 2
        traceback.print_exc()
        for p in postos_ok:
            meta.error(p, f"fase gravação PG: {e}")
    finally:
        pg.close()
        meta.save()

    return rc


if __name__ == "__main__":
    sys.exit(main())
