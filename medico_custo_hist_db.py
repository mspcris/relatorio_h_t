"""
medico_custo_hist_db.py — histórico diário do cadastro de agendas médicas.

Guarda MUDANÇA, não foto. A página `medico_custo.html` continua sendo a foto de
hoje (JSON gerado pelo cron); estas tabelas respondem a outra pergunta: **como
estava o cadastro no dia que eu escolher**, e o que mudou de lá para cá.

Por que não copiar o JSON inteiro por dia:
  1,2 MB × 365 = 440 MB/ano, e mesmo assim não responde "quando o valor do Dr. X
  mudou" sem varrer o ano. Versionando cada agenda a carga inicial é de 776
  linhas e depois só entra o delta — algumas dezenas por semana.

Chave natural da agenda (MEDIDA, não deduzida — 776 chaves para 776 linhas em
02/08/2026):

    posto + id_medico + id_especialidade + dia_semana

Repare que o HORÁRIO está fora da chave de propósito. Uma agenda que sai das 7h
para as 8h é a MESMA agenda com horário novo — é isso que se quer ver no
histórico. Se `hora_inicio` entrasse na chave, viraria "uma agenda removida e
outra criada" e a comparação se perderia justamente no caso mais interessante.

Tabelas (prefixo `mc_`, no mesmo RDS do `ti_*` e do `public.servicos`):

  mc_execucao       uma linha por rodada do ETL, SEMPRE — inclusive quando nada
                    mudou. Sem ela não dá para distinguir "nada mudou" de "o ETL
                    não rodou", e um gráfico plano pelos dois motivos é idêntico.
                    Guarda a cobertura (postos ok/erro) da rodada.

  mc_agenda_versao  uma linha por VERSÃO de agenda, com valido_de/valido_ate
                    (nulo = vigente). O conteúdo vai em JSONB — assim o ETL pode
                    ganhar campo novo sem migration, e reconstruir um dia é
                    devolver o mesmo dicionário que a página já sabe ler.

  mc_mudanca        o diário legível: o que mudou, de quanto para quanto, e o
                    efeito em R$/mês. Dá para derivar da tabela acima com
                    self-join, mas é ele que a tela mostra — guardar pronto é
                    barato e evita consulta torta.

REGRA DE OURO — posto que falhou no ETL não remove agenda nenhuma.
Se Campo Grande der timeout numa madrugada, fechar as agendas dele grava
"45 agendas removidas, −R$ 180 mil/mês". Economia falsa é pior que dado
faltando: tem exatamente a cara do resultado que se está procurando. Só os
postos que voltaram OK naquela execução são processados — os outros ficam
congelados como estavam. Mesma família do kill-switch implícito do incidente
de 2026-05-06.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import datetime, timedelta, timezone

from sqlalchemy import (
    JSON, Boolean, Column, Date, DateTime, Index, Integer, Numeric, String,
    Text, create_engine,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base, sessionmaker

# JSONB no Postgres (produção) e JSON no SQLite — assim a lógica de detecção de
# mudança roda em teste local, sem RDS. É o que permite provar a guarda do
# "posto que falhou não remove agenda" antes de escrever em produção.
JSON_COL = JSONB().with_variant(JSON(), "sqlite")

log = logging.getLogger(__name__)
_BRT = timezone(timedelta(hours=-3))

# Reaproveita a engine do catálogo de serviços quando existe — é o MESMO banco;
# abrir um segundo pool para o mesmo DSN é desperdício de conexão. (Mesma
# decisão do custos_ti_db.py. Compartilhar CONEXÃO não é compartilhar KPI:
# nenhum cálculo, tabela ou rota é comum entre os módulos.)
try:  # pragma: no cover - caminho normal em produção
    from servicos_db import pg_engine as _shared_engine
except Exception:  # noqa: BLE001 - dev sem PG_RDS_* configurado
    _shared_engine = None


def _build_dsn() -> str:
    host = os.environ["PG_RDS_HOST"]
    port = os.environ.get("PG_RDS_PORT", "9432")
    db = os.environ.get("PG_RDS_DB", "relatorio_h_t")
    usr = os.environ["PG_RDS_USER"]
    pwd = os.environ["PG_RDS_PASSWORD"]
    ssl = os.environ.get("PG_RDS_SSLMODE", "require")
    return f"postgresql+psycopg2://{usr}:{pwd}@{host}:{port}/{db}?sslmode={ssl}"


def _make_engine():
    """Engine do RDS, ou None sem credenciais.

    Importar este módulo NÃO pode explodir por falta de .env: o app.py carrega
    blueprints em try/except e um ImportError aqui derrubaria a página inteira
    em vez de só o histórico.
    """
    if _shared_engine is not None:
        return _shared_engine
    try:
        return create_engine(_build_dsn(), pool_pre_ping=True, pool_recycle=1800)
    except KeyError as e:
        log.warning("medico_custo_hist_db sem conexão: variável %s ausente.", e)
        return None


pg_engine = _make_engine()
McSession = sessionmaker(bind=pg_engine, autocommit=False, autoflush=False)
McBase = declarative_base()


def now_brt() -> datetime:
    return datetime.now(_BRT).replace(tzinfo=None)


def hoje_brt():
    return datetime.now(_BRT).date()


# ── O que conta como "mudou" ────────────────────────────────────────────────
# SÓ campo de cadastro. Nada de derivado (valor_hora, custo_mensal, modalidade,
# alertas): no dia em que eu mexer numa constante do ETL — SEMANAS_NO_MES, por
# exemplo — as 776 agendas "mudariam" ao mesmo tempo e o histórico viraria lixo.
# Lista EXPLÍCITA e não lista de exclusão: campo derivado novo não entra aqui
# por descuido.
CAMPOS_CADASTRO = (
    "medico", "crm", "especialidade", "descricao", "sala", "cbos",
    "hora_inicio", "hora_fim", "almoco_inicio", "almoco_fim",
    "minutos_brutos", "minutos_almoco",
    "valor_plantao", "valor_consulta_clube", "qtd_custo",
    "vagas", "vagas_maxima",
    "oc", "www", "tel",
    "agenda_quinzenal", "temporario", "recebe_por_comissao",
    "atendimento_online", "acolhimento", "exibe_no_f3",
    "exibe_de", "exibe_ate", "data_plantao",
)

# Rótulo de cada campo para a tela — "hora_inicio" não é frase de gente.
ROTULO_CAMPO = {
    "medico": "nome do médico", "crm": "CRM", "especialidade": "especialidade",
    "descricao": "descrição", "sala": "sala", "cbos": "CBO",
    "hora_inicio": "início", "hora_fim": "fim",
    "almoco_inicio": "início do almoço", "almoco_fim": "fim do almoço",
    "minutos_brutos": "minutos de jornada", "minutos_almoco": "minutos de almoço",
    "valor_plantao": "valor do plantão", "valor_consulta_clube": "valor consulta clube",
    "qtd_custo": "quantidade de custo", "vagas": "vagas", "vagas_maxima": "vagas (máximo)",
    "oc": "ordem de chegada", "www": "agendamento pela internet", "tel": "agendamento por telefone",
    "agenda_quinzenal": "agenda quinzenal", "temporario": "temporário",
    "recebe_por_comissao": "recebe por comissão", "atendimento_online": "atendimento online",
    "acolhimento": "acolhimento", "exibe_no_f3": "exibe no F3",
    "exibe_de": "exibe a partir de", "exibe_ate": "exibe até",
    "data_plantao": "data do plantão",
}

TIPOS_MUDANCA = ("nova", "alterada", "removida")


def chave_agenda(l: dict) -> str:
    """posto|id_medico|id_especialidade|dia_semana — a identidade da agenda.

    Medida em 02/08/2026: 776 chaves distintas para 776 linhas, nos 13 postos.
    Se um dia essa unicidade cair, o ETL avisa em vez de sobrescrever em
    silêncio (é o mesmo erro do idLancamentoServico documentado no CLAUDE.md).
    """
    return "|".join(str(l.get(k)) for k in
                    ("posto", "id_medico", "id_especialidade", "dia_semana"))


def hash_cadastro(l: dict) -> str:
    """SHA1 dos campos de cadastro — muda só quando o cadastro muda de fato."""
    payload = json.dumps({k: l.get(k) for k in CAMPOS_CADASTRO},
                         sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


class McExecucao(McBase):
    """Uma rodada do ETL. Grava SEMPRE, mesmo sem nenhuma mudança."""
    __tablename__ = "mc_execucao"

    id = Column(Integer, primary_key=True)
    data = Column(Date, nullable=False, unique=True, index=True)
    gerado_em = Column(DateTime, nullable=False, default=now_brt)
    postos_ok = Column(Integer, nullable=False, default=0)
    postos_erro = Column(Integer, nullable=False, default=0)
    # quais falharam, para a tela dizer o nome em vez de só um número
    postos_falhos = Column(Text, default="")
    # completa=False → a foto daquele dia tem posto faltando. A tela AVISA, senão
    # o custo menor daquele dia parece economia.
    completa = Column(Boolean, nullable=False, default=True)
    linhas = Column(Integer, nullable=False, default=0)
    linhas_plantao = Column(Integer, nullable=False, default=0)
    medicos = Column(Integer, nullable=False, default=0)
    custo_semanal = Column(Numeric(14, 2), default=0)
    custo_mensal = Column(Numeric(14, 2), default=0)
    # regras vigentes naquele dia (piso, fator fora da curva...), para dar para
    # saber depois com que régua aquela foto foi medida
    parametros = Column(JSON_COL)
    novas = Column(Integer, nullable=False, default=0)
    alteradas = Column(Integer, nullable=False, default=0)
    removidas = Column(Integer, nullable=False, default=0)


class McAgendaVersao(McBase):
    """Uma versão de uma agenda. valido_ate nulo = é a versão vigente."""
    __tablename__ = "mc_agenda_versao"

    id = Column(Integer, primary_key=True)
    chave = Column(String(120), nullable=False)
    posto = Column(String(4), nullable=False)
    id_medico = Column(Integer)
    id_especialidade = Column(Integer)
    medico = Column(String(160))
    especialidade = Column(String(160))
    dia_semana = Column(String(20))
    hash = Column(String(40), nullable=False)
    # A linha inteira do ETL. JSONB para reconstruir um dia devolvendo
    # exatamente o dicionário que a página já sabe ler — e para o ETL ganhar
    # campo novo sem migration.
    dados = Column(JSON_COL, nullable=False)
    valido_de = Column(Date, nullable=False)
    valido_ate = Column(Date)            # nulo = vigente hoje
    custo_mensal = Column(Numeric(12, 2))   # desnormalizado: soma sem abrir o JSONB

    __table_args__ = (
        # reconstrução de um dia: valido_de <= D < valido_ate
        Index("ix_mc_versao_periodo", "valido_de", "valido_ate"),
        # versão vigente de uma agenda
        Index("ix_mc_versao_chave", "chave", "valido_ate"),
        Index("ix_mc_versao_posto", "posto", "valido_ate"),
    )


class McMudanca(McBase):
    """O diário legível: o que mudou, de quanto para quanto, e o efeito no mês."""
    __tablename__ = "mc_mudanca"

    id = Column(Integer, primary_key=True)
    data = Column(Date, nullable=False, index=True)
    # É "detectado em", NÃO "alterado em": o cadastro pode mudar às 14h e o ETL
    # só ver às 02:50 do dia seguinte. A tela precisa falar assim.
    detectado_em = Column(DateTime, nullable=False, default=now_brt)
    chave = Column(String(120), nullable=False, index=True)
    tipo = Column(String(12), nullable=False)      # nova | alterada | removida
    posto = Column(String(4), nullable=False)
    medico = Column(String(160))
    especialidade = Column(String(160))
    dia_semana = Column(String(20))
    campo = Column(String(40))        # nulo em nova/removida
    valor_de = Column(Text)
    valor_para = Column(Text)
    # efeito em R$/mês: + entrou custo, − saiu custo. Em "alterada" só o campo
    # valor_plantao mexe nisso; nos outros campos vai 0.
    delta_mensal = Column(Numeric(12, 2), default=0)

    __table_args__ = (Index("ix_mc_mudanca_data_posto", "data", "posto"),)


def criar_tabelas() -> None:
    if pg_engine is None:
        raise RuntimeError("sem conexão com o RDS (PG_RDS_* ausente no ambiente)")
    McBase.metadata.create_all(pg_engine)
