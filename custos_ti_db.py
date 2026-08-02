"""
custos_ti_db.py — Modelos SQLAlchemy do módulo "Custos de TI".

Banco: Postgres RDS AWS (mesmas credenciais PG_RDS_* do servicos_db.py / ETLs
financeiros). Tabelas com prefixo `ti_`. Só este projeto (relatorio_h_t) escreve.

Por que Postgres e não JSON como o custos_ia:
  o custos_ia guarda SNAPSHOTS por mês (um arquivo por provedor/mês) — formato
  bom para congelar o que a Costs API devolveu. Aqui o dado é relacional e
  editado à mão o tempo todo (centro ↔ conta ↔ lançamento ↔ forma de pagamento),
  com dedupe por ID de transação do fornecedor. Isso é tabela.

Modelo:

  ti_centro_custo    "IA", "Infra", "Banco de Dados"... Cada centro vira um item
                     no menu à esquerda e ganha uma página própria.
                     fonte='ia' → a página é a custos_ia.html já existente e o
                     total do mês vem dos snapshots dela (NÃO de ti_lancamento),
                     para não contar duas vezes.

  ti_forma_pagamento Cartão (bandeira + 4 últimos), boleto, pix, débito
                     automático... É o "em que cartão é pago" do cadastro.

  ti_conta           A assinatura/serviço contratado dentro de um centro
                     ("OpenAI API", "Contabo VM", "RDS Postgres"). É o cadastro
                     recorrente; o gasto de cada mês é o lançamento.

  ti_lancamento      O gasto efetivo de uma competência (YYYY-MM). Pode nascer
                     à mão ou de importação (texto colado da Meta, API Graph).
                     `origem` + `external_id` têm índice único → reimportar o
                     mesmo extrato não duplica nada.

Câmbio: cada lançamento guarda `valor` + `moeda` (original, imutável) e o
`valor_brl` + `cotacao` congelados no momento do registro. Assim o histórico
não muda quando o dólar mexe. A cotação vigente por mês fica em ti_cotacao.
"""
from __future__ import annotations

import os
import re
from datetime import datetime, timedelta, timezone

from sqlalchemy import (
    Boolean, CheckConstraint, Column, Date, DateTime, ForeignKey, Index,
    Integer, Numeric, String, Text, UniqueConstraint, create_engine,
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

_BRT = timezone(timedelta(hours=-3))

MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")

# Reaproveita a engine do catálogo de serviços quando disponível — é o MESMO
# banco; abrir um segundo pool para o mesmo DSN é desperdício de conexão.
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
    """Engine do RDS, ou None se as credenciais não estiverem no ambiente.

    Importar este módulo NÃO pode explodir por falta de .env — o app.py carrega
    os blueprints em try/except e um ImportError aqui derrubaria a página inteira
    em vez de só o módulo. Sem credenciais, o erro aparece no primeiro uso.
    """
    if _shared_engine is not None:
        return _shared_engine
    try:
        return create_engine(_build_dsn(), pool_pre_ping=True, pool_recycle=1800)
    except KeyError as e:
        import logging
        logging.getLogger(__name__).warning(
            "custos_ti_db sem conexão: variável %s ausente no ambiente.", e)
        return None


pg_engine = _make_engine()
TiSession = sessionmaker(bind=pg_engine, autocommit=False, autoflush=False)
TiBase = declarative_base()


def now_brt() -> datetime:
    return datetime.now(_BRT).replace(tzinfo=None)


# Vocabulários fechados. Mantidos como tupla + CheckConstraint no banco para o
# valor inválido estourar na hora do INSERT, e não virar linha órfã silenciosa
# (lição do incidente 2026-05-06: whitelist que devolve default esconde bug).
FONTES_CENTRO = ("manual", "ia")
TIPOS_PAGAMENTO = (
    "cartao_credito", "cartao_debito", "boleto", "pix",
    "debito_automatico", "transferencia", "outro",
)
RECORRENCIAS = ("mensal", "anual", "variavel", "unica")
STATUS_LANC = ("previsto", "pago")
ORIGENS_LANC = ("manual", "meta_texto", "meta_api", "ia_snapshot", "import_csv")
MOEDAS = ("BRL", "USD", "EUR")


class CentroCusto(TiBase):
    """Um tema de custo (IA, Infra, Banco de Dados...). Vira página + item de menu."""

    __tablename__ = "ti_centro_custo"

    id = Column(Integer, primary_key=True)
    key = Column(String(60), nullable=False, unique=True, index=True)
    nome = Column(String(120), nullable=False)
    descricao = Column(Text, nullable=True)
    icone = Column(String(60), nullable=False, default="fas fa-folder")
    cor = Column(String(20), nullable=False, default="#20c997")
    ordem = Column(Integer, nullable=False, default=100)
    ativo = Column(Boolean, nullable=False, default=True)
    # 'manual' → lançamentos desta tabela; 'ia' → total vem do custos_ia
    fonte = Column(String(20), nullable=False, default="manual")
    # href customizado (só usado por centros fonte='ia' apontando p/ /custos_ia)
    href = Column(String(200), nullable=True)
    created_at = Column(DateTime, nullable=False, default=now_brt)
    updated_at = Column(DateTime, nullable=False, default=now_brt, onupdate=now_brt)

    __table_args__ = (
        CheckConstraint(f"fonte IN {FONTES_CENTRO}", name="ck_ti_centro_fonte"),
        Index("ix_ti_centro_ordem", "ordem", "nome"),
    )

    @property
    def url(self) -> str:
        return self.href or f"/custos_ti/{self.key}"

    def to_dict(self) -> dict:
        return {
            "id": self.id, "key": self.key, "nome": self.nome,
            "descricao": self.descricao, "icone": self.icone, "cor": self.cor,
            "ordem": self.ordem, "ativo": self.ativo, "fonte": self.fonte,
            "url": self.url,
        }


class FormaPagamento(TiBase):
    """Como a conta é paga: cartão X, boleto, pix, débito automático."""

    __tablename__ = "ti_forma_pagamento"

    id = Column(Integer, primary_key=True)
    nome = Column(String(120), nullable=False)
    tipo = Column(String(30), nullable=False, default="cartao_credito")
    bandeira = Column(String(40), nullable=True)     # Visa, Mastercard...
    ultimos4 = Column(String(4), nullable=True)      # 6852
    titular = Column(String(120), nullable=True)
    dia_vencimento = Column(Integer, nullable=True)  # dia do mês (boleto/fatura)
    moeda_padrao = Column(String(3), nullable=False, default="BRL")
    ativo = Column(Boolean, nullable=False, default=True)
    obs = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=now_brt)
    updated_at = Column(DateTime, nullable=False, default=now_brt, onupdate=now_brt)

    __table_args__ = (
        CheckConstraint(f"tipo IN {TIPOS_PAGAMENTO}", name="ck_ti_forma_tipo"),
        CheckConstraint(f"moeda_padrao IN {MOEDAS}", name="ck_ti_forma_moeda"),
        CheckConstraint(
            "dia_vencimento IS NULL OR (dia_vencimento BETWEEN 1 AND 31)",
            name="ck_ti_forma_dia",
        ),
        # Dois cartões da mesma bandeira com os mesmos 4 dígitos seriam o mesmo
        # cartão — a unicidade é o que deixa o importador reusar em vez de criar.
        UniqueConstraint("bandeira", "ultimos4", name="uq_ti_forma_cartao"),
    )

    @property
    def rotulo(self) -> str:
        if self.ultimos4:
            return f"{self.nome} ···· {self.ultimos4}"
        return self.nome

    def to_dict(self) -> dict:
        return {
            "id": self.id, "nome": self.nome, "tipo": self.tipo,
            "bandeira": self.bandeira, "ultimos4": self.ultimos4,
            "titular": self.titular, "dia_vencimento": self.dia_vencimento,
            "moeda_padrao": self.moeda_padrao, "ativo": self.ativo,
            "obs": self.obs, "rotulo": self.rotulo,
        }


class Conta(TiBase):
    """Serviço contratado dentro de um centro ("OpenAI API", "Contabo VM")."""

    __tablename__ = "ti_conta"

    id = Column(Integer, primary_key=True)
    centro_id = Column(Integer, ForeignKey("ti_centro_custo.id", ondelete="CASCADE"),
                       nullable=False, index=True)
    nome = Column(String(160), nullable=False)
    fornecedor = Column(String(120), nullable=True)
    forma_pagamento_id = Column(Integer, ForeignKey("ti_forma_pagamento.id",
                                                    ondelete="SET NULL"), nullable=True)
    recorrencia = Column(String(20), nullable=False, default="mensal")
    valor_previsto = Column(Numeric(14, 4), nullable=True)
    moeda = Column(String(3), nullable=False, default="BRL")
    dia_vencimento = Column(Integer, nullable=True)
    desde = Column(String(7), nullable=True)   # 'YYYY-MM'
    ate = Column(String(7), nullable=True)     # 'YYYY-MM' (null = vigente)
    url_painel = Column(String(300), nullable=True)
    ativo = Column(Boolean, nullable=False, default=True)
    obs = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=now_brt)
    updated_at = Column(DateTime, nullable=False, default=now_brt, onupdate=now_brt)

    centro = relationship("CentroCusto")
    forma = relationship("FormaPagamento")

    __table_args__ = (
        CheckConstraint(f"recorrencia IN {RECORRENCIAS}", name="ck_ti_conta_recorrencia"),
        CheckConstraint(f"moeda IN {MOEDAS}", name="ck_ti_conta_moeda"),
        Index("ix_ti_conta_centro_nome", "centro_id", "nome"),
    )

    def to_dict(self) -> dict:
        return {
            "id": self.id, "centro_id": self.centro_id, "nome": self.nome,
            "fornecedor": self.fornecedor,
            "forma_pagamento_id": self.forma_pagamento_id,
            "forma_rotulo": self.forma.rotulo if self.forma else None,
            "recorrencia": self.recorrencia,
            "valor_previsto": _f(self.valor_previsto),
            "moeda": self.moeda, "dia_vencimento": self.dia_vencimento,
            "desde": self.desde, "ate": self.ate, "url_painel": self.url_painel,
            "ativo": self.ativo, "obs": self.obs,
        }


class Lancamento(TiBase):
    """O gasto de uma competência. Unidade que alimenta todos os gráficos."""

    __tablename__ = "ti_lancamento"

    id = Column(Integer, primary_key=True)
    centro_id = Column(Integer, ForeignKey("ti_centro_custo.id", ondelete="CASCADE"),
                       nullable=False, index=True)
    conta_id = Column(Integer, ForeignKey("ti_conta.id", ondelete="SET NULL"),
                      nullable=True, index=True)
    competencia = Column(String(7), nullable=False, index=True)   # 'YYYY-MM'
    data_pagamento = Column(Date, nullable=True)
    descricao = Column(String(240), nullable=False)
    fornecedor = Column(String(120), nullable=True)
    forma_pagamento_id = Column(Integer, ForeignKey("ti_forma_pagamento.id",
                                                    ondelete="SET NULL"), nullable=True)
    valor = Column(Numeric(14, 4), nullable=False, default=0)
    moeda = Column(String(3), nullable=False, default="BRL")
    cotacao = Column(Numeric(12, 6), nullable=True)     # USD→BRL usada
    # Os DOIS congelados no registro. Assim a despesa em dólar mostra o dólar
    # EXATO da fatura — sem passar por cotação nenhuma — e a despesa em real
    # mostra o real exato. Só o valor da moeda oposta é convertido.
    valor_brl = Column(Numeric(14, 4), nullable=False, default=0)
    valor_usd = Column(Numeric(14, 4), nullable=False, default=0)
    status = Column(String(12), nullable=False, default="pago")
    origem = Column(String(20), nullable=False, default="manual")
    external_id = Column(String(120), nullable=True)
    obs = Column(Text, nullable=True)
    created_by = Column(String(160), nullable=True)
    created_at = Column(DateTime, nullable=False, default=now_brt)
    updated_at = Column(DateTime, nullable=False, default=now_brt, onupdate=now_brt)

    centro = relationship("CentroCusto")
    conta = relationship("Conta")
    forma = relationship("FormaPagamento")

    __table_args__ = (
        CheckConstraint(f"status IN {STATUS_LANC}", name="ck_ti_lanc_status"),
        CheckConstraint(f"origem IN {ORIGENS_LANC}", name="ck_ti_lanc_origem"),
        CheckConstraint(f"moeda IN {MOEDAS}", name="ck_ti_lanc_moeda"),
        # Reimportar o mesmo extrato não duplica: o ID da transação do
        # fornecedor + a origem formam a chave natural. NULL não colide no
        # Postgres, então lançamento manual (external_id NULL) fica livre.
        UniqueConstraint("origem", "external_id", name="uq_ti_lanc_externo"),
        Index("ix_ti_lanc_comp_centro", "competencia", "centro_id"),
    )

    def to_dict(self) -> dict:
        return {
            "id": self.id, "centro_id": self.centro_id,
            "centro_nome": self.centro.nome if self.centro else None,
            "centro_key": self.centro.key if self.centro else None,
            "conta_id": self.conta_id,
            "conta_nome": self.conta.nome if self.conta else None,
            "competencia": self.competencia,
            "data_pagamento": self.data_pagamento.isoformat() if self.data_pagamento else None,
            "descricao": self.descricao, "fornecedor": self.fornecedor,
            "forma_pagamento_id": self.forma_pagamento_id,
            "forma_rotulo": self.forma.rotulo if self.forma else None,
            "valor": _f(self.valor), "moeda": self.moeda,
            "cotacao": _f(self.cotacao), "valor_brl": _f(self.valor_brl),
            "valor_usd": _f(self.valor_usd),
            # True quando o valor daquela moeda é o ORIGINAL da fatura (não
            # passou por conversão) — a tela marca os convertidos com "≈".
            "exato_brl": self.moeda == "BRL", "exato_usd": self.moeda == "USD",
            "status": self.status, "origem": self.origem,
            "external_id": self.external_id, "obs": self.obs,
            "created_by": self.created_by,
        }


class Cotacao(TiBase):
    """Cotação USD→BRL vigente por competência (usada ao registrar em USD)."""

    __tablename__ = "ti_cotacao"

    competencia = Column(String(7), primary_key=True)   # 'YYYY-MM'
    usd_brl = Column(Numeric(12, 6), nullable=False)
    fonte = Column(String(60), nullable=True)
    updated_at = Column(DateTime, nullable=False, default=now_brt, onupdate=now_brt)

    def to_dict(self) -> dict:
        return {"competencia": self.competencia, "usd_brl": _f(self.usd_brl),
                "fonte": self.fonte}


def _f(v) -> float | None:
    """Numeric do SQLAlchemy vem como Decimal — o jsonify do Flask não serializa."""
    return None if v is None else float(v)


def init_ti_db() -> None:
    """Cria as tabelas ti_* no RDS se ainda não existirem (idempotente)."""
    if pg_engine is None:
        raise RuntimeError(
            "Sem conexão com o Postgres RDS: defina PG_RDS_HOST, PG_RDS_USER e "
            "PG_RDS_PASSWORD no .env antes de rodar a migration."
        )
    TiBase.metadata.create_all(pg_engine)
