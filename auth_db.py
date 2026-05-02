"""
auth_db.py — Modelos SQLAlchemy e helpers do banco de autenticação.
Banco: SQLite em AUTH_DB_PATH (default /var/lib/camim-auth/camim_auth.db)
"""
import os
import secrets
from datetime import datetime, timedelta, timezone

_BRT = timezone(timedelta(hours=-3))

from sqlalchemy import (
    create_engine, Column, Integer, String, Boolean, DateTime, ForeignKey, Text, Float, text,
    UniqueConstraint, Index
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
from werkzeug.security import generate_password_hash, check_password_hash

DB_PATH = os.environ.get("AUTH_DB_PATH", "/var/lib/camim-auth/camim_auth.db")
engine = create_engine(
    f"sqlite:///{DB_PATH}",
    connect_args={"check_same_thread": False},
)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
Base = declarative_base()


class User(Base):
    __tablename__ = "users"

    id           = Column(Integer, primary_key=True)
    email        = Column(String(200), unique=True, nullable=False, index=True)
    nome         = Column(String(200), default="")
    senha_hash   = Column(String(256), nullable=False)
    is_admin     = Column(Boolean, default=False, nullable=False)
    ativo        = Column(Boolean, default=True,  nullable=False)
    all_pages    = Column(Boolean, default=True,  nullable=False)
    pode_desbloquear = Column(Boolean, default=False, nullable=False)
    has_openai_account = Column(Boolean, default=False, nullable=False)
    id_usuario_sqlserver = Column(Integer, nullable=True)
    login_campinho = Column(String(100), nullable=True)
    reset_token  = Column(String(100), nullable=True)
    reset_expires = Column(DateTime, nullable=True)

    postos = relationship(
        "UserPosto", back_populates="user",
        cascade="all, delete-orphan", lazy="select"
    )
    page_permissions = relationship(
        "UserPagePermission", back_populates="user",
        cascade="all, delete-orphan", lazy="select"
    )
    login_history = relationship(
        "LoginHistory", back_populates="user",
        cascade="all, delete-orphan", lazy="select"
    )
    ia_conversas = relationship(
        "IAConversa", back_populates="user",
        cascade="all, delete-orphan", lazy="select"
    )

    def set_senha(self, senha: str):
        self.senha_hash = generate_password_hash(senha)

    def check_senha(self, senha: str) -> bool:
        return check_password_hash(self.senha_hash, senha)

    def gerar_reset_token(self) -> str:
        self.reset_token = secrets.token_urlsafe(32)
        self.reset_expires = datetime.utcnow() + timedelta(hours=1)
        return self.reset_token

    def reset_valido(self, token: str) -> bool:
        return (
            bool(token)
            and self.reset_token == token
            and self.reset_expires is not None
            and datetime.utcnow() < self.reset_expires
        )

    def lista_postos(self) -> list:
        return sorted(p.posto for p in self.postos)

    def lista_paginas(self) -> list:
        return sorted(p.page_key for p in self.page_permissions)


class UserPosto(Base):
    __tablename__ = "user_postos"

    id      = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    posto   = Column(String(10), nullable=False)
    user    = relationship("User", back_populates="postos")


class UserPagePermission(Base):
    __tablename__ = "user_page_permissions"

    id       = Column(Integer, primary_key=True)
    user_id  = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    page_key = Column(String(100), nullable=False)
    user     = relationship("User", back_populates="page_permissions")


class LoginHistory(Base):
    __tablename__ = "login_history"

    id         = Column(Integer, primary_key=True)
    user_id    = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    ip         = Column(String(50), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    user       = relationship("User", back_populates="login_history")


class PageUsagePing(Base):
    """Pings de 1 minuto por usuário/página. Cada ping = 1 minuto arredondado para baixo.

    Tempo por página = COUNT(DISTINCT minute_bucket) * 60s. UNIQUE(user, page, bucket)
    evita contagem dupla se o navegador mandar 2 pings no mesmo minuto.
    """
    __tablename__ = "page_usage_pings"

    id            = Column(Integer, primary_key=True)
    user_id       = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"),
                           nullable=False, index=True)
    page          = Column(String(200), nullable=False)
    # minuto arredondado para baixo: UTC, formato "YYYY-MM-DD HH:MM:00"
    minute_bucket = Column(String(20), nullable=False)
    created_at    = Column(DateTime, default=datetime.utcnow, nullable=False)

    __table_args__ = (
        UniqueConstraint("user_id", "page", "minute_bucket", name="uq_ping_user_page_min"),
        Index("ix_ping_user_bucket", "user_id", "minute_bucket"),
    )


class IAConversa(Base):
    __tablename__ = "ia_conversas"

    id         = Column(Integer, primary_key=True)
    user_id    = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    pagina     = Column(String(200), nullable=True)
    pergunta   = Column(Text, nullable=False)
    resposta   = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    # Uso e custo por chamada (preenchidos após a resposta do LLM)
    provider         = Column(String(20),  nullable=True)   # groq | openai | anthropic
    model            = Column(String(100), nullable=True)
    prompt_tokens    = Column(Integer,     nullable=True)
    completion_tokens = Column(Integer,    nullable=True)
    total_tokens     = Column(Integer,     nullable=True)
    cost_usd         = Column(Float,       nullable=True)
    user       = relationship("User", back_populates="ia_conversas")


class HistoricoDesbloqueio(Base):
    """Log local de ações de desbloqueio de agenda feitas pelo app."""
    __tablename__ = "historico_desbloqueio"

    id              = Column(Integer, primary_key=True)
    user_id         = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    user_email      = Column(String(200), nullable=False)
    user_nome       = Column(String(200), default="")
    posto           = Column(String(10), nullable=False)
    id_especialidade = Column(Integer, nullable=False)
    especialidade   = Column(String(200), default="")
    acao            = Column(String(50), nullable=False)   # 'retirar_data_fim' | 'prorrogar_agenda'
    valor_antigo    = Column(String(100), nullable=True)
    valor_novo      = Column(String(100), nullable=True)
    snapshot        = Column(Text, nullable=True)          # JSON do card completo no momento da ação
    created_at      = Column(DateTime, default=lambda: datetime.now(_BRT).replace(tzinfo=None), nullable=False)

    user = relationship("User", backref="historico_desbloqueios")


class KPIContexto(Base):
    """Contexto de negócio por KPI — editável pelo admin, injetado no system prompt."""
    __tablename__ = "kpi_contexto"

    id         = Column(Integer, primary_key=True)
    kpi_slug   = Column(String(100), unique=True, nullable=False, index=True)
    titulo     = Column(String(200), default="")
    contexto   = Column(Text, default="")
    updated_at = Column(DateTime, default=datetime.utcnow)


class IAConfigGlobal(Base):
    """Configurações globais da IA — chave/valor editáveis pelo admin."""
    __tablename__ = "ia_config_global"

    id         = Column(Integer, primary_key=True)
    chave      = Column(String(100), unique=True, nullable=False, index=True)
    valor      = Column(Text, default="")
    updated_at = Column(DateTime, default=datetime.utcnow)


_REGRAS_GERAIS_DEFAULT = """\
Em perguntas comparativas (quando o usuário perguntar sobre diferenças, variações, aumentos, quedas entre períodos), \
siga estas regras obrigatoriamente:

1. OUTLIERS OBRIGATÓRIOS: O sistema já entrega os dados pré-calculados com as maiores variações. \
Apresente TODOS os itens com variação discrepante em relação ao padrão do período — não limite a top 3 ou top 5. \
Se 95% dos itens variam 10% e alguns variam 30%, mostre TODOS esses casos anômalos, um por linha.

2. FORMATO DAS VARIAÇÕES: Cada item de despesa ou receita em uma linha separada, com valor absoluto e percentual. \
Exemplo: "- DAS/Simples Nacional: R$31.877,21 → R$45.973,58  (+R$14.096,37 / +44,2%)"

3. SEPARAÇÃO CLARA: Primeiro os aumentos significativos (ordenados do maior ao menor variação), \
depois as reduções significativas. Seção separada para cada direção.

4. CONTEXTO DO OUTLIER: Após listar os itens anômalos, explique brevemente o que pode ter causado \
cada variação expressiva, com base no conhecimento do setor de saúde.

5. CONCLUSÃO: Feche com um parágrafo sintetizando o impacto geral no resultado e na margem.\
"""


# KPIs conhecidos com títulos padrão
_KPI_DEFAULTS = [
    ("receita_despesa",        "KPI Receitas x Despesas"),
    ("receita_despesa_rateio", "KPI R x D com Rateio"),
    ("alimentacao",            "KPI Alimentação"),
    ("vendas",                 "KPI Vendas / Metas"),
    ("medicos",                "KPI Médicos"),
    ("consultas",              "KPI Consultas (Status)"),
    ("notas_rps",              "KPI Notas x RPS"),
    ("prescricoes",            "KPI Prescrições"),
    ("clientes",               "KPI Clientes"),
    ("indices_oficiais",       "KPI Índices Oficiais"),
    ("liberty",                "KPI CAMIM Liberty"),
]


class RegraAnomalia(Base):
    """Regras configuráveis do detector de auditoria financeira.

    Tipos suportados (parametros_json):
      mm_pct                 {"janela": 12, "pct": 10}
      zscore_robusto         {"threshold": 3.0}
      gap_temporal           {"meses_vazios": 1}
      fornecedor_novo        {}
      nao_recorrente_pct     {"pct_posto": 1.0}
      benford_mad            {"amarelo": 0.012, "vermelho": 0.022}

    escopo_postos / escopo_tipos:
      "*"        = aplica a todos
      "A,B,X"    = aplica só a esses
      "!A,X"     = aplica a todos exceto esses
    """
    __tablename__ = "regras_anomalia"

    id              = Column(Integer, primary_key=True)
    nome            = Column(String(120), nullable=False)
    tipo            = Column(String(40), nullable=False, index=True)
    parametros_json = Column(Text, nullable=False, default="{}")
    escopo_postos   = Column(String(120), nullable=False, default="*")
    escopo_tipos    = Column(String(255), nullable=False, default="*")
    ativa           = Column(Boolean,  nullable=False, default=True, index=True)
    criado_por      = Column(String(120))
    criado_em       = Column(DateTime, default=lambda: datetime.now(_BRT))
    atualizado_em   = Column(DateTime, default=lambda: datetime.now(_BRT),
                              onupdate=lambda: datetime.now(_BRT))
    observacao      = Column(Text, default="")


class AnomaliaVerificacao(Base):
    """Registro de itens da lista de auditoria já vistos por um adm.

    chave_anomalia = sha1 estável de (posto, id_conta_tipo, mes_ref, regra_id, evidencia_extra).
    Item verificado some da lista até nova evidência aparecer (nova chave).
    """
    __tablename__ = "anomalia_verificacao"

    chave_anomalia  = Column(String(64), primary_key=True)
    posto           = Column(String(2),  nullable=False, index=True)
    id_conta_tipo   = Column(Integer,    nullable=True, index=True)
    mes_ref         = Column(String(7),  nullable=True)            # "2026-04"
    regra_id        = Column(Integer,    nullable=True)
    verificado_por  = Column(String(120), nullable=False)
    verificado_em   = Column(DateTime,    nullable=False,
                              default=lambda: datetime.now(_BRT))
    observacao      = Column(Text, default="")


_REGRAS_AUDITORIA_DEFAULT = [
    # nome, tipo, parametros_json, observacao
    ("MM12 — variação > 10%",      "mm_pct",
        '{"janela": 12, "pct": 10}',  "Saída acima da média móvel 12m em mais de 10%."),
    ("MM6 — variação > 10%",       "mm_pct",
        '{"janela": 6,  "pct": 10}',  "Saída acima da média móvel 6m em mais de 10%."),
    ("MM3 — variação > 10%",       "mm_pct",
        '{"janela": 3,  "pct": 10}',  "Saída acima da média móvel 3m em mais de 10%."),
    ("MM24 — variação > 10%",      "mm_pct",
        '{"janela": 24, "pct": 10}',  "Saída acima da média móvel 24m em mais de 10%."),
    ("Z-score robusto > 3",        "zscore_robusto",
        '{"threshold": 3.0}',         "Outlier estatístico contra mediana/MAD da própria conta."),
    ("Gap temporal — 1 mês",       "gap_temporal",
        '{"meses_vazios": 1}',        "Conta regular sem lançamento neste mês."),
    ("Fornecedor novo no tipo",    "fornecedor_novo",
        '{}',                         "Lançamento com fornecedor inédito naquele tipo de conta do posto."),
    ("Não-recorrente — 1% do posto","nao_recorrente_pct",
        '{"pct_posto": 1.0}',         "Lançamento isolado >= 1% do total do posto no mês."),
    ("Benford — MAD",              "benford_mad",
        '{"amarelo": 0.012, "vermelho": 0.022}',
                                      "Cor do botão Benford derivada do MAD por posto."),
]


def init_db():
    Base.metadata.create_all(engine)
    # Migration: add all_pages column to existing users (safe if already exists)
    with engine.connect() as _conn:
        try:
            _conn.execute(text("ALTER TABLE users ADD COLUMN all_pages INTEGER NOT NULL DEFAULT 1"))
            _conn.commit()
        except Exception:
            pass  # column already exists
        try:
            _conn.execute(text("ALTER TABLE users ADD COLUMN pode_desbloquear INTEGER NOT NULL DEFAULT 0"))
            _conn.commit()
        except Exception:
            pass  # column already exists
        try:
            _conn.execute(text("ALTER TABLE users ADD COLUMN id_usuario_sqlserver INTEGER"))
            _conn.commit()
        except Exception:
            pass
        try:
            _conn.execute(text("ALTER TABLE users ADD COLUMN login_campinho VARCHAR(100)"))
            _conn.commit()
        except Exception:
            pass
        try:
            _conn.execute(text("ALTER TABLE users ADD COLUMN has_openai_account INTEGER NOT NULL DEFAULT 0"))
            _conn.commit()
            # Coluna recém-criada: marca os 3 usuários que já têm conta na OpenAI
            _conn.execute(text(
                "UPDATE users SET has_openai_account=1 "
                "WHERE email IN ('ronald@camim.com.br', 'cristiano@camim.com.br', 'leonardo@camim.com.br')"
            ))
            _conn.commit()
        except Exception:
            pass  # column already exists; admin gerencia via UI
        try:
            _conn.execute(text("ALTER TABLE historico_desbloqueio ADD COLUMN snapshot TEXT"))
            _conn.commit()
        except Exception:
            pass
        # Migrations para ia_conversas: novas colunas de uso/custo
        for _sql in [
            "ALTER TABLE ia_conversas ADD COLUMN provider VARCHAR(20)",
            "ALTER TABLE ia_conversas ADD COLUMN model VARCHAR(100)",
            "ALTER TABLE ia_conversas ADD COLUMN prompt_tokens INTEGER",
            "ALTER TABLE ia_conversas ADD COLUMN completion_tokens INTEGER",
            "ALTER TABLE ia_conversas ADD COLUMN total_tokens INTEGER",
            "ALTER TABLE ia_conversas ADD COLUMN cost_usd REAL",
        ]:
            try:
                _conn.execute(text(_sql))
                _conn.commit()
            except Exception:
                pass
        # Índices para consultas analíticas
        for _sql in [
            "CREATE INDEX IF NOT EXISTS idx_iaconv_created ON ia_conversas(created_at)",
            "CREATE INDEX IF NOT EXISTS idx_iaconv_user    ON ia_conversas(user_id)",
            "CREATE INDEX IF NOT EXISTS idx_iaconv_provider ON ia_conversas(provider)",
        ]:
            try:
                _conn.execute(text(_sql))
                _conn.commit()
            except Exception:
                pass
    db = SessionLocal()
    try:
        # KPIs conhecidos
        for slug, titulo in _KPI_DEFAULTS:
            if not db.query(KPIContexto).filter_by(kpi_slug=slug).first():
                db.add(KPIContexto(kpi_slug=slug, titulo=titulo, contexto=""))
        # Regras gerais — seed com valor padrão se ainda não existe
        if not db.query(IAConfigGlobal).filter_by(chave="regras_gerais").first():
            db.add(IAConfigGlobal(chave="regras_gerais", valor=_REGRAS_GERAIS_DEFAULT))
        # Regras de auditoria financeira — seed só se a tabela está vazia
        if db.query(RegraAnomalia).count() == 0:
            for nome, tipo, params, obs in _REGRAS_AUDITORIA_DEFAULT:
                db.add(RegraAnomalia(
                    nome=nome, tipo=tipo, parametros_json=params,
                    escopo_postos="*", escopo_tipos="*", ativa=True,
                    criado_por="system", observacao=obs,
                ))
        db.commit()
    except Exception:
        db.rollback()
    finally:
        db.close()


def get_user_by_email(db, email: str):
    return db.query(User).filter_by(email=email.lower().strip()).first()


def get_user_by_id(db, uid: int):
    return db.get(User, uid)
