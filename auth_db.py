"""
auth_db.py — Modelos SQLAlchemy e helpers do banco de autenticação.
Banco: SQLite em AUTH_DB_PATH (default /var/lib/camim-auth/camim_auth.db)
"""
import os
import secrets
from datetime import datetime, timedelta, timezone

_BRT = timezone(timedelta(hours=-3))

from sqlalchemy import (
    create_engine, Column, Integer, String, Boolean, DateTime, ForeignKey, Text, Float, text
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
        db.commit()
    except Exception:
        db.rollback()
    finally:
        db.close()


def get_user_by_email(db, email: str):
    return db.query(User).filter_by(email=email.lower().strip()).first()


def get_user_by_id(db, uid: int):
    return db.get(User, uid)
