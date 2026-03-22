"""
auth_db.py — Modelos SQLAlchemy e helpers do banco de autenticação.
Banco: SQLite em AUTH_DB_PATH (default /var/lib/camim-auth/camim_auth.db)
"""
import os
import secrets
from datetime import datetime, timedelta

from sqlalchemy import (
    create_engine, Column, Integer, String, Boolean, DateTime, ForeignKey, Text
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
    reset_token  = Column(String(100), nullable=True)
    reset_expires = Column(DateTime, nullable=True)

    postos = relationship(
        "UserPosto", back_populates="user",
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


class UserPosto(Base):
    __tablename__ = "user_postos"

    id      = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    posto   = Column(String(10), nullable=False)
    user    = relationship("User", back_populates="postos")


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
    user       = relationship("User", back_populates="ia_conversas")


class KPIContexto(Base):
    """Contexto de negócio por KPI — editável pelo admin, injetado no system prompt."""
    __tablename__ = "kpi_contexto"

    id         = Column(Integer, primary_key=True)
    kpi_slug   = Column(String(100), unique=True, nullable=False, index=True)
    titulo     = Column(String(200), default="")
    contexto   = Column(Text, default="")
    updated_at = Column(DateTime, default=datetime.utcnow)


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
    # Garante que todos os KPIs conhecidos têm uma linha na tabela
    db = SessionLocal()
    try:
        for slug, titulo in _KPI_DEFAULTS:
            exists = db.query(KPIContexto).filter_by(kpi_slug=slug).first()
            if not exists:
                db.add(KPIContexto(kpi_slug=slug, titulo=titulo, contexto=""))
        db.commit()
    except Exception:
        db.rollback()
    finally:
        db.close()


def get_user_by_email(db, email: str):
    return db.query(User).filter_by(email=email.lower().strip()).first()


def get_user_by_id(db, uid: int):
    return db.get(User, uid)
