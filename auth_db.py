"""
auth_db.py — Modelos SQLAlchemy e helpers do banco de autenticação.
Banco: SQLite em AUTH_DB_PATH (default /opt/relatorio_h_t/camim_auth.db)
"""
import os
import secrets
from datetime import datetime, timedelta

from sqlalchemy import (
    create_engine, Column, Integer, String, Boolean, DateTime, ForeignKey
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
from werkzeug.security import generate_password_hash, check_password_hash

DB_PATH = os.environ.get("AUTH_DB_PATH", "/opt/relatorio_h_t/camim_auth.db")
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


def init_db():
    Base.metadata.create_all(engine)


def get_user_by_email(db, email: str):
    return db.query(User).filter_by(email=email.lower().strip()).first()


def get_user_by_id(db, uid: int):
    return db.get(User, uid)
