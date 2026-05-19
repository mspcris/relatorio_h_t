"""
servicos_db.py — Model SQLAlchemy + helpers do catálogo de Serviços (Mais Serviços / Extras / KPIs).

Banco: Postgres RDS AWS (mesmas credenciais PG_RDS_* dos ETLs financeiros).
Tabela: public.servicos

Fonte-de-verdade dos cards exibidos em mais_servicos.html e dos checkboxes
de "Acesso a páginas" no admin. O repo `mspcris/intranet` consome a mesma
tabela via SELECT — mas só este projeto (relatorio_h_t) escreve.

Decisão arquitetural: 2026-05-19 — migração de PAGINAS_DISPONIVEIS (hardcoded
em auth_routes.py) + _TEMPLATE_TO_PAGINA (app.py) + cards estáticos do
mais_servicos.html para esta tabela. Ver memória project_servicos_no_rds.
"""
import os
from datetime import datetime, timedelta, timezone

from sqlalchemy import (
    create_engine, Column, Integer, String, Boolean, DateTime, Text,
    CheckConstraint, Index,
)
from sqlalchemy.orm import declarative_base, sessionmaker

_BRT = timezone(timedelta(hours=-3))


def _build_dsn() -> str:
    host = os.environ["PG_RDS_HOST"]
    port = os.environ.get("PG_RDS_PORT", "9432")
    db   = os.environ.get("PG_RDS_DB", "relatorio_h_t")
    usr  = os.environ["PG_RDS_USER"]
    pwd  = os.environ["PG_RDS_PASSWORD"]
    ssl  = os.environ.get("PG_RDS_SSLMODE", "require")
    return f"postgresql+psycopg2://{usr}:{pwd}@{host}:{port}/{db}?sslmode={ssl}"


pg_engine = create_engine(_build_dsn(), pool_pre_ping=True, pool_recycle=1800)
PgSession = sessionmaker(bind=pg_engine, autocommit=False, autoflush=False)
PgBase = declarative_base()


GROUPS = ("kpi", "mais", "extras")
LOCKS  = ("free", "bronze", "prata", "ouro")


class Servico(PgBase):
    """Um item exibido em mais_servicos.html e/ou na grade de acesso do admin.

    `group_name`:
      - kpi    → dashboards do menu KPI (sem cadeado).
      - mais   → cards de "Mais Serviços" (com cadeado).
      - extras → grupo Extras (Planejamento PCs, Notas Fiscais NBS/IBS/CBS, etc.).

    `lock` só faz sentido para group_name ∈ {mais, extras}. Para KPIs fica NULL.
      - free   → sem IDCamim (público).
      - bronze → exige IDCamim.
      - prata  → IDCamim + cadastro na plataforma.
      - ouro   → IDCamim + cadastro + superadmin/diretor.
    """
    __tablename__ = "servicos"

    id          = Column(Integer, primary_key=True)
    key         = Column(String(100), nullable=False, unique=True, index=True)
    label       = Column(String(200), nullable=False)
    href        = Column(String(500), nullable=False)
    group_name  = Column(String(20),  nullable=False)
    lock        = Column(String(10),  nullable=True)
    ordem       = Column(Integer,     nullable=False, default=0)
    ativo       = Column(Boolean,     nullable=False, default=True)
    descricao   = Column(Text,        nullable=True)
    icone       = Column(String(80),  nullable=True)
    cor         = Column(String(30),  nullable=True)
    created_at  = Column(DateTime, default=lambda: datetime.now(_BRT).replace(tzinfo=None), nullable=False)
    updated_at  = Column(DateTime, default=lambda: datetime.now(_BRT).replace(tzinfo=None),
                          onupdate=lambda: datetime.now(_BRT).replace(tzinfo=None), nullable=False)

    __table_args__ = (
        CheckConstraint(f"group_name IN {GROUPS}", name="ck_servicos_group_name"),
        CheckConstraint(
            f"(lock IS NULL AND group_name = 'kpi') OR (lock IN {LOCKS})",
            name="ck_servicos_lock_por_grupo",
        ),
        Index("ix_servicos_group_ordem", "group_name", "ordem"),
    )

    def to_dict(self) -> dict:
        return {
            "key":   self.key,
            "label": self.label,
            "href":  self.href,
            "group": self.group_name,
            "lock":  self.lock,
            "ordem": self.ordem,
            "ativo": self.ativo,
            "descricao": self.descricao,
            "icone": self.icone,
            "cor":   self.cor,
        }

    @property
    def is_external(self) -> bool:
        return self.href.startswith("http://") or self.href.startswith("https://")


def init_pg_db() -> None:
    """Cria a tabela public.servicos no RDS se ainda não existir."""
    PgBase.metadata.create_all(pg_engine)


def listar_servicos(db, *, somente_ativos: bool = True, grupo: str | None = None) -> list[Servico]:
    q = db.query(Servico)
    if somente_ativos:
        q = q.filter(Servico.ativo.is_(True))
    if grupo:
        q = q.filter(Servico.group_name == grupo)
    return q.order_by(Servico.group_name, Servico.ordem, Servico.label).all()
