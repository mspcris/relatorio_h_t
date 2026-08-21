"""
controle_pjs_db.py — Modelos SQLAlchemy do módulo "Controle de PJs".

Banco: Postgres RDS AWS (mesmas credenciais PG_RDS_* do servicos_db.py /
custos_ti_db.py). Tabelas com prefixo `pj_`. Só este projeto escreve.

Contexto de negócio (2026-08-21): a partir de 01/09/2026 todo boleto de
prestador de serviço (empresa PJ) chega exclusivamente no alias
prestadores@camim.com.br (cai na caixa do Cristiano). Esta página é o controle
central: cadastro da empresa com contrato anexado, boletos por competência
(emitidos contra um ou vários postos), nota fiscal anexada e a soma mensal por
empresa. São poucos PJs — anexo (PDF) vai inteiro no Postgres, como o
ti_email_auditoria já faz, e é servido sob demanda (to_dict NUNCA devolve os
bytes, senão a listagem carrega megabytes à toa).

ACESSO — mais restrito que o custos_ti: nem all_pages nem is_admin entram.
Só o dono (CONTROLE_PJS_OWNER, default cristiano@camim.com.br) e quem tiver a
permissão explícita `controle_pjs` em user_page_permissions — bit que SÓ o dono
concede, pela própria página (o modal do /admin nem lista essa chave, e o
admin_editar preserva a linha ao regravar as demais).

Modelo:

  pj_empresa   A empresa prestadora (PJ). Guarda vigência do contrato
               (contrato_inicio/fim) — o(s) PDF(s) do contrato ficam em
               pj_arquivo tipo='contrato'.

  pj_boleto    Um boleto de uma competência (YYYY-MM). `postos` é lista de
               letras separadas por vírgula ("C,G,Y") — o mesmo prestador
               emite contra vários postos (colaborador em R, outro em C+G+Y).
               A soma mensal por empresa sai daqui.

  pj_arquivo   Qualquer anexo: contrato (empresa), boleto/NF (boleto ou
               empresa+competência), ou anexo vindo de e-mail ainda não
               confirmado (email_id preenchido, resto NULL).

  pj_email     Fila de e-mails do alias prestadores@ — alimentada pelo
               import_email_pjs.py (leitura PEEK, nunca marca como lido:
               a caixa é a pessoal do Cristiano). NADA vira boleto sozinho;
               o item espera confirmação humana na página (mesma regra do
               custos_ti: valor lido nunca entra calado).
"""
from __future__ import annotations

import os
import re
from datetime import datetime, timedelta, timezone

from sqlalchemy import (
    Boolean, CheckConstraint, Column, Date, DateTime, ForeignKey, Index,
    Integer, LargeBinary, Numeric, String, Text, create_engine,
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

_BRT = timezone(timedelta(hours=-3))

MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")

# E-mail do dono da página. Não é is_admin nem all_pages de propósito.
OWNER_EMAIL = os.environ.get("CONTROLE_PJS_OWNER", "cristiano@camim.com.br").lower()

PAGE_KEY = "controle_pjs"

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
    """Engine do RDS, ou None sem credenciais — importar este módulo não pode
    explodir por falta de .env (o app.py carrega o blueprint em try/except)."""
    if _shared_engine is not None:
        return _shared_engine
    try:
        return create_engine(_build_dsn(), pool_pre_ping=True, pool_recycle=280)
    except KeyError:
        return None


pj_engine = _make_engine()
Base = declarative_base()
PjSession = sessionmaker(bind=pj_engine) if pj_engine is not None else None


def agora_brt() -> datetime:
    return datetime.now(_BRT).replace(tzinfo=None)


def competencia_ok(s: str) -> bool:
    return bool(s and MONTH_RE.match(s))


class PjEmpresa(Base):
    __tablename__ = "pj_empresa"

    id = Column(Integer, primary_key=True)
    nome = Column(String(200), nullable=False)
    cnpj = Column(String(20), nullable=True)
    # Remetentes conhecidos (e-mails ou domínios, separados por vírgula) — é o
    # que o robô de e-mail usa para reconhecer de quem veio a fatura.
    email_remetente = Column(String(400), nullable=True)
    contato = Column(String(200), nullable=True)
    telefone = Column(String(40), nullable=True)
    observacao = Column(Text, nullable=True)
    contrato_inicio = Column(Date, nullable=True)
    contrato_fim = Column(Date, nullable=True)  # NULL = sem prazo (vigente)
    ativo = Column(Boolean, nullable=False, default=True)
    criado_em = Column(DateTime, nullable=False, default=agora_brt)
    atualizado_em = Column(DateTime, nullable=False, default=agora_brt, onupdate=agora_brt)

    boletos = relationship("PjBoleto", back_populates="empresa")
    arquivos = relationship("PjArquivo", back_populates="empresa")

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "nome": self.nome,
            "cnpj": self.cnpj or "",
            "email_remetente": self.email_remetente or "",
            "contato": self.contato or "",
            "telefone": self.telefone or "",
            "observacao": self.observacao or "",
            "contrato_inicio": self.contrato_inicio.isoformat() if self.contrato_inicio else None,
            "contrato_fim": self.contrato_fim.isoformat() if self.contrato_fim else None,
            "ativo": bool(self.ativo),
        }


class PjEmail(Base):
    __tablename__ = "pj_email"

    id = Column(Integer, primary_key=True)
    # Message-ID do e-mail — é o dedupe do robô: reprocessar a caixa não
    # duplica nada, mesmo sem depender de \Seen (a caixa é lida pelo dono).
    message_id = Column(String(300), nullable=False, unique=True)
    assunto = Column(String(500), nullable=True)
    remetente = Column(String(300), nullable=True)
    data_email = Column(DateTime, nullable=True)
    corpo_trecho = Column(Text, nullable=True)
    status = Column(String(12), nullable=False, default="pendente")
    empresa_id = Column(Integer, ForeignKey("pj_empresa.id", ondelete="SET NULL"), nullable=True)
    boleto_id = Column(Integer, ForeignKey("pj_boleto.id", ondelete="SET NULL"), nullable=True)
    criado_em = Column(DateTime, nullable=False, default=agora_brt)

    __table_args__ = (
        CheckConstraint("status IN ('pendente','lancado','descartado')",
                        name="ck_pj_email_status"),
    )

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "assunto": self.assunto or "",
            "remetente": self.remetente or "",
            "data_email": self.data_email.strftime("%d/%m/%Y %H:%M") if self.data_email else "",
            "corpo_trecho": self.corpo_trecho or "",
            "status": self.status,
            "empresa_id": self.empresa_id,
            "boleto_id": self.boleto_id,
        }


class PjBoleto(Base):
    __tablename__ = "pj_boleto"

    id = Column(Integer, primary_key=True)
    empresa_id = Column(Integer, ForeignKey("pj_empresa.id", ondelete="RESTRICT"), nullable=False)
    competencia = Column(String(7), nullable=False)  # YYYY-MM
    valor = Column(Numeric(14, 2), nullable=False)
    vencimento = Column(Date, nullable=True)
    # Letras dos postos contra os quais o boleto foi emitido ("R" ou "C,G,Y").
    # Vazio = não informado. Nomes NUNCA aqui — sempre de alarmes_db.POSTOS_NOMES.
    postos = Column(String(80), nullable=False, default="")
    descricao = Column(String(300), nullable=True)
    status = Column(String(12), nullable=False, default="recebido")
    pago_em = Column(Date, nullable=True)
    origem = Column(String(10), nullable=False, default="manual")  # manual | email
    criado_por = Column(String(200), nullable=True)
    criado_em = Column(DateTime, nullable=False, default=agora_brt)

    empresa = relationship("PjEmpresa", back_populates="boletos")
    arquivos = relationship("PjArquivo", back_populates="boleto")

    __table_args__ = (
        CheckConstraint("status IN ('recebido','conferido','pago')",
                        name="ck_pj_boleto_status"),
        Index("ix_pj_boleto_emp_comp", "empresa_id", "competencia"),
        Index("ix_pj_boleto_comp", "competencia"),
    )

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "empresa_id": self.empresa_id,
            "competencia": self.competencia,
            "valor": float(self.valor or 0),
            "vencimento": self.vencimento.isoformat() if self.vencimento else None,
            "postos": [p for p in (self.postos or "").split(",") if p],
            "descricao": self.descricao or "",
            "status": self.status,
            "pago_em": self.pago_em.isoformat() if self.pago_em else None,
            "origem": self.origem,
            "criado_por": self.criado_por or "",
            "criado_em": self.criado_em.strftime("%d/%m/%Y") if self.criado_em else "",
            "arquivos": [a.to_dict() for a in sorted(self.arquivos, key=lambda x: x.id)],
        }


class PjArquivo(Base):
    __tablename__ = "pj_arquivo"

    id = Column(Integer, primary_key=True)
    empresa_id = Column(Integer, ForeignKey("pj_empresa.id", ondelete="CASCADE"), nullable=True)
    boleto_id = Column(Integer, ForeignKey("pj_boleto.id", ondelete="CASCADE"), nullable=True)
    email_id = Column(Integer, ForeignKey("pj_email.id", ondelete="CASCADE"), nullable=True)
    tipo = Column(String(12), nullable=False, default="outro")  # contrato|boleto|nf|outro
    competencia = Column(String(7), nullable=True)  # p/ NF mensal solta (sem boleto)
    nome = Column(String(260), nullable=False)
    mime = Column(String(100), nullable=True)
    tamanho = Column(Integer, nullable=False, default=0)
    conteudo = Column(LargeBinary, nullable=False)
    enviado_por = Column(String(200), nullable=True)
    criado_em = Column(DateTime, nullable=False, default=agora_brt)

    empresa = relationship("PjEmpresa", back_populates="arquivos")
    boleto = relationship("PjBoleto", back_populates="arquivos")
    email = relationship("PjEmail")

    __table_args__ = (
        CheckConstraint("tipo IN ('contrato','boleto','nf','outro')",
                        name="ck_pj_arquivo_tipo"),
        CheckConstraint("empresa_id IS NOT NULL OR boleto_id IS NOT NULL OR email_id IS NOT NULL",
                        name="ck_pj_arquivo_dono"),
    )

    def to_dict(self) -> dict:
        # `conteudo` NÃO entra — a listagem carregaria megabytes de PDF por nada.
        # O arquivo é servido sob demanda em /api/controle-pjs/arquivos/<id>.
        return {
            "id": self.id,
            "empresa_id": self.empresa_id,
            "boleto_id": self.boleto_id,
            "email_id": self.email_id,
            "tipo": self.tipo,
            "competencia": self.competencia,
            "nome": self.nome,
            "mime": self.mime or "application/octet-stream",
            "tamanho": self.tamanho or 0,
            "enviado_por": self.enviado_por or "",
            "criado_em": self.criado_em.strftime("%d/%m/%Y") if self.criado_em else "",
        }


def init_pj_schema(engine=None) -> None:
    """create_all idempotente — usado pelo migrate_controle_pjs.py."""
    eng = engine if engine is not None else pj_engine
    if eng is None:
        raise RuntimeError("PG_RDS_* não configurado no ambiente")
    Base.metadata.create_all(eng)
