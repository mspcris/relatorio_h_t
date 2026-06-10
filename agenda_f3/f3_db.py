"""
f3_db.py — conexão com Postgres "f3" + helpers de upsert atômico por posto.

A app (app.py) lê dados daqui; o ETL (export_agenda_dia.py) escreve aqui.
Schema definido em sql/init_f3.sql.

Regra crítica de atomicidade (incidente 2026-05-06 documentado no CLAUDE.md
não se aplica aqui, mas o princípio é o mesmo): o ETL só chama replace_posto()
DEPOIS de ter baixado e validado os dados do SQL Server. Se algo falhar antes,
o dado antigo permanece intacto e a UI mostra um banner alertando "última
atualização há X minutos".
"""
import os
from datetime import datetime, timezone

from sqlalchemy import (
    create_engine, Column, Integer, BigInteger, String, Boolean, Date, Text,
    SmallInteger, DateTime, Index, text,
)
from sqlalchemy.orm import declarative_base, sessionmaker


# ── Engine ────────────────────────────────────────────────────────────────────

def _build_dsn() -> str:
    host = os.environ["PG_RDS_HOST"]
    port = os.environ.get("PG_RDS_PORT", "9432")
    db   = os.environ.get("PG_RDS_DB_F3", "f3")
    usr  = os.environ["PG_RDS_USER"]
    pwd  = os.environ["PG_RDS_PASSWORD"]
    ssl  = os.environ.get("PG_RDS_SSLMODE", "require")
    return f"postgresql+psycopg2://{usr}:{pwd}@{host}:{port}/{db}?sslmode={ssl}"


engine = create_engine(_build_dsn(), pool_pre_ping=True, pool_recycle=1800)
Session = sessionmaker(bind=engine, autocommit=False, autoflush=False)
Base = declarative_base()


# ── Models ────────────────────────────────────────────────────────────────────

class AgendaDia(Base):
    __tablename__ = "agenda_dia"

    id               = Column(BigInteger, primary_key=True, autoincrement=True)
    posto            = Column(String(1), nullable=False)
    data             = Column(Date,      nullable=False)
    matricula        = Column(BigInteger)        # bigint p/ segurança
    cfcliente        = Column(String(4))
    posto_cliente    = Column(String(4))
    paciente         = Column(Text)
    idade            = Column(Integer)
    especialidade    = Column(Text)
    medico           = Column(Text)
    hora_prevista    = Column(String(5))
    hora_confirmacao = Column(String(5))
    dias_agend_cons  = Column(BigInteger)        # bigint p/ casos extremos DATEDIFF
    atendido         = Column(Text)
    desistencia      = Column(SmallInteger, default=0, nullable=False)
    situacao         = Column(Text)
    pagou_no_dia     = Column(Boolean, default=False, nullable=False)
    idendereco       = Column(BigInteger)
    observacao       = Column(Text)
    gerado_em        = Column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        Index("idx_agenda_dia_posto_data", "posto", "data"),
    )


class AgendaDiaMeta(Base):
    __tablename__ = "agenda_dia_meta"

    posto       = Column(String(1), primary_key=True)
    gerado_em   = Column(DateTime(timezone=True), nullable=False)
    sucesso     = Column(Boolean, nullable=False)
    erro        = Column(Text)
    n_registros = Column(Integer, nullable=False, default=0)


class AgendaDiaRun(Base):
    __tablename__ = "agenda_dia_run"

    id               = Column(Integer, primary_key=True, default=1)
    iniciado_em      = Column(DateTime(timezone=True))
    terminou_em      = Column(DateTime(timezone=True))
    duracao_seg      = Column(Integer)
    total_postos_ok  = Column(Integer)
    total_postos_err = Column(Integer)
    total_pacientes  = Column(Integer)


# ── Helpers de escrita atômica ───────────────────────────────────────────────

def replace_posto(posto: str, datas: list, pacientes: list, gerado_em: datetime):
    """REPLACE atômico dos dados de um posto.

    1) BEGIN
    2) DELETE WHERE posto=X AND data IN (...)
    3) INSERT linhas novas
    4) UPSERT agenda_dia_meta(posto) com sucesso=True
    5) COMMIT

    Se qualquer passo falhar, ROLLBACK completo — dado antigo permanece.
    Idempotente: rodar 2x produz o mesmo estado final.

    `pacientes` é uma lista de dicts com chaves matching as colunas de AgendaDia
    (exceto id e gerado_em — esses são preenchidos aqui).
    `datas` é a lista de datas (date) que esses pacientes cobrem (pra o DELETE
    saber o escopo). Tipicamente [hoje, amanhã].
    """
    s = Session()
    try:
        s.execute(
            text("DELETE FROM agenda_dia WHERE posto = :p AND data = ANY(:datas)"),
            {"p": posto, "datas": datas},
        )

        if pacientes:
            for p in pacientes:
                p["posto"]     = posto
                p["gerado_em"] = gerado_em
            s.execute(AgendaDia.__table__.insert(), pacientes)

        s.execute(text("""
            INSERT INTO agenda_dia_meta (posto, gerado_em, sucesso, erro, n_registros)
            VALUES (:posto, :gerado_em, TRUE, NULL, :n)
            ON CONFLICT (posto) DO UPDATE SET
                gerado_em   = EXCLUDED.gerado_em,
                sucesso     = TRUE,
                erro        = NULL,
                n_registros = EXCLUDED.n_registros
        """), {"posto": posto, "gerado_em": gerado_em, "n": len(pacientes)})

        s.commit()
    except Exception:
        s.rollback()
        raise
    finally:
        s.close()


def mark_posto_failure(posto: str, erro: str, gerado_em: datetime | None = None):
    """Registra falha do posto em agenda_dia_meta.

    NÃO toca agenda_dia — dado antigo do posto permanece intacto.
    A UI verá um gerado_em antigo (banner amarelo/vermelho) + sucesso=False.
    """
    if gerado_em is None:
        gerado_em = datetime.now(timezone.utc)

    s = Session()
    try:
        s.execute(text("""
            INSERT INTO agenda_dia_meta (posto, gerado_em, sucesso, erro, n_registros)
            VALUES (:posto, :gerado_em, FALSE, :erro, 0)
            ON CONFLICT (posto) DO UPDATE SET
                gerado_em = EXCLUDED.gerado_em,
                sucesso   = FALSE,
                erro      = EXCLUDED.erro
        """), {"posto": posto, "gerado_em": gerado_em, "erro": str(erro)[:1000]})
        s.commit()
    except Exception:
        s.rollback()
        raise
    finally:
        s.close()


def update_run(iniciado_em, terminou_em, duracao_seg, total_postos_ok,
               total_postos_err, total_pacientes):
    """Atualiza a linha única de agenda_dia_run."""
    s = Session()
    try:
        s.execute(text("""
            INSERT INTO agenda_dia_run (id, iniciado_em, terminou_em, duracao_seg,
                                        total_postos_ok, total_postos_err, total_pacientes)
            VALUES (1, :ini, :fim, :dur, :ok, :err, :pac)
            ON CONFLICT (id) DO UPDATE SET
                iniciado_em      = EXCLUDED.iniciado_em,
                terminou_em      = EXCLUDED.terminou_em,
                duracao_seg      = EXCLUDED.duracao_seg,
                total_postos_ok  = EXCLUDED.total_postos_ok,
                total_postos_err = EXCLUDED.total_postos_err,
                total_pacientes  = EXCLUDED.total_pacientes
        """), {
            "ini": iniciado_em, "fim": terminou_em, "dur": duracao_seg,
            "ok": total_postos_ok, "err": total_postos_err, "pac": total_pacientes,
        })
        s.commit()
    finally:
        s.close()


# ── Helpers de leitura (consumidos pela app Flask) ───────────────────────────

def fetch_agenda(posto: str, data_iso: str) -> tuple[list[dict], datetime | None]:
    """Retorna (pacientes, gerado_em) de um posto/data.

    pacientes: lista de dicts com colunas de AgendaDia (sem id).
    gerado_em: timestamp da última escrita bem-sucedida (None se nunca rodou).
    """
    s = Session()
    try:
        rows = s.execute(text("""
            SELECT matricula, cfcliente, posto_cliente, paciente, idade, especialidade,
                   medico, hora_prevista, hora_confirmacao, dias_agend_cons, atendido,
                   desistencia, situacao, pagou_no_dia, idendereco, observacao, gerado_em
              FROM agenda_dia
             WHERE posto = :p AND data = :d
        """), {"p": posto, "d": data_iso}).mappings().all()

        pacientes = [dict(r) for r in rows]
        gerado_em = pacientes[0]["gerado_em"] if pacientes else None

        if not pacientes:
            # Posto pode estar válido mas sem agenda; ainda assim queremos
            # mostrar o gerado_em da última rodada de sucesso.
            meta = s.execute(text("""
                SELECT gerado_em FROM agenda_dia_meta
                 WHERE posto = :p AND sucesso = TRUE
            """), {"p": posto}).first()
            if meta:
                gerado_em = meta[0]

        # Remove gerado_em de cada paciente (vai no payload top-level)
        for p in pacientes:
            p.pop("gerado_em", None)

        return pacientes, gerado_em
    finally:
        s.close()


def fetch_meta_all() -> dict:
    """Status por posto: {posto: {gerado_em, sucesso, erro, n_registros}}."""
    s = Session()
    try:
        rows = s.execute(text("""
            SELECT posto, gerado_em, sucesso, erro, n_registros
              FROM agenda_dia_meta
        """)).mappings().all()
        return {r["posto"]: dict(r) for r in rows}
    finally:
        s.close()
