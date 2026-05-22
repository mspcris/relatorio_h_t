-- =============================================================================
-- agenda_f3 — schema do banco "f3" no RDS Postgres
-- =============================================================================
-- Aplicar UMA VEZ, depois do CREATE DATABASE f3:
--   psql -h <RDS_HOST> -U camim_pg -d f3 -f /opt/agenda_f3/sql/init_f3.sql
--
-- Objetivo: backup/cache do snapshot da agenda do dia. O ETL roda a cada 30
-- minutos e faz REPLACE atômico por posto (DELETE+INSERT em transação) só
-- DEPOIS de ter baixado e validado os dados do SQL Server CAMIM. Se o SQL
-- cair, o snapshot anterior fica intacto.
-- =============================================================================

-- Snapshot atual da agenda (REPLACE por posto a cada execução bem-sucedida)
CREATE TABLE IF NOT EXISTS agenda_dia (
    id               BIGSERIAL    PRIMARY KEY,
    posto            CHAR(1)      NOT NULL,
    data             DATE         NOT NULL,
    matricula        BIGINT,                 -- bigint p/ segurança (KPI já viu erro out-of-range)
    cfcliente        VARCHAR(4),
    posto_cliente    VARCHAR(4),
    paciente         TEXT,
    idade            INTEGER,
    especialidade    TEXT,
    medico           TEXT,
    hora_prevista    CHAR(5),
    hora_confirmacao CHAR(5),
    dias_agend_cons  BIGINT,                 -- bigint p/ casos extremos de DATEDIFF
    atendido         TEXT,
    desistencia      SMALLINT     NOT NULL DEFAULT 0,
    situacao         TEXT,
    pagou_no_dia     BOOLEAN      NOT NULL DEFAULT false,
    idendereco       BIGINT,
    gerado_em        TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_agenda_dia_posto_data
    ON agenda_dia (posto, data);

-- Status da última rodada por posto (atualizado SEMPRE — inclusive em falha)
CREATE TABLE IF NOT EXISTS agenda_dia_meta (
    posto        CHAR(1)      PRIMARY KEY,
    gerado_em    TIMESTAMPTZ  NOT NULL,
    sucesso      BOOLEAN      NOT NULL,
    erro         TEXT,
    n_registros  INTEGER      NOT NULL DEFAULT 0
);

-- Meta global da última execução do ETL (1 linha só)
CREATE TABLE IF NOT EXISTS agenda_dia_run (
    id               INTEGER      PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    iniciado_em      TIMESTAMPTZ,
    terminou_em      TIMESTAMPTZ,
    duracao_seg      INTEGER,
    total_postos_ok  INTEGER,
    total_postos_err INTEGER,
    total_pacientes  INTEGER
);

-- Garante uma linha em agenda_dia_run pra o ETL sempre poder UPDATE
INSERT INTO agenda_dia_run (id) VALUES (1)
    ON CONFLICT (id) DO NOTHING;
