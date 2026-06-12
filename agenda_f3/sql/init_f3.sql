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
    idlancamento     BIGINT,                 -- chave p/ confirmar presença (UPDATE pontual)
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
    observacao       TEXT,                   -- Cad_Lancamento.Observacao (log da agenda/reagendamento)
    medico_sala      TEXT,                   -- Cad_Medico.Sala (ex.: "SALA 7", "MAPA")
    medico_obs       TEXT,                   -- falta/fechamento: MÉDICO FALTOU (total) / HORÁRIO ALTERADO (parcial)
    gerado_em        TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Migração idempotente p/ bancos que já tinham a tabela sem as colunas novas
ALTER TABLE agenda_dia ADD COLUMN IF NOT EXISTS observacao   TEXT;
ALTER TABLE agenda_dia ADD COLUMN IF NOT EXISTS medico_sala  TEXT;
ALTER TABLE agenda_dia ADD COLUMN IF NOT EXISTS medico_obs   TEXT;
ALTER TABLE agenda_dia ADD COLUMN IF NOT EXISTS idlancamento BIGINT;

CREATE INDEX IF NOT EXISTS idx_agenda_dia_posto_data
    ON agenda_dia (posto, data);

-- Confirmação de presença faz UPDATE pontual por (posto, idlancamento)
CREATE INDEX IF NOT EXISTS idx_agenda_dia_idlancamento
    ON agenda_dia (posto, idlancamento);

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

-- CRM criado pela agenda (local-first: grava aqui na hora, worker sobe pra
-- Campinho via API camila3/sp_CRM_Insert em background, com retry).
-- A app também cria esta tabela no boot (f3_db.ensure_crm_table) — este DDL
-- existe pra documentação/aplicação manual.
CREATE TABLE IF NOT EXISTS crm_local (
    id                   BIGSERIAL    PRIMARY KEY,
    posto                CHAR(1)      NOT NULL,    -- posto da agenda (lançamento)
    idlancamento         BIGINT       NOT NULL,
    posto_cliente        VARCHAR(4),               -- posto do cliente (resolve matrícula)
    matricula            BIGINT,
    paciente             TEXT,
    id_motivo            INTEGER      NOT NULL,    -- Cad_ClienteHistoricoMotivo
    motivo               TEXT,
    id_tipo              INTEGER      NOT NULL,    -- Cad_ClienteHistoricoTipo
    tipo                 TEXT,
    pessoa               TEXT,
    telefone             TEXT,
    historico            TEXT         NOT NULL,
    criado_por           TEXT,                     -- login_campinho do operador
    id_usuario_campinho  INTEGER,                  -- idUsuario em sis_usuario do banco C
    criado_em            TIMESTAMPTZ  NOT NULL,
    sync_status          VARCHAR(16)  NOT NULL DEFAULT 'pendente',  -- pendente|erro|enviado
    sync_attempts        INTEGER      NOT NULL DEFAULT 0,
    sync_error           TEXT,
    synced_at            TIMESTAMPTZ,
    claimed_em           TIMESTAMPTZ,              -- claim atômico do upload (2 workers)
    id_cliente_historico BIGINT,                   -- retorno de Campinho
    protocolo            TEXT
);

-- Um CRM por lançamento da agenda
CREATE UNIQUE INDEX IF NOT EXISTS idx_crm_local_posto_lanc
    ON crm_local (posto, idlancamento);
