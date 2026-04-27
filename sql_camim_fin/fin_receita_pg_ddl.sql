-- DDL Postgres: relatorio_h_t.fin_receita
-- Espelha o padrão de fin_despesa (export_fin_despesa_pg.py).
-- Subset enxuto da vw_Fin_Receita (~30 colunas) — só o que importa pra
-- auditoria/Benford/detector. Expandir depois se necessário.
--
-- Aplicação manual (uma vez):
--   psql ... -f fin_receita_pg_ddl.sql
-- ou via psycopg2 / SQLAlchemy.

CREATE TABLE IF NOT EXISTS fin_receita (
    posto              CHAR(1)        NOT NULL,
    id_receita         INTEGER        NOT NULL,
    id_lancamento      INTEGER,
    id_plano           INTEGER,
    id_conta           INTEGER,
    id_conta_tipo      INTEGER,
    id_cliente         INTEGER,
    valor_devido       NUMERIC(14,2),
    valor_pago         NUMERIC(14,2),
    data_vencimento    TIMESTAMP,
    data_pagamento     TIMESTAMP,
    data_cancelamento  TIMESTAMP,
    data_prestacao     TIMESTAMP,
    descricao          VARCHAR(255),
    comentario         TEXT,
    plano              VARCHAR(120),
    tipo               VARCHAR(120),
    conta              VARCHAR(120),
    classe             VARCHAR(120),
    cliente            VARCHAR(255),
    matricula          BIGINT,
    responsavel        VARCHAR(255),
    situacao           VARCHAR(80),
    cobrador           VARCHAR(120),
    corretor           VARCHAR(120),
    usuario            VARCHAR(120),
    forma              VARCHAR(80),
    forma_tipo         VARCHAR(80),
    forma_numero       VARCHAR(80),
    talao              INTEGER,
    nosso_numero       VARCHAR(80),
    endereco           VARCHAR(255),
    bairro             VARCHAR(120),
    cep                VARCHAR(20),
    rota               VARCHAR(40),
    parcela            VARCHAR(40),
    imported_at        TIMESTAMP      NOT NULL DEFAULT NOW(),
    PRIMARY KEY (posto, id_receita)
);

CREATE INDEX IF NOT EXISTS ix_fin_receita_posto          ON fin_receita(posto);
CREATE INDEX IF NOT EXISTS ix_fin_receita_data_pagamento ON fin_receita(data_pagamento);
CREATE INDEX IF NOT EXISTS ix_fin_receita_id_conta_tipo  ON fin_receita(id_conta_tipo);
CREATE INDEX IF NOT EXISTS ix_fin_receita_tipo           ON fin_receita(tipo);
CREATE INDEX IF NOT EXISTS ix_fin_receita_classe         ON fin_receita(classe);
CREATE INDEX IF NOT EXISTS ix_fin_receita_cliente        ON fin_receita(cliente);
