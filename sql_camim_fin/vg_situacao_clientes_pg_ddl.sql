-- ============================================================================
-- KPI Indicadores Vinícius Gama — Indicador 1: Situação dos Clientes
--
-- Banco: Postgres RDS AWS, DB relatorio_h_t (mesmas credenciais PG_RDS_*).
-- ETL:   export_vg_situacao_clientes.py (full rebuild por posto a cada run).
--
-- Dados ISOLADOS deste KPI — nenhuma outra página/ETL lê ou escreve aqui.
--
-- O ETL executa este CREATE TABLE IF NOT EXISTS automaticamente no boot;
-- este arquivo é a referência canônica + o INSERT do catálogo de serviços
-- (esse sim, aplicar manualmente UMA vez).
-- ============================================================================

CREATE TABLE IF NOT EXISTS kpi_vg_situacao_clientes (
    id                        BIGSERIAL PRIMARY KEY,
    posto                     CHAR(1)       NOT NULL,
    id_cliente                INTEGER       NOT NULL,
    matricula                 VARCHAR(20),
    nome                      VARCHAR(200),
    cpf                       VARCHAR(20),
    data_admissao             DATE,
    situacao                  VARCHAR(80),
    situacao_clube            VARCHAR(80),
    idade                     INTEGER,
    sexo                      VARCHAR(20),
    responsavel               VARCHAR(200),
    telefone_whatsapp         VARCHAR(40),
    telefone_celular          VARCHAR(40),
    valor_devido              NUMERIC(14,2),
    valor_pago                NUMERIC(14,2),
    receitas_qtd              INTEGER NOT NULL DEFAULT 0,  -- linhas em vw_fin_receita2 (idcontatipo=5) somadas
    dependentes_qtd           INTEGER NOT NULL DEFAULT 0,
    consultas_dia_adesao_qtd  INTEGER NOT NULL DEFAULT 0,
    usou_plano_dia_adesao     BOOLEAN NOT NULL DEFAULT FALSE,
    consultas_futuras_qtd     INTEGER NOT NULL DEFAULT 0,  -- relativo a "hoje" do último run do ETL
    tem_consulta_futura       BOOLEAN NOT NULL DEFAULT FALSE,
    matriculas_anteriores_qtd INTEGER NOT NULL DEFAULT 0,  -- matrículas distintas ≠ a atual (tit+dep+resp, todos os postos)
    mat_ant_titular_qtd       INTEGER NOT NULL DEFAULT 0,
    mat_ant_dependente_qtd    INTEGER NOT NULL DEFAULT 0,
    mat_ant_responsavel_qtd   INTEGER NOT NULL DEFAULT 0,
    atualizado_em             TIMESTAMP NOT NULL DEFAULT now(),
    UNIQUE (posto, id_cliente)
);

CREATE INDEX IF NOT EXISTS ix_vg_sit_cli_data_admissao
    ON kpi_vg_situacao_clientes (data_admissao);
CREATE INDEX IF NOT EXISTS ix_vg_sit_cli_posto
    ON kpi_vg_situacao_clientes (posto);

-- Detalhe: cada matrícula anterior encontrada por CPF (titular + dependentes
-- da matrícula nova), pesquisada em TODOS os postos, nos 3 papéis.
CREATE TABLE IF NOT EXISTS kpi_vg_matriculas_anteriores (
    id                       BIGSERIAL PRIMARY KEY,
    posto                    CHAR(1)     NOT NULL,  -- posto da matrícula NOVA
    id_cliente               INTEGER     NOT NULL,  -- idcliente da matrícula NOVA
    cpf                      VARCHAR(20),
    pessoa_nome              VARCHAR(200),
    papel_na_nova            VARCHAR(12) NOT NULL,  -- titular | dependente
    posto_anterior           CHAR(1),
    id_cliente_anterior      INTEGER,
    id_dependente_anterior   INTEGER,
    matricula_anterior       VARCHAR(20),
    papel_anterior           VARCHAR(12),           -- titular | dependente | responsavel
    nome_anterior            VARCHAR(200),
    data_admissao_anterior   DATE,
    desativado_anterior      BOOLEAN,
    atualizado_em            TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_vg_mat_ant_cliente
    ON kpi_vg_matriculas_anteriores (posto, id_cliente);

-- ============================================================================
-- Catálogo de serviços (controle de acesso / admin) — aplicar 1x manualmente
-- ============================================================================
INSERT INTO servicos (key, label, href, group_name, lock, ordem, ativo,
                      descricao, icone, created_at, updated_at)
VALUES ('indicadores_vg',
        'Indicadores Vinícius Gama',
        'indicadores_vg.html',
        'kpi', NULL, 920, TRUE,
        'Situação dos clientes admitidos: uso do plano no dia da adesão, consultas futuras, dependentes e matrículas anteriores',
        'fas fa-user-check', now(), now())
ON CONFLICT (key) DO NOTHING;
