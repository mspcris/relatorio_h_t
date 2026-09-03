-- sql_ctrlq_desbloqueio_aud.sql
-- Auditoria (vw_Sis_Historico, Tabela = Cad_Especialidade) de cada
-- idEspecialidade com DataFimExibicao preenchida.
--
-- Quem decide o que é "gatilho" é o ETL (ctrlq_desbloqueio.py): ele procura
-- a linha cujo Detalhe traz "DataFimExibicao de (vazio) para <data>" — é o
-- momento em que o ERP abriu o desbloqueio (mudança de custo/tempo semanal)
-- — e recorta o histórico DESDE esse ponto. Por isso aqui vem o histórico
-- inteiro dos últimos 365 dias, sem janela relativa à data fim.
--
-- Por que não a janela "10 dias antes da DataFimExibicao" que existia antes:
-- a data fim é definida para +8 dias e depois PRORROGADA (Dr. Milton, R:
-- gatilho em 24/07, data fim 05/09). Com a janela antiga o gatilho ficava
-- fora, caía no fallback "últimos 5" e a página não conseguia dizer por que
-- o registro estava ali (2026-09-03).
--
-- aud_fallback = 1 marca os IDs sem NENHUMA linha nos 365 dias — para esses
-- vêm os 10 últimos registros, só para não ficar em branco.

WITH esp AS (
    SELECT ce.idEspecialidade
    FROM cad_especialidade ce
    WHERE ce.Desativado = 0
      AND ce.DataFimExibicao IS NOT NULL
      AND ce.Temporario = 0
),
hist AS (
    SELECT
        CAST(h.id AS INT)        AS idEspecialidade,
        h.idHistorico            AS aud_idHistorico,
        h.[Data]                 AS aud_data,
        h.[Usuário]              AS aud_usuario,
        h.Detalhe                AS aud_detalhe,
        h.Comando                AS aud_comando,
        h.[Descrição]            AS aud_descricao,
        h.Computador             AS aud_computador
    FROM vw_Sis_Historico h
    INNER JOIN esp e ON CAST(h.id AS INT) = e.idEspecialidade
    WHERE h.Tabela = 'Cad_Especialidade'
),
recente AS (
    SELECT *, 0 AS aud_fallback
    FROM hist
    WHERE aud_data >= DATEADD(DAY, -365, GETDATE())
),
sem_recente AS (
    SELECT e.idEspecialidade
    FROM esp e
    WHERE NOT EXISTS (SELECT 1 FROM recente r WHERE r.idEspecialidade = e.idEspecialidade)
),
fallback AS (
    SELECT idEspecialidade, aud_idHistorico, aud_data, aud_usuario, aud_detalhe,
           aud_comando, aud_descricao, aud_computador, 1 AS aud_fallback
    FROM (
        SELECT h.*, ROW_NUMBER() OVER (PARTITION BY h.idEspecialidade ORDER BY h.aud_idHistorico DESC) AS rn
        FROM hist h
        INNER JOIN sem_recente s ON s.idEspecialidade = h.idEspecialidade
    ) x
    WHERE rn <= 10
)
SELECT * FROM recente
UNION ALL
SELECT * FROM fallback
ORDER BY idEspecialidade, aud_idHistorico;

-- END sql_ctrlq_desbloqueio_aud.sql
