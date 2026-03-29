-- sql_ctrlq_desbloqueio_aud.sql
-- Registros de vw_Sis_Historico (Cad_Especialidade) para cada idEspecialidade
-- com DataFimExibicao preenchida.
--
-- Lógica:
--   1) Busca registros na janela [DataFimExibicao - 10 dias, DataFimExibicao]
--   2) Se não encontrar nada na janela, traz os 5 últimos registros do idEspecialidade
--      e marca com aud_fallback = 1

WITH esp AS (
    SELECT ce.idEspecialidade, ce.DataFimExibicao
    FROM cad_especialidade ce
    WHERE ce.Desativado = 0
      AND ce.DataFimExibicao IS NOT NULL
      AND ce.Temporario = 0
),
-- Registros na janela de 10 dias antes da DataFimExibicao
janela AS (
    SELECT
        CAST(h.id AS INT)        AS idEspecialidade,
        h.idHistorico            AS aud_idHistorico,
        h.[Data]                 AS aud_data,
        h.[Usuário]              AS aud_usuario,
        h.Detalhe                AS aud_detalhe,
        h.Comando                AS aud_comando,
        h.[Descrição]            AS aud_descricao,
        h.Computador             AS aud_computador,
        0                        AS aud_fallback
    FROM vw_Sis_Historico h
    INNER JOIN esp e ON CAST(h.id AS INT) = e.idEspecialidade
    WHERE h.Tabela = 'Cad_Especialidade'
      AND CAST(h.[Data] AS DATE) BETWEEN DATEADD(DAY, -10, e.DataFimExibicao)
                                      AND e.DataFimExibicao
),
-- IDs que NÃO têm registros na janela
sem_janela AS (
    SELECT e.idEspecialidade
    FROM esp e
    WHERE NOT EXISTS (
        SELECT 1 FROM janela j WHERE j.idEspecialidade = e.idEspecialidade
    )
),
-- Últimos 5 registros para cada ID sem janela
fallback AS (
    SELECT
        idEspecialidade,
        aud_idHistorico,
        aud_data,
        aud_usuario,
        aud_detalhe,
        aud_comando,
        aud_descricao,
        aud_computador,
        1 AS aud_fallback
    FROM (
        SELECT
            CAST(h.id AS INT)        AS idEspecialidade,
            h.idHistorico            AS aud_idHistorico,
            h.[Data]                 AS aud_data,
            h.[Usuário]              AS aud_usuario,
            h.Detalhe                AS aud_detalhe,
            h.Comando                AS aud_comando,
            h.[Descrição]            AS aud_descricao,
            h.Computador             AS aud_computador,
            ROW_NUMBER() OVER (PARTITION BY CAST(h.id AS INT) ORDER BY h.idHistorico DESC) AS rn
        FROM vw_Sis_Historico h
        INNER JOIN sem_janela sj ON CAST(h.id AS INT) = sj.idEspecialidade
        WHERE h.Tabela = 'Cad_Especialidade'
    ) x
    WHERE rn <= 5
)

SELECT * FROM janela
UNION ALL
SELECT * FROM fallback
ORDER BY idEspecialidade, aud_idHistorico;

-- END sql_ctrlq_desbloqueio_aud.sql
