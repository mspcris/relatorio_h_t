-- sql_ctrlq_desbloqueio_aud.sql
-- Todos os registros de vw_Sis_Historico (Cad_Especialidade) no dia em que
-- DataFimExibicao foi alterada pela última vez para cada idEspecialidade.
-- Executada com fallback silencioso: se falhar, aud_historico fica vazio.

SELECT
    CAST(h.id AS INT)        AS idEspecialidade,
    h.idHistorico            AS aud_idHistorico,
    h.[Data]                 AS aud_data,
    h.[Usuário]              AS aud_usuario,
    h.Detalhe                AS aud_detalhe,
    h.Comando                AS aud_comando,
    h.Descrição              AS aud_descricao,
    h.Computador             AS aud_computador
FROM vw_Sis_Historico h
INNER JOIN (
    -- Dia em que DataFimExibicao foi alterada pela última vez (por idEspecialidade)
    SELECT id, CAST([Data] AS DATE) AS dia
    FROM (
        SELECT id, [Data],
               ROW_NUMBER() OVER (PARTITION BY id ORDER BY idHistorico DESC) AS rn
        FROM vw_Sis_Historico
        WHERE Tabela  = 'Cad_Especialidade'
          AND Comando = 'Edição'
          AND Detalhe LIKE '%DataFimExibicao%'
    ) x
    WHERE rn = 1
) ref ON CAST(h.id AS INT) = CAST(ref.id AS INT)
WHERE h.Tabela = 'Cad_Especialidade'
  AND CAST(h.[Data] AS DATE) = ref.dia
ORDER BY CAST(h.id AS INT), h.idHistorico;

-- END sql_ctrlq_desbloqueio_aud.sql
