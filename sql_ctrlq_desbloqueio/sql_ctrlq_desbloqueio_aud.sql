-- sql_ctrlq_desbloqueio_aud.sql
-- Última alteração que definiu DataFimExibicao para cada idEspecialidade.
-- Consultada separadamente (vw_Sis_Historico pode exigir permissões extras).
-- Executada com fallback silencioso: se falhar, campos aud_* ficam nulos.

SELECT
    CAST(id AS INT)   AS idEspecialidade,
    idHistorico       AS aud_idHistorico,
    [Data]            AS aud_data,
    [Usuário]         AS aud_usuario,
    Detalhe           AS aud_detalhe
FROM (
    SELECT
        id, idHistorico, [Data], [Usuário], Detalhe,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY idHistorico DESC) AS rn
    FROM vw_Sis_Historico
    WHERE Tabela  = 'Cad_Especialidade'
      AND Comando = 'Edição'
      AND Detalhe LIKE '%DataFimExibicao%'
) x
WHERE rn = 1;

-- END sql_ctrlq_desbloqueio_aud.sql
