SET NOCOUNT ON;

SELECT
    YEAR(DataConsulta)  AS ano,
    MONTH(DataConsulta) AS mes,
    Especialidade,
    NomeMedico as medico,
    Atendido as 'status',
    COUNT(*) AS qtde
FROM vw_Cad_LancamentoProntuarioComDesistencia
WHERE DataConsulta >= :ini
  AND DataConsulta <  :fim
  AND Servico LIKE '%consulta%'
  AND Desistencia = 0
  AND HoraPrevistaConsulta is NOT NULL
GROUP BY
    YEAR(DataConsulta),
    MONTH(DataConsulta),
    Especialidade,
    NomeMedico,
    Atendido
ORDER BY
    ano,
    mes;