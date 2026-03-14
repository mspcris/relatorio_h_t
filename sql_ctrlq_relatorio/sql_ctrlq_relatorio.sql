-- SQL_CTRLQ_RELATORIO.SQL

DECLARE @asof date = DATEADD(day, -1, CONVERT(date, GETDATE()));  -- ontem
DECLARE @start date = DATEADD(month, -12, DATEFROMPARTS(YEAR(@asof), MONTH(@asof), 1)); -- 12 meses completos + mês atual

WITH base AS (
  SELECT
    eh.DataHoraInclusao,
    CONVERT(date, eh.DataHoraInclusao) AS DataDia,
    EOMONTH(eh.DataHoraInclusao)       AS DataFechamentoMes,

    m.idmedico,
    m.nome,
    m.crm,
    eh.Especialidade,

    m.CertificadoPFX,
    eh.PermitirAgendamentoquenuncaconsultou,
    m.PessoaJuridica,
    eh.NumeroRQE,
    eh.temporario
  FROM Cad_EspecialidadeHistorico eh
  LEFT JOIN cad_medico m ON m.idmedico = eh.idmedico
  WHERE eh.Desativado = 0
    AND CONVERT(date, eh.DataHoraInclusao) >= @start
    AND CONVERT(date, eh.DataHoraInclusao) <= @asof
    AND m.nome NOT LIKE '%sede%'
    AND m.nome NOT LIKE '%agendamento%'
    AND m.nome NOT LIKE '%teste%'
    AND m.nome NOT LIKE '%fake%'
    AND eh.especialidade NOT IN (
      'enfermeiro','eletrocardiograma','espirometria','biologista','mamografia',
      'tecnico em radiologia','HOLTER 24 HRS','mapa'
    )
),
fechamento AS (
  SELECT *
  FROM base
  WHERE
    (
      -- meses fechados: pega só o último dia do mês
      DataFechamentoMes < EOMONTH(@asof)
      AND DataDia = DataFechamentoMes
    )
    OR
    (
      -- mês vigente: pega o snapshot de ontem
      DataFechamentoMes = EOMONTH(@asof)
      AND DataDia = @asof
    )
),
dedup AS (
  SELECT
    *,
ROW_NUMBER() OVER (
  PARTITION BY DataFechamentoMes, idmedico
  ORDER BY temporario ASC, DataHoraInclusao DESC
) rn

  FROM fechamento
)
SELECT
  DataFechamentoMes,
  DataHoraInclusao,
  nome AS medico,
  crm,
  especialidade,
  temporario,
  IIF(CertificadoPFX IS NULL, 0, 1) AS certificadodigital,
  permitiragendamentoquenuncaconsultou,
  pessoajuridica,
  IIF(NumeroRQE IS NULL, 0, 1) AS rqe
FROM dedup
WHERE rn = 1
  AND Temporario = 0
ORDER BY DataFechamentoMes, medico;

-- END SQL_CTRLQ_RELATORIO.SQL
