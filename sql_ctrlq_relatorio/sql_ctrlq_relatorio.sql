-- SQL_CTRLQ_RELATORIO.SQL
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
  WHERE DataDia = DataFechamentoMes   -- só o último dia do mês (30/31/28/29)
),
dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY DataFechamentoMes, idmedico
      ORDER BY
        CASE WHEN PessoaJuridica = 1 THEN 0 ELSE 1 END, -- prioriza PJ=1
        DataHoraInclusao DESC,                          -- desempate: mais recente no dia
        Especialidade ASC                               -- desempate final determinístico
    ) AS rn
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
WHERE rn = 1 and Temporario = 0
ORDER BY DataFechamentoMes, medico;
-- END SQL_CTRLQ_RELATORIO.SQL