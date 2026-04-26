-- ============================================================
-- Pre-Agendamento - query base por posto
-- Usada por export_preagendamento.py (ETL noturno)
-- Parametros bind: dt_ini e dt_fim (ambos DD/MM/YYYY, fim exclusivo)
-- IMPORTANTE: nao usar dois pontos antes dos nomes dos parametros nos
-- comentarios - o SQLAlchemy detecta e tenta bindar de novo.
--
-- Mantem ambas populacoes (desistencia 0 e 1) - front filtra.
-- READ UNCOMMITTED para reduzir tempo (dados analiticos, nao transacionais).
-- ============================================================
SET NOCOUNT ON;
SET DATEFORMAT dmy;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    l.idLancamento                                                    AS id_lancamento,
    c.matriculal                                                      AS matricula,
    f3.Paciente                                                       AS paciente,
    f3.Atendido                                                       AS atendido,
    CONVERT(varchar(10), l.[Data], 120)                               AS data_lancamento,
    CONVERT(varchar(19), l.DataHoraNotificacaoPreAgendamento, 120)    AS data_push,
    CONVERT(varchar(19), l.DataConfirmacaoAgendamentoConsulta, 120)   AS data_conf_agend,
    CONVERT(varchar(10), l.DataConsulta, 120)                         AS data_consulta,
    CONVERT(varchar(5),  l.HoraPrevistaConsulta, 108)                 AS hora_consulta,
    CONVERT(varchar(19), l.dataconfirmacaoConsulta, 120)              AS data_conf_chegada,
    f3.Dif_dias_agend_cons                                            AS dif_dias,
    f3.NomeMedico                                                     AS medico,
    f3.Especialidade                                                  AS especialidade,
    l.MarcadoViaAgendaUnificada                                       AS via_asu,
    l.MarcadoViaWeb                                                   AS via_web,
    l.CtrlF6                                                          AS via_f6,
    l.ValorPago                                                       AS valor_pago,
    l.idCliente                                                       AS id_cliente,
    l.codigo                                                          AS talao,
    l.consulta                                                        AS nro_consulta,
    f3.desistencia                                                    AS desistencia
FROM cad_lancamento                          l   WITH (NOLOCK)
JOIN Cad_LancamentoServico                   ls  WITH (NOLOCK)  ON ls.idLancamento = l.idLancamento
JOIN vw_Cad_LancamentoProntuarioComDesistencia f3                 ON ls.idLancamentoServico = f3.idLancamentoServico
JOIN vw_cad_cliente                          c                    ON c.idCliente = l.idCliente
WHERE l.consulta              IS NOT NULL
  AND l.desativado            = 0
  AND l.HoraPrevistaConsulta  IS NOT NULL
  AND l.DataConsulta          >= :dt_ini
  AND l.DataConsulta          <  :dt_fim
