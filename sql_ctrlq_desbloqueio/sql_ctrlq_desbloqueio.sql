-- SQL_CTRLQ_DESBLOQUEIO.SQL
-- Solicitações de desbloqueio de agenda: registros de cad_especialidade com
-- DataFimExibicao definida (Temporario=0, Desativado=0).
-- Traz auditoria (vw_Sis_Historico) para saber quando/quem solicitou
-- e snapshot histórico anterior (Cad_EspecialidadeHistorico) para comparação.

SELECT
    -- Identificação
    ce.idEspecialidade,
    ce.idMedico,
    m.nome                                         AS medico,
    m.crm,
    ce.Especialidade,

    -- Período do desbloqueio
    ce.DataInicioExibicao,
    ce.DataFimExibicao,
    ce.ObservacaoDesbloqueio,

    -- Flag atual
    ce.PermitirAgendamentoquenuncaconsultou         AS atual_PermitirSemConsulta,

    -- Custos atuais (por dia da semana)
    ce.ValorCustoSegunda,      ce.ValorCustoTerca,    ce.ValorCustoQuarta,
    ce.ValorCustoQuinta,       ce.ValorCustoSexta,    ce.ValorCustoSabado,    ce.ValorCustoDomingo,
    ce.QuantidadeCustoSegunda, ce.QuantidadeCustoTerca, ce.QuantidadeCustoQuarta,
    ce.QuantidadeCustoQuinta,  ce.QuantidadeCustoSexta, ce.QuantidadeCustoSabado, ce.QuantidadeCustoDomingo,

    -- Auditoria: quando e por quem o DataFimExibicao foi definido
    aud.idHistorico                                AS aud_idHistorico,
    aud.[Data]                                     AS aud_data,
    aud.[Usuário]                                  AS aud_usuario,
    aud.Detalhe                                    AS aud_detalhe,

    -- Snapshot histórico imediatamente anterior à solicitação de desbloqueio
    h.DataHoraInclusao                             AS hist_DataHoraInclusao,
    h.PermitirAgendamentoquenuncaconsultou         AS hist_PermitirSemConsulta,
    h.ValorCustoSegunda                            AS hist_ValorCustoSegunda,
    h.ValorCustoTerca                              AS hist_ValorCustoTerca,
    h.ValorCustoQuarta                             AS hist_ValorCustoQuarta,
    h.ValorCustoQuinta                             AS hist_ValorCustoQuinta,
    h.ValorCustoSexta                              AS hist_ValorCustoSexta,
    h.ValorCustoSabado                             AS hist_ValorCustoSabado,
    h.ValorCustoDomingo                            AS hist_ValorCustoDomingo,
    h.QuantidadeCustoSegunda                       AS hist_QuantidadeCustoSegunda,
    h.QuantidadeCustoTerca                         AS hist_QuantidadeCustoTerca,
    h.QuantidadeCustoQuarta                        AS hist_QuantidadeCustoQuarta,
    h.QuantidadeCustoQuinta                        AS hist_QuantidadeCustoQuinta,
    h.QuantidadeCustoSexta                         AS hist_QuantidadeCustoSexta,
    h.QuantidadeCustoSabado                        AS hist_QuantidadeCustoSabado,
    h.QuantidadeCustoDomingo                       AS hist_QuantidadeCustoDomingo

FROM cad_especialidade ce
INNER JOIN cad_medico m ON m.idmedico = ce.idMedico

-- Última alteração que definiu (ou redefiniu) o DataFimExibicao
OUTER APPLY (
    SELECT TOP 1
        idHistorico,
        [Data],
        [Usuário],
        Detalhe
    FROM vw_Sis_Historico
    WHERE id        = ce.idEspecialidade
      AND Tabela    = 'Cad_Especialidade'
      AND Comando   = 'Edição'
      AND Detalhe  LIKE '%DataFimExibicao%'
    ORDER BY idHistorico DESC
) aud

-- Estado anterior: snapshot imediatamente antes da solicitação
OUTER APPLY (
    SELECT TOP 1
        DataHoraInclusao,
        PermitirAgendamentoquenuncaconsultou,
        ValorCustoSegunda,      ValorCustoTerca,    ValorCustoQuarta,
        ValorCustoQuinta,       ValorCustoSexta,    ValorCustoSabado,    ValorCustoDomingo,
        QuantidadeCustoSegunda, QuantidadeCustoTerca, QuantidadeCustoQuarta,
        QuantidadeCustoQuinta,  QuantidadeCustoSexta, QuantidadeCustoSabado, QuantidadeCustoDomingo
    FROM Cad_EspecialidadeHistorico
    WHERE idmedico      = ce.idMedico
      AND Especialidade = ce.Especialidade
      AND DataHoraInclusao < ISNULL(aud.[Data], ISNULL(ce.DataInicioExibicao, ce.DataFimExibicao))
    ORDER BY DataHoraInclusao DESC
) h

WHERE ce.Desativado = 0
  AND ce.DataFimExibicao IS NOT NULL
  AND ce.Temporario = 0
  AND m.nome NOT LIKE '%sede%'
  AND m.nome NOT LIKE '%agendamento%'
  AND m.nome NOT LIKE '%teste%'
  AND m.nome NOT LIKE '%fake%'
ORDER BY ce.DataFimExibicao DESC, m.nome;

-- END SQL_CTRLQ_DESBLOQUEIO.SQL
