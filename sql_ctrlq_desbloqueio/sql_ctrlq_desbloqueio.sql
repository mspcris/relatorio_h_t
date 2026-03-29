-- SQL_CTRLQ_DESBLOQUEIO.SQL
-- Solicitações de desbloqueio de agenda (Temporario=0, Desativado=0,
-- DataFimExibicao definida). Retorna dados completos de horário e custo
-- para produção (atual) e histórico (Cad_EspecialidadeHistorico anterior).

SELECT
    -- Identificação
    ce.idEspecialidade,
    ce.idMedico,
    m.nome            AS medico,
    m.crm,
    ce.Especialidade,

    -- Período do desbloqueio
    ce.DataInicioExibicao,
    ce.DataFimExibicao,
    ce.ObservacaoDesbloqueio,
    ce.PermitirAgendamentoquenuncaconsultou AS atual_PermitirSemConsulta,

    -- ── PRODUÇÃO: flags, horários e custos por dia ────────────────────────
    ce.Segunda,     ce.SegundaHoraInicio, ce.SegundaHoraFim,
                    ce.SegundaAlmoco,     ce.SegundaAlmocoinicio, ce.SegundaAlmocoFim,
                    ce.ValorCustoSegunda, ce.QuantidadeCustoSegunda,

    ce.Terca,       ce.TercaHoraInicio,   ce.TercaHoraFim,
                    ce.TercaAlmoco,       ce.TercaAlmocoinicio,   ce.TercaAlmocoFim,
                    ce.ValorCustoTerca,   ce.QuantidadeCustoTerca,

    ce.Quarta,      ce.QuartaHoraInicio,  ce.QuartaHoraFim,
                    ce.QuartaAlmoco,      ce.QuartaAlmocoinicio,  ce.QuartaAlmocoFim,
                    ce.ValorCustoQuarta,  ce.QuantidadeCustoQuarta,

    ce.Quinta,      ce.QuintaHoraInicio,  ce.QuintaHoraFim,
                    ce.QuintaAlmoco,      ce.QuintaAlmocoinicio,  ce.QuintaAlmocoFim,
                    ce.ValorCustoQuinta,  ce.QuantidadeCustoQuinta,

    ce.Sexta,       ce.SextaHoraInicio,   ce.SextaHoraFim,
                    ce.SextaAlmoco,       ce.SextaAlmocoinicio,   ce.SextaAlmocoFim,
                    ce.ValorCustoSexta,   ce.QuantidadeCustoSexta,

    ce.Sabado,      ce.SabadoHoraInicio,  ce.SabadoHoraFim,
                    ce.SabadoAlmoco,      ce.SabadoAlmocoinicio,  ce.SabadoAlmocoFim,
                    ce.ValorCustoSabado,  ce.QuantidadeCustoSabado,

    ce.Domingo,     ce.DomingoHoraInicio, ce.DomingoHoraFim,
                    ce.DomingoAlmoco,     ce.DomingoAlmocoinicio, ce.DomingoAlmocoFim,
                    ce.ValorCustoDomingo, ce.QuantidadeCustoDomingo,

    -- ── HISTÓRICO: snapshot anterior (Cad_EspecialidadeHistorico) ─────────
    h.DataHoraInclusao                           AS hist_DataHoraInclusao,
    h.PermitirAgendamentoquenuncaconsultou        AS hist_PermitirSemConsulta,

    h.Segunda        AS hist_Segunda,
    h.SegundaHoraInicio   AS hist_SegundaHoraInicio,
    h.SegundaHoraFim      AS hist_SegundaHoraFim,
    h.SegundaAlmoco       AS hist_SegundaAlmoco,
    h.SegundaAlmocoinicio AS hist_SegundaAlmocoinicio,
    h.SegundaAlmocoFim    AS hist_SegundaAlmocoFim,
    h.ValorCustoSegunda   AS hist_ValorCustoSegunda,
    h.QuantidadeCustoSegunda AS hist_QuantidadeCustoSegunda,

    h.Terca          AS hist_Terca,
    h.TercaHoraInicio     AS hist_TercaHoraInicio,
    h.TercaHoraFim        AS hist_TercaHoraFim,
    h.TercaAlmoco         AS hist_TercaAlmoco,
    h.TercaAlmocoinicio   AS hist_TercaAlmocoinicio,
    h.TercaAlmocoFim      AS hist_TercaAlmocoFim,
    h.ValorCustoTerca     AS hist_ValorCustoTerca,
    h.QuantidadeCustoTerca AS hist_QuantidadeCustoTerca,

    h.Quarta         AS hist_Quarta,
    h.QuartaHoraInicio    AS hist_QuartaHoraInicio,
    h.QuartaHoraFim       AS hist_QuartaHoraFim,
    h.QuartaAlmoco        AS hist_QuartaAlmoco,
    h.QuartaAlmocoinicio  AS hist_QuartaAlmocoinicio,
    h.QuartaAlmocoFim     AS hist_QuartaAlmocoFim,
    h.ValorCustoQuarta    AS hist_ValorCustoQuarta,
    h.QuantidadeCustoQuarta AS hist_QuantidadeCustoQuarta,

    h.Quinta         AS hist_Quinta,
    h.QuintaHoraInicio    AS hist_QuintaHoraInicio,
    h.QuintaHoraFim       AS hist_QuintaHoraFim,
    h.QuintaAlmoco        AS hist_QuintaAlmoco,
    h.QuintaAlmocoinicio  AS hist_QuintaAlmocoinicio,
    h.QuintaAlmocoFim     AS hist_QuintaAlmocoFim,
    h.ValorCustoQuinta    AS hist_ValorCustoQuinta,
    h.QuantidadeCustoQuinta AS hist_QuantidadeCustoQuinta,

    h.Sexta          AS hist_Sexta,
    h.SextaHoraInicio     AS hist_SextaHoraInicio,
    h.SextaHoraFim        AS hist_SextaHoraFim,
    h.SextaAlmoco         AS hist_SextaAlmoco,
    h.SextaAlmocoinicio   AS hist_SextaAlmocoinicio,
    h.SextaAlmocoFim      AS hist_SextaAlmocoFim,
    h.ValorCustoSexta     AS hist_ValorCustoSexta,
    h.QuantidadeCustoSexta AS hist_QuantidadeCustoSexta,

    h.Sabado         AS hist_Sabado,
    h.SabadoHoraInicio    AS hist_SabadoHoraInicio,
    h.SabadoHoraFim       AS hist_SabadoHoraFim,
    h.SabadoAlmoco        AS hist_SabadoAlmoco,
    h.SabadoAlmocoinicio  AS hist_SabadoAlmocoinicio,
    h.SabadoAlmocoFim     AS hist_SabadoAlmocoFim,
    h.ValorCustoSabado    AS hist_ValorCustoSabado,
    h.QuantidadeCustoSabado AS hist_QuantidadeCustoSabado,

    h.Domingo        AS hist_Domingo,
    h.DomingoHoraInicio   AS hist_DomingoHoraInicio,
    h.DomingoHoraFim      AS hist_DomingoHoraFim,
    h.DomingoAlmoco       AS hist_DomingoAlmoco,
    h.DomingoAlmocoinicio AS hist_DomingoAlmocoinicio,
    h.DomingoAlmocoFim    AS hist_DomingoAlmocoFim,
    h.ValorCustoDomingo   AS hist_ValorCustoDomingo,
    h.QuantidadeCustoDomingo AS hist_QuantidadeCustoDomingo

FROM cad_especialidade ce
INNER JOIN cad_medico m ON m.idmedico = ce.idMedico

-- Snapshot imediatamente anterior ao início do desbloqueio
OUTER APPLY (
    SELECT TOP 1
        DataHoraInclusao,
        PermitirAgendamentoquenuncaconsultou,
        Segunda, SegundaHoraInicio, SegundaHoraFim,
               SegundaAlmoco, SegundaAlmocoinicio, SegundaAlmocoFim,
               ValorCustoSegunda, QuantidadeCustoSegunda,
        Terca,  TercaHoraInicio,  TercaHoraFim,
               TercaAlmoco,  TercaAlmocoinicio,  TercaAlmocoFim,
               ValorCustoTerca,  QuantidadeCustoTerca,
        Quarta, QuartaHoraInicio, QuartaHoraFim,
               QuartaAlmoco, QuartaAlmocoinicio, QuartaAlmocoFim,
               ValorCustoQuarta, QuantidadeCustoQuarta,
        Quinta, QuintaHoraInicio, QuintaHoraFim,
               QuintaAlmoco, QuintaAlmocoinicio, QuintaAlmocoFim,
               ValorCustoQuinta, QuantidadeCustoQuinta,
        Sexta,  SextaHoraInicio,  SextaHoraFim,
               SextaAlmoco,  SextaAlmocoinicio,  SextaAlmocoFim,
               ValorCustoSexta,  QuantidadeCustoSexta,
        Sabado, SabadoHoraInicio, SabadoHoraFim,
               SabadoAlmoco, SabadoAlmocoinicio, SabadoAlmocoFim,
               ValorCustoSabado, QuantidadeCustoSabado,
        Domingo, DomingoHoraInicio, DomingoHoraFim,
               DomingoAlmoco, DomingoAlmocoinicio, DomingoAlmocoFim,
               ValorCustoDomingo, QuantidadeCustoDomingo
    FROM Cad_EspecialidadeHistorico
    WHERE idmedico      = ce.idMedico
      AND Especialidade = ce.Especialidade
      AND DataHoraInclusao < ce.DataFimExibicao
    ORDER BY DataHoraInclusao DESC
) h

WHERE ce.Desativado = 0
  AND ce.DataFimExibicao IS NOT NULL
  AND ce.Temporario = 0
  AND m.nome NOT LIKE '%sede%'
  AND m.nome NOT LIKE '%agendamento%'
  AND m.nome NOT LIKE '%fake%'
ORDER BY ce.DataFimExibicao DESC, m.nome;

-- END SQL_CTRLQ_DESBLOQUEIO.SQL
