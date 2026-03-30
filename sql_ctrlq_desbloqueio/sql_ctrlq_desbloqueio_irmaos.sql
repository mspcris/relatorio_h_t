-- sql_ctrlq_desbloqueio_irmaos.sql
-- Para cada registro com DataFimExibicao (expirando), busca OUTROS registros
-- ativos do mesmo médico + especialidade (os "irmãos").
-- Isso permite mostrar o quadro COMPLETO atual da médica em PRODUÇÃO.

SELECT
    ce_main.idEspecialidade  AS parent_idEspecialidade,

    s.idEspecialidade,
    s.DataInicioExibicao,
    s.DataFimExibicao,

    s.Segunda,     s.SegundaHoraInicio, s.SegundaHoraFim,
                   s.SegundaAlmoco,     s.SegundaAlmocoinicio, s.SegundaAlmocoFim,
                   s.ValorCustoSegunda, s.QuantidadeCustoSegunda,

    s.Terca,       s.TercaHoraInicio,   s.TercaHoraFim,
                   s.TercaAlmoco,       s.TercaAlmocoinicio,   s.TercaAlmocoFim,
                   s.ValorCustoTerca,   s.QuantidadeCustoTerca,

    s.Quarta,      s.QuartaHoraInicio,  s.QuartaHoraFim,
                   s.QuartaAlmoco,      s.QuartaAlmocoinicio,  s.QuartaAlmocoFim,
                   s.ValorCustoQuarta,  s.QuantidadeCustoQuarta,

    s.Quinta,      s.QuintaHoraInicio,  s.QuintaHoraFim,
                   s.QuintaAlmoco,      s.QuintaAlmocoinicio,  s.QuintaAlmocoFim,
                   s.ValorCustoQuinta,  s.QuantidadeCustoQuinta,

    s.Sexta,       s.SextaHoraInicio,   s.SextaHoraFim,
                   s.SextaAlmoco,       s.SextaAlmocoinicio,   s.SextaAlmocoFim,
                   s.ValorCustoSexta,   s.QuantidadeCustoSexta,

    s.Sabado,      s.SabadoHoraInicio,  s.SabadoHoraFim,
                   s.SabadoAlmoco,      s.SabadoAlmocoinicio,  s.SabadoAlmocoFim,
                   s.ValorCustoSabado,  s.QuantidadeCustoSabado,

    s.Domingo,     s.DomingoHoraInicio, s.DomingoHoraFim,
                   s.DomingoAlmoco,     s.DomingoAlmocoinicio, s.DomingoAlmocoFim,
                   s.ValorCustoDomingo, s.QuantidadeCustoDomingo

FROM cad_especialidade ce_main
INNER JOIN cad_especialidade s
    ON  s.idMedico       = ce_main.idMedico
    AND s.Especialidade  = ce_main.Especialidade
    AND s.idEspecialidade != ce_main.idEspecialidade
    AND s.Desativado     = 0
    AND s.Temporario     = 0
WHERE ce_main.Desativado      = 0
  AND ce_main.DataFimExibicao IS NOT NULL
  AND ce_main.Temporario      = 0
ORDER BY ce_main.idEspecialidade, s.idEspecialidade;

-- END sql_ctrlq_desbloqueio_irmaos.sql
