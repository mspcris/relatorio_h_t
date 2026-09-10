-- sql_ctrlq_desbloqueio_cobertura.sql
--
-- COBERTURA DO TURNO — quem mais atende a MESMA especialidade no posto.
--
-- Diferente do arquivo "irmaos", que traz outras agendas do MESMO MÉDICO, aqui
-- vêm TODAS as agendas ativas da especialidade, de qualquer médico. É isso que
-- permite responder a pergunta que o cartão precisa responder: "quem cobria
-- este horário antes, e quanto o turno passou a custar".
--
-- Motivo (decidido em 2026-09-10 com o Cristiano): num plantão de ordem de
-- chegada a fonte de verdade é o HORÁRIO, não o idEspecialidade. Médico sai,
-- outro entra, e o turno continua o mesmo. Ancorar a leitura no cadastro do
-- médico nunca responde "o que aconteceu com o sábado de manhã".
--
-- Traz também as agendas com DataFimExibicao vencida (Desativado = 0), porque
-- elas continuam valendo no ERP e continuam custando.
--
-- ATENÇÃO ao editar: nada de literal de hora com dois-pontos neste arquivo,
-- nem dentro de comentário. O text() do SQLAlchemy varre o arquivo inteiro e
-- lê o trecho depois do dois-pontos como bind param.

SELECT
    ce_main.idEspecialidade  AS parent_idEspecialidade,

    c.idEspecialidade,
    c.idMedico,
    mc.nome                  AS medico,
    mc.crm,
    c.Especialidade,
    c.Temporario,
    c.DataInicioExibicao,
    c.DataFimExibicao,
    c.MedicoRecebePorComissao,

    c.Segunda,  c.SegundaHoraInicio, c.SegundaHoraFim,
                c.SegundaAlmoco,     c.SegundaAlmocoinicio, c.SegundaAlmocoFim,
                c.ValorCustoSegunda, c.QuantidadeCustoSegunda,
                c.SegundaOrdemChegada, c.SegundaInternet, c.SegundaTelefone,

    c.Terca,    c.TercaHoraInicio,   c.TercaHoraFim,
                c.TercaAlmoco,       c.TercaAlmocoinicio,   c.TercaAlmocoFim,
                c.ValorCustoTerca,   c.QuantidadeCustoTerca,
                c.TercaOrdemChegada, c.TercaInternet, c.TercaTelefone,

    c.Quarta,   c.QuartaHoraInicio,  c.QuartaHoraFim,
                c.QuartaAlmoco,      c.QuartaAlmocoinicio,  c.QuartaAlmocoFim,
                c.ValorCustoQuarta,  c.QuantidadeCustoQuarta,
                c.QuartaOrdemChegada, c.QuartaInternet, c.QuartaTelefone,

    c.Quinta,   c.QuintaHoraInicio,  c.QuintaHoraFim,
                c.QuintaAlmoco,      c.QuintaAlmocoinicio,  c.QuintaAlmocoFim,
                c.ValorCustoQuinta,  c.QuantidadeCustoQuinta,
                c.QuintaOrdemChegada, c.QuintaInternet, c.QuintaTelefone,

    c.Sexta,    c.SextaHoraInicio,   c.SextaHoraFim,
                c.SextaAlmoco,       c.SextaAlmocoinicio,   c.SextaAlmocoFim,
                c.ValorCustoSexta,   c.QuantidadeCustoSexta,
                c.SextaOrdemChegada, c.SextaInternet, c.SextaTelefone,

    c.Sabado,   c.SabadoHoraInicio,  c.SabadoHoraFim,
                c.SabadoAlmoco,      c.SabadoAlmocoinicio,  c.SabadoAlmocoFim,
                c.ValorCustoSabado,  c.QuantidadeCustoSabado,
                c.SabadoOrdemChegada, c.SabadoInternet, c.SabadoTelefone,

    c.Domingo,  c.DomingoHoraInicio, c.DomingoHoraFim,
                c.DomingoAlmoco,     c.DomingoAlmocoinicio, c.DomingoAlmocoFim,
                c.ValorCustoDomingo, c.QuantidadeCustoDomingo,
                c.DomingoOrdemChegada, c.DomingoInternet, c.DomingoTelefone

FROM cad_especialidade ce_main
INNER JOIN cad_especialidade c
        ON  c.Especialidade   = ce_main.Especialidade
        AND c.Desativado      = 0
        AND c.Temporario      = 0
INNER JOIN cad_medico mc ON mc.idmedico = c.idMedico

WHERE ce_main.Desativado      = 0
  AND ce_main.DataFimExibicao IS NOT NULL
  AND ce_main.Temporario      = 0
  AND mc.nome NOT LIKE '%sede%'
  AND mc.nome NOT LIKE '%agendamento%'
  AND mc.nome NOT LIKE '%fake%'

ORDER BY ce_main.idEspecialidade, c.idEspecialidade;

-- END sql_ctrlq_desbloqueio_cobertura.sql
