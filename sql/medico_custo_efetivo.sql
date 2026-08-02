-- ============================================================================
-- medico_custo_efetivo.sql — Custo nominal contratado de cada médico, por dia
-- da semana, em UM posto. O ETL (export_medico_custo.py) roda isto nos 13.
--
-- Base do partido do Cristiano (CROSS APPLY desnormalizando os 7 dias),
-- ampliada com tudo que a cad_especialidade tem e que muda o custo REAL:
--
--   Almoço            — SegundaAlmoco/AlmocoInicio/AlmocoFim. Sem descontar,
--                       o valor/hora sai menor do que é. Um plantão das 8h às 18h
--                       com 1h de almoço são 9h de trabalho, não 10.
--   Vagas             — <Dia>Quantidade e <Dia>QuantidadeMaxima. É o que
--                       permite calcular custo POR CONSULTA, que é o número
--                       que interessa de verdade.
--   QuantidadeCusto   — quantidade atrelada ao valor de custo do dia.
--   MedicoRecebePorComissao — quando 1, o valor fixo NÃO é o custo real.
--                       Tem que aparecer sinalizado, senão o total mente.
--   AgendaQuinzenal   — plantão de 15 em 15 dias: no mês custa METADE do que
--                       a projeção semanal ingênua diria.
--   DataInicioExibicao / DataFimExibicao — agenda com validade; fora da janela
--                       o custo não deveria contar.
--
-- Só leitura. Nenhum filtro de data: é cadastro, não movimento.
--
-- ATENÇÃO (regra do CLAUDE.md): nada de literal de hora com dois-pontos aqui —
-- o text() do SQLAlchemy trata dois-pontos+dígito como bind param e estoura
-- com "A value is required for bind parameter". E isso vale para COMENTÁRIO
-- também, não só para literal: foi um comentário que quebrou esta query na
-- primeira execução. Escreva 8h30, nunca 8 dois-pontos 30.
-- ============================================================================
WITH Plantoes AS (
    SELECT
        DB_NAME() AS posto,

        m.idMedico,
        LTRIM(RTRIM(m.Nome))                    AS medico,
        LTRIM(RTRIM(m.CRM))                     AS crm,
        LTRIM(RTRIM(m.ConselhoProfissional))    AS conselho,
        LTRIM(RTRIM(m.ConselhoNumero))          AS conselho_numero,
        LTRIM(RTRIM(m.ConselhoUF))              AS conselho_uf,
        LTRIM(RTRIM(m.CPF))                     AS cpf,
        LTRIM(RTRIM(m.Telefone))                AS telefone,
        LTRIM(RTRIM(m.TelefoneWhatsApp))        AS whatsapp,
        LTRIM(RTRIM(m.Email))                   AS email,
        LTRIM(RTRIM(m.Especializacao))          AS especializacao,
        m.Sexo                                  AS sexo,
        m.DataInclusao                          AS medico_desde,
        ISNULL(m.PessoaJuridica, 0)             AS pessoa_juridica,
        ISNULL(m.GerarPagamentoMedicoAutomatico, 0) AS pagamento_automatico,
        TRY_CONVERT(DECIMAL(18,2), m.valormedico)   AS valor_medico_cadastro,

        e.idEspecialidade,
        LTRIM(RTRIM(e.Especialidade))           AS especialidade,
        LTRIM(RTRIM(e.Descricao))               AS descricao,
        LTRIM(RTRIM(e.CodigoCBOS))              AS cbos,
        LTRIM(RTRIM(e.Sala))                    AS sala,
        ISNULL(e.AgendaQuinzenal, 0)            AS agenda_quinzenal,
        ISNULL(e.Temporario, 0)                 AS temporario,
        ISNULL(e.MedicoRecebePorComissao, 0)    AS recebe_por_comissao,
        ISNULL(e.AtendimentoOnline, 0)          AS atendimento_online,
        ISNULL(e.Acolhimento, 0)                AS acolhimento,
        ISNULL(e.ExibirnoF3, 0)                 AS exibe_no_f3,
        e.DataPlantao                           AS data_plantao,
        e.DataInicioExibicao                    AS exibe_de,
        e.DataFimExibicao                       AS exibe_ate,
        e.idadeMinima                           AS idade_minima,
        e.idadeMaxima                           AS idade_maxima,
        -- Nova Iguaçu NÃO tem esta coluna — os postos têm schemas diferentes.
        -- O ETL troca o placeholder por NULL onde a coluna não existir.
        {{OPC:e.valorconsultaclube:DECIMAL(18,2)}} AS valor_consulta_clube,

        d.dia_ordem,
        d.dia_semana,
        -- as colunas de hora são varchar 'HH:MM'; TIME(0) devolve NULL no lixo
        TRY_CONVERT(TIME(0), d.hora_inicio)     AS hora_inicio,
        TRY_CONVERT(TIME(0), d.hora_fim)        AS hora_fim,
        TRY_CONVERT(TIME(0), d.almoco_inicio)   AS almoco_inicio,
        TRY_CONVERT(TIME(0), d.almoco_fim)      AS almoco_fim,
        ISNULL(d.tem_almoco, 0)                 AS tem_almoco,
        TRY_CONVERT(DECIMAL(18,2), d.valor_plantao) AS valor_plantao,
        TRY_CONVERT(INT, d.vagas)               AS vagas,
        TRY_CONVERT(INT, d.vagas_maxima)        AS vagas_maxima,
        TRY_CONVERT(INT, d.qtd_custo)           AS qtd_custo,
        ISNULL(d.dia_ativo, 0)                  AS dia_ativo,
        -- MODALIDADE DE ATENDIMENTO (Cristiano, 2026-08-02):
        --   OC  = ordem de chegada / livre demanda. Clínico geral é quase todo
        --         OC — só ~1% tem agenda.
        --   WWW = agendamento pela internet · TEL = pela central telefônica.
        -- Um médico pode ter OC e agendamento AO MESMO TEMPO: ex. 10 números
        -- agendados e o resto por ordem de chegada. Por isso são três bits
        -- independentes, e não um campo único de tipo de agenda.
        ISNULL(d.oc, 0)                         AS oc,
        ISNULL(d.www, 0)                        AS www,
        ISNULL(d.tel, 0)                        AS tel

    FROM cad_especialidade e
    INNER JOIN cad_medico m
        ON m.idMedico = e.idMedico

    CROSS APPLY (VALUES
        (1, 'Segunda', e.SegundaHoraInicio, e.SegundaHoraFim,
            e.SegundaAlmocoinicio, e.SegundaAlmocoFim, e.SegundaAlmoco,
            e.ValorCustoSegunda, e.SegundaQuantidade, e.SegundaQuantidadeMaxima,
            e.QuantidadeCustoSegunda, e.Segunda,
            e.SegundaOrdemChegada, e.SegundaInternet, e.SegundaTelefone),
        (2, 'Terça',   e.TercaHoraInicio,   e.TercaHoraFim,
            e.TercaAlmocoinicio,   e.TercaAlmocoFim,   e.TercaAlmoco,
            e.ValorCustoTerca,   e.TercaQuantidade,   e.TercaQuantidadeMaxima,
            e.QuantidadeCustoTerca,   e.Terca,
            e.TercaOrdemChegada, e.TercaInternet, e.TercaTelefone),
        (3, 'Quarta',  e.QuartaHoraInicio,  e.QuartaHoraFim,
            e.QuartaAlmocoinicio,  e.QuartaAlmocoFim,  e.QuartaAlmoco,
            e.ValorCustoQuarta,  e.QuartaQuantidade,  e.QuartaQuantidadeMaxima,
            e.QuantidadeCustoQuarta,  e.Quarta,
            e.QuartaOrdemChegada, e.QuartaInternet, e.QuartaTelefone),
        (4, 'Quinta',  e.QuintaHoraInicio,  e.QuintaHoraFim,
            e.QuintaAlmocoinicio,  e.QuintaAlmocoFim,  e.QuintaAlmoco,
            e.ValorCustoQuinta,  e.QuintaQuantidade,  e.QuintaQuantidadeMaxima,
            e.QuantidadeCustoQuinta,  e.Quinta,
            e.QuintaOrdemChegada, e.QuintaInternet, e.QuintaTelefone),
        (5, 'Sexta',   e.SextaHoraInicio,   e.SextaHoraFim,
            e.SextaAlmocoinicio,   e.SextaAlmocoFim,   e.SextaAlmoco,
            e.ValorCustoSexta,   e.SextaQuantidade,   e.SextaQuantidadeMaxima,
            e.QuantidadeCustoSexta,   e.Sexta,
            e.SextaOrdemChegada, e.SextaInternet, e.SextaTelefone),
        (6, 'Sábado',  e.SabadoHoraInicio,  e.SabadoHoraFim,
            e.SabadoAlmocoinicio,  e.SabadoAlmocoFim,  e.SabadoAlmoco,
            e.ValorCustoSabado,  e.SabadoQuantidade,  e.SabadoQuantidadeMaxima,
            e.QuantidadeCustoSabado,  e.Sabado,
            e.SabadoOrdemChegada, e.SabadoInternet, e.SabadoTelefone),
        (7, 'Domingo', e.DomingoHoraInicio, e.DomingoHoraFim,
            e.DomingoAlmocoinicio, e.DomingoAlmocoFim, e.DomingoAlmoco,
            e.ValorCustoDomingo, e.DomingoQuantidade, e.DomingoQuantidadeMaxima,
            e.QuantidadeCustoDomingo, e.Domingo,
            e.DomingoOrdemChegada, e.DomingoInternet, e.DomingoTelefone)
    ) d (dia_ordem, dia_semana, hora_inicio, hora_fim,
         almoco_inicio, almoco_fim, tem_almoco,
         valor_plantao, vagas, vagas_maxima, qtd_custo, dia_ativo,
         oc, www, tel)

    WHERE ISNULL(e.Desativado, 0) = 0
      AND ISNULL(m.Desativado, 0) = 0
      -- só linha que representa custo: sem valor não há o que analisar
      AND TRY_CONVERT(DECIMAL(18,2), d.valor_plantao) > 0.01
)
SELECT
    p.*,

    -- Minutos brutos do plantão. Vira negativo quando o turno atravessa a
    -- meia-noite (22h → 2h) — nesse caso soma um dia.
    CASE
        WHEN p.hora_inicio IS NULL OR p.hora_fim IS NULL THEN NULL
        WHEN DATEDIFF(MINUTE, p.hora_inicio, p.hora_fim) < 0
            THEN DATEDIFF(MINUTE, p.hora_inicio, p.hora_fim) + 1440
        ELSE DATEDIFF(MINUTE, p.hora_inicio, p.hora_fim)
    END AS minutos_brutos,

    -- Minutos de almoço, só quando a flag está ligada E os dois horários
    -- existem. Sem isso o valor/hora sai subestimado.
    CASE
        WHEN p.tem_almoco = 0 THEN 0
        WHEN p.almoco_inicio IS NULL OR p.almoco_fim IS NULL THEN 0
        WHEN DATEDIFF(MINUTE, p.almoco_inicio, p.almoco_fim) < 0 THEN 0
        ELSE DATEDIFF(MINUTE, p.almoco_inicio, p.almoco_fim)
    END AS minutos_almoco,

    -- Total semanal contratado do médico NO POSTO (soma de todas as
    -- especialidades e dias dele) — a janela repete em toda linha dele.
    SUM(p.valor_plantao) OVER (PARTITION BY p.posto, p.idMedico)
        AS total_semanal_medico,
    -- e o mesmo por especialidade, para ver quanto cada uma pesa
    SUM(p.valor_plantao) OVER (PARTITION BY p.posto, p.idMedico, p.idEspecialidade)
        AS total_semanal_especialidade,
    COUNT(*) OVER (PARTITION BY p.posto, p.idMedico) AS dias_na_semana

FROM Plantoes p
ORDER BY p.especialidade, p.medico, p.dia_ordem;
