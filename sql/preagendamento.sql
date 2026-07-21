-- ============================================================
-- Pre-Agendamento - query base por posto
-- Usada por export_preagendamento.py (ETL noturno)
-- Parametros bind: dt_ini e dt_fim (ambos DD/MM/YYYY, fim exclusivo)
-- IMPORTANTE: nao usar dois pontos antes dos nomes dos parametros nos
-- comentarios - o SQLAlchemy detecta e tenta bindar de novo.
--
-- Mantem ambas populacoes (desistencia 0 e 1) - front filtra.
-- READ UNCOMMITTED para reduzir tempo (dados analiticos, nao transacionais).
--
-- ------------------------------------------------------------
-- 2026-07-21: saimos de vw_Cad_LancamentoProntuarioComDesistencia para as
-- tabelas base. Medido em abr-jun/2026 (3 meses): posto B 8,1s -> 3,9s, posto G
-- 10,6s -> 5,1s. Repetindo a medicao o ganho variou entre 2,1x e 3,4x conforme
-- a carga do servidor - conte com ~2x no pior caso.
--
-- O ganho vem de NAO calcular tres status que a view calcula e que aqui nao
-- interessam - o foco desta pagina e quem faltou:
--   PENDENCIA DE GUIA      -> exigia a UDF escalar dbo.PossuiGuia(idLancamento),
--                             chamada linha a linha (mata paralelismo no plano)
--   PENDENCIA DE PAGAMENTO -> exigia left join em Fin_Receita
--   PENDENCIA RECEPCAO     -> exigia left join na view aninhada
--                             vw_Cad_ClienteDiasInadimplenciaANSview
-- Essas tres linhas viram Faltou/Ausente/Nao atendido, que e onde ja caiam na
-- classificacao do dashboard. Impacto medido: 10 linhas em ~7.000, zero
-- mudanca nos baldes compareceu/falta/pendente/medico_faltou.
--
-- PENDENCIA RECEPCAO era o cliente que marcou e ficou inadimplente. Decisao do
-- Cristiano em 2026-07-21: nao acompanhar por aqui, tratar como excecao no
-- balcao. Ver CLAUDE.md.
--
-- ORDEM DO CASE - mf.idFalta fica ENTRE StatusAtendimento e StatusAguardar.
-- Na view a ordem era Aguardando, Atendido, MedicoFaltou; aqui e Atendido,
-- MedicoFaltou, Aguardando. Os dois motivos:
--   - MF tem que ganhar de 'Aguardando': na view, um lancamento com falta do
--     medico E StatusAguardar=1 saia como 'Aguardando' e era contado como
--     COMPARECEU. "Medico faltou" e excluido da analise (regra do CLAUDE.md),
--     entao nao pode virar comparecimento.
--   - MF NAO pode ganhar de 'Atendido': StatusAtendimento=1 e fato registrado,
--     a pessoa foi atendida (medico substituto, falta lancada errado etc.).
--     Descartar isso jogaria fora atendimento real. Decisao do Cristiano em
--     2026-07-21, sobre 1 caso medido em 11.012 linhas (posto G, 3 meses).
--
-- Os filtros Codigo > 0, DataEstorno IS NULL e ExibenoProntuarioF3 /
-- PermitirAgendamentoF6eCTRLF6 estavam no WHERE da view e sao reproduzidos
-- abaixo. Sem eles a query devolve linhas a mais (estornos e servicos que nao
-- aparecem no F3).
--
-- Nada de literal de hora com dois pontos na comparacao do Cad_MedicoFalta - o
-- text() do SQLAlchemy leria o trecho como bind param. Por isso DATEPART em vez
-- do convert(varchar(5), ..., 114) que a view usa. E equivalente.
-- ============================================================
SET NOCOUNT ON;
SET DATEFORMAT dmy;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    l.idLancamento                                                    AS id_lancamento,
    c.matriculal                                                      AS matricula,
    CASE
        WHEN d.idDependente IS NOT NULL AND LEN(ISNULL(d.NomeSocial, '')) > 0
            THEN d.NomeSocial + '(SOCIAL) '
        WHEN d.idDependente IS NULL     AND LEN(ISNULL(cli.NomeSocial, '')) > 0
            THEN cli.NomeSocial + '(SOCIAL) '
        ELSE ''
    END + ISNULL(d.Nome, cli.Nome)                                    AS paciente,
    CASE
        WHEN ls.StatusAtendimento = 1                                    THEN 'Atendido'
        WHEN mf.idFalta IS NOT NULL                                      THEN 'MÉDICO faltou'
        WHEN ls.StatusAguardar = 1                                       THEN 'Aguardando'
        WHEN l.Falta = 1                                                 THEN 'Faltou'
        WHEN CAST(l.DataConsulta AS date) <= CAST(GETDATE()-1 AS date)   THEN 'Faltou'
        WHEN ls.StatusAtendimento = 0                                    THEN 'Ausente'
        WHEN ls.DataMaterial IS NOT NULL AND sc.AtendidoComDataMaterial = 1 THEN 'Atendido'
        ELSE 'Não atendido'
    END                                                               AS atendido,
    CASE
        WHEN ls.MotivoDesistencia LIKE 'Consulta pré agendada não foi confirmada%'
            THEN 'Pré agendamento não confirmado'
        WHEN ls.MotivoDesistencia LIKE 'CANCELADO PELO CLIENTE NO SITE OU APP DA CAMIM%'
            THEN 'Cliente no APP'
        WHEN ls.DataDesistencia IS NOT NULL
            THEN 'Outros'
        ELSE NULL
    END                                                               AS origem_cancelamento,
    CONVERT(varchar(10), l.[Data], 120)                               AS data_lancamento,
    CONVERT(varchar(19), l.DataHoraNotificacaoPreAgendamento, 120)    AS data_push,
    CONVERT(varchar(19), l.DataConfirmacaoAgendamentoConsulta, 120)   AS data_conf_agend,
    CONVERT(varchar(10), l.DataConsulta, 120)                         AS data_consulta,
    CONVERT(varchar(5),  l.HoraPrevistaConsulta, 108)                 AS hora_consulta,
    CONVERT(varchar(19), l.dataconfirmacaoConsulta, 120)              AS data_conf_chegada,
    DATEDIFF(day, l.[Data], l.DataConsulta)                           AS dif_dias,
    m.Nome                                                            AS medico,
    CASE WHEN sc.ExibenoProntuario = 1 THEN sc.Classe
         ELSE es.Especialidade END                                    AS especialidade,
    l.MarcadoViaAgendaUnificada                                       AS via_asu,
    l.MarcadoViaWeb                                                   AS via_web,
    l.CtrlF6                                                          AS via_f6,
    l.ValorPago                                                       AS valor_pago,
    l.idCliente                                                       AS id_cliente,
    l.codigo                                                          AS talao,
    l.consulta                                                        AS nro_consulta,
    CAST(ISNULL(ls.Desistencia, 0) AS bit)                            AS desistencia
FROM cad_lancamento             l   WITH (NOLOCK)
JOIN Cad_LancamentoServico      ls  WITH (NOLOCK) ON ls.idLancamento    = l.idLancamento
JOIN Cad_Especialidade          es                ON es.idEspecialidade = l.idEspecialidade
                                                 AND es.ExibirNoF3      = 1
JOIN Cad_Medico                 m                 ON m.idMedico         = l.idMedico
JOIN Cad_Servico                ss                ON ss.idServico       = ls.idServico
JOIN Cad_ServicoClasse          sc                ON sc.idClasse        = ss.idClasse
JOIN Cad_Cliente                cli               ON cli.idCliente      = l.idCliente
JOIN vw_cad_cliente             c                 ON c.idCliente        = l.idCliente
LEFT JOIN Cad_ClienteDependente d                 ON d.idDependente     = l.idDependente
LEFT JOIN Cad_MedicoFalta       mf                ON mf.idMedico        = l.idMedico
                                                 AND mf.Desativado      = 0
                                                 AND CAST(mf.DataHora AS date) = CAST(l.DataConsulta AS date)
                                                 AND DATEPART(hour,   mf.DataHoraFim) = 23
                                                 AND DATEPART(minute, mf.DataHoraFim) = 59
                                                 AND DATEPART(hour,   mf.DataHora)    = 0
                                                 AND DATEPART(minute, mf.DataHora)    = 0
                                                 AND mf.Especialidade   = es.Especialidade
WHERE l.consulta              IS NOT NULL
  AND l.desativado            = 0
  AND l.HoraPrevistaConsulta  IS NOT NULL
  AND l.DataConsulta          >= :dt_ini
  AND l.DataConsulta          <  :dt_fim
  -- filtros herdados do WHERE da view (ver cabecalho)
  AND l.Codigo                > 0
  AND l.DataEstorno           IS NULL
  AND (ss.ExibenoProntuarioF3 = 1 OR ss.PermitirAgendamentoF6eCTRLF6 = 1)
