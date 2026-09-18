-- ============================================================
-- Atendimentos por dia x medico - denominador do KPI de Prescricoes
-- Rodado por export_governanca.py (por posto e por mes), grava
-- dados/{POSTO}_{AAAA-MM}_atendimento.csv
--
-- Pergunta (Cristiano, 2026-09-18): quantas receitas saem por atendimento.
-- Proporcao simples: prescricoes do periodo / atendidos do periodo.
--
-- Atendimento = CONSULTA (idLancamento), nao servico: um lancamento tem N
-- servicos, e basta um deles estar atendido para a consulta contar uma vez.
--
-- Regra de "Atendido" copiada do CASE de sql/preagendamento.sql (que por sua
-- vez replica a vw_Cad_LancamentoProntuarioComDesistencia que a Agenda do Dia
-- do F3 le): StatusAtendimento = 1, ou DataMaterial preenchida em classe com
-- AtendidoComDataMaterial = 1. Filtros do WHERE da view reproduzidos:
-- Codigo maior que zero, sem estorno, servico que aparece no F3.
--
-- SEM filtro de HoraPrevistaConsulta (o pre-agendamento tem): clinico geral
-- e ordem de chegada, nao tem hora prevista, e o filtro sumia com ele.
--
-- Nao usar dois pontos seguidos de palavra em comentario - o text() do
-- SQLAlchemy le como bind param.
-- ============================================================
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    CAST(x.DataConsulta AS date)          AS data_consulta,
    CAST(x.Medico AS varchar(200))        AS medico,
    COUNT(*)                              AS atendidos
FROM (
    SELECT DISTINCT l.idLancamento, l.DataConsulta, m.Nome AS Medico
    FROM cad_lancamento        l  WITH (NOLOCK)
    JOIN Cad_LancamentoServico ls WITH (NOLOCK) ON ls.idLancamento    = l.idLancamento
    JOIN Cad_Especialidade     es               ON es.idEspecialidade = l.idEspecialidade
                                               AND es.ExibirNoF3      = 1
    JOIN Cad_Medico            m                ON m.idMedico         = l.idMedico
    JOIN Cad_Servico           ss               ON ss.idServico       = ls.idServico
    JOIN Cad_ServicoClasse     sc               ON sc.idClasse        = ss.idClasse
    WHERE l.desativado           = 0
      AND l.Codigo               > 0
      AND l.DataEstorno          IS NULL
      AND (ss.ExibenoProntuarioF3 = 1 OR ss.PermitirAgendamentoF6eCTRLF6 = 1)
      AND ISNULL(ls.Desistencia, 0) = 0
      AND (ls.StatusAtendimento = 1
           OR (ls.DataMaterial IS NOT NULL AND sc.AtendidoComDataMaterial = 1))
      AND m.Nome <> 'TESTE - PROFISSIONAL PARA TESTES'
      AND l.DataConsulta >= CAST(:ini AS date)
      AND l.DataConsulta <  CAST(:fim AS date)
) x
GROUP BY CAST(x.DataConsulta AS date), CAST(x.Medico AS varchar(200));
