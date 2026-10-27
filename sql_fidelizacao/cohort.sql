-- Cohort de fidelização (turmas por mês de admissão) — leitura apenas.
-- Mesma regra da PC_Fin_Fidelizacao do ERP, com três diferenças decididas em
-- 2026-10-27, porque a procedure dava números impossíveis no KPI:
--   1) sem o corte de "admitidos nos últimos 2 anos" (a janela vem do parâmetro
--      ini), para a curva de 24 e 36 meses ter turma com idade suficiente;
--   2) conta CLIENTES distintos, não linhas de receita (cliente com duas
--      mensalidades do mesmo mês contava duas vezes);
--   3) nos meses seguintes só conta quem é da turma (a procedure contava
--      qualquer cliente com aquela data de admissão, e a retenção passava de 100%).
-- Turma = cliente ativo, matrícula > 0, não PJ, admitido no mês, com a
-- mensalidade do mês da admissão paga. Ficou no mês X = a mensalidade com
-- referência no mês X foi paga (em qualquer data).
-- ATENÇÃO: nada de dois-pontos fora do parâmetro, nem em comentário (text() do
-- SQLAlchemy lê como bind param).

SET NOCOUNT ON;

IF OBJECT_ID('tempdb..#coorte') IS NOT NULL DROP TABLE #coorte;

SELECT DISTINCT
    c.idCliente,
    YEAR(c.DataAdmissao)  AS ano,
    MONTH(c.DataAdmissao) AS mes
INTO #coorte
FROM Cad_Cliente c
JOIN Sis_Empresa emp ON emp.idEndereco = c.idEndereco
JOIN Fin_Receita r   ON r.idCliente = c.idCliente
                    AND r.idContaTipo = 5
                    AND r.DataPagamento IS NOT NULL
                    AND YEAR(r.DataMensalidade)  = YEAR(c.DataAdmissao)
                    AND MONTH(r.DataMensalidade) = MONTH(c.DataAdmissao)
WHERE c.Desativado = 0
  AND c.Matricula > 0
  AND c.Tipo <> 'J'
  AND (c.Matricula < 9999999 OR c.idPlano = emp.idPlanoAtendimentoClube)
  AND c.DataAdmissao >= :ini;

CREATE INDEX ix_coorte ON #coorte (idCliente);

SELECT
    k.ano  AS AnoAdmissao,
    k.mes  AS MesAdmissao,
    adm.qtd AS QuantidadeAdmissao,
    YEAR(r.[data Referencia])  AS AnoReferencia,
    MONTH(r.[data Referencia]) AS MesReferencia,
    COUNT(DISTINCT r.idCliente) AS QuantidadeRecebida
FROM #coorte k
JOIN (SELECT ano, mes, COUNT(*) AS qtd FROM #coorte GROUP BY ano, mes) adm
  ON adm.ano = k.ano AND adm.mes = k.mes
JOIN vw_Fin_ReceitaRapido2 r
  ON r.idCliente = k.idCliente
 AND r.idContaTipo = 5
 AND r.[data de pagamento] IS NOT NULL
 AND r.[data Referencia] IS NOT NULL
GROUP BY k.ano, k.mes, adm.qtd, YEAR(r.[data Referencia]), MONTH(r.[data Referencia])
ORDER BY 1, 2, 4, 5;
