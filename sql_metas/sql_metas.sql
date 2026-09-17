SET NOCOUNT ON;

-- Linha canônica do mês = a de MENOR id ativo, a mesma que a API /api/metas lê.
-- Sem o TOP 1 a query devolvia as duas linhas de um mês duplicado e o ETL ficava
-- com a primeira que o servidor entregasse, enquanto a tela de cadastro mostrava
-- a outra (set/2026, posto A: card com 0 e modal com 220).
SELECT TOP 1
  e.codigo,
  meta.ano,
  meta.mes,
  meta.DataReferencia,
  meta.Meta2Mensalidade,
  meta.Meta2Venda
FROM Cad_MetaFilial meta
CROSS JOIN sis_empresa emp
JOIN cad_endereco e ON e.idendereco = emp.idendereco
WHERE
  meta.desativado = 0
  AND meta.DataReferencia >= :ini
  AND meta.DataReferencia <  :fim
ORDER BY meta.idMetaFilial;
