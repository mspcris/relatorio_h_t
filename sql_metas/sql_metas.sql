SET NOCOUNT ON;

SELECT
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
  AND meta.DataReferencia <  :fim;