SELECT
    e.codigo AS posto,
    COUNT(*) AS total
FROM vw_Cad_Cliente cc
JOIN sis_empresa emp
       ON emp.idEndereco = cc.idEndereco
join Cad_Endereco e on emp.idEndereco = e.idEndereco
WHERE cc.Desativado = 0
  AND cc.Situação = 'adimplente'
  AND cc.[Data de cancelamento auto] IS NULL
GROUP BY e.codigo
ORDER BY e.codigo;

