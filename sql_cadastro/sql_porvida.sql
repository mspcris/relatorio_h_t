SELECT
    e.codigo AS posto,
    CASE
        WHEN cp.idade BETWEEN  0 AND 18 THEN '0 a 18'
        WHEN cp.idade BETWEEN 19 AND 23 THEN '19 a 23'
        WHEN cp.idade BETWEEN 24 AND 28 THEN '24 a 28'
        WHEN cp.idade BETWEEN 29 AND 33 THEN '29 a 33'
        WHEN cp.idade BETWEEN 34 AND 38 THEN '34 a 38'
        WHEN cp.idade BETWEEN 39 AND 43 THEN '39 a 43'
        WHEN cp.idade BETWEEN 44 AND 48 THEN '44 a 48'
        WHEN cp.idade BETWEEN 49 AND 53 THEN '49 a 53'
        WHEN cp.idade BETWEEN 54 AND 58 THEN '54 a 58'
        WHEN cp.idade >= 59            THEN '59+'
        ELSE 'Sem idade'
    END AS faixa_etaria,
    COUNT(*) AS total
FROM vw_Cad_PacienteView cp
JOIN sis_empresa emp ON emp.idendereco = cp.idendereco
join cad_endereco e on e.idendereco = emp.idendereco
join cad_cliente cc on cc.idCliente = cp.idCliente
WHERE cp.desativado = 0
  AND cp.idade IS NOT NULL
  AND cp.[Situação] = 'Adimplente'
  --AND cc.tipo = 'F'
  AND (cp.CanceladoANS = 0)
GROUP BY
    e.codigo,
    CASE
        WHEN cp.idade BETWEEN  0 AND 18 THEN '0 a 18'
        WHEN cp.idade BETWEEN 19 AND 23 THEN '19 a 23'
        WHEN cp.idade BETWEEN 24 AND 28 THEN '24 a 28'
        WHEN cp.idade BETWEEN 29 AND 33 THEN '29 a 33'
        WHEN cp.idade BETWEEN 34 AND 38 THEN '34 a 38'
        WHEN cp.idade BETWEEN 39 AND 43 THEN '39 a 43'
        WHEN cp.idade BETWEEN 44 AND 48 THEN '44 a 48'
        WHEN cp.idade BETWEEN 49 AND 53 THEN '49 a 53'
        WHEN cp.idade BETWEEN 54 AND 58 THEN '54 a 58'
        WHEN cp.idade >= 59            THEN '59+'
        ELSE 'Sem idade'
    END
ORDER BY e.codigo, faixa_etaria;





