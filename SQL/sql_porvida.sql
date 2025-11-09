Era assim:

SELECT
r2.idCliente, isnull(cd.IdadeComputada, c.IdadeCOMPUTADA) as idade,

case when idade < 19 then '0 a 18' else '19 a 200' end as faixa_etaria


FROM vw_Fin_Receita2 r2
LEFT JOIN cad_cliente c ON c.idcliente = r2.idcliente
join Cad_ClienteDependente cd on cd.idcliente = c.idCliente
WHERE (0=0)
  AND (r2.[Data Referencia] >= '01/10/2025')
  AND (r2.[Data Referencia] <  '01/11/2025')
  AND (r2.[DataAdmissao]  >= '01/10/2025')
  AND (r2.[DataAdmissao]  <  '01/11/2025')
  AND (r2.[Situação] = 'pago')
  AND (r2.[Tipo] = 'Mensalidade')
  AND (c.Desativado = 0);


Evoluiu para isso, mas, ainda está errado:
WITH base AS (
  SELECT
    r2.idCliente,
    COALESCE(cd.IdadeComputada, c.IdadeComputada) AS idade
  FROM vw_Fin_Receita2 r2
  LEFT JOIN cad_cliente c            ON c.idcliente = r2.idcliente
  LEFT JOIN Cad_ClienteDependente cd ON cd.idcliente = c.idCliente
  WHERE r2.[Data Referencia] >= '01/10/2025' AND r2.[Data Referencia] < '01/11/2025'
    AND r2.[DataAdmissao]  >= '01/10/2025' AND r2.[DataAdmissao]  < '01/11/2025'
    AND r2.[Situação] = 'pago'
    AND r2.[Tipo] = 'Mensalidade'
    AND c.Desativado = 0
)
SELECT
  CASE
    WHEN idade BETWEEN  0 AND 18 THEN '0 a 18'
    WHEN idade BETWEEN 19 AND 23 THEN '19 a 23'
    WHEN idade BETWEEN 24 AND 28 THEN '24 a 28'
    WHEN idade BETWEEN 29 AND 33 THEN '29 a 33'
    WHEN idade BETWEEN 34 AND 38 THEN '34 a 38'
    WHEN idade BETWEEN 39 AND 43 THEN '39 a 43'
    WHEN idade BETWEEN 44 AND 48 THEN '44 a 48'
    WHEN idade BETWEEN 49 AND 53 THEN '49 a 53'
    WHEN idade BETWEEN 54 AND 58 THEN '54 a 58'
    WHEN idade >= 59            THEN '59+'
    ELSE 'Sem idade'
  END AS faixa_etaria,
  COUNT(*) AS qtd                -- conta linhas
  -- COUNT(DISTINCT idCliente) AS qtd_pessoas -- use isto se quiser clientes únicos
FROM base
GROUP BY
  CASE
    WHEN idade BETWEEN  0 AND 18 THEN '0 a 18'
    WHEN idade BETWEEN 19 AND 23 THEN '19 a 23'
    WHEN idade BETWEEN 24 AND 28 THEN '24 a 28'
    WHEN idade BETWEEN 29 AND 33 THEN '29 a 33'
    WHEN idade BETWEEN 34 AND 38 THEN '34 a 38'
    WHEN idade BETWEEN 39 AND 43 THEN '39 a 43'
    WHEN idade BETWEEN 44 AND 48 THEN '44 a 48'
    WHEN idade BETWEEN 49 AND 53 THEN '49 a 53'
    WHEN idade BETWEEN 54 AND 58 THEN '54 a 58'
    WHEN idade >= 59            THEN '59+'
    ELSE 'Sem idade'
  END
ORDER BY
  MIN(CASE
        WHEN idade BETWEEN  0 AND 18 THEN 1
        WHEN idade BETWEEN 19 AND 23 THEN 2
        WHEN idade BETWEEN 24 AND 28 THEN 3
        WHEN idade BETWEEN 29 AND 33 THEN 4
        WHEN idade BETWEEN 34 AND 38 THEN 5
        WHEN idade BETWEEN 39 AND 43 THEN 6
        WHEN idade BETWEEN 44 AND 48 THEN 7
        WHEN idade BETWEEN 49 AND 53 THEN 8
        WHEN idade BETWEEN 54 AND 58 THEN 9
        WHEN idade >= 59            THEN 10
        ELSE 99
      END);


Eu vou ter de fazer o select de mensalidade fazer for no id cliente e selecionar no f5 quem estiver no for por idcliente. 