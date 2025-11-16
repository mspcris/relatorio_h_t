SELECT
    cp.plano,
    cp.idcliente,
    cp.id,
    cp.idade,
    cp.matricula,
    cp.CF,
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
    END AS faixa_etaria
FROM vw_Cad_PacienteView cp
LEFT JOIN sis_empresa emp
       ON emp.idendereco = cp.idendereco
WHERE (cp.desativado = 0)
  AND (cp.[Situação] = 'Adimplente')
  AND cp.idendereco = emp.idendereco
  -- incremento: só novos idcliente
  AND cp.idcliente > :last_id;
  and cp.idade is not null
  
