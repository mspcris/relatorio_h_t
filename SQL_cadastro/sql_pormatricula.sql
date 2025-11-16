SELECT
    cc.idcliente,
    cc.cobrador,
    cc.plano,
    cc.dataadmissao,
    cc.matricula,
    cc.cf,
    cc.tipo,
    cc.[valor mensalidade]
FROM vw_Cad_Cliente cc
LEFT JOIN sis_empresa emp
       ON emp.idEndereco = cc.idEndereco
WHERE (cc.Desativado = 0)
  AND (cc.[Situação] = 'adimplente')
  AND cc.idEndereco = emp.idendereco
  AND [Data de cancelamento auto] IS NULL
  -- incremento: só novos idcliente
  AND cc.idcliente > :last_id;
