SELECT
  r2.Matricula,
  c.idEndereco,
  ISNULL(NULLIF(CONCAT_WS('-', r2.corretor, r2.SubCorretor), ''), 'Sem corretor') AS corretor_sub,
  r2.idcliente, r2.idReceita,
  r2.[Valor pago],
  r2.[Data de pagamento],
  r2.plano,
  r2.Cobrador
FROM vw_Fin_Receita2 r2
LEFT JOIN cad_cliente c ON c.idcliente = r2.idcliente
WHERE (0=0)
  AND (r2.[Data Referencia] >= '01/10/2025')
  AND (r2.[Data Referencia] <  '01/11/2025')
  AND (r2.[DataAdmissao]  >= '01/10/2025')
  AND (r2.[DataAdmissao]  <  '01/11/2025')
  AND (r2.[Situação] = 'pago')
  AND (r2.[Tipo] = 'Mensalidade')
  AND (c.Desativado = 0);
