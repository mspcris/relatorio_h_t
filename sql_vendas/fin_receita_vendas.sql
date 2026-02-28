
SELECT
    r2.Matricula,
    c.idEndereco,
    r2.corretor,
    r2.subcorretor,
    r2.idcliente,
    r2.idReceita,
    r2.[Valor pago],
    r2.[Data de pagamento],
    r2.plano,
    r2.Cobrador
FROM vw_Fin_Receita2 r2
LEFT JOIN cad_cliente c ON c.idcliente = r2.idcliente
WHERE (0=0)
  AND (r2.[Data prestaçao] >= :ini)
  AND (r2.[Data prestaçao] <  :fim)
  AND (r2.[DataAdmissao]   >= :ini)
  AND (r2.[DataAdmissao]   <  :fim)
  AND (r2.[Situação] = 'pago')
  AND (r2.[Tipo] = 'Mensalidade')
  AND (c.Desativado = 0);
