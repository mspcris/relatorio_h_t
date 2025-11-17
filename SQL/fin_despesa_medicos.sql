SELECT d.iddespesa, d.valor AS medicos
FROM Fin_Despesa d
JOIN fin_plano     fp ON fp.idplano = d.idPlano
JOIN fin_contatipo ct ON ct.idcontatipo = d.idContaTipo
WHERE d.idContaTipo <> 11
  AND d.DataPagamentoAuto >= '01/10/2025'
  AND d.DataPagamentoAuto <  '31/10/2025'
  AND d.DataPagamento IS NOT NULL
  AND (d.DataCancelamento IS NULL OR d.DataCancelamento = '')
  AND (fp.Descricao = 'MEDICINA CONVENCIONAL' or  fp.Descricao like 'sal%')
  AND (ct.Tipo = 'plantão medico' or ct.Tipo = 'médico - hora médica')