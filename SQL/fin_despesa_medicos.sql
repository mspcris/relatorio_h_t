SELECT d.iddespesa, d.valor AS medicos
FROM Fin_Despesa d
JOIN fin_plano     fp ON fp.idplano = d.idPlano
                     AND fp.Descricao = 'MEDICINA CONVENCIONAL'
JOIN fin_contatipo ct ON ct.idcontatipo = d.idContaTipo
                     AND ct.Tipo = 'médico - hora médica'
WHERE d.idContaTipo <> 11
  AND d.DataPagamentoAuto >= :ini
  AND d.DataPagamentoAuto <  :fim
  AND d.DataPagamento IS NOT NULL
  AND (d.DataCancelamento IS NULL OR d.DataCancelamento = '')