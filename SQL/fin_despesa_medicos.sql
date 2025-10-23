Select d.[iddespesa], d.[valor] as medicos from Fin_Despesa d
Where idContaTipo<>11
 and d.[DatapagamentoAuto] >= :ini
 and d.[DatapagamentoAuto] < :fim
 and (D .DataPagamento IS NOT NULL and D .DataCancelamento IS NULL)
 and d.[idPlano] = (select idplano from fin_plano fp where fp.Descricao = 'MEDICINA CONVENCIONAL' and desativado = 0)
 and d.[idContaTipo] = (select idcontatipo from fin_contatipo ct where ct.Tipo = 'médico - hora médica' and desativado = 0)
