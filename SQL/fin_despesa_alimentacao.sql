Select d.[iddespesa], d.[valor] as alimentacao from Fin_Despesa d
Where idContaTipo<>11
 and d.[DatapagamentoAuto] >= :ini
 and d.[DatapagamentoAuto] < :fim
 and (D .DataPagamento IS NOT NULL and D .DataCancelamento IS NULL)
 and d.[idPlano] = (select idplano from fin_plano fp where fp.Descricao = 'alimentação funcionários' and desativado = 0)
