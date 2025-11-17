Select d.[iddespesa], d.[valor] as alimentacao from Fin_Despesa d
left join Fin_Plano p on p.idPlano = d.idPlano
left join Fin_ContaTipo ct on ct.idContaTipo = d.idContaTipo
Where d.idContaTipo<>11
 and d.[DatapagamentoAuto] >= :ini
 and d.[DatapagamentoAuto] < :fim
 and (D .DataPagamento IS NOT NULL and D .DataCancelamento IS NULL)
 and p.Descricao in ('alimentação funcionários', 'DESPESAS FIXAS', 'DESPESAS DIVERSAS', 'COMPRAS DIVERSAS')
 and ct.Tipo in ('ALIMENTAÇÃO CAFÉ/LANCHE', 'ALIMENTAÇÃO ALMOÇO/JANTA', 'PADARIA', 'REFEICAO')

 --(select idplano from fin_plano fp where fp.Descricao = 'alimentação funcionários' and desativado = 0)
