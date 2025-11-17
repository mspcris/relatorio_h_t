Select r.[idreceita], r.[ValorPago] as Mensalidades from fin_receita r
Where r.[DataPagamentoAuto] >= :ini
  and r.[DataPagamentoAuto] < :fim    
  and r.[DataPagamento] IS NOT NULL
  and r.[idcontaTipo] = 5
  and dataabono is null