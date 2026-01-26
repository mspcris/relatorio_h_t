 Select 
ct.tipo,
sum(r.[Valorpago]) as valorpago
from Fin_Receita r
LEFT JOIN Fin_ContaTipo CT ON CT.idContaTipo = R.idContaTipo
Where (0=0)
 and (R.DataPagamentoAuto >= :ini)
 and (R.DataPagamentoAuto < :fim)  
 and R.DataPagamento IS NOT NULL
 group by ct.tipo