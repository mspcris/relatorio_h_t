   Select 
isnull(forma.forma,'egide') as forma,
sum(r.[Valorpago]) as valorpago
from Fin_Receita r
left join Fin_Forma forma on forma.idForma = R.idForma
Where (0=0)
 and (R.DataPagamentoAuto >= :ini)
 and (R.DataPagamentoAuto < :fim)  
 and  R.DataPagamento IS NOT NULL
 group by forma.forma