select 
YEAR(r.DataPagamento) as ano, 
month(r.DataPagamento) as mes, 
day(DataPagamento) as dia, 
count(*) as mensalidades, 
sum(r.ValorPago) as valor 
from Fin_Receita r
where 
    DataPagamento is NOT NULL
    and r.DataPagamento >= '01/01/2020'
  --and r.DataPagamento < '01/02/2026'
    and r.idContaTipo = 5
group by YEAR(r.DataPagamento), month(r.DataPagamento), day(DataPagamento)
order by ano, mes, dia