select
  count(r2.idcliente) as mrr_count,
  sum(r2.[Valor devido]) as mrr_valor
from vw_fin_receita2 r2
cross join sis_empresa emp
where r2.[Data referencia] >= :ini
  and r2.[Data referencia] < :fim
  and r2.idEndereco = emp.idEndereco
  and isnull(r2.CanceladoANS,0) = 0
  and (r2.[cliente Situação] = N'adimplente' or r2.[cliente Situação] = N'inadimplente')
  and r2.tipo = N'mensalidade'
