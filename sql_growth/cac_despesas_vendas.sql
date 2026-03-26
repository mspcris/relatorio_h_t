select
  sum([Valor pago]) as cac_despesas_vendas
from vw_Fin_Despesa
where (idContaTipo <> 11)
  and ([Data prestação] >= :ini)
  and ([Data prestação] < :fim)
  and ([Situação] = N'Pago')
  and plano = N'DESPESAS DE VENDAS'
