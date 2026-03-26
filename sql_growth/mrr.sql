select
  count(idcliente) as mrr_count,
  sum([Valor devido]) as mrr_valor
from vw_fin_receita2
where [Data de vencimento] >= :ini
  and [Data de vencimento] < :fim
  and isnull(CanceladoANS,0) = 0
  and (Plano is NOT NULL or plano <> N'sócio')
  and [Situação] <> N'pré-Cadastro'
  and idcontatipo = 5
  and [Cliente Situação] in (N'particular', N'inadimplente', N'adimplente')
