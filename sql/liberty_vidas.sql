Select  c.CanceladoANS,
idcliente, 0 as idDependente, c.tipo, c.DataAdmissao, c.DataCancelamentoANS,
datediff(month, c.DataAdmissao,getdate()) as 'meses de plano',
datediff(day, c.DataAdmissao,getdate()) as 'dias de plano',
c.cobrador, c.idade, c.origem, c.[Valor mensalidade]
from 
vw_Cad_cliente c
-- left join cad_plano p on p.idPlano = c.idPlano 
Where (c.Desativado = 0)
-- and (c.[Situação] = 'Adimplente')
 and (c.[Plano] = 'CAMIM LIBERTY')
union 
Select c.CanceladoANS,
c.idcliente, d.idDependente, c.tipo, d.DataAdmissao, c.DataCancelamentoANS,
datediff(month, d.DataAdmissao,getdate()) as 'meses de plano',
datediff(day, d.DataAdmissao,getdate()) as 'dias de plano',
cobrador, idade, c.origem, c.[Valor mensalidade]
from 
Cad_ClienteDependente d
join vw_Cad_Cliente C on C.idCliente = d.idCliente
Where (c.Desativado = 0) and (d.Desativado = 0)
 and (c.[Plano] = 'CAMIM LIBERTY')
-- and (c.[Situação] = 'Adimplente')
order by idcliente, idDependente