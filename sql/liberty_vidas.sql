
--Validar o número de vidas no liberty. Acredito que esteja pegando de outros postos no relatório no site.  ( 08/12/25 - Validado com select abaixo )

Select  c.CanceladoANS,
idcliente, 0 as idDependente, c.tipo, c.DataAdmissao, c.DataCancelamentoANS,
datediff(month, c.DataAdmissao,getdate()) as 'meses de plano',
datediff(day, c.DataAdmissao,getdate()) as 'dias de plano',
c.cobrador, c.idade, c.origem, c.[Valor mensalidade]
from 
vw_Cad_cliente c
left join sis_empresa emp on emp.idendereco = c.idendereco
Where (c.Desativado = 0)
 and (c.[Plano] = 'CAMIM LIBERTY')
 and c.idEndereco = emp.idEndereco
 --and c.CanceladoANS = 0 o front kpi_Liberty.html tem inteligência no js para mostrar as vidas mês a mês considerando datacancelamentoans. Portanto, não posso subir esta regra e vou deixar comentado para não esquecer.

union 

Select c.CanceladoANS,
c.idcliente, d.idDependente, c.tipo, d.DataAdmissao, c.DataCancelamentoANS,
datediff(month, d.DataAdmissao,getdate()) as 'meses de plano',
datediff(day, d.DataAdmissao,getdate()) as 'dias de plano',
cobrador, idade, c.origem, c.[Valor mensalidade]
from 
Cad_ClienteDependente d
join vw_Cad_Cliente C on C.idCliente = d.idCliente
left join sis_empresa emp on emp.idendereco = c.idendereco
Where (c.Desativado = 0) and (d.Desativado = 0)
 and (c.[Plano] = 'CAMIM LIBERTY')
 and c.idEndereco = emp.idEndereco
 --and c.CanceladoANS = 0 o front kpi_Liberty.html tem inteligência no js para mostrar as vidas mês a mês considerando datacancelamentoans. Portanto, não posso subir esta regra e vou deixar comentado para não esquecer.
order by idcliente, idDependente