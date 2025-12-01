Select
e.codigo AS posto,
vwc.tipo,
count(*) as matriculas, avg(vwc.Valormensalidadeultimopagamento) as ticket_medio_real, 
avg(vwc.[valor mensalidade]) as ticket_medio_previsto
from 
vw_Cad_Cliente vwc
join sis_empresa emp on emp.idendereco = vwc.idendereco
join Cad_Endereco e on emp.idEndereco = e.idEndereco
Where (vwc.Desativado = 0)
 and (vwc.[Situação] = 'adimplente')
 and vwc.idEndereco = emp.idEndereco
 AND (vwc.CanceladoANS = 0)
 and vwc.Convênio = 0
 and vwc.[Valor mensalidade] > 48
 group by e.codigo, vwc.tipo