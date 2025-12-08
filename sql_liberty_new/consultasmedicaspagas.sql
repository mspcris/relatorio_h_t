Select 
r2.idlancamento, r2.matricula, p.Nome, r2.tipo, r2.plano, r2.classe, r2.especialidade, r2.[data prestaçao], r2.[valor pago]
from vw_Fin_Receita2 r2
left join cad_lancamento l on l.idlancamento = r2.idlancamento
left join vw_Cad_PacienteView p on p.idcliente = r2.idCliente
Where (0=0)
 and ([Data prestaçao] >= :ini)
 and ([Data prestaçao] < :fim)   
 and (r2.[Situação] = 'pago')
 and (r2.[Plano] = 'CAMIM LIBERTY')
 and (r2.classe = 'consulta')