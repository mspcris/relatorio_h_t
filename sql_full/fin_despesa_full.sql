Select 
iddespesa, 
planoprincipal, 
plano, 
tipo, 
[data prestaçao] as dataprestacao,
[valor pago] as valorpago
from
vw_Fin_Despesa
Where (idContaTipo<>11)
 and ([Data prestação] >= :ini)
 and ([Data prestação] < :fim)
 and ([Situação] = 'Pago')
