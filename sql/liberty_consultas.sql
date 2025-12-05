Select 
tipo, plano, classe, especialidade, [data prestaçao], [valor pago]
from
vw_Fin_Receita2
Where (0=0)
 and ([Data prestaçao] >= :ini)
 and ([Data prestaçao] < :fim)  
 and ([Situação] = 'pago')
 and ([Plano] = 'CAMIM LIBERTY')
 and classe = 'consulta'

