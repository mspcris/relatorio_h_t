Select 
--tipo, plano, forma, [valor pago]
count(*) as 'qtd_menslidades', forma
from
vw_Fin_Receita2
Where (0=0)
 and ([Data prestaçao] >= :ini)
 and ([Data prestaçao] < :fim)    
 and ([Situação] = 'pago')
 and ([Plano] = 'CAMIM LIBERTY')
 and tipo = 'mensalidade' 
 group by forma