Select 
tipo,
classe, 
[cobrador fatura] as cobradorfatura, 
Situação as situacao,
matriculal,
forma,
descrição as descricao,
[data referencia] as datareferencia,
[valor devido] as valordevido,
[Data prestaçao] as dataprestacao,
[valor pago] as valorpago
from vw_Fin_Receita2
Where (0=0)
 and ([Data prestaçao] >= :ini) 
 and ([Data prestaçao] < :fim)    
 and ([Situação] = 'pago')
