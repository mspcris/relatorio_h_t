Select 
--sum([Valor pago])
classe, 
[cobrador fatura] as cobradorfatura, 
tipo, 
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
 and ([Data prestaçao] >= '01/10/2025') 
 and ([Data prestaçao] < '01/11/2025')    
 and ([Situação] = 'pago')
