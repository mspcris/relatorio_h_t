Select 
--sum([Valor pago])
iddespesa, 
planoprincipal, 
plano, 
tipo, 
usuarioinclusao,
corretor, 
subcorretor, 
[valor fatura] as valorfatura, 
médico as medico, 
[data de vencimento] as datadevencimento,
[valor devido] as valordevido, 
[valor pago] as valorpago,
pessoa, 
matricula, 
ordempagamento,
digitoutalao,
atendido,
situação as situacao
from
vw_Fin_Despesa
Where (idContaTipo<>11)
 and ([Data prestação] >= '01/10/2025')
 and ([Data prestação] < '01/11/2025')
 and ([Situação] = 'Pago')
