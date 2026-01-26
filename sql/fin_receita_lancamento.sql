 Select 
classe.classe,
sum(r.[Valorpago]) as valorpago
from Fin_Receita r
LEFT JOIN Fin_ContaTipo CT ON CT.idContaTipo = R.idContaTipo
LEFT JOIN Cad_Lancamento L ON L.idLancamento = R.idLancamento
LEFT JOIN vw_Cad_LancamentoServicoMin Mi ON Mi.idLancamento = L.idLancamento
LEFT JOIN Cad_Servico SS ON SS.idServico = Mi.idServico
LEFT JOIN Cad_ServicoClasse Classe ON Classe.idClasse = SS.idClasse
Where (0=0)
 and (R.DataPagamentoAuto >= :ini)
 and (R.DataPagamentoAuto < :fim)  
  and R.DataPagamento IS NOT NULL
 and ct.tipo = 'lancamento'
 group by classe.classe
