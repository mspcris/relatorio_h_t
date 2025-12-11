SELECT 
    ct.tipo,
    sum(d.Valorpago) as 'valorpago'
FROM Fin_Despesa d 
LEFT JOIN Fin_ContaTipo CT ON CT.idContaTipo = D.idContaTipo
WHERE 
    (d.idContaTipo <> 11)
 and (DataPagamentoAuto >= :ini) 
 and (DataPagamentoAuto < :fim)  
 AND DataPagamento IS NOT NULL
GROUP BY 
    ct.tipo
   