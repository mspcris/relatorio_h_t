SELECT 
    ct.tipo,
    sum(d.Valorpago) as 'valorpago'
FROM Fin_Despesa d 
LEFT JOIN Fin_ContaTipo CT ON CT.idContaTipo = D.idContaTipo
WHERE 
    (d.idContaTipo <> 11)
 and (DataPagamentoAuto >= '01/11/2025') 
 and (DataPagamentoAuto < '01/12/2025')  
 AND DataPagamento IS NOT NULL
GROUP BY 
    ct.tipo
   