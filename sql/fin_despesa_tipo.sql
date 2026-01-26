SELECT 
    pp.PlanoPrincipal,
    isnull(p.descricao,'sem plano') as plano,
    ct.tipo,
    sum(d.Valorpago) as 'valorpago'
FROM Fin_Despesa d 
LEFT JOIN Fin_ContaTipo CT ON CT.idContaTipo = D.idContaTipo
LEFT JOIN Fin_Plano P ON P.idPlano = isnull(D.idPlano,ct.idPlano)
LEFT JOIN Fin_PlanoPrincipal pp ON pp.idPlanoPrincipal = P.idPlanoPrincipal
WHERE 
    (d.idContaTipo <> 11)
 and (DataPagamentoAuto >= :ini) 
 and (DataPagamentoAuto < :fim)  
 AND DataPagamento IS NOT NULL
GROUP BY 
    pp.PlanoPrincipal,
    p.Descricao, 
    ct.tipo
   




