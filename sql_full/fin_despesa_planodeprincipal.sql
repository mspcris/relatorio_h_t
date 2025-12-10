SELECT 
    isnull(pp.PlanoPrincipal,'sem plano principal') as PlanoPrincipal,
    sum(d.Valorpago) as 'valorpago'
FROM Fin_Despesa d
LEFT JOIN Fin_ContaTipo CT ON CT.idContaTipo = D.idContaTipo
LEFT JOIN Fin_Plano P ON P.idPlano = isnull(D.idPlano,ct.idPlano)
left join fin_planoprincipal pp on pp.idplanoprincipal =p.idplanoPrincipal
WHERE 
    (d.idContaTipo <> 11)
 and (DataPagamentoAuto >= :ini) 
 and (DataPagamentoAuto < :fim)  
 AND DataPagamento IS NOT NULL
GROUP BY 
    pp.PlanoPrincipal
