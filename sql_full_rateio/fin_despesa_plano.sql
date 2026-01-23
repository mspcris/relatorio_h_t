SELECT dr.endereco_letra,
    pp.PlanoPrincipal,
    isnull(p.descricao,'sem plano') as plano,
    sum(dr.ValorRateio) as 'valorpago'
FROM vw_Fin_DespesaRateio_tela dr
left join fin_despesa D on d.idDespesa = dr.idDespesa
LEFT JOIN Fin_ContaTipo CT ON CT.idContaTipo = D.idContaTipo
LEFT JOIN Fin_Plano P ON P.idPlano = isnull(D.idPlano,ct.idPlano)
LEFT JOIN Fin_PlanoPrincipal pp ON pp.idPlanoPrincipal = P.idPlanoPrincipal

WHERE
    (d.idContaTipo <> 11)
 and (DataPagamentoAuto >= :ini)
 and (DataPagamentoAuto < :fim)
 AND DataPagamento IS NOT NULL
GROUP BY
    dr.Endereco,
    pp.PlanoPrincipal,
    p.Descricao
