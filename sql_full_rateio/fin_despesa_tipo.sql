    SELECT
    e.codigo as 'posto_lancamento',
    dr.endereco_letra as endereco,
    pp.PlanoPrincipal,
    isnull(p.descricao,'sem plano') as plano,
    ct.tipo,
    dr.comentario,
    (dr.[Valor Rateio]) as 'valorpago'
FROM vw_Fin_DespesaRateio_tela dr
cross join sis_empresa emp
left join Cad_Endereco e on e.idEndereco = emp.idEndereco
left join fin_despesa D on d.idDespesa = dr.idDespesa
LEFT JOIN Fin_ContaTipo CT ON CT.idContaTipo = D.idContaTipo
LEFT JOIN Fin_Plano P ON P.idPlano = isnull(D.idPlano,ct.idPlano)
LEFT JOIN Fin_PlanoPrincipal pp ON pp.idPlanoPrincipal = P.idPlanoPrincipal
WHERE
    (d.idContaTipo <> 11)
 and (DataPagamentoAuto >= :ini)
 and (DataPagamentoAuto < :fim)
 AND DataPagamento IS NOT NULL
