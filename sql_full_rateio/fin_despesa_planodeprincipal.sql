SELECT
    e.codigo as 'posto_lancamento',
    dr.endereco_letra as endereco, 
    isnull(pp.PlanoPrincipal,'sem plano principal') as PlanoPrincipal,
    dr.comentario,
    (dr.[Valor Rateio]) as 'valorpago'
FROM vw_Fin_DespesaRateio_tela dr
cross join sis_empresa emp
left join Cad_Endereco e on e.idEndereco = emp.idEndereco
left join fin_despesa D on d.idDespesa = dr.idDespesa
LEFT JOIN Fin_ContaTipo CT ON CT.idContaTipo = D.idContaTipo
LEFT JOIN Fin_Plano P ON P.idPlano = isnull(D.idPlano,ct.idPlano)
left join fin_planoprincipal pp on pp.idplanoprincipal =p.idplanoPrincipal
WHERE
    (d.idContaTipo <> 11)
 and (DataPagamentoAuto >= :ini)
 and (DataPagamentoAuto < :fim)
 AND DataPagamento IS NOT NULL
