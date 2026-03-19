SELECT
    CAST(nf.DataEmissao AS DATE) AS data_emissao,
    e.codigo,
    nf.origem,
    nf.Empresa,
    COUNT(*) AS qtd_notas,
    CAST(SUM(nf.valor_servicos) AS DECIMAL(18,2)) AS valor_notas_emitidas,
    SUM(CASE WHEN nf.Cancelada = 'Sim' THEN 1 ELSE 0 END) AS qtd_notas_canceladas,
    CAST(SUM(CASE WHEN nf.Cancelada = 'Sim' THEN nf.valor_servicos ELSE 0 END) AS DECIMAL(18,2)) AS valor_notas_canceladas,
    COUNT(*) - SUM(CASE WHEN nf.Cancelada = 'Sim' THEN 1 ELSE 0 END) AS qtd_notas_contabilizadas,
    CAST(
        SUM(nf.valor_servicos)
        - SUM(CASE WHEN nf.Cancelada = 'Sim' THEN nf.valor_servicos ELSE 0 END)
    AS DECIMAL(18,2)) AS valor_notas_contabilizadas
FROM dbo.vw_notas_emitidas_base nf
CROSS JOIN dbo.sis_empresa emp
LEFT JOIN dbo.cad_endereco e ON e.idEndereco = emp.idEndereco
WHERE nf.Desativado = 0
  AND nf.DataEmissao >= :ini
  AND nf.DataEmissao <  :fim
GROUP BY
    CAST(nf.DataEmissao AS DATE),
    e.codigo,
    nf.origem,
    nf.Empresa
ORDER BY
    CAST(nf.DataEmissao AS DATE);
