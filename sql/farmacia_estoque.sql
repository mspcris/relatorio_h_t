-- ============================================================================
-- farmacia_estoque.sql — foto do CADASTRO + ESTOQUE de cada produto da
-- farmácia do posto (uma linha por produto, incluindo desativados — o ETL
-- precisa deles para nomear produto que teve saída e depois foi desativado).
--
-- Reproduz a conta da vw_Cad_ProdutoFarmacia do CAMIM sem a UDF escalar
-- vw_cad_ProdutoLoteAVencer (que roda linha a linha) e sem a subquery
-- correlacionada por produto: lotes e última entrada são agregados UMA vez.
--
--   estoque_farmacia  = lotes ativos, Farmacia = 1, validade > ontem
--                       (é o QuantidadeFarmacia da view — o número que o
--                       Cristiano vê no sistema)
--   estoque_total     = idem sem o filtro Farmacia
--   estoque_vencido   = lotes ativos com quantidade > 0 e validade vencida
--   qtd_vence_90d     = quantidade em lotes que vencem nos próximos 90 dias
--   proximo_vencimento= menor validade futura com quantidade > 0
--
-- QuantidadeEnfermaria de Cad_Produto vem só como referência: o Cristiano
-- confirmou em 2026-09-03 que a enfermaria NÃO é controlada — o campo só
-- acumula envios (1,6 milhão de gazes, saldos negativos). Não é estoque.
--
-- Cad_ProdutoGrupo só está preenchido em Anchieta (19 grupos) e Campo Grande
-- (1); nos outros 11 postos grupo sai NULL — a página mostra "sem grupo".
-- ============================================================================
SELECT
    p.idProduto                               AS id_produto,
    LTRIM(RTRIM(p.Produto))                   AS produto,
    ISNULL(p.Desativado, 0)                   AS desativado,
    p.QuantidadeMinima                        AS qtd_minima,
    p.QuantidadeEnfermaria                    AS qtd_enfermaria_sistema,
    p.QuantidadeMinimaReposicaoEnfermaria     AS qtd_min_reposicao_enfermaria,
    p.QuantidadeIdealEnfermaria               AS qtd_ideal_enfermaria,
    LTRIM(RTRIM(p.Substancia))                AS substancia,
    LTRIM(RTRIM(p.NomeComercial))             AS nome_comercial,
    p.idServico                               AS id_servico,
    LTRIM(RTRIM(s.Servico))                   AS servico,
    LTRIM(RTRIM(pg.Grupo))                    AS grupo,
    ISNULL(l.estoque_farmacia, 0)             AS estoque_farmacia,
    ISNULL(l.estoque_total, 0)                AS estoque_total,
    ISNULL(l.estoque_vencido, 0)              AS estoque_vencido,
    ISNULL(l.qtd_vence_90d, 0)                AS qtd_vence_90d,
    l.proximo_vencimento                      AS proximo_vencimento,
    ult.DataEntrada                           AS ultima_entrada,
    LTRIM(RTRIM(ult.Fornecedor))              AS ultima_entrada_fornecedor,
    ult.Valor                                 AS ultima_entrada_valor_unit,
    ult.qtd                                   AS ultima_entrada_qtd
FROM Cad_Produto p WITH (NOLOCK)
LEFT JOIN Cad_Servico s        WITH (NOLOCK) ON s.idServico = p.idServico
LEFT JOIN Cad_ProdutoGrupo pg  WITH (NOLOCK) ON pg.idprodutoGrupo = p.idProdutoGrupo
LEFT JOIN (
    SELECT
        pl.idproduto,
        SUM(CASE WHEN pl.Farmacia = 1 AND pl.datavalidade > DATEADD(day, -1, GETDATE())
                 THEN pl.quantidade ELSE 0 END)                                AS estoque_farmacia,
        SUM(CASE WHEN pl.datavalidade > DATEADD(day, -1, GETDATE())
                 THEN pl.quantidade ELSE 0 END)                                AS estoque_total,
        SUM(CASE WHEN pl.quantidade > 0 AND pl.datavalidade <= DATEADD(day, -1, GETDATE())
                 THEN pl.quantidade ELSE 0 END)                                AS estoque_vencido,
        SUM(CASE WHEN pl.quantidade > 0 AND pl.datavalidade >= GETDATE()
                      AND pl.datavalidade <= DATEADD(day, 90, GETDATE())
                 THEN pl.quantidade ELSE 0 END)                                AS qtd_vence_90d,
        MIN(CASE WHEN pl.quantidade > 0 AND pl.datavalidade >= GETDATE()
                 THEN pl.datavalidade END)                                     AS proximo_vencimento
    FROM Cad_ProdutoLote pl WITH (NOLOCK)
    WHERE ISNULL(pl.Desativado, 0) = 0
    GROUP BY pl.idproduto
) l ON l.idproduto = p.idProduto
LEFT JOIN (
    SELECT idProduto, DataEntrada, Fornecedor, Valor, qtd
    FROM (
        SELECT
            ei.idProduto, e.DataEntrada, f.Fornecedor, ei.Valor,
            ei.Quantidade * ISNULL(NULLIF(ei.CaixaCom, 0), 1) AS qtd,
            ROW_NUMBER() OVER (PARTITION BY ei.idProduto
                               ORDER BY e.DataEntrada DESC, ei.idEntradaItem DESC) AS rn
        FROM Est_EntradaItem ei WITH (NOLOCK)
        JOIN Est_Entrada e      WITH (NOLOCK) ON e.idEntrada = ei.idEntrada
        LEFT JOIN Cad_Fornecedor f WITH (NOLOCK) ON f.idFornecedor = e.idFornecedor
        WHERE ISNULL(e.Almoxarifado, 0) = 0
          AND e.DataCancelamento IS NULL
          AND e.DataEntrada < DATEADD(day, 1, CAST(GETDATE() AS date))
    ) x
    WHERE x.rn = 1
) ult ON ult.idProduto = p.idProduto
WHERE ISNULL(p.Almoxarifado, 0) = 0
