-- ============================================================================
-- farmacia_entradas.sql — ENTRADAS na farmácia do posto, agregadas por
-- produto × mês × fornecedor.
--
-- Responde "de onde veio": compra de fornecedor, transferência de outro posto
-- (o fornecedor vem cadastrado com o nome do posto — 'ANCHIETA', 'CAMPINHO',
-- 'CAMIM CAMPO GRANDE'; o ETL casa isso com cad_endereco) ou acerto de
-- estoque ('ACERTO DE ESTOQUE', 'DEVOLUÇÃO', 'JÁ EM ESTOQUE (RECONTAGEM)').
--
-- UNIDADE (medido em 2026-09-03, postos A e R): Est_EntradaItem.Quantidade é
-- o número de CAIXAS (quase sempre 1) e CaixaCom é quantas unidades tem a
-- caixa. Unidades = Quantidade × CaixaCom — é exatamente o QuantidadeTotal da
-- vw_Est_Entradaitem do CAMIM. Somar só Quantidade daria "1 tubo" onde
-- entraram 1.200.
-- Valor: ValorCaixa quando existe, senão Valor unitário × unidades.
--
-- Corte superior em amanhã: há entradas digitadas com ano 2270 e 5015.
-- O parâmetro ini é datetime (bind do pyodbc) — NUNCA string. Não escrever dois-pontos
-- antes de "ini" nem em comentário: o text() do SQLAlchemy vira bind param.
-- ============================================================================
SELECT
    ei.idProduto                                         AS id_produto,
    YEAR(e.DataEntrada) * 100 + MONTH(e.DataEntrada)     AS ym,
    LTRIM(RTRIM(ISNULL(f.Fornecedor, '')))               AS fornecedor,
    COUNT(*)                                             AS itens,
    COUNT(DISTINCT e.idEntrada)                          AS entradas,
    SUM(ei.Quantidade * ISNULL(NULLIF(ei.CaixaCom, 0), 1)) AS qtd,
    SUM(ei.Quantidade * ISNULL(NULLIF(ei.ValorCaixa, 0),
            ISNULL(ei.Valor, 0) * ISNULL(NULLIF(ei.CaixaCom, 0), 1)))  AS valor,
    MAX(e.DataEntrada)                                   AS ultima
FROM Est_EntradaItem ei WITH (NOLOCK)
JOIN Est_Entrada e      WITH (NOLOCK) ON e.idEntrada = ei.idEntrada
JOIN Cad_Produto p      WITH (NOLOCK) ON p.idProduto = ei.idProduto
LEFT JOIN Cad_Fornecedor f WITH (NOLOCK) ON f.idFornecedor = e.idFornecedor
WHERE e.DataEntrada >= :ini
  AND e.DataEntrada <  DATEADD(day, 1, CAST(GETDATE() AS date))
  AND ISNULL(e.Almoxarifado, 0) = 0
  AND ISNULL(p.Almoxarifado, 0) = 0
  AND ISNULL(e.Desativado, 0) = 0
  AND e.DataCancelamento IS NULL
GROUP BY
    ei.idProduto,
    YEAR(e.DataEntrada) * 100 + MONTH(e.DataEntrada),
    LTRIM(RTRIM(ISNULL(f.Fornecedor, '')))
