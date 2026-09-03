-- ============================================================================
-- farmacia_saidas.sql — SAÍDAS da farmácia do posto, agregadas por
-- produto × mês × destino (endereço) × setor.
--
-- É o CONSUMO do posto: tudo que a farmácia entregou (enfermagem, laboratório,
-- recepção...) ou mandou para outro posto / descarte. Quem classifica o
-- destino (interna / outro posto / descarte / outro) é o ETL, olhando
-- destino_codigo contra cad_endereco — aqui só sai o dado bruto.
--
-- Filtros espelham a vw_Est_Saidaitem + vw_Est_Saida do CAMIM:
--   • Almoxarifado = 0 no cabeçalho E no produto (papelaria/limpeza ficam fora)
--   • cabeçalho ativo e não cancelado, item não cancelado
--   • Gravado sai como COLUNA: saída ainda em digitação (Gravado NULL,
--     Numero 0) não é consumo — a vw_Est_Saida do CAMIM corta Gravado = 1 —
--     mas é ~5 % do volume (A: 564 itens / 50.949 un em 12m contra 13.134 /
--     1.015.567 gravados) e a página avisa quanto ficou de fora. O ETL separa.
--
-- O parâmetro ini (sem dois-pontos aqui: o text() do SQLAlchemy lê dois-pontos+palavra ATÉ EM
-- COMENTÁRIO como bind param) é datetime. Tabela base → sem problema de DATEFORMAT,
-- mas NUNCA passar string aqui (regra do CLAUDE.md).
-- Nada de literal de hora com dois-pontos neste arquivo (text() do SQLAlchemy).
-- ============================================================================
SELECT
    si.idProduto                                   AS id_produto,
    YEAR(s.DataSaida) * 100 + MONTH(s.DataSaida)   AS ym,
    ISNULL(s.idEndereco, -1)                       AS id_endereco,
    LTRIM(RTRIM(ce.Codigo))                        AS destino_codigo,
    LTRIM(RTRIM(ce.Descricao))                     AS destino,
    ISNULL(s.idSetor, -1)                          AS id_setor,
    LTRIM(RTRIM(st.Setor))                         AS setor,
    CASE WHEN ISNULL(s.Gravado, 0) = 1 THEN 1 ELSE 0 END AS gravado,
    COUNT(*)                                       AS itens,
    COUNT(DISTINCT s.idSaida)                      AS saidas,
    SUM(si.Quantidade)                             AS qtd,
    MAX(s.DataSaida)                               AS ultima
FROM Est_SaidaItem si WITH (NOLOCK)
JOIN Est_Saida s      WITH (NOLOCK) ON s.idSaida = si.idSaida
JOIN Cad_Produto p    WITH (NOLOCK) ON p.idProduto = si.idProduto
LEFT JOIN Cad_Endereco ce WITH (NOLOCK) ON ce.idEndereco = s.idEndereco
LEFT JOIN sis_setor st    WITH (NOLOCK) ON st.idSetor = s.idSetor
WHERE s.DataSaida >= :ini
  AND s.DataSaida <  DATEADD(day, 1, CAST(GETDATE() AS date))
  AND ISNULL(s.Almoxarifado, 0) = 0
  AND ISNULL(p.Almoxarifado, 0) = 0
  AND ISNULL(s.Desativado, 0) = 0
  AND s.DataCancelamento IS NULL
  AND si.DataCancelamento IS NULL
GROUP BY
    si.idProduto,
    YEAR(s.DataSaida) * 100 + MONTH(s.DataSaida),
    ISNULL(s.idEndereco, -1), LTRIM(RTRIM(ce.Codigo)), LTRIM(RTRIM(ce.Descricao)),
    ISNULL(s.idSetor, -1), LTRIM(RTRIM(st.Setor)),
    CASE WHEN ISNULL(s.Gravado, 0) = 1 THEN 1 ELSE 0 END
