-- Mensalidades por DIA e HORA do pagamento — mesma regra de
-- sql_mensalidades_por_dia.sql, só que com a hora. A coluna `vendas` sai 0
-- daqui: o export_metas preenche com a regra única de venda (CSV de planos do
-- export_vendas, 2026-09-19).
-- Serve ao "ritmo do dia" do avisos_gerenciais: comparar hoje até as 10h com
-- os outros meses até as 10h do mesmo dia. O pagamento entra em lotes (retorno
-- do banco no começo da tarde), então "até ontem" ou "fração do dia" enganam.
SET NOCOUNT ON;

SELECT
    DAY(r.DataPagamentoAuto)               AS dia,
    DATEPART(HOUR, r.DataPagamentoAuto)    AS hora,
    COUNT(*)                               AS mens,
    0                                      AS vendas
FROM Fin_Receita r
JOIN Cad_Cliente cl ON cl.idCliente = r.idCliente
WHERE
    r.DataPagamentoAuto IS NOT NULL
    AND r.DataPagamentoAuto >= :ini
    AND r.DataPagamentoAuto <  :fim
    AND r.idContaTipo = 5
    AND cl.Desativado = 0
GROUP BY
    DAY(r.DataPagamentoAuto),
    DATEPART(HOUR, r.DataPagamentoAuto)
ORDER BY
    dia, hora;
