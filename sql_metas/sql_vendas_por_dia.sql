SET NOCOUNT ON;
SET DATEFIRST 1;
SET LANGUAGE Portuguese;

SELECT
    YEAR(r.DataPagamentoAuto) AS ano,
    MONTH(r.DataPagamentoAuto) AS mes,
    DATENAME(MONTH, r.DataPagamentoAuto) AS mes_nome,
    DAY(r.DataPagamentoAuto) AS dia,
    DATENAME(WEEKDAY, r.DataPagamentoAuto) AS dia_sem,
    COUNT(DISTINCT CASE
        WHEN r.idContaTipo = 5
         AND MONTH(r.DataMensalidade) = MONTH(cl.DataAdmissao)
         AND YEAR(r.DataMensalidade)  = YEAR(cl.DataAdmissao)
        THEN r.idCliente
    END) AS vendas_dia
FROM Fin_Receita r
JOIN Cad_Cliente cl ON cl.idCliente = r.idCliente
WHERE
    r.DataPagamentoAuto IS NOT NULL
    AND r.DataPagamentoAuto >= :ini
    AND r.DataPagamentoAuto <  :fim
    AND r.idContaTipo = 5
    AND cl.Desativado = 0
GROUP BY
    YEAR(r.DataPagamentoAuto),
    MONTH(r.DataPagamentoAuto),
    DATENAME(MONTH, r.DataPagamentoAuto),
    DAY(r.DataPagamentoAuto),
    DATENAME(WEEKDAY, r.DataPagamentoAuto)
ORDER BY
    ano, mes, dia;