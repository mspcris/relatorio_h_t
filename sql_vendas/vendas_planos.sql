-- Vendas de PLANO do mês (export_vendas.py, KPI Vendas).
-- Regras do Cristiano (2026-09-19):
--   * plano = MENSALIDADE PAGA + TAXA DE INSCRIÇÃO PAGA, em qualquer mês;
--     taxa ABONADA não é paga; cliente DESATIVADO não conta (o ETL marca).
--   * a venda cai no MÊS DO PAGAMENTO = quando o plano ficou completo, isto é,
--     a data mais tarde entre a 1a mensalidade paga e a 1a taxa paga.
--   * quem não completou aparece no mês da ADMISSÃO, como pendente.
-- Nada é descartado: o ETL marca o que falta em cada matrícula.
-- Cada posto é tratado sozinho (Cristiano, 2026-09-19): o cliente que se
-- consulta em outro posto ganha cópia do cadastro lá, mas o plano e os
-- pagamentos moram no banco do posto dele. Então entra
--   (a) quem tem mensalidade ou taxa LANÇADA neste banco (qualquer posto de
--       cadastro — matrícula de outro posto pagando aqui é aberração e o ETL
--       marca como tal), e
--   (b) cliente DESTE posto com matrícula e plano mas nada lançado (aberração).
-- Fica de fora só a cópia de cliente de outro posto sem lançamento aqui e o
-- cadastro sem plano (particular, matrícula 0 ou CPF) — medido em ago/2026.
-- "Pago" = DataPagamento preenchida, igual à coluna Situação da vw_Fin_Receita2.
-- A DATA do pagamento é a do LANÇAMENTO (DataPagamentoAuto, com hora), a mesma
-- que o kpi_metas sempre usou — boleto pago dia 30 que o banco devolve dia 2
-- cai no dia 2. Sem Auto, vale DataPagamento.
-- Esta é a ÚNICA definição de venda: o export_metas monta vendas por dia e
-- por hora a partir do CSV que esta query gera (Cristiano, 2026-09-19
-- "vendeu 10 em A tem de aparecer 10 em todas as telas").
-- Lê TABELAS (datas como parâmetro), nada de view. Sem dois-pontos em comentário.
SELECT
    cl.idCliente,
    cl.Matricula,
    cl.Nome,
    cl.DataAdmissao,
    cl.Desativado,
    e.Codigo                AS posto_cliente,
    eb.Codigo               AS posto_banco,
    pp.Plano                AS plano,
    co.Nome                 AS corretor,
    cl.SubCorretor          AS subcorretor,
    m.lancadas              AS mens_lancadas,
    m.pagas                 AS mens_pagas,
    m.abonadas              AS mens_abonadas,
    m.canceladas            AS mens_canceladas,
    m1.ValorPago            AS mens_valor,
    m1.DataPagamento        AS mens_data,
    t.lancadas              AS taxa_lancadas,
    t.pagas                 AS taxa_pagas,
    t.abonadas              AS taxa_abonadas,
    t.canceladas            AS taxa_canceladas,
    t1.ValorPago            AS taxa_valor,
    t1.DataPagamento        AS taxa_data,
    CASE WHEN m1.DataPagamento IS NULL OR t1.DataPagamento IS NULL THEN NULL
         WHEN m1.DataPagamento > t1.DataPagamento THEN m1.DataPagamento
         ELSE t1.DataPagamento END AS data_completo
FROM Cad_Cliente cl
LEFT JOIN Cad_Endereco e  ON e.idEndereco  = cl.idEndereco
CROSS JOIN sis_empresa emp
JOIN Cad_Endereco eb      ON eb.idEndereco = emp.idEndereco
LEFT JOIN Cad_Plano pp    ON pp.idPlano    = cl.idPlano
LEFT JOIN Cad_Corretor co ON co.idCorretor = cl.idCorretor
OUTER APPLY (
    SELECT COUNT(*) AS lancadas,
           SUM(CASE WHEN r.DataPagamento IS NOT NULL THEN 1 ELSE 0 END) AS pagas,
           SUM(CASE WHEN r.DataPagamento IS NULL AND r.DataAbono IS NOT NULL THEN 1 ELSE 0 END) AS abonadas,
           SUM(CASE WHEN r.DataPagamento IS NULL AND r.DataCancelamento IS NOT NULL THEN 1 ELSE 0 END) AS canceladas
    FROM Fin_Receita r JOIN Fin_ContaTipo ct ON ct.idContaTipo = r.idContaTipo
    WHERE r.idCliente = cl.idCliente AND ct.Tipo = 'Mensalidade'
) m
OUTER APPLY (
    SELECT TOP 1 r.ValorPago, COALESCE(r.DataPagamentoAuto, r.DataPagamento) AS DataPagamento
    FROM Fin_Receita r JOIN Fin_ContaTipo ct ON ct.idContaTipo = r.idContaTipo
    WHERE r.idCliente = cl.idCliente AND ct.Tipo = 'Mensalidade' AND r.DataPagamento IS NOT NULL
    ORDER BY COALESCE(r.DataPagamentoAuto, r.DataPagamento), r.idReceita
) m1
OUTER APPLY (
    SELECT COUNT(*) AS lancadas,
           SUM(CASE WHEN r.DataPagamento IS NOT NULL THEN 1 ELSE 0 END) AS pagas,
           SUM(CASE WHEN r.DataPagamento IS NULL AND r.DataAbono IS NOT NULL THEN 1 ELSE 0 END) AS abonadas,
           SUM(CASE WHEN r.DataPagamento IS NULL AND r.DataCancelamento IS NOT NULL THEN 1 ELSE 0 END) AS canceladas
    FROM Fin_Receita r JOIN Fin_ContaTipo ct ON ct.idContaTipo = r.idContaTipo
    WHERE r.idCliente = cl.idCliente AND ct.Tipo = 'Taxa de inscrição'
) t
OUTER APPLY (
    SELECT TOP 1 r.ValorPago, COALESCE(r.DataPagamentoAuto, r.DataPagamento) AS DataPagamento
    FROM Fin_Receita r JOIN Fin_ContaTipo ct ON ct.idContaTipo = r.idContaTipo
    WHERE r.idCliente = cl.idCliente AND ct.Tipo = 'Taxa de inscrição' AND r.DataPagamento IS NOT NULL
    ORDER BY COALESCE(r.DataPagamentoAuto, r.DataPagamento), r.idReceita
) t1
-- candidatos = admitidos no mês + quem pagou mensalidade ou taxa no mês
WHERE cl.idCliente IN (
        SELECT r.idCliente
        FROM Fin_Receita r JOIN Fin_ContaTipo ct ON ct.idContaTipo = r.idContaTipo
        WHERE ct.Tipo IN ('Mensalidade', 'Taxa de inscrição')
          AND r.DataPagamento IS NOT NULL
          AND ((r.DataPagamentoAuto >= :ini AND r.DataPagamentoAuto < :fim)
               OR (r.DataPagamentoAuto IS NULL AND r.DataPagamento >= :ini AND r.DataPagamento < :fim))
        UNION
        SELECT c2.idCliente FROM Cad_Cliente c2
        WHERE c2.DataAdmissao >= :ini AND c2.DataAdmissao < :fim)
  AND (
        -- completo: cai no mês em que ficou completo
        (m1.DataPagamento IS NOT NULL AND t1.DataPagamento IS NOT NULL
         AND (CASE WHEN m1.DataPagamento > t1.DataPagamento THEN m1.DataPagamento ELSE t1.DataPagamento END) >= :ini
         AND (CASE WHEN m1.DataPagamento > t1.DataPagamento THEN m1.DataPagamento ELSE t1.DataPagamento END) <  :fim)
     OR
        -- incompleto: cai no mês da admissão
        ((m1.DataPagamento IS NULL OR t1.DataPagamento IS NULL)
         AND cl.DataAdmissao >= :ini AND cl.DataAdmissao < :fim
         AND (m.lancadas > 0 OR t.lancadas > 0
              OR (e.Codigo = eb.Codigo AND cl.Matricula > 0 AND cl.idPlano IS NOT NULL)))
      )
ORDER BY cl.DataAdmissao, cl.Matricula;
