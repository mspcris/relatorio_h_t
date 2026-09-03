-- ============================================================================
-- farmacia_consumo.sql — CONSUMO REAL: medicamento lançado ao PACIENTE,
-- agregado por serviço × mês × classe.
--
-- É a segunda leitura que o Cristiano usa (2026-09-03), a partir da
-- vw_Cad_LancamentoServicos:
--     Situação = 'Normal' AND Classe IN ('MEDICAMENTO 120', ...)
--     AND LancadoNoFaturamento = 'Não'
-- Reescrita nas tabelas base (a view junta 20+ tabelas e calcula comissão
-- linha a linha). Cada filtro dela vira:
--   Situação = 'Normal'        → LAN.DataEstorno IS NULL   (vw_Cad_Lancamento)
--   [Data e hora]              → LAN.Data
--   Classe                     → Cad_ServicoClasse via Cad_Servico.idClasse
--   LancadoNoFaturamento='Não' → usuário que lançou NÃO é de faturamento
--                                (sis_usuario.faturamento <> 1). Medido em
--                                2026-09-03: 0 lançamentos de faturamento em
--                                3 meses nos 13 postos — o filtro fica por
--                                fidelidade, não muda número hoje.
--   Codigo IS NOT NULL         → WHERE da própria vw_Cad_Lancamento
--
-- CLASSES: o pedido citava 'MEDICAMENTO 120' e 'MEDICAMENTO 90'. A 90 NÃO
-- EXISTE em posto nenhum (medido nos 13). As que existem: MEDICAMENTO 120,
-- MEDICAMENTO 30, MEDICAMENTO, MEDICAMENTO EXTERNO. A classe sai como coluna
-- e quem escolhe é a página (padrão: todas menos EXTERNO, que é remédio que
-- o paciente traz — não sai do estoque). Não fixar lista aqui.
--
-- Liga com o produto da farmácia por Cad_Produto.idServico (o ETL faz o
-- join no Python). Só 156 dos 355 produtos de Anchieta têm idServico —
-- material (gaze, luva, seringa) não é "lançado ao paciente", só os
-- medicamentos. É esperado.
--
-- O parâmetro ini é datetime (bind do pyodbc) — NUNCA string. Não escrever dois-pontos
-- antes de "ini" nem em comentário: o text() do SQLAlchemy vira bind param.
-- ============================================================================
SELECT
    LS.idServico                                  AS id_servico,
    LTRIM(RTRIM(S.Servico))                       AS servico,
    LTRIM(RTRIM(C.Classe))                        AS classe,
    YEAR(LAN.Data) * 100 + MONTH(LAN.Data)        AS ym,
    COUNT(*)                                      AS lancamentos,
    COUNT(DISTINCT LAN.idLancamento)              AS atendimentos,
    COUNT(DISTINCT LAN.idCliente)                 AS clientes,
    SUM(LS.Quantidade)                            AS qtd,
    SUM(CASE WHEN ISNULL(LS.Plano, 0) = 1 THEN LS.Quantidade ELSE 0 END) AS qtd_plano,
    MAX(LAN.Data)                                 AS ultima
FROM Cad_LancamentoServico LS WITH (NOLOCK)
JOIN Cad_Servico S         WITH (NOLOCK) ON S.idServico = LS.idServico
JOIN Cad_ServicoClasse C   WITH (NOLOCK) ON C.idClasse = S.idClasse
JOIN Cad_Lancamento LAN    WITH (NOLOCK) ON LAN.idLancamento = LS.idLancamento
LEFT JOIN sis_usuario UL   WITH (NOLOCK) ON UL.idUsuario = LAN.idUsuario
WHERE LAN.Data >= :ini
  AND LAN.Data <  DATEADD(day, 1, CAST(GETDATE() AS date))
  AND LAN.DataEstorno IS NULL
  AND LAN.Codigo IS NOT NULL
  AND C.Classe LIKE 'MEDICAMENTO%'
  AND ISNULL(UL.faturamento, 0) <> 1
GROUP BY
    LS.idServico, LTRIM(RTRIM(S.Servico)), LTRIM(RTRIM(C.Classe)),
    YEAR(LAN.Data) * 100 + MONTH(LAN.Data)
