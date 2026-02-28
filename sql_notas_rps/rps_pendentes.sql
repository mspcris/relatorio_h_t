SET NOCOUNT ON;

SELECT
    origem,
    codigo,
    Empresa,
    qtd,
    total
FROM dbo.vw_rps_pendentes_base
WHERE ano_emissao = YEAR(:ini)
  AND mes_num     = MONTH(:ini)
ORDER BY
    codigo,
    origem,
    Empresa;