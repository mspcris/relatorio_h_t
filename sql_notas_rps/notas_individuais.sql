SELECT * FROM (
    SELECT
        CAST('Clinica' AS NVARCHAR(30))                             AS origem,
        nf.NotaFiscal,
        CAST(nf.Empresa  AS NVARCHAR(200)) COLLATE DATABASE_DEFAULT AS Empresa,
        CAST(nf.DataEmissao AS DATE)                                AS data_emissao,
        CAST(nf.Cliente  AS NVARCHAR(200)) COLLATE DATABASE_DEFAULT AS Cliente,
        CAST(nf.CPF      AS NVARCHAR(30))  COLLATE DATABASE_DEFAULT AS cpf,
        CAST(nf.CNPJ     AS NVARCHAR(30))  COLLATE DATABASE_DEFAULT AS cnpj,
        CAST(nf.v51_Valor_Servicos AS DECIMAL(18,2))                AS valor,
        CAST(nf.Cancelada AS NVARCHAR(10)) COLLATE DATABASE_DEFAULT AS Cancelada
    FROM dbo.vw_Fin_NotaFiscalEmitida nf
    WHERE nf.Desativado = 0

    UNION ALL

    SELECT
        CAST('OperadoraCamim' AS NVARCHAR(30)),
        nf.NotaFiscal,
        CAST(nf.Empresa  AS NVARCHAR(200)) COLLATE DATABASE_DEFAULT,
        CAST(nf.DataEmissao AS DATE),
        CAST(nf.Cliente  AS NVARCHAR(200)) COLLATE DATABASE_DEFAULT,
        CAST(nf.CPF      AS NVARCHAR(30))  COLLATE DATABASE_DEFAULT,
        CAST(nf.CNPJ     AS NVARCHAR(30))  COLLATE DATABASE_DEFAULT,
        CAST(nf.v51_Valor_Servicos AS DECIMAL(18,2)),
        CAST(nf.Cancelada AS NVARCHAR(10)) COLLATE DATABASE_DEFAULT
    FROM dbo.vw_Fin_NotaFiscalEmitidaOperadoraCamim nf
    WHERE nf.Desativado = 0

    UNION ALL

    SELECT
        CAST('OperadoraSDM' AS NVARCHAR(30)),
        nf.NotaFiscal,
        CAST(nf.Empresa  AS NVARCHAR(200)) COLLATE DATABASE_DEFAULT,
        CAST(nf.DataEmissao AS DATE),
        CAST(nf.Cliente  AS NVARCHAR(200)) COLLATE DATABASE_DEFAULT,
        CAST(nf.CPF      AS NVARCHAR(30))  COLLATE DATABASE_DEFAULT,
        CAST(nf.CNPJ     AS NVARCHAR(30))  COLLATE DATABASE_DEFAULT,
        CAST(nf.v51_Valor_Servicos AS DECIMAL(18,2)),
        CAST(nf.Cancelada AS NVARCHAR(10)) COLLATE DATABASE_DEFAULT
    FROM dbo.vw_Fin_NotaFiscalEmitidaOperadoraSDM nf
    WHERE nf.Desativado = 0
) t
WHERE t.data_emissao >= :ini
  AND t.data_emissao <  :fim
ORDER BY t.data_emissao, t.origem, t.NotaFiscal;
