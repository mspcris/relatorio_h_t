-- Uma linha por nota emitida (as tres views do ERP), mais o que a nota E:
-- matricula do cliente (Cad_Cliente) e o codigo/descricao do servico (Fin_Nota).
-- As views nao expoem essas colunas, entao elas entram por LEFT JOIN em um
-- derived table com UMA linha por numero de nota + CNPJ do prestador (o mesmo
-- numero pode ter mais de um retorno; fica o retorno mais recente). Assim o
-- conjunto de linhas continua exatamente o das views.
-- ATENCAO: nada de dois-pontos seguido de palavra fora dos binds, nem em
-- comentario (o text() do SQLAlchemy le como parametro).
SELECT
    t.origem, t.NotaFiscal, t.Empresa, t.data_emissao, t.Cliente, t.cpf, t.cnpj,
    t.valor, t.Cancelada,
    -- ISNULL de proposito: coluna de texto com NULL vira NaN no pandas novo e
    -- NaN no JSON derruba a leitura da pagina inteira.
    ISNULL(x.matricula, '')         AS matricula,
    ISNULL(x.servico_codigo, '')    AS servico_codigo,
    ISNULL(x.servico_descricao, '') AS servico_descricao
FROM (
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
LEFT JOIN (
    SELECT nf, prestador, matricula, servico_codigo, servico_descricao
    FROM (
        SELECT
            r.v02_Numero_Nota_Fiscal                                             AS nf,
            CAST(r.v12_CPF_CNPJ_Prestador AS NVARCHAR(30)) COLLATE DATABASE_DEFAULT AS prestador,
            -- Matricula vem do CADASTRO do cliente (Fin_Receita -> Cad_Cliente).
            -- A de Fin_Nota estoura em 32 bits quando a matricula e o CPF
            -- (particular) e vira outro numero; so serve de reserva quando a
            -- nota nao tem receita ligada.
            CASE WHEN cli.Matricula > 0 THEN CAST(cli.Matricula AS NVARCHAR(20))
                 WHEN cli.idCliente IS NULL AND n.Matricula > 0 THEN CAST(n.Matricula AS NVARCHAR(20))
            END                                                                  AS matricula,
            CAST(n.ServicoCodigo    AS NVARCHAR(50))  COLLATE DATABASE_DEFAULT   AS servico_codigo,
            CAST(n.ServicoDescricao AS NVARCHAR(250)) COLLATE DATABASE_DEFAULT   AS servico_descricao,
            ROW_NUMBER() OVER (
                PARTITION BY r.v02_Numero_Nota_Fiscal, r.v12_CPF_CNPJ_Prestador
                ORDER BY r.idNotaFiscalRetorno DESC
            ) AS rn
        FROM dbo.Fin_NotaFiscalRetorno r
        JOIN dbo.Fin_Nota n ON n.idNota = r.idNota
        LEFT JOIN dbo.Fin_Receita rc  ON rc.idReceita  = n.idReceita
        LEFT JOIN dbo.Cad_Cliente cli ON cli.idCliente = rc.idCliente
        WHERE r.EmissorNacional = 1
          AND r.v06_Data_Emissao_Nota_Fiscal >= :ini
          AND r.v06_Data_Emissao_Nota_Fiscal <  :fim
    ) z
    WHERE z.rn = 1
) x ON x.nf = t.NotaFiscal AND x.prestador = t.cnpj
WHERE t.data_emissao >= :ini
  AND t.data_emissao <  :fim
ORDER BY t.data_emissao, t.origem, t.NotaFiscal;
