-- Contabilidade - Notas. Uma linha por nota emitida, com o maximo de dados da
-- nota. O conjunto de linhas e o MESMO das tres views do ERP (as mesmas do
-- KPI Notas x RPS, para os numeros baterem entre as telas); o resto entra por
-- LEFT JOIN num derived table com UMA linha por numero de nota + CNPJ do
-- prestador (o mesmo numero pode ter mais de um retorno; fica o mais recente).
--
-- "Nome CAMIM do servico" (servico_camim):
--   * nota com lancamento (Fin_Receita.idLancamento): os servicos do
--     lancamento (Cad_LancamentoServico -> Cad_Servico.Servico), so os da
--     classe da nota quando ela tem classe (a nota sai por classe), e todos
--     quando o filtro por classe nao acha nenhum;
--   * nota sem lancamento (mensalidade, taxa, acordo): fica vazio e a tela
--     usa o tipo de receita (Fin_ContaTipo.Tipo).
--
-- Matricula vem de Cad_Cliente (a de Fin_Nota estoura em 32 bits quando a
-- matricula e o CPF).
--
-- ATENCAO: nada de dois-pontos seguido de palavra fora dos binds, nem em
-- comentario (o text() do SQLAlchemy le como parametro). ISNULL em todo texto:
-- NULL vira NaN no pandas e NaN no JSON derruba a leitura.
SELECT
    t.origem, t.NotaFiscal, t.Empresa, t.cnpj, t.data_emissao, t.Cliente, t.cpf,
    t.valor, t.Cancelada,
    ISNULL(x.hora, '')            AS hora,
    ISNULL(x.data_canc, '')       AS data_canc,
    ISNULL(x.cod_canc, '')        AS cod_canc,
    ISNULL(x.cod_verif, '')       AS cod_verif,
    ISNULL(x.rps, '')             AS rps,
    ISNULL(x.serie, '')           AS serie,
    ISNULL(x.chave, '')           AS chave,
    ISNULL(x.cidade, '')          AS cidade,
    ISNULL(x.uf, '')              AS uf,
    ISNULL(x.matricula, '')       AS matricula,
    ISNULL(x.serv_cod, '')        AS serv_cod,
    ISNULL(x.serv_desc, '')       AS serv_desc,
    ISNULL(x.trib_nac, '')        AS trib_nac,
    ISNULL(x.nbs, '')             AS nbs,
    ISNULL(x.tipo_receita, '')    AS tipo_receita,
    ISNULL(x.classe_camim, '')    AS classe_camim,
    ISNULL(x.serv_camim, '')      AS serv_camim,
    ISNULL(x.discriminacao, '')   AS discriminacao,
    ISNULL(x.valor_iss, 0)        AS valor_iss,
    ISNULL(x.id_receita, 0)       AS id_receita,
    ISNULL(x.id_lancamento, 0)    AS id_lancamento
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
    SELECT z.*,
        COALESCE(
            STUFF((SELECT '; ' + LTRIM(RTRIM(s.Servico))
                          + CASE WHEN ls.Quantidade > 1 THEN ' (x' + CAST(ls.Quantidade AS VARCHAR(6)) + ')' ELSE '' END
                   FROM dbo.Cad_LancamentoServico ls
                   JOIN dbo.Cad_Servico s ON s.idServico = ls.idServico
                   WHERE ls.idLancamento = z.id_lancamento AND z.id_lancamento > 0
                     AND ISNULL(ls.Desistencia, 0) = 0
                     AND z.id_classe > 0 AND s.idClasse = z.id_classe
                   ORDER BY ls.idLancamentoServico
                   FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, ''),
            STUFF((SELECT '; ' + LTRIM(RTRIM(s.Servico))
                          + CASE WHEN ls.Quantidade > 1 THEN ' (x' + CAST(ls.Quantidade AS VARCHAR(6)) + ')' ELSE '' END
                   FROM dbo.Cad_LancamentoServico ls
                   JOIN dbo.Cad_Servico s ON s.idServico = ls.idServico
                   WHERE ls.idLancamento = z.id_lancamento AND z.id_lancamento > 0
                     AND ISNULL(ls.Desistencia, 0) = 0
                   ORDER BY ls.idLancamentoServico
                   FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '')
        ) AS serv_camim
    FROM (
        SELECT
            r.v02_Numero_Nota_Fiscal                                               AS nf,
            CAST(r.v12_CPF_CNPJ_Prestador AS NVARCHAR(30)) COLLATE DATABASE_DEFAULT AS prestador,
            CONVERT(VARCHAR(5), r.v06_Data_Emissao_Nota_Fiscal, 108)               AS hora,
            CONVERT(VARCHAR(10), n.DataCancelamento, 23)                           AS data_canc,
            CAST(n.CodigoCancelamento AS NVARCHAR(50)) COLLATE DATABASE_DEFAULT    AS cod_canc,
            CAST(r.v05_Codigo_Verificao AS NVARCHAR(20)) COLLATE DATABASE_DEFAULT  AS cod_verif,
            CAST(r.V09_Numero_RPS AS NVARCHAR(20)) COLLATE DATABASE_DEFAULT        AS rps,
            CAST(r.v08_Serie_RPS AS NVARCHAR(10)) COLLATE DATABASE_DEFAULT         AS serie,
            CAST(r.xChaveNFSe AS NVARCHAR(100)) COLLATE DATABASE_DEFAULT           AS chave,
            CAST(n.Cidade AS NVARCHAR(60)) COLLATE DATABASE_DEFAULT                AS cidade,
            CAST(n.Estado AS NVARCHAR(2)) COLLATE DATABASE_DEFAULT                 AS uf,
            CASE WHEN cli.Matricula > 0 THEN CAST(cli.Matricula AS NVARCHAR(20))
                 WHEN cli.idCliente IS NULL AND n.Matricula > 0 THEN CAST(n.Matricula AS NVARCHAR(20))
            END                                                                    AS matricula,
            CAST(n.ServicoCodigo AS NVARCHAR(50)) COLLATE DATABASE_DEFAULT         AS serv_cod,
            CAST(n.ServicoDescricao AS NVARCHAR(250)) COLLATE DATABASE_DEFAULT     AS serv_desc,
            CAST(n.CodigoTributacaoNacional AS NVARCHAR(20)) COLLATE DATABASE_DEFAULT AS trib_nac,
            CAST(n.CodigoNBS AS NVARCHAR(20)) COLLATE DATABASE_DEFAULT             AS nbs,
            CAST(ct.Tipo AS NVARCHAR(100)) COLLATE DATABASE_DEFAULT                AS tipo_receita,
            CAST(cl.Classe AS NVARCHAR(100)) COLLATE DATABASE_DEFAULT              AS classe_camim,
            CAST(n.ServicoComentario AS NVARCHAR(200)) COLLATE DATABASE_DEFAULT    AS discriminacao,
            CAST(n.ValorISS AS DECIMAL(18,2))                                      AS valor_iss,
            n.idReceita                                                            AS id_receita,
            ISNULL(rc.idLancamento, 0)                                             AS id_lancamento,
            ISNULL(n.idClasse, 0)                                                  AS id_classe,
            ROW_NUMBER() OVER (
                PARTITION BY r.v02_Numero_Nota_Fiscal, r.v12_CPF_CNPJ_Prestador
                ORDER BY r.idNotaFiscalRetorno DESC
            ) AS rn
        FROM dbo.Fin_NotaFiscalRetorno r
        JOIN dbo.Fin_Nota n ON n.idNota = r.idNota
        LEFT JOIN dbo.Fin_Receita rc       ON rc.idReceita  = n.idReceita
        LEFT JOIN dbo.Cad_Cliente cli      ON cli.idCliente = rc.idCliente
        LEFT JOIN dbo.Fin_ContaTipo ct     ON ct.idContaTipo = rc.idContaTipo
        LEFT JOIN dbo.Cad_ServicoClasse cl ON cl.idClasse    = n.idClasse
        WHERE r.EmissorNacional = 1
          AND r.v06_Data_Emissao_Nota_Fiscal >= :ini
          AND r.v06_Data_Emissao_Nota_Fiscal <  :fim
    ) z
    WHERE z.rn = 1
) x ON x.nf = t.NotaFiscal AND x.prestador = t.cnpj
WHERE t.data_emissao >= :ini
  AND t.data_emissao <  :fim
ORDER BY t.data_emissao, t.origem, t.NotaFiscal;
