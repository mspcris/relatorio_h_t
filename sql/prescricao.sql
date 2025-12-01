SET NOCOUNT ON;

-- Stage ENF sempre permitido
CREATE TABLE #enf (
  data_consulta   date,
  medico          varchar(200),
  tipo_prescricao varchar(80),
  valor           int
);

INSERT INTO #enf (data_consulta, medico, tipo_prescricao, valor)
SELECT
    CAST(T.DataConsulta AS date)                           AS data_consulta
  , CAST(T.Medico AS varchar(200))                        AS medico
  , CAST('PRESCRICAO INTERNA ENFERMARIA' AS varchar(80))  AS tipo_prescricao
  , CAST(1 AS int)                                        AS valor
FROM dbo.[VW_Cad_ProntuarioPrescricaoEnfermaria] T
WHERE T.desativado = 0
  AND LEN(T.Titulo) > 0
  AND T.[Situação] = 'ativo'
  AND T.Gravado = 1
  AND T.Medico <> 'TESTE - PROFISSIONAL PARA TESTES';

-- Stage GERAL: cria vazia; só popula se houver permissão
CREATE TABLE #geral (
  data_consulta   date,
  medico          varchar(200),
  tipo_prescricao varchar(80),
  valor           int
);

IF HAS_PERMS_BY_NAME(N'dbo.vw_Cad_ProntuarioPrescricao', 'OBJECT', 'SELECT') = 1
BEGIN
  INSERT INTO #geral (data_consulta, medico, tipo_prescricao, valor)
  SELECT
      CAST(T.DataConsulta AS date)          AS data_consulta
    , CAST(T.[Médico] AS varchar(200))      AS medico
    , CAST(T.tipoPrescricao AS varchar(80)) AS tipo_prescricao
    , CAST(1 AS int)                        AS valor
  FROM dbo.[vw_Cad_ProntuarioPrescricao] T
  WHERE T.desativado = 0
    AND T.[Médico] <> 'TESTE - PROFISSIONAL PARA TESTES'
    AND (T.tipoPrescricao IS NOT NULL AND T.tipoPrescricao <> '');
END
-- else: mantém #geral vazia, sem tocar na view bloqueada

-- Saída final (aplica os DOIS parâmetros nomeados só aqui)
SELECT u.data_consulta, u.medico, u.tipo_prescricao, u.valor
FROM (
  SELECT data_consulta, medico, tipo_prescricao, valor FROM #enf
  UNION ALL
  SELECT data_consulta, medico, tipo_prescricao, valor FROM #geral
) AS u
WHERE u.data_consulta >= CAST(:ini AS date)
  AND u.data_consulta <  CAST(:fim AS date);
