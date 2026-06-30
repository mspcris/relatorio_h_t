-- Objetivo (teto mensal) de emissão de NF por CNPJ/categoria do posto.
-- sis_empresa tem 1 linha por posto; MAX() é só defensivo.
-- Laboratorio costuma vir NULL (só existe em Campinho).
SELECT
    MAX(emp.ValorMaximoMensalNFcamim)       AS objetivo_camim,
    MAX(emp.ValorMaximoMensalNFclinica)     AS objetivo_clinica,
    MAX(emp.ValorMaximoMensalNFsdm)         AS objetivo_sdm,
    MAX(emp.ValorMaximoMensalNFlaboratorio) AS objetivo_laboratorio
FROM dbo.sis_empresa emp;
