-- fidelizacao_cliente.sql
-- Procedure wrapper para extrair dados de fidelização de clientes
-- Retorna: AnoAdmissao, MesAdmissao, QuantidadeAdmissao, AnoReferencia, MesReferencia, QuantidadeRecebida, PercentualFidelizacao

EXEC PC_Fin_Fidelizacao @database = @ini
