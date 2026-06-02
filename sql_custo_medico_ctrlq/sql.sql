-- Custo médico por dia da semana, por médico/especialidade ATIVA.
-- Usada pelo botão "Custo Médico no Ctrl-Q" (KPI Custo Médico). Roda em TODOS
-- os postos (export_custo_medico_ctrlq.py) e gera JSON consolidado pra busca
-- por nome / CRM (ConselhoNumero) / CPF.
--
-- cad_especialidade: 1 linha por médico+especialidade, com o custo por dia da
-- semana (ValorCusto<Dia>). cad_medico: dados do médico (Nome, CRM, CPF).
-- desativado=0 = especialidade ativa.
SELECT
    m.Nome                          AS medico,
    LTRIM(RTRIM(m.ConselhoProfissional)) AS conselho,
    LTRIM(RTRIM(m.ConselhoNumero))  AS crm,
    LTRIM(RTRIM(m.CPF))             AS cpf,
    LTRIM(RTRIM(e.Especialidade))   AS especialidade,
    e.ValorCustoSegunda,
    e.ValorCustoTerca,
    e.ValorCustoQuarta,
    e.ValorCustoQuinta,
    e.ValorCustoSexta,
    e.ValorCustoSabado,
    e.ValorCustoDomingo
FROM cad_especialidade e
JOIN cad_medico m ON m.idmedico = e.idmedico
WHERE e.desativado = 0
ORDER BY m.Nome, especialidade;
