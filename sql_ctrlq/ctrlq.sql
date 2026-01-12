WITH x AS (
  SELECT
    medico, crm, Especialidade,
    CertificadoDigital, PermitirAgendamentoquenuncaconsultou, PessoaJuridica, NumeroRQE, temporario,
    ROW_NUMBER() OVER (PARTITION BY medico, crm ORDER BY Especialidade) AS rn
  FROM vw_Cad_Especialidade
  WHERE Desativado = 0
    AND medico NOT LIKE '%sede%'
    AND medico NOT LIKE '%agendamento%'
    AND medico NOT LIKE '%teste%'
    AND especialidade NOT IN 
    (
    'enfermeiro',
    'eletrocardiograma', 
    'espirometria', 
    'biologista',
    'mamografia', 
    'tecnico em radiologia',
    'HOLTER 24 HRS',
    'mapa'
    )
)
SELECT
  medico, crm, especialidade, temporario,
  certificadodigital, permitiragendamentoquenuncaconsultou, pessoajuridica, IIF(numerorqe IS NULL, 0, 1) AS rqe
FROM x
WHERE rn = 1 
order by Especialidade ASC
