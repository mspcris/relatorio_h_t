SELECT COUNT(*) as cancelamentos
FROM Cad_Cliente c
CROSS JOIN Sis_Empresa emp
WHERE c.Desativado = 0
  AND ISNULL(
        c.DataCancelamentoAuto,
        CASE
            WHEN c.DataCancelamentoANS IS NOT NULL
             AND c.DataReativadoANS IS NULL
                THEN c.DataCancelamentoANS
            WHEN c.DataCancelamentoANS > c.DataReativadoANS
                THEN c.DataCancelamentoANS
        END
      ) >= :ini
  AND ISNULL(
        c.DataCancelamentoAuto,
        CASE
            WHEN c.DataCancelamentoANS IS NOT NULL
             AND c.DataReativadoANS IS NULL
                THEN c.DataCancelamentoANS
            WHEN c.DataCancelamentoANS > c.DataReativadoANS
                THEN c.DataCancelamentoANS
        END
      ) < :fim
  AND c.idEndereco = emp.idEndereco
