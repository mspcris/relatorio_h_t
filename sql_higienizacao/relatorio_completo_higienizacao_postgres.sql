-- Relatorio completo de higienizacao (PostgreSQL)
-- Banco: gestao_higienizacao
-- Execute com:
--   sudo -u postgres psql -d gestao_higienizacao -f sql_higienizacao/relatorio_completo_higienizacao_postgres.sql
--
-- Ajuste o periodo na CTE "params" conforme necessario.

DROP TABLE IF EXISTS tmp_higienizacao_base;

CREATE TEMP TABLE tmp_higienizacao_base AS
WITH params AS (
  SELECT
    CAST('2026-03-01 00:00:00-03' AS timestamptz) AS dt_ini,
    CAST('2026-04-02 00:00:00-03' AS timestamptz) AS dt_fim
)
SELECT
  hl.id AS log_id,
  hl.created_at,
  (hl.created_at AT TIME ZONE 'America/Sao_Paulo') AS created_at_sp,
  (hl.created_at AT TIME ZONE 'America/Sao_Paulo')::date AS dia_sp,
  hl.environment_id,
  e.name AS ambiente,
  e.periodicity AS periodicidade,
  e.active AS ambiente_ativo,
  e.location_id,
  COALESCE(l.name, '(sem local)') AS local,
  hl.employee_id,
  COALESCE(NULLIF(BTRIM(hl.employee_name), ''), hl.employee_id, 'SEM_NOME') AS colaborador,
  hl.qr_code_id,
  q.status AS qr_status,
  q.deactivated_at AS qr_desativado_em,
  CASE
    WHEN hl.selfie_url IS NULL OR BTRIM(hl.selfie_url) = '' THEN false
    ELSE true
  END AS tem_selfie
FROM public.hygiene_logs hl
LEFT JOIN public.environments e ON e.id = hl.environment_id
LEFT JOIN public.locations l ON l.id = e.location_id
LEFT JOIN public.qr_codes q ON q.id = hl.qr_code_id
CROSS JOIN params p
WHERE hl.created_at >= p.dt_ini
  AND hl.created_at < p.dt_fim;

-- 1) Resumo geral
SELECT
  'resumo_geral' AS secao,
  COUNT(*) AS total_logs,
  MIN(created_at) AS primeiro_log,
  MAX(created_at) AS ultimo_log,
  COUNT(DISTINCT dia_sp) AS dias_com_registro,
  ROUND(COUNT(*)::numeric / NULLIF(COUNT(DISTINCT dia_sp), 0), 2) AS media_logs_por_dia_ativo,
  COUNT(DISTINCT employee_id) AS colaboradores_distintos,
  COUNT(DISTINCT environment_id) AS ambientes_com_log,
  COUNT(DISTINCT qr_code_id) AS qrcodes_utilizados,
  COUNT(*) FILTER (WHERE tem_selfie) AS logs_com_selfie,
  ROUND(100.0 * COUNT(*) FILTER (WHERE tem_selfie) / NULLIF(COUNT(*), 0), 2) AS pct_logs_com_selfie
FROM tmp_higienizacao_base;

-- 2) Serie diaria
SELECT
  'serie_diaria' AS secao,
  dia_sp,
  COUNT(*) AS total_logs,
  COUNT(DISTINCT environment_id) AS ambientes,
  COUNT(DISTINCT employee_id) AS colaboradores
FROM tmp_higienizacao_base
GROUP BY dia_sp
ORDER BY dia_sp;

-- 3) Cobertura por local
SELECT
  'cobertura_local' AS secao,
  l.name AS local,
  COUNT(e.id) AS total_ambientes,
  COUNT(e.id) FILTER (WHERE h.ultimo_dia IS NOT NULL) AS ambientes_com_log,
  COUNT(e.id) FILTER (WHERE h.ultimo_dia IS NULL) AS ambientes_sem_log
FROM public.locations l
LEFT JOIN public.environments e ON e.location_id = l.id
LEFT JOIN (
  SELECT environment_id, MAX(dia_sp) AS ultimo_dia
  FROM tmp_higienizacao_base
  GROUP BY environment_id
) h ON h.environment_id = e.id
GROUP BY l.name
ORDER BY total_ambientes DESC, local;

-- 4) Ranking por colaborador
SELECT
  'ranking_colaborador' AS secao,
  colaborador,
  employee_id,
  COUNT(*) AS total_logs,
  MIN(created_at) AS primeiro_log,
  MAX(created_at) AS ultimo_log
FROM tmp_higienizacao_base
GROUP BY colaborador, employee_id
ORDER BY total_logs DESC, colaborador;

-- 5) Ambientes com maior atividade
SELECT
  'atividade_ambiente' AS secao,
  local,
  ambiente,
  periodicidade,
  COUNT(*) AS total_logs,
  MIN(created_at) AS primeiro_log,
  MAX(created_at) AS ultimo_log
FROM tmp_higienizacao_base
GROUP BY local, ambiente, periodicidade
ORDER BY total_logs DESC, local, ambiente;

-- 6) Aderencia por periodicidade (com base na data atual)
WITH env_status AS (
  SELECT
    e.id,
    e.periodicity AS periodicidade,
    MAX(t.dia_sp) AS ultimo_dia_log
  FROM public.environments e
  LEFT JOIN tmp_higienizacao_base t ON t.environment_id = e.id
  GROUP BY e.id, e.periodicity
)
SELECT
  'aderencia_periodicidade' AS secao,
  periodicidade,
  COUNT(*) AS total_ambientes,
  COUNT(*) FILTER (WHERE ultimo_dia_log IS NULL) AS sem_registro,
  COUNT(*) FILTER (
    WHERE periodicidade = 'diaria'
      AND ultimo_dia_log = (NOW() AT TIME ZONE 'America/Sao_Paulo')::date
  ) AS diaria_em_dia,
  COUNT(*) FILTER (
    WHERE periodicidade = 'diaria'
      AND (ultimo_dia_log IS NULL OR ultimo_dia_log < (NOW() AT TIME ZONE 'America/Sao_Paulo')::date)
  ) AS diaria_atrasada,
  COUNT(*) FILTER (
    WHERE periodicidade = 'semanal'
      AND ultimo_dia_log >= ((NOW() AT TIME ZONE 'America/Sao_Paulo')::date - 6)
  ) AS semanal_em_dia,
  COUNT(*) FILTER (
    WHERE periodicidade = 'semanal'
      AND (ultimo_dia_log IS NULL OR ultimo_dia_log < ((NOW() AT TIME ZONE 'America/Sao_Paulo')::date - 6))
  ) AS semanal_atrasada
FROM env_status
GROUP BY periodicidade
ORDER BY periodicidade;

-- 7) Ambientes pendentes/atrasados
WITH env_status AS (
  SELECT
    e.id,
    COALESCE(l.name, '(sem local)') AS local,
    e.name AS ambiente,
    e.periodicity AS periodicidade,
    MAX(t.dia_sp) AS ultimo_dia_log
  FROM public.environments e
  LEFT JOIN public.locations l ON l.id = e.location_id
  LEFT JOIN tmp_higienizacao_base t ON t.environment_id = e.id
  GROUP BY e.id, l.name, e.name, e.periodicity
)
SELECT
  'pendencias' AS secao,
  local,
  ambiente,
  periodicidade,
  COALESCE(TO_CHAR(ultimo_dia_log, 'YYYY-MM-DD'), 'SEM_LOG') AS ultimo_log_dia
FROM env_status
WHERE ultimo_dia_log IS NULL
   OR (periodicidade = 'diaria' AND ultimo_dia_log < (NOW() AT TIME ZONE 'America/Sao_Paulo')::date)
   OR (periodicidade = 'semanal' AND ultimo_dia_log < ((NOW() AT TIME ZONE 'America/Sao_Paulo')::date - 6))
ORDER BY local, periodicidade, ambiente;

-- 8) QRCodes sem uso no periodo
SELECT
  'qrcode_sem_uso' AS secao,
  COALESCE(l.name, '(sem local)') AS local,
  e.name AS ambiente,
  q.id AS qr_code_id,
  q.status AS qr_status,
  q.deactivated_at AS qr_desativado_em
FROM public.qr_codes q
LEFT JOIN public.environments e ON e.id = q.environment_id
LEFT JOIN public.locations l ON l.id = e.location_id
LEFT JOIN (
  SELECT DISTINCT qr_code_id
  FROM tmp_higienizacao_base
) used ON used.qr_code_id = q.id
WHERE used.qr_code_id IS NULL
ORDER BY local, ambiente, qr_code_id;

-- 9) Ultimos registros detalhados
SELECT
  'ultimos_registros' AS secao,
  log_id,
  created_at_sp AS data_hora_sp,
  local,
  ambiente,
  periodicidade,
  colaborador,
  employee_id,
  qr_code_id,
  CASE WHEN tem_selfie THEN 'com_selfie' ELSE 'sem_selfie' END AS evidencia
FROM tmp_higienizacao_base
ORDER BY created_at DESC
LIMIT 50;
