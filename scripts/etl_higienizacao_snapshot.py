#!/usr/bin/env python3
# etl_higienizacao_snapshot.py
# ETL: Higienização — gera JSON analítico com registros, duplicidades,
#      cobertura, intervalos e métricas por posto/setor/funcionário.
# Fonte: PostgreSQL em 217.216.85.81 (gestao_higienizacao) via SSH.
# Gera: json_consolidado/higienizacao_snapshot.json
# Cron: 0 * * * * (a cada hora)

import json
import os
import subprocess
import sys
import time
from datetime import datetime
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from etl_meta import ETLMeta

# =========================
# Constantes
# =========================

BASE_DIR   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JSON_DIR   = os.path.join(BASE_DIR, "json_consolidado")
LOG_DIR    = os.path.join(BASE_DIR, "logs")
LOG_FILE   = os.path.join(LOG_DIR, f"export_higienizacao_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
OUT_PATH   = os.path.join(JSON_DIR, "higienizacao_snapshot.json")

SSH_HOST   = os.getenv("HIGIENIZACAO_DB_SSH", "root@217.216.85.81")
DB_NAME    = os.getenv("HIGIENIZACAO_DB_NAME", "gestao_higienizacao")


# =========================
# Logging & Timing
# =========================

class Logger:
    def __init__(self, log_file):
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        self.fh = open(log_file, 'w', encoding='utf-8')

    def write(self, msg: str):
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        line = f"[{ts}] {msg}"
        print(msg, flush=True)
        self.fh.write(line + '\n')
        self.fh.flush()

    def close(self):
        self.fh.close()


class Timer:
    def __init__(self, name: str):
        self.name    = name
        self.elapsed = 0.0

    def __enter__(self):
        self._t0 = time.time()
        return self

    def __exit__(self, *args):
        self.elapsed = time.time() - self._t0

    def fmt(self):
        e = self.elapsed
        if e < 1:    return f"{e*1000:.0f}ms"
        if e < 60:   return f"{e:.1f}s"
        return f"{int(e//60)}m {e%60:.0f}s"


logger = Logger(LOG_FILE)


# =========================
# SSH + SQL
# =========================

def run_sql(sql: str) -> list[str]:
    """Executa SQL no PostgreSQL remoto via SSH, retorna linhas de texto."""
    sql_safe = sql.replace("\\", "\\\\").replace('"', '\\"')
    remote_cmd = f'sudo -u postgres psql -d {DB_NAME} -AtF \'|\' -c "{sql_safe}"'
    p = subprocess.run(
        [
            "ssh",
            "-o", "BatchMode=yes",
            "-o", "StrictHostKeyChecking=no",
            "-o", "UserKnownHostsFile=/dev/null",
            "-o", "ConnectTimeout=10",
            SSH_HOST,
            remote_cmd,
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )
    if p.returncode != 0:
        raise RuntimeError((p.stderr or p.stdout or "falha no ssh/psql").strip())
    return [ln for ln in p.stdout.splitlines() if ln.strip()]


# =========================
# Queries
# =========================

SQL_REGISTROS = """
SELECT
  hl.id,
  COALESCE(l.name, '(sem posto)') AS posto,
  COALESCE(l.id::text, '') AS posto_id,
  COALESCE(e.name, '(sem setor)') AS setor,
  COALESCE(e.id::text, '') AS ambiente_id,
  COALESCE(e.periodicity, '(sem periodicidade)') AS periodicidade,
  hl.employee_id,
  COALESCE(NULLIF(BTRIM(hl.employee_name), ''), hl.employee_id, 'SEM_FUNCIONARIO') AS funcionario,
  TO_CHAR(hl.created_at AT TIME ZONE 'America/Sao_Paulo', 'YYYY-MM-DD HH24:MI:SS') AS data_hora,
  TO_CHAR((hl.created_at AT TIME ZONE 'America/Sao_Paulo')::date, 'YYYY-MM-DD') AS dia,
  CASE WHEN hl.selfie_url IS NOT NULL AND BTRIM(hl.selfie_url) != '' THEN 'sim' ELSE 'nao' END AS tem_selfie,
  COALESCE(hl.qr_code_id::text, '') AS qr_code_id
FROM public.hygiene_logs hl
LEFT JOIN public.environments e ON e.id = hl.environment_id
LEFT JOIN public.locations l ON l.id = e.location_id
ORDER BY hl.created_at DESC
LIMIT 200000
"""

SQL_AMBIENTES = """
SELECT
  COALESCE(l.name, '(sem posto)') AS posto,
  COALESCE(l.id::text, '') AS posto_id,
  e.id AS ambiente_id,
  e.name AS setor,
  e.periodicity AS periodicidade,
  e.active,
  COALESCE(TO_CHAR(MAX(hl.created_at AT TIME ZONE 'America/Sao_Paulo'), 'YYYY-MM-DD HH24:MI:SS'), '') AS ultimo_log,
  COALESCE(TO_CHAR(MAX((hl.created_at AT TIME ZONE 'America/Sao_Paulo')::date), 'YYYY-MM-DD'), '') AS ultimo_dia_log,
  COUNT(hl.id) AS total_logs
FROM public.environments e
LEFT JOIN public.locations l ON l.id = e.location_id
LEFT JOIN public.hygiene_logs hl ON hl.environment_id = e.id
GROUP BY l.name, l.id, e.id, e.name, e.periodicity, e.active
ORDER BY posto, setor
"""

SQL_DUPLICIDADES = """
WITH ordered AS (
  SELECT
    hl.id,
    hl.environment_id,
    e.name AS ambiente,
    l.name AS local,
    hl.employee_id,
    COALESCE(NULLIF(BTRIM(hl.employee_name), ''), hl.employee_id, 'SEM') AS funcionario,
    hl.created_at AT TIME ZONE 'America/Sao_Paulo' AS ts,
    LAG(hl.created_at AT TIME ZONE 'America/Sao_Paulo')
      OVER (PARTITION BY hl.environment_id ORDER BY hl.created_at) AS ts_anterior,
    LAG(hl.employee_id)
      OVER (PARTITION BY hl.environment_id ORDER BY hl.created_at) AS emp_anterior,
    LAG(COALESCE(NULLIF(BTRIM(hl.employee_name), ''), hl.employee_id, 'SEM'))
      OVER (PARTITION BY hl.environment_id ORDER BY hl.created_at) AS func_anterior
  FROM public.hygiene_logs hl
  LEFT JOIN public.environments e ON e.id = hl.environment_id
  LEFT JOIN public.locations l ON l.id = e.location_id
)
SELECT
  COALESCE(local, '(sem posto)') AS posto,
  COALESCE(ambiente, '(sem setor)') AS setor,
  func_anterior AS funcionario_1,
  emp_anterior AS emp_id_1,
  TO_CHAR(ts_anterior, 'YYYY-MM-DD HH24:MI') AS hora_1,
  funcionario AS funcionario_2,
  employee_id AS emp_id_2,
  TO_CHAR(ts, 'YYYY-MM-DD HH24:MI') AS hora_2,
  TO_CHAR(ts::date, 'YYYY-MM-DD') AS dia,
  EXTRACT(EPOCH FROM (ts - ts_anterior))::int / 60 AS minutos,
  CASE WHEN employee_id = emp_anterior THEN 'mesma_pessoa' ELSE 'pessoas_diferentes' END AS tipo
FROM ordered
WHERE ts_anterior IS NOT NULL
  AND EXTRACT(EPOCH FROM (ts - ts_anterior)) < 3600
ORDER BY ts DESC
"""

SQL_LOCATIONS = """
SELECT id, name, active FROM public.locations ORDER BY name
"""


# =========================
# Parsers
# =========================

def parse_registros(lines: list[str]) -> list[dict]:
    out = []
    for ln in lines:
        p = ln.split("|")
        if len(p) != 12:
            continue
        out.append({
            "id":             p[0].strip(),
            "posto":          p[1].strip() or "(sem posto)",
            "posto_id":       p[2].strip(),
            "setor":          p[3].strip() or "(sem setor)",
            "ambiente_id":    p[4].strip(),
            "periodicidade":  p[5].strip() or "(sem periodicidade)",
            "funcionario_id": p[6].strip(),
            "funcionario":    p[7].strip() or "SEM_FUNCIONARIO",
            "data_hora":      p[8].strip(),
            "dia":            p[9].strip(),
            "tem_selfie":     p[10].strip() == "sim",
            "qr_code_id":     p[11].strip(),
        })
    return out


def parse_ambientes(lines: list[str]) -> list[dict]:
    out = []
    for ln in lines:
        p = ln.split("|")
        if len(p) != 9:
            continue
        out.append({
            "posto":          p[0].strip() or "(sem posto)",
            "posto_id":       p[1].strip(),
            "ambiente_id":    p[2].strip(),
            "setor":          p[3].strip() or "(sem setor)",
            "periodicidade":  p[4].strip() or "(sem periodicidade)",
            "ativo":          p[5].strip() == "t",
            "ultimo_log":     p[6].strip(),
            "ultimo_dia_log": p[7].strip(),
            "total_logs":     int(p[8].strip() or "0"),
        })
    return out


def parse_duplicidades(lines: list[str]) -> list[dict]:
    out = []
    for ln in lines:
        p = ln.split("|")
        if len(p) != 11:
            continue
        out.append({
            "posto":          p[0].strip(),
            "setor":          p[1].strip(),
            "funcionario_1":  p[2].strip(),
            "emp_id_1":       p[3].strip(),
            "hora_1":         p[4].strip(),
            "funcionario_2":  p[5].strip(),
            "emp_id_2":       p[6].strip(),
            "hora_2":         p[7].strip(),
            "dia":            p[8].strip(),
            "minutos":        int(p[9].strip() or "0"),
            "tipo":           p[10].strip(),
        })
    return out


def parse_locations(lines: list[str]) -> list[dict]:
    out = []
    for ln in lines:
        p = ln.split("|")
        if len(p) != 3:
            continue
        out.append({
            "id":     p[0].strip(),
            "nome":   p[1].strip(),
            "ativo":  p[2].strip() == "t",
        })
    return out


# =========================
# Main
# =========================

def main():
    meta = ETLMeta('etl_higienizacao_snapshot', 'json_consolidado')

    os.makedirs(JSON_DIR, exist_ok=True)
    t0 = time.time()

    logger.write("=" * 60)
    logger.write("ETL HIGIENIZAÇÃO — Snapshot Analítico")
    logger.write("=" * 60)
    logger.write(f"  SSH: {SSH_HOST}")
    logger.write(f"  DB:  {DB_NAME}")
    logger.write(f"  Out: {OUT_PATH}")
    logger.write("")

    # 1) Registros
    with Timer("registros") as t:
        registros = parse_registros(run_sql(SQL_REGISTROS))
    logger.write(f"  Registros:     {len(registros):>6d} | {t.fmt()}")

    # 2) Ambientes (com último log e contagem)
    with Timer("ambientes") as t:
        ambientes = parse_ambientes(run_sql(SQL_AMBIENTES))
    logger.write(f"  Ambientes:     {len(ambientes):>6d} | {t.fmt()}")

    # 3) Duplicidades (intervalo < 60min no mesmo ambiente)
    with Timer("duplicidades") as t:
        duplicidades = parse_duplicidades(run_sql(SQL_DUPLICIDADES))
    logger.write(f"  Duplicidades:  {len(duplicidades):>6d} | {t.fmt()}")

    # 4) Locations
    with Timer("locations") as t:
        locations = parse_locations(run_sql(SQL_LOCATIONS))
    logger.write(f"  Locations:     {len(locations):>6d} | {t.fmt()}")

    # Timestamp
    now_sp = datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%Y-%m-%d %H:%M:%S %z")

    # Payload
    payload = {
        "updated_at": now_sp,
        "source":     "etl_higienizacao_snapshot.py",
        "registros":  registros,
        "ambientes":  ambientes,
        "duplicidades": duplicidades,
        "locations":  locations,
    }

    # Salvar
    with Timer("salvar") as t:
        tmp = OUT_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
        os.replace(tmp, OUT_PATH)
    logger.write(f"  Salvar JSON:   {t.fmt()}")

    meta.ok('geral')
    meta.save()

    elapsed = time.time() - t0
    logger.write("")
    logger.write(f"  Snapshot salvo: {OUT_PATH}")
    logger.write(f"  Tempo total:    {elapsed:.1f}s")
    logger.write(f"  Log:            {LOG_FILE}")
    logger.close()


if __name__ == "__main__":
    main()
