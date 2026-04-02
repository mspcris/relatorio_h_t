#!/usr/bin/env python3
import json
import os
import subprocess
from datetime import datetime
from zoneinfo import ZoneInfo


SSH_HOST = os.getenv("HIGIENIZACAO_DB_SSH", "root@217.216.85.81")
DB_NAME = os.getenv("HIGIENIZACAO_DB_NAME", "gestao_higienizacao")
OUT_PATH = os.getenv(
    "HIGIENIZACAO_SNAPSHOT_PATH",
    "/opt/relatorio_h_t/json_consolidado/higienizacao_snapshot.json",
)


def run_sql(sql: str) -> list[str]:
    sql_safe = sql.replace("\\", "\\\\").replace('"', '\\"')
    remote_cmd = f"sudo -u postgres psql -d {DB_NAME} -AtF '|' -c \"{sql_safe}\""
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


def parse_registros(lines: list[str]) -> list[dict]:
    out = []
    for ln in lines:
        p = ln.split("|")
        if len(p) != 10:
            continue
        out.append({
            "posto": p[0].strip() or "(sem posto)",
            "setor": p[1].strip() or "(sem setor)",
            "periodicidade": p[2].strip() or "(sem periodicidade)",
            "funcionario": p[3].strip() or "SEM_FUNCIONARIO",
            "funcionario_id": p[4].strip(),
            "data_hora": p[5].strip(),
            "dia": p[6].strip(),
            "ambiente_id": p[7].strip(),
            "posto_id": p[8].strip(),
            "qr_code_id": p[9].strip(),
        })
    return out


def parse_ambientes(lines: list[str]) -> list[dict]:
    out = []
    for ln in lines:
        p = ln.split("|")
        if len(p) != 4:
            continue
        out.append({
            "posto": p[0].strip() or "(sem posto)",
            "setor": p[1].strip() or "(sem setor)",
            "periodicidade": p[2].strip() or "(sem periodicidade)",
            "ultimo_dia_log": p[3].strip(),
        })
    return out


def main() -> None:
    sql_registros = """
SELECT
  COALESCE(l.name, '(sem posto)') AS posto,
  COALESCE(e.name, '(sem setor)') AS setor,
  COALESCE(e.periodicity, '(sem periodicidade)') AS periodicidade,
  COALESCE(NULLIF(BTRIM(hl.employee_name), ''), hl.employee_id, 'SEM_FUNCIONARIO') AS funcionario,
  COALESCE(hl.employee_id, '') AS funcionario_id,
  TO_CHAR((hl.created_at AT TIME ZONE 'America/Sao_Paulo'), 'YYYY-MM-DD HH24:MI:SS') AS data_hora,
  TO_CHAR((hl.created_at AT TIME ZONE 'America/Sao_Paulo')::date, 'YYYY-MM-DD') AS dia,
  COALESCE(e.id::text, '') AS ambiente_id,
  COALESCE(l.id::text, '') AS posto_id,
  COALESCE(hl.qr_code_id::text, '') AS qr_code_id
FROM public.hygiene_logs hl
LEFT JOIN public.environments e ON e.id = hl.environment_id
LEFT JOIN public.locations l ON l.id = e.location_id
ORDER BY hl.created_at DESC
LIMIT 200000;
"""

    sql_ambientes = """
SELECT
  COALESCE(l.name, '(sem posto)') AS posto,
  COALESCE(e.name, '(sem setor)') AS setor,
  COALESCE(e.periodicity, '(sem periodicidade)') AS periodicidade,
  TO_CHAR(MAX((hl.created_at AT TIME ZONE 'America/Sao_Paulo')::date), 'YYYY-MM-DD') AS ultimo_dia_log
FROM public.environments e
LEFT JOIN public.locations l ON l.id = e.location_id
LEFT JOIN public.hygiene_logs hl ON hl.environment_id = e.id
GROUP BY l.name, e.name, e.periodicity
ORDER BY posto, setor;
"""

    registros = parse_registros(run_sql(sql_registros))
    ambientes = parse_ambientes(run_sql(sql_ambientes))
    now_sp = datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%Y-%m-%d %H:%M:%S %z")

    payload = {
        "updated_at": now_sp,
        "source": "etl_higienizacao_snapshot.py",
        "registros": registros,
        "ambientes": ambientes,
    }

    out_dir = os.path.dirname(OUT_PATH) or "."
    os.makedirs(out_dir, exist_ok=True)
    tmp = OUT_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))
    os.replace(tmp, OUT_PATH)

    print(f"OK snapshot={OUT_PATH} registros={len(registros)} ambientes={len(ambientes)} updated_at={now_sp}")


if __name__ == "__main__":
    main()

