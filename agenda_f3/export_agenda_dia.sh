#!/bin/bash
# Wrapper de cron pra export_agenda_dia.py (agenda_f3).
#
# Roda a cada 30 min via /etc/cron.d/agenda_f3 (deployado pelo workflow).
# Usa flock pra garantir que duas execuções não rodem simultaneamente
# (queries paralelas contra 13 SQL Server CAMIM podem ficar lentas se
# se sobrepuserem).

set -euo pipefail

BASE_DIR="/opt/agenda_f3"
LOCK_FILE="/var/lock/agenda_f3_etl.lock"
PYTHON="${BASE_DIR}/venv/bin/python"
SCRIPT="${BASE_DIR}/export_agenda_dia.py"

# flock -n: sai imediatamente se o lock estiver ocupado (não enfileira).
exec flock -n "${LOCK_FILE}" "${PYTHON}" "${SCRIPT}"
