#!/usr/bin/env bash
set -euo pipefail

# Ambiente mínimo p/ cron
export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
export LANG=pt_BR.UTF-8
export LC_ALL=pt_BR.UTF-8

cd /opt/relatorio_h_t
mkdir -p /var/log/relatorio_h_t

# Evita concorrência (lock próprio + flock do cron)
exec 9> /opt/relatorio_h_t/locks/custo_medico_ctrlq.lock 2>/dev/null || true
flock -n 9 || { echo "$(date -Is) job=custo_medico_ctrlq já em execução"; exit 0; }

# shellcheck disable=SC1091
source .venv/bin/activate
python3 export_custo_medico_ctrlq.py
