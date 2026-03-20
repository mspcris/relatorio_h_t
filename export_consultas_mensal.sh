#!/usr/bin/env bash
set -euo pipefail

# Ambiente mínimo p/ cron
export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
export LANG=pt_BR.UTF-8
export LC_ALL=pt_BR.UTF-8

cd /opt/relatorio_h_t
mkdir -p logs

# Evita concorrência
exec 9> /opt/relatorio_h_t/.export_consultas_mensal.lock
flock -n 9 || { echo "$(date -Is) já existe execução em andamento"; exit 0; }

# venv
# shellcheck disable=SC1091
source .venv/bin/activate

# auditoria mínima de runtime (mesmo padrão)
echo "$(date -Is) user=$(whoami) py=$(command -v python) ver=$(python -V 2>&1)" >> /var/log/relatorio_h_t/job_audit.log

# Run + log
python3 export_consultas_mensal_json.py >> logs/export_consultas_mensal_json.log 2>&1

# Sync para www (mesmo padrão do job)
 /opt/relatorio_h_t/sync_www.sh >> /var/log/relatorio_h_t/sync_www.log 2>&1
