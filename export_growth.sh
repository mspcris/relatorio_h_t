#!/usr/bin/env bash
set -euo pipefail

export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
export LANG=pt_BR.UTF-8
export LC_ALL=pt_BR.UTF-8

cd /opt/relatorio_h_t
mkdir -p logs

echo "$(date -Is) user=$(whoami) py=$(command -v python3) ver=$(python3 -V 2>&1)" >> /var/log/relatorio_h_t/job_audit.log

exec 9> /opt/relatorio_h_t/.export_growth.lock
flock -n 9 || { echo "$(date -Is) já existe execução em andamento" >> logs/export_growth.log; exit 0; }

source .venv/bin/activate

python3 export_growth.py >> logs/export_growth.log 2>&1

/opt/relatorio_h_t/sync_www.sh >> /var/log/relatorio_h_t/sync_www.log 2>&1
