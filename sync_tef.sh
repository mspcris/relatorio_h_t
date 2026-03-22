#!/usr/bin/env bash
set -euo pipefail

# Ambiente mínimo p/ cron
export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
export LANG=pt_BR.UTF-8
export LC_ALL=pt_BR.UTF-8

cd /opt/relatorio_h_t
mkdir -p logs
mkdir -p /var/log/relatorio_h_t

# Auditoria mínima de runtime
echo "$(date -Is) user=$(whoami) py=$(command -v python3) ver=$(python3 -V 2>&1)" \
  >> /var/log/relatorio_h_t/job_audit.log

# Evita concorrência
exec 9> /opt/relatorio_h_t/.sync_tef.lock
flock -n 9 || { echo "$(date -Is) já existe execução em andamento"; exit 0; }

# shellcheck disable=SC1091
source .venv/bin/activate

python3 sync_tef.py "$@"
