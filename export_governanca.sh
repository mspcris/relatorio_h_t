#!/usr/bin/env bash
set -euo pipefail
cd /opt/relatorio_h_t

# venv
source /opt/relatorio_h_t/.venv/bin/activate

# auditoria mínima de runtime
echo "$(date -Is) user=$(whoami) py=$(command -v python) ver=$(python -V 2>&1)" >> /var/log/relatorio_h_t/job_audit.log


#!/usr/bin/env bash
set -euo pipefail

# Ambiente mínimo p/ cron
export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
export LANG=pt_BR.UTF-8
export LC_ALL=pt_BR.UTF-8

cd /opt/relatorio_h_t
mkdir -p logs

# Evita concorrência
exec 9> /opt/relatorio_h_t/.export_governanca.lock
flock -n 9 || { echo "$(date -Is) já existe execução em andamento"; exit 0; }

# ===== Opção A: ativando o venv =====
# shellcheck disable=SC1091
source .venv/bin/activate

# Run + log
python3 export_governanca.py
# ===== Opção B (equivalente): sem activate =====
# /opt/relatorio_h_t/.venv/bin/python3 export_governanca.py >> logs/export_governanca.log 2>&1
/opt/relatorio_h_t/sync_www.sh >>/var/log/relatorio_h_t/sync_www.log 2>&1
