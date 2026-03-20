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
echo "$(date -Is) job=export_ctrlq_relatorio user=$(whoami) py=$(command -v python3) ver=$(python3 -V 2>&1)" \
  >> /var/log/relatorio_h_t/job_audit.log

# Evita concorrência
exec 9> /opt/relatorio_h_t/.export_ctrlq_relatorio.lock
flock -n 9 || { echo "$(date -Is) job=export_ctrlq_relatorio já existe execução em andamento" >> /var/log/relatorio_h_t/cron_export_ctrlq_relatorio.log; exit 0; }

# venv
# shellcheck disable=SC1091
source /opt/relatorio_h_t/.venv/bin/activate

# Run + log
/opt/relatorio_h_t/.venv/bin/python3 /opt/relatorio_h_t/ctrlq_export_relatorio.py >> /var/log/relatorio_h_t/cron_export_ctrlq_relatorio.log 2>&1

# Sync do site (se sua publicação depende disso)
# (mantenha se o CTRLQ precisa aparecer no www automaticamente)
if [ -x /opt/relatorio_h_t/sync_www.sh ]; then
  /opt/relatorio_h_t/sync_www.sh >> /var/log/relatorio_h_t/sync_www.log 2>&1
fi
