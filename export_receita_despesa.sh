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
echo "$(date -Is) job=export_receita_despesa user=$(whoami) py=$(command -v python3) ver=$(python3 -V 2>&1)" \
  >> /var/log/relatorio_h_t/job_audit.log

# Evita concorrência
exec 9> /opt/relatorio_h_t/.export_receita_despesa.lock
flock -n 9 || { echo "$(date -Is) já existe execução em andamento (export_receita_despesa)"; exit 0; }

# Ativa o venv
# shellcheck disable=SC1091
source .venv/bin/activate

# Execução principal + log próprio
python3 export_receita_despesa.py >> logs/export_receita_despesa.log 2>&1

# Sync de estáticos / site
/opt/relatorio_h_t/sync_www.sh >> /var/log/relatorio_h_t/sync_www.log 2>&1
