#!/usr/bin/env bash
set -euo pipefail

# Ambiente mínimo p/ cron
export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
export LANG=pt_BR.UTF-8
export LC_ALL=pt_BR.UTF-8

cd /opt/relatorio_h_t
mkdir -p logs

# Auditoria mínima de runtime
echo "$(date -Is) user=$(whoami) py=$(command -v python3) ver=$(python3 -V 2>&1)" >> /var/log/relatorio_h_t/job_audit.log

# Evita concorrência
exec 9> /opt/relatorio_h_t/.export_notas_rps.lock
flock -n 9 || { echo "$(date -Is) já existe execução em andamento" >> logs/export_notas_rps.log; exit 0; }

# Ativa venv
# shellcheck disable=SC1091
source .venv/bin/activate

# Run + log
python3 export_notas_rps.py >> logs/export_notas_rps.log 2>&1

# Publica JSONs no www (mesma estratégia do governança)
# (se sync_www.sh já copia json_notas_rps, ótimo; senão, ajuste no script)
 /opt/relatorio_h_t/sync_www.sh >> /var/log/relatorio_h_t/sync_www.log 2>&1
