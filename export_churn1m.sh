#!/usr/bin/env bash
set -euo pipefail

# ETL do KPI "Churn de um mês" (kpi_churn_1m.html).
# Parte dos CSVs de venda (regra única, export_vendas.py) e lê, nos 13 postos,
# a mensalidade de referência do mês seguinte de quem fez o plano.
# Só SELECT nos SQL Servers — não escreve em posto nenhum.
# Saída: json_vendas/churn1m.json
#
# Chamado pelo cron via /bin/bash (o rsync do deploy chega sem bit de
# execução — armadilha documentada no CLAUDE.md).

export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
export LANG=pt_BR.UTF-8
export LC_ALL=pt_BR.UTF-8

cd /opt/relatorio_h_t
mkdir -p logs

exec 9> /opt/relatorio_h_t/.export_churn1m.lock
flock -n 9 || { echo "$(date -Is) já existe execução em andamento"; exit 0; }

# shellcheck disable=SC1091
source .venv/bin/activate

echo "$(date -Is) user=$(whoami) py=$(command -v python)" >> /var/log/relatorio_h_t/job_audit.log

python3 export_churn1m.py "$@" >> logs/export_churn1m.log 2>&1

/opt/relatorio_h_t/sync_www.sh >> /var/log/relatorio_h_t/sync_www.log 2>&1
