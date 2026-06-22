#!/usr/bin/env bash
set -euo pipefail

# Wrapper de cron do ETL "Custos com IA" (snapshot horário da OpenAI).
# NÃO chama sync_www: os snapshots são lidos pelo Flask direto de CUSTOS_IA_DIR,
# nunca expostos em /var/www (dados financeiros).

export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
export LANG=pt_BR.UTF-8
export LC_ALL=pt_BR.UTF-8

cd /opt/relatorio_h_t
mkdir -p logs

# Evita concorrência
exec 9> /opt/relatorio_h_t/.export_custos_ia.lock
flock -n 9 || { echo "$(date -Is) já existe execução em andamento"; exit 0; }

# venv
# shellcheck disable=SC1091
source .venv/bin/activate

python3 export_custos_ia.py >> logs/export_custos_ia.log 2>&1
