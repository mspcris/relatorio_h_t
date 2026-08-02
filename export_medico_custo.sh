#!/usr/bin/env bash
set -euo pipefail

# ETL do "Médico · Custo Efetivo Nominal". Lê o cadastro (cad_medico +
# cad_especialidade) dos 13 postos e gera json_consolidado/medico_custo.json.
# É CADASTRO, não movimento: uma leitura por dia basta.
# Só SELECT nos SQL Servers — não escreve em posto nenhum.

export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
export LANG=pt_BR.UTF-8
export LC_ALL=pt_BR.UTF-8

cd /opt/relatorio_h_t
mkdir -p logs

exec 9> /opt/relatorio_h_t/.export_medico_custo.lock
flock -n 9 || { echo "$(date -Is) já existe execução em andamento"; exit 0; }

# shellcheck disable=SC1091
source .venv/bin/activate

echo "$(date -Is) user=$(whoami) py=$(command -v python)" >> /var/log/relatorio_h_t/job_audit.log

python3 export_medico_custo.py >> logs/export_medico_custo.log 2>&1

/opt/relatorio_h_t/sync_www.sh >> /var/log/relatorio_h_t/sync_www.log 2>&1
