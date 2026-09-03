#!/usr/bin/env bash
set -euo pipefail

# ETL "Farmácia · Saídas e Consumo". Lê saídas (Est_Saida), entradas
# (Est_Entrada), consumo lançado ao paciente (Cad_LancamentoServico, classes
# MEDICAMENTO*) e a foto do estoque (Cad_ProdutoLote) dos 13 postos e gera
# json_consolidado/farmacia_<POSTO>.json + farmacia_index.json.
# Só SELECT nos SQL Servers — não escreve em posto nenhum.
#
# Chamado pelo cron via /bin/bash (o rsync do deploy chega sem bit de
# execução — armadilha documentada no CLAUDE.md).

export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
export LANG=pt_BR.UTF-8
export LC_ALL=pt_BR.UTF-8

cd /opt/relatorio_h_t
mkdir -p logs

exec 9> /opt/relatorio_h_t/.export_farmacia.lock
flock -n 9 || { echo "$(date -Is) já existe execução em andamento"; exit 0; }

# shellcheck disable=SC1091
source .venv/bin/activate

echo "$(date -Is) user=$(whoami) py=$(command -v python)" >> /var/log/relatorio_h_t/job_audit.log

python3 export_farmacia.py "$@" >> logs/export_farmacia.log 2>&1

/opt/relatorio_h_t/sync_www.sh >> /var/log/relatorio_h_t/sync_www.log 2>&1
