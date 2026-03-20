#!/usr/bin/env bash
set -euo pipefail

###############################################
# Ambiente mínimo para execução via cron
###############################################
export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
export LANG=pt_BR.UTF-8
export LC_ALL=pt_BR.UTF-8

###############################################
# Path raiz do projeto
###############################################
cd /opt/relatorio_h_t
mkdir -p logs

###############################################
# Evita concorrência
###############################################
exec 9> /opt/relatorio_h_t/.export_cadastro_vendas.lock
flock -n 9 || { 
    echo "$(date -Is) já existe execução em andamento"; 
    exit 0; 
}

###############################################
# Ativa venv
###############################################
# shellcheck disable=SC1091
source .venv/bin/activate

###############################################
# Auditoria mínima (igual export_governanca.sh)
###############################################
echo "$(date -Is) user=$(whoami) py=$(command -v python3) ver=$(python3 -V 2>&1)" \
  >> /var/log/relatorio_h_t/job_audit.log

###############################################
# Execução 1 — Export de Vendas
###############################################
echo "$(date -Is) [RUN] export_vendas.py" >> logs/export_cadastro_vendas.log
python3 export_vendas.py >> logs/export_cadastro_vendas.log 2>&1

###############################################
# Execução 2 — Export Cadastro Cliente Incremental
###############################################
echo "$(date -Is) [RUN] export_cad_cliente_incremental.py" >> logs/export_cadastro_vendas.log
python3 export_cad_cliente_incremental.py >> logs/export_cadastro_vendas.log 2>&1

###############################################
# Após export, sincroniza www (igual governança)
###############################################
/opt/relatorio_h_t/sync_www.sh >> /var/log/relatorio_h_t/sync_www.log 2>&1

echo "$(date -Is) workflow export_cadastro_vendas finalizado" >> logs/export_cadastro_vendas.log
