#!/usr/bin/env bash
# Wrapper do Monitor de Leads (cron horário :10, /etc/cron.d/monitor-leads).
# Chamar via /bin/bash — o deploy copia .sh sem bit de execução.
set -euo pipefail
export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
export LANG=pt_BR.UTF-8 LC_ALL=pt_BR.UTF-8

cd /opt/relatorio_h_t
mkdir -p logs

exec 9>/opt/relatorio_h_t/.monitor_leads.lock
flock -n 9 || { echo "$(date -Is) execução anterior em andamento"; exit 0; }

.venv/bin/python export_monitor_leads.py

# Estudos de leads/vendas (retorno por canal, horário de pico, tendência).
# Roda depois do volume horário; falha aqui NÃO derruba o robô principal.
.venv/bin/python export_leads_estudos.py || echo "$(date -Is) export_leads_estudos falhou (segue)"
