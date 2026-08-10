#!/usr/bin/env bash
# Wrapper do robô-fiscal TEMPORÁRIO dos disparos (expira 2026-08-17).
# Chamado por /etc/cron.d/monitor-previsao-wpp como www-data, via
# /bin/bash (o deploy copia .sh sem bit de execução — armadilha conhecida).
set -euo pipefail
export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
export LANG=pt_BR.UTF-8 LC_ALL=pt_BR.UTF-8

cd /opt/relatorio_h_t

# Evita sobreposição (a análise leva ~3 min; margem larga)
exec 9>/opt/camim-auth/wpp_previsao_state/monitor_previsao.lock
flock -n 9 || { echo "$(date -Is) execução anterior ainda em andamento"; exit 0; }

.venv/bin/python monitor_previsao_wpp.py
