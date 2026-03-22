#!/bin/bash
# sync_wpp.sh — Wrapper cron para o engine WhatsApp Cobrança
#
# Configurar no crontab:
#   crontab -e
#   */15 * * * * /opt/relatorio_h_t/sync_wpp.sh >> /var/log/sync_wpp.log 2>&1

set -euo pipefail

echo "=== $(date '+%Y-%m-%d %H:%M:%S') ==="
cd /opt/relatorio_h_t
source .venv/bin/activate
python send_whatsapp_cobranca.py
