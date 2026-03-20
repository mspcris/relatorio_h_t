#!/usr/bin/env bash
set -euo pipefail

cd /opt/relatorio_h_t
source /opt/relatorio_h_t/.venv/bin/activate

# auditoria
mkdir -p /var/log/relatorio_h_t
echo "$(date -Is) user=$(whoami) py=$(command -v python) ver=$(python -V 2>&1)" >> /var/log/relatorio_h_t/job_audit.log

# saída
OUT_DIR=/opt/relatorio_h_t/export_trello
mkdir -p "$OUT_DIR"

python3 export_trello.py --board 1lAOxrLe --out "$OUT_DIR"
