#!/usr/bin/env bash
set -euo pipefail

cd /opt/relatorio_h_t
mkdir -p /var/log/relatorio_h_t

# Auditoria mínima de runtime
echo "$(date -Is) user=$(whoami) py=$(command -v python3) ver=$(python3 -V 2>&1)" \
  >> /var/log/relatorio_h_t/job_audit.log

# shellcheck disable=SC1091
source /opt/relatorio_h_t/.venv/bin/activate

python3 build_relatorio_html.py
