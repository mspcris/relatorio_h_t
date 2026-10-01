#!/usr/bin/env bash
set -euo pipefail

# Coleta do app "Contabilidade - Notas" (uma nota por linha, por posto e mês).
# Só SELECT no ERP. Grava em json_contab_notas/, que NÃO vai para /var/www:
# o dado só sai pela API (/api/contabilidade_notas/), que confere a permissão.
# Argumentos são repassados ao Python (ex.: --meses 2026-07,2026-08).

export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
export LANG=pt_BR.UTF-8
export LC_ALL=pt_BR.UTF-8

cd /opt/relatorio_h_t
mkdir -p logs /var/log/relatorio_h_t

echo "$(date -Is) user=$(whoami) job=export_contab_notas args=${*:-}" >> /var/log/relatorio_h_t/job_audit.log

# shellcheck disable=SC1091
source .venv/bin/activate

echo "=== $(date -Is) export_contab_notas ${*:-} ==="
python3 export_contab_notas.py "$@"
