#!/usr/bin/env bash
set -euo pipefail

# Contas fixas que chegam em auditoria@camim.com.br -> custos_ti.
#
# SEM ARGUMENTO roda --probe, que só lê a caixa e não grava nada. Quem grava em
# produção é o --run, e ele fica escrito na LINHA DO CRON, não escondido aqui:
# quem abre o crontab tem que enxergar que aquela linha escreve no banco.

# Ambiente mínimo p/ cron
export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
export LANG=pt_BR.UTF-8
export LC_ALL=pt_BR.UTF-8

cd /opt/relatorio_h_t
mkdir -p logs
mkdir -p /var/log/relatorio_h_t

# Auditoria mínima de runtime
echo "$(date -Is) user=$(whoami) py=$(command -v python3) args=${*:---probe}" \
  >> /var/log/relatorio_h_t/job_audit.log

# Evita concorrência: duas execuções juntas leriam os mesmos não-lidos.
exec 9> /opt/relatorio_h_t/.import_email_custos_ti.lock
flock -n 9 || { echo "$(date -Is) já existe execução em andamento"; exit 0; }

# shellcheck disable=SC1091
source .venv/bin/activate

echo "=== $(date -Is) import_email_custos_ti ${*:---probe} ==="
python3 import_email_custos_ti.py "$@"
