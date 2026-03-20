
#!/usr/bin/env bash
set -euo pipefail
cd /opt/relatorio_h_t

# venv
source /opt/relatorio_h_t/.venv/bin/activate

# auditoria mínima de runtime
echo "$(date -Is) user=$(whoami) py=$(command -v python) ver=$(python -V 2>&1)" >> /var/log/relatorio_h_t/job_audit.log


#!/usr/bin/env bash
set -euo pipefail
cd /opt/relatorio_h_t
source .venv/bin/activate
/opt/relatorio_h_t/.venv/bin/python3 export_harvest.py
