#!/bin/bash
# run_export_fidelizacao.sh
# Wrapper para executar export_fidelizacao.py diariamente
# Executado via cron às 4h40min

set -e

SCRIPT_DIR="/opt/relatorio_h_t"
LOG_DIR="/var/log/camim"
LOG_FILE="$LOG_DIR/export_fidelizacao.log"
DB_PATH="$SCRIPT_DIR/fidelizacao.db"

# Criar log dir se não existir
mkdir -p "$LOG_DIR"

{
    echo "========== $(date '+%Y-%m-%d %H:%M:%S') =========="
    echo "Iniciando export_fidelizacao.py (todos os postos)..."

    cd "$SCRIPT_DIR"
    source .venv/bin/activate

    # Executar exportador para todos os postos configurados no .env
    python3 export_fidelizacao.py

    echo "✅ Export concluído com sucesso"

    # Copiar HTML para /var/www se não existir
    if [ ! -f "/var/www/kpi_fidelizacao_cliente.html" ]; then
        cp "$SCRIPT_DIR/kpi_fidelizacao_cliente.html" "/var/www/"
        echo "✅ HTML copiado para /var/www"
    fi

    echo "========== $(date '+%Y-%m-%d %H:%M:%S') =========="

} >> "$LOG_FILE" 2>&1

exit 0
