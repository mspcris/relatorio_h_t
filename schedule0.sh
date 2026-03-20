# schedule0.sh
#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Uso:
  schedule0.sh [--project-dir DIR] [--out DIR] [--site] [--igpm-code CODIGO]

Padrões:
  --project-dir  = diretório do projeto (default: cwd)
  --out          = PROJECT_DIR/json_consolidado
  --site         = adiciona flag --site ao Python
  --igpm-code    = ex.: IGP12_IGPMG12
EOF
}

PROJECT_DIR="$(pwd)"
OUT_DIR=""
SITE=0
IGPM_CODE=""

# parse
while [[ $# -gt 0 ]]; do
  case "$1" in
    --project-dir) PROJECT_DIR="$2"; shift 2;;
    --out|--out-dir) OUT_DIR="$2"; shift 2;;
    --site) SITE=1; shift;;
    --igpm-code) IGPM_CODE="$2"; shift 2;;
    -h|--help) usage; exit 0;;
    *) echo "arg inválido: $1"; usage; exit 2;;
  esac
done
OUT_DIR="${OUT_DIR:-$PROJECT_DIR/json_consolidado}"

mkdir -p "$OUT_DIR"

# escolhe python do venv se existir
PY="$PROJECT_DIR/.venv/bin/python"
if [[ ! -x "$PY" ]]; then
  PY="$(command -v python3 || command -v python)"
fi

# monta args
args=(
  "$PROJECT_DIR/indicadores_etl.py"
  --ptax 2020-01-01 today
  --ipca 2020-01 thismonth
  --igpm 2020-01 thismonth
  --out "$OUT_DIR"
)
[[ -n "$IGPM_CODE" ]] && args+=( --igpm-code "$IGPM_CODE" )
[[ $SITE -eq 1 ]] && args+=( --site )

exec "$PY" "${args[@]}"
