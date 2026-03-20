#!/usr/bin/env bash
set -euo pipefail
umask 002

[ "$(id -un)" = "appuser" ] || { echo "erro: execute como appuser"; exit 1; }

# === paths ===
ROOT="/opt/relatorio_h_t"
PY="$ROOT/.venv/bin/python"

WWW="/var/www"
OUT_JSON="$WWW/json_consolidado"
OUT_HARV="$WWW/export_harvest"
OUT_TREL="$WWW/export_trello"
BOARD_ID="1lAOxrLe"   # Trello

# === util ===
rm_only_list() {
  local dir="$1"; shift
  mkdir -p "$dir"
  for f in "$@"; do
    rm -f -- "$dir/$f" "$dir/${f%.json}.josn"
  done
}

# === preparar destinos (sem mexer no resto do /var/www) ===
mkdir -p "$OUT_JSON" "$OUT_HARV" "$OUT_TREL"

cd "$ROOT"

# 1) limpar SOMENTE os JSONs-alvo em /var/www/json_consolidado
rm_only_list "$OUT_JSON" \
  "consolidado_mensal_por_posto.json" \
  "consolidado_mensal.json" \
  "percentuais_mensais_por_posto.json" \
  "prescricao_hoje.json" \
  "Prescricao_mensal.json"

# 2) gerar JSONs consolidados direto em /var/www/json_consolidado
"$PY" "$ROOT/indicadores_etl.py" \
  --ptax 2020-01-01 today \
  --ipca 2020-01 thismonth \
  --igpm 2020-01 thismonth \
  --out "$OUT_JSON"

# 3) Harvest -> CSV em /var/www/export_harvest
"$PY" "$ROOT/export_harvest.py" --out "$OUT_HARV"

# 4) Trello -> artefatos em /var/www/export_trello
"$PY" "$ROOT/export_trello.py" --board "$BOARD_ID" --out "$OUT_TREL"

# 5) Governança: roda em /opt (gera /opt/relatorio_h_t/dados e JSON lá)
"$PY" "$ROOT/export_governanca.py"

# 6) HTML: gera em /opt e publica em /var/www (apaga o antigo e copia o novo)
"$PY" "$ROOT/build_relatorio_html.py"
if [[ -f "$ROOT/trello_harvest.html" ]]; then
  rm -f "$WWW/trello_harvest.html"
  cp -a "$ROOT/trello_harvest.html" "$WWW/trello_harvest.html"
fi

# 7) symlink estável para o CSV mais recente do Harvest
LATEST_H="$(ls -1t "$OUT_HARV"/* 2>/dev/null | head -1 || true)"
[[ -n "$LATEST_H" ]] && ln -sfn "$LATEST_H" "$OUT_HARV/current.csv"

echo "OK"
