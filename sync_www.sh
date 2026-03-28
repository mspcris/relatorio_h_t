#!/usr/bin/env bash
set -euo pipefail
umask 002

VENV="/opt/relatorio_h_t/.venv"
SRC="/opt/relatorio_h_t"
DST="/var/www"
TPL="/opt/camim-auth/templates"
LOG_DIR="/var/log/relatorio_h_t"
LOG="$LOG_DIR/sync_www.log"

WEB_GRP="www-data"
OWNER_USER="appuser"

mkdir -p "$LOG_DIR"; : >"$LOG" || true
exec >>"$LOG" 2>&1
echo "[$(date -Is)] START pid=$$"

# venv apenas para manter padrão de execução
[ -f "$VENV/bin/activate" ] && source "$VENV/bin/activate"
echo "$(date -Is) user=$(whoami)"

# -----------------------------------------------------------
# 0) Cópia local de SQLs: sql_cadastro -> SQL_cadastro
# -----------------------------------------------------------

SRC_SQL_CAD="$SRC/sql_cadastro"
DST_SQL_CAD="$SRC/SQL_cadastro"

if [ -d "$SRC_SQL_CAD" ]; then
  echo "sync sql_cadastro -> $DST_SQL_CAD"
  install -d -m 2775 -g "$WEB_GRP" "$DST_SQL_CAD"
  rsync -rlvt \
    --omit-dir-times --no-perms --no-owner --no-group \
    --chmod=D2775,F664 \
    --chown="$OWNER_USER:$WEB_GRP" \
    "$SRC_SQL_CAD"/ "$DST_SQL_CAD"/
else
  echo "warn: diretório origem $SRC_SQL_CAD não existe"
fi

# -----------------------------------------------------------
# helpers
# -----------------------------------------------------------

# Copia arquivo único
copy_file() {
  local srcf="$1" dstd="$2"
  [ -f "$srcf" ] || return 0
  install -d -m 2775 -g "$WEB_GRP" "$dstd"
  install -p -m 664 -o "$OWNER_USER" -g "$WEB_GRP" "$srcf" "$dstd/"
}

# Copia todos .json de um diretório
copy_json_dir() {
  local sd="$1" dd="$2"
  [ -d "$sd" ] || { echo "warn: diretório origem $sd não existe"; return 0; }
  install -d -m 2775 -g "$WEB_GRP" "$dd"
  find "$sd" -maxdepth 1 -type f -name '*.json' -print0 \
    | xargs -0 -I{} install -p -m 664 -o "$OWNER_USER" -g "$WEB_GRP" "{}" "$dd/"
}

# Copia diretório recursivamente (árvore inteira)
copy_dir_tree() {
  local sd="$1" dd="$2"
  [ -d "$sd" ] || { echo "warn: diretório origem $sd não existe"; return 0; }

  install -d -m 2775 -g "$WEB_GRP" "$dd"
  rsync -rlvt \
    --omit-dir-times --no-perms --no-owner --no-group \
    --chmod=D2775,F664 \
    --chown="$OWNER_USER:$WEB_GRP" \
    "$sd"/ "$dd"/
}

# -----------------------------------------------------------
# garantir diretórios de destino
# -----------------------------------------------------------

install -d -m 2775 -g "$WEB_GRP" \
  "$DST/export_trello" "$DST/export_harvest" \
  "$DST/json_consolidado" "$DST/json_rateio" "$DST/json_fin_full" \
  "$TPL/export_trello" "$TPL/export_harvest" \
  "$TPL/json_consolidado" "$TPL/json_rateio" "$TPL/json_fin_full" \
  "$DST/json_ctrlq_relatorio" "$TPL/json_ctrlq_relatorio" \
  "$DST/json_ctrlq_desbloqueio" "$TPL/json_ctrlq_desbloqueio" \
  "$DST/json_consultas_mensal" "$TPL/json_consultas_mensal" \
  "$DST/json_notas_rps" "$TPL/json_notas_rps" \
  "$DST/json_metas" "$TPL/json_metas"

# -----------------------------------------------------------
# login.html — nginx serve diretamente de /var/www
# (deploy copia só para /opt/camim-auth/templates; sync mantém /var/www atualizado)
# -----------------------------------------------------------

echo "sync login.html -> $DST"
copy_file "$TPL/login.html" "$DST"

# -----------------------------------------------------------
# Harvest — export mais novo
# -----------------------------------------------------------

if newest=$(find "$SRC/export_harvest" -maxdepth 1 -type f -printf '%T@ %p\0' \
  | sort -z -n | tail -z -n1 | cut -z -d' ' -f2- | tr -d '\0'); then
  echo "harvest -> $(basename "$newest")"
  copy_file "$newest" "$DST/export_harvest"
  copy_file "$newest" "$TPL/export_harvest"
else
  echo "warn: nenhum arquivo em $SRC/export_harvest"
fi

# -----------------------------------------------------------
# Trello — export mais novo
# -----------------------------------------------------------

if newest=$(find "$SRC/export_trello" -maxdepth 1 -type f -printf '%T@ %p\0' \
  | sort -z -n | tail -z -n1 | cut -z -d' ' -f2- | tr -d '\0'); then
  echo "trello  -> $(basename "$newest")"
  copy_file "$newest" "$DST/export_trello"
  copy_file "$newest" "$TPL/export_trello"
else
  echo "warn: nenhum arquivo em $SRC/export_trello"
fi

# -----------------------------------------------------------
# Relatório HTML principal
# -----------------------------------------------------------

if [ -f "$SRC/trello_harvest.html" ]; then
  copy_file "$SRC/trello_harvest.html" "$DST"
  copy_file "$SRC/trello_harvest.html" "$TPL"
fi

# -----------------------------------------------------------
# KPI Notas x RPS (HTML)
# -----------------------------------------------------------

if [ -f "$SRC/kpi_notas_rps.html" ]; then
  copy_file "$SRC/kpi_notas_rps.html" "$DST"
  copy_file "$SRC/kpi_notas_rps.html" "$TPL"
fi

# -----------------------------------------------------------
# JSONs consolidados
# -----------------------------------------------------------

copy_json_dir "$SRC/json_consolidado" "$DST/json_consolidado"
copy_json_dir "$SRC/json_consolidado" "$TPL/json_consolidado"

# -----------------------------------------------------------
# JSON RATEIO
# -----------------------------------------------------------
echo "sync json_rateio -> $DST/json_rateio e $TPL/json_rateio"
copy_dir_tree "$SRC/json_rateio" "$DST/json_rateio"
copy_dir_tree "$SRC/json_rateio" "$TPL/json_rateio"

# -----------------------------------------------------------
# JSON FIN FULL
# -----------------------------------------------------------
echo "sync json_fin_full -> $DST/json_fin_full e $TPL/json_fin_full"
copy_dir_tree "$SRC/json_fin_full" "$DST/json_fin_full"
copy_dir_tree "$SRC/json_fin_full" "$TPL/json_fin_full"

# -----------------------------------------------------------
# VENDAS — SQL, dados e JSON
# -----------------------------------------------------------

echo "sync sql_vendas -> $DST/sql_vendas e $TPL/sql_vendas"
copy_dir_tree "$SRC/sql_vendas" "$DST/sql_vendas"
copy_dir_tree "$SRC/sql_vendas" "$TPL/sql_vendas"

echo "sync dados_vendas -> $DST/dados_vendas e $TPL/dados_vendas"
copy_dir_tree "$SRC/dados_vendas" "$DST/dados_vendas"
copy_dir_tree "$SRC/dados_vendas" "$TPL/dados_vendas"

echo "sync json_vendas -> $DST/json_vendas e $TPL/json_vendas"
copy_dir_tree "$SRC/json_vendas" "$DST/json_vendas"
copy_dir_tree "$SRC/json_vendas" "$TPL/json_vendas"

# -----------------------------------------------------------
# JSON CADASTRO
# -----------------------------------------------------------

echo "sync json_cadastro -> $DST/json_cadastro e $TPL/json_cadastro"
copy_dir_tree "$SRC/json_cadastro" "$DST/json_cadastro"
copy_dir_tree "$SRC/json_cadastro" "$TPL/json_cadastro"

# -----------------------------------------------------------
# CTRLQ RELATORIO
# -----------------------------------------------------------

echo "sync json_ctrlq_relatorio -> $DST/json_ctrlq_relatorio e $TPL/json_ctrlq_relatorio"
copy_dir_tree "$SRC/json_ctrlq_relatorio" "$DST/json_ctrlq_relatorio"
copy_dir_tree "$SRC/json_ctrlq_relatorio" "$TPL/json_ctrlq_relatorio"

# -----------------------------------------------------------
# CTRLQ DESBLOQUEIO
# -----------------------------------------------------------

echo "sync json_ctrlq_desbloqueio -> $DST/json_ctrlq_desbloqueio e $TPL/json_ctrlq_desbloqueio"
copy_dir_tree "$SRC/json_ctrlq_desbloqueio" "$DST/json_ctrlq_desbloqueio"
copy_dir_tree "$SRC/json_ctrlq_desbloqueio" "$TPL/json_ctrlq_desbloqueio"

# -----------------------------------------------------------
# JSON CONSULTAS MENSAL
# -----------------------------------------------------------

echo "sync json_consultas_mensal -> $DST/json_consultas_mensal e $TPL/json_consultas_mensal"
copy_dir_tree "$SRC/json_consultas_mensal" "$DST/json_consultas_mensal"
copy_dir_tree "$SRC/json_consultas_mensal" "$TPL/json_consultas_mensal"

# -----------------------------------------------------------
# JSON NOTAS RPS
# -----------------------------------------------------------

echo "sync json_notas_rps -> $DST/json_notas_rps e $TPL/json_notas_rps"
copy_dir_tree "$SRC/json_notas_rps" "$DST/json_notas_rps"
copy_dir_tree "$SRC/json_notas_rps" "$TPL/json_notas_rps"

# -----------------------------------------------------------
# JSON METAS
# -----------------------------------------------------------

echo "sync json_metas -> $DST/json_metas e $TPL/json_metas"
copy_dir_tree "$SRC/json_metas" "$DST/json_metas"
copy_dir_tree "$SRC/json_metas" "$TPL/json_metas"

# -----------------------------------------------------------
# postos_acl.json -> /etc/nginx
# -----------------------------------------------------------

ACL_SRC="$SRC/postos_acl.json"
ACL_DST="/etc/nginx"

if [ -f "$ACL_SRC" ]; then
  echo "copiando postos_acl.json -> /etc/nginx"
  copy_file "$ACL_SRC" "$ACL_DST"
else
  echo "warn: postos_acl.json não encontrado em $SRC"
fi

echo "[$(date -Is)] END rc=$?"
