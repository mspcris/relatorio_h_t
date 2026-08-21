#!/bin/bash
# import_email_pjs.sh — wrapper do robô da caixa prestadores@ (Controle de PJs).
#
# Sem argumento roda --probe (só leitura, nada gravado). O --run fica escrito
# NA LINHA DO CRON, para quem abre o crontab enxergar qual linha escreve:
#   30 8,14,20 * * * /bin/bash /opt/relatorio_h_t/import_email_pjs.sh --run >> /var/log/relatorio_h_t/import_email_pjs.log 2>&1
#
# Chamar via /bin/bash é OBRIGATÓRIO: o rsync do deploy usa --no-perms e o .sh
# chega sem bit de execução (armadilha documentada no CLAUDE.md).
# flock: execução sobreposta sai na hora em vez de duplicar leitura.
LOCK=/tmp/import_email_pjs.lock
exec /usr/bin/flock -n "$LOCK" /opt/relatorio_h_t/.venv/bin/python /opt/relatorio_h_t/import_email_pjs.py "$@"
