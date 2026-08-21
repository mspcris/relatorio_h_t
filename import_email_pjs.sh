#!/bin/bash
# import_email_pjs.sh — wrapper do robô da caixa prestadores@ (Controle de PJs).
#
# Sem argumento roda --probe (só leitura, nada gravado).
#
# O CRON DE VERDADE está versionado em cron/relatorio_ht (deploy step 8E copia
# para /etc/cron.d/) — 3×/dia às 08:30/14:30/20:30, com --run escrito NA LINHA,
# chamando o .py direto com flock. Este .sh é o atalho para rodar NA MÃO:
#   /bin/bash /opt/relatorio_h_t/import_email_pjs.sh          # probe
#   /bin/bash /opt/relatorio_h_t/import_email_pjs.sh --run    # grava na fila
# Chamar via /bin/bash é OBRIGATÓRIO: o rsync do deploy usa --no-perms e o .sh
# chega sem bit de execução (armadilha documentada no CLAUDE.md).
LOCK=/tmp/import_email_pjs.lock
exec /usr/bin/flock -n "$LOCK" /opt/relatorio_h_t/.venv/bin/python /opt/relatorio_h_t/import_email_pjs.py "$@"
