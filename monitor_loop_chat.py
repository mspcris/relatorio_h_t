#!/usr/bin/env python3
"""Monitor de recaída — loop de cobrança no chat (incidente 07→14/08/2026).

Contexto: um batch de 19 itens do robô de cobrança (07/08 18:15:55 UTC) ficou
preso na fila interna da api-chat e foi reprocessado ~19 mil vezes por item,
criando 343.422 mensagens / 343.392 tickets lixo no camim_chat_production
(RDS AWS) até parar sozinho em 14/08 13:08 UTC. A limpeza marcou tudo com
deletedAt (backup de ids em zz_loop0708_bkp_*).

Este monitor roda de hora em hora (cron :20, /etc/cron.d/monitor-loop-chat),
SOMENTE LEITURA no banco do chat, e alerta Cristiano por e-mail + WhatsApp
(Evolution API, mesma instância dos alarmes) se:

  A) surgir mensagem NOVA ligada aos 18 webhooks presos (fila reviveu);
  B) mensagem lixo do incidente voltar a ficar visível (deletedAt=NULL);
  C) padrão de LOOP NOVO em qualquer cliente: >=30 tickets/1h10 para o mesmo
     customer, ou >=60 mensagens/1h10 no mesmo ticket. (Na rodada normal do
     robô cada cliente recebe no máx. 1 registro por campanha.)

Estado (baseline) em logs/monitor_loop_chat_state.json.
Uso: sem argumento = checagem normal · --teste = envia "instalado" nos 2 canais.
Remover: rm /etc/cron.d/monitor-loop-chat + este arquivo.
"""
import json
import os
import smtplib
import sys
from datetime import datetime
from email.mime.text import MIMEText

import pymysql
from dotenv import load_dotenv

load_dotenv('/opt/relatorio_h_t/.env')

BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
STATE_PATH = os.path.join(BASE_DIR, 'logs', 'monitor_loop_chat_state.json')

DEST_EMAIL = os.getenv('MONITOR_LOOP_EMAIL', 'cristiano@camim.com.br')
DEST_WPP   = os.getenv('MONITOR_LOOP_WPP',   '5521994317573')

EVOLUTION_BASE_URL = os.getenv('EVOLUTION_BASE_URL', '').rstrip('/')
EVOLUTION_API_KEY  = os.getenv('EVOLUTION_API_KEY',  '')
EVOLUTION_INSTANCE = os.getenv('EVOLUTION_INSTANCE', '')
EMAIL_HOST     = os.getenv('ALARM_EMAIL_HOST', 'smtp.gmail.com')
EMAIL_PORT     = int(os.getenv('ALARM_EMAIL_PORT', '465'))
EMAIL_USER     = os.getenv('ALARM_EMAIL_USER', '')
EMAIL_PASSWORD = os.getenv('ALARM_EMAIL_PASSWORD', '')

# Os 18 webhooks do batch preso de 07/08 18:15:55 UTC (fonte: tabela Webhook,
# source='batch'; cada um foi replicado ~19.079x). Se a fila do worker da
# api-chat reviver, é NELES que as mensagens novas aparecem.
WEBHOOKS_PRESOS = [
    'cmsj9muex04iorop8279t2xcw', 'cmsj9muf204iprop8pwlq7vip', 'cmsj9muf604iqrop84lb4xwkr',
    'cmsj9muf904irrop8gnjbwe6g', 'cmsj9mufc04isrop85d463vxl', 'cmsj9mufh04itrop8w7g2ean3',
    'cmsj9mufm04iurop8xjhqcflw', 'cmsj9mufp04ivrop8ja75v5ic', 'cmsj9mufs04iwrop8b4oi6nve',
    'cmsj9mufw04ixrop8ufs8s3z2', 'cmsj9mug004iyrop8jyg7txd0', 'cmsj9mug304izrop8r5efzgoc',
    'cmsj9mug704j0rop8akdkdhtm', 'cmsj9mugb04j1rop8zcbbflli', 'cmsj9mugf04j2rop8bwopjyz8',
    'cmsj9mugi04j3rop8or5cecw9', 'cmsj9mugn04j4rop8z9f1e3v6', 'cmsj9mugt04j5rop8unc3q8cc',
]
WH_IN = ','.join("'%s'" % w for w in WEBHOOKS_PRESOS)

LIMIAR_TICKETS_POR_CLIENTE = 30   # tickets/1h10 pro mesmo customer
LIMIAR_MSGS_POR_TICKET     = 60   # mensagens/1h10 no mesmo ticket


def _conn_chat():
    cfg = {}
    for ln in open('/etc/camim-auth.env'):
        ln = ln.strip()
        if ln.startswith('CHAT_MYSQL_'):
            k, v = ln.split('=', 1)
            cfg[k] = v
    return pymysql.connect(
        host=cfg['CHAT_MYSQL_HOST'], user=cfg['CHAT_MYSQL_USER'],
        password=cfg['CHAT_MYSQL_PASSWORD'], database=cfg['CHAT_MYSQL_DATABASE'],
        connect_timeout=15, read_timeout=60,
    )


def _estado():
    try:
        with open(STATE_PATH) as f:
            return json.load(f)
    except Exception:
        return {}


def _salvar_estado(st):
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    with open(STATE_PATH, 'w') as f:
        json.dump(st, f)


def enviar_email(assunto, corpo):
    if not EMAIL_USER or not EMAIL_PASSWORD:
        print('email nao configurado')
        return False
    msg = MIMEText(corpo, 'plain', 'utf-8')
    msg['Subject'] = assunto
    msg['From'] = EMAIL_USER
    msg['To'] = DEST_EMAIL
    with smtplib.SMTP_SSL(EMAIL_HOST, EMAIL_PORT) as s:
        s.login(EMAIL_USER, EMAIL_PASSWORD)
        s.sendmail(EMAIL_USER, [DEST_EMAIL], msg.as_string())
    return True


def enviar_wpp(texto):
    if not (EVOLUTION_BASE_URL and EVOLUTION_API_KEY and EVOLUTION_INSTANCE):
        print('evolution nao configurada')
        return False
    from urllib import request as _req
    payload = json.dumps({'number': DEST_WPP, 'text': texto}).encode('utf-8')
    req = _req.Request(
        f'{EVOLUTION_BASE_URL}/message/sendText/{EVOLUTION_INSTANCE}',
        data=payload, method='POST',
        headers={'apikey': EVOLUTION_API_KEY, 'Content-Type': 'application/json'},
    )
    with _req.urlopen(req, timeout=15):
        pass
    return True


def alertar(assunto, corpo):
    ok_mail = ok_zap = False
    try:
        ok_mail = enviar_email(assunto, corpo)
    except Exception as e:
        print('erro email:', e)
    try:
        ok_zap = enviar_wpp(f'{assunto}\n\n{corpo}')
    except Exception as e:
        print('erro wpp:', e)
    print(f'alerta enviado email={ok_mail} wpp={ok_zap}')


def checar():
    agora = datetime.now().strftime('%d/%m %H:%M')
    conn = _conn_chat()
    cur = conn.cursor()

    cur.execute(f'SELECT COUNT(*) FROM Message WHERE webhookId IN ({WH_IN})')
    total_presos = cur.fetchone()[0]
    cur.execute(f'SELECT COUNT(*) FROM Message WHERE webhookId IN ({WH_IN}) AND deletedAt IS NULL')
    visiveis = cur.fetchone()[0]

    cur.execute(
        'SELECT customerId, COUNT(*) c FROM Ticket '
        'WHERE createdAt >= UTC_TIMESTAMP() - INTERVAL 70 MINUTE '
        'GROUP BY customerId HAVING c >= %s ORDER BY c DESC LIMIT 5',
        (LIMIAR_TICKETS_POR_CLIENTE,))
    flood_tickets = cur.fetchall()

    cur.execute(
        'SELECT ticketId, COUNT(*) c FROM Message '
        'WHERE createdAt >= UTC_TIMESTAMP() - INTERVAL 70 MINUTE '
        'GROUP BY ticketId HAVING c >= %s ORDER BY c DESC LIMIT 5',
        (LIMIAR_MSGS_POR_TICKET,))
    flood_msgs = cur.fetchall()
    conn.close()

    st = _estado()
    baseline = st.get('baseline_total_presos')
    problemas = []

    if baseline is not None and total_presos > baseline:
        problemas.append(
            f'A) A FILA REVIVEU: {total_presos - baseline} mensagens novas ligadas '
            f'aos 18 webhooks presos do incidente (total {total_presos}, era {baseline}).')
    if visiveis > 0:
        problemas.append(
            f'B) {visiveis} mensagens lixo do incidente estão VISÍVEIS de novo '
            f'(deletedAt=NULL) — limpeza revertida ou inserção nova.')
    for cid, c in flood_tickets:
        problemas.append(f'C) Padrão de loop novo: customer {cid} ganhou {c} tickets na última 1h10.')
    for tid, c in flood_msgs:
        problemas.append(f'C) Padrão de loop novo: ticket {tid} ganhou {c} mensagens na última 1h10.')

    st['baseline_total_presos'] = max(total_presos, baseline or 0)
    st['ultima_checagem'] = agora
    _salvar_estado(st)

    if problemas:
        corpo = (
            f'Monitor de recaída do loop de cobrança — {agora}\n\n' + '\n'.join(problemas) +
            '\n\nO que fazer: conferir a fila do worker da api-chat na AWS '
            '(webhooks source=batch de 07/08 18:15:55 UTC) e avisar o Robson. '
            'Critério e backup da limpeza: tabelas zz_loop0708_bkp_* no camim_chat_production.'
        )
        alertar('🚨 [ALERTA] Recaída do loop de cobrança no chat', corpo)
    else:
        print(f'{agora} ok — presos={total_presos} visiveis={visiveis} flood=0')


def main():
    if '--teste' in sys.argv:
        alertar(
            '✅ Monitor de recaída do loop instalado',
            'Cron horário ativo na vps154 (minuto :20). Vigia: fila dos 18 webhooks '
            'presos, reaparecimento das mensagens limpas e padrão de loop novo '
            '(>=30 tickets/h por cliente ou >=60 msgs/h por ticket). '
            'Canais: este WhatsApp + cristiano@camim.com.br.')
        return
    try:
        checar()
    except Exception as e:
        print('ERRO monitor:', e)
        try:
            enviar_email('⚠️ Monitor de recaída do loop FALHOU',
                         f'O monitor horário não conseguiu checar o banco do chat: {e}')
        except Exception as e2:
            print('erro ao avisar falha:', e2)
        raise


if __name__ == '__main__':
    main()
