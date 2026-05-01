#!/usr/bin/env python3
"""
disparar_alarmes.py
Dispatcher de alarmes CAMIM — verificação e disparo de notificações.

Executado pelo cron a cada minuto (verifica se algum alarme está agendado para o minuto atual).

Cron (cada minuto):
  * * * * * /opt/relatorio_h_t/.venv/bin/python /opt/relatorio_h_t/disparar_alarmes.py \
    >> /opt/relatorio_h_t/logs/alarmes.log 2>&1
"""

import os
import sys
import json
import sqlite3
import smtplib
import logging
from datetime import date, datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

sys.path.insert(0, '/opt/camim-auth')
sys.path.insert(0, '/opt/relatorio_h_t')

from dotenv import load_dotenv
load_dotenv('/opt/relatorio_h_t/.env')

import alarmes_db as adb

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)
log = logging.getLogger(__name__)

# ── Configuração de envio ─────────────────────────────────────────────────────

EVOLUTION_BASE_URL = os.getenv('EVOLUTION_BASE_URL', '').rstrip('/')
EVOLUTION_API_KEY  = os.getenv('EVOLUTION_API_KEY',  '')
EVOLUTION_INSTANCE = os.getenv('EVOLUTION_INSTANCE', '')

EMAIL_HOST     = os.getenv('ALARM_EMAIL_HOST',     'smtp.gmail.com')
EMAIL_PORT     = int(os.getenv('ALARM_EMAIL_PORT', '465'))
EMAIL_USER     = os.getenv('ALARM_EMAIL_USER',     '')
EMAIL_PASSWORD = os.getenv('ALARM_EMAIL_PASSWORD', '')
EMAIL_FROM     = os.getenv('ALARM_EMAIL_FROM',     '') or EMAIL_USER

APP_URL = os.getenv('APP_BASE_URL', 'https://kpi.camim.com.br')

# ── Coleta de status dos serviços ─────────────────────────────────────────────

def _dias_para_status(dias):
    if dias is None or dias >= 999: return 'horrivel'
    if dias == 0:  return 'otimo'
    if dias == 1:  return 'bom'
    if dias == 2:  return 'ok'
    if dias == 3:  return 'ruim'
    if dias == 4:  return 'pessimo'
    return 'horrivel'


def _ultimo_para_dias(ultimo_str):
    if not ultimo_str:
        return 999
    try:
        dt = datetime.fromisoformat(str(ultimo_str)).date()
        return (date.today() - dt).days
    except Exception:
        return 999


def status_push(posto):
    push_db = os.getenv('PUSH_LOG_DB', '/opt/push_clientes/push_log.db')
    try:
        conn = sqlite3.connect(f'file:{push_db}?mode=ro', uri=True)
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()]
        tabela = next((t for t in tables if 'push' in t.lower() or 'log' in t.lower()),
                      tables[0] if tables else None)
        if not tabela:
            conn.close()
            return 'horrivel'
        cols = [r[1] for r in conn.execute(f'PRAGMA table_info({tabela})').fetchall()]
        col_data  = next((c for c in cols if any(k in c.lower() for k in ('data', 'hora', 'created', 'time', 'sent'))), None)
        col_posto = next((c for c in cols if 'posto' in c.lower()), None)
        col_modo  = next((c for c in cols if 'modo' in c.lower()), None)
        if not col_data or not col_posto:
            conn.close()
            return 'horrivel'
        if col_modo:
            row = conn.execute(
                f"SELECT MAX({col_data}) FROM {tabela} WHERE {col_modo}='producao' AND {col_posto}=?",
                (posto,)
            ).fetchone()
        else:
            row = conn.execute(
                f"SELECT MAX({col_data}) FROM {tabela} WHERE {col_posto}=?", (posto,)
            ).fetchone()
        conn.close()
        return _dias_para_status(_ultimo_para_dias(row[0] if row else None))
    except Exception as e:
        log.warning('status_push(%s): %s', posto, e)
        return 'horrivel'


def status_email(posto):
    kpi_db = os.getenv('KPI_DB_PATH', '/opt/relatorio_h_t/camim_kpi.db')
    try:
        conn = sqlite3.connect(f'file:{kpi_db}?mode=ro', uri=True)
        row = conn.execute(
            "SELECT MAX(datahora) FROM ind_email WHERE posto=? AND titulo_categoria='Boleto'",
            (posto,)
        ).fetchone()
        conn.close()
        return _dias_para_status(_ultimo_para_dias(row[0] if row else None))
    except Exception as e:
        log.warning('status_email(%s): %s', posto, e)
        return 'horrivel'


def status_tef(posto):
    kpi_db = os.getenv('KPI_DB_PATH', '/opt/relatorio_h_t/camim_kpi.db')
    try:
        conn = sqlite3.connect(f'file:{kpi_db}?mode=ro', uri=True)
        row = conn.execute(
            "SELECT MAX(datahora) FROM ind_tef WHERE posto=?", (posto,)
        ).fetchone()
        conn.close()
        return _dias_para_status(_ultimo_para_dias(row[0] if row else None))
    except Exception as e:
        log.warning('status_tef(%s): %s', posto, e)
        return 'horrivel'


def status_wpp(posto):
    wpp_db = os.getenv('WAPP_CTRL_DB', '/opt/camim-auth/whatsapp_cobranca.db')
    try:
        conn = sqlite3.connect(f'file:{wpp_db}?mode=ro', uri=True)
        row = conn.execute("""
            SELECT MAX(e.enviado_em)
            FROM envios e
            JOIN campanhas c ON c.id = e.campanha_id
            WHERE c.ativa=1 AND e.posto=? AND e.status='accepted'
        """, (posto,)).fetchone()
        conn.close()
        return _dias_para_status(_ultimo_para_dias(row[0] if row else None))
    except Exception as e:
        log.warning('status_wpp(%s): %s', posto, e)
        return 'horrivel'


def get_status(servico, posto):
    fn = {'push': status_push, 'email': status_email, 'tef': status_tef, 'wpp': status_wpp}
    return fn.get(servico, lambda p: 'horrivel')(posto)


# ── Envio de mensagens ────────────────────────────────────────────────────────

def _limpar_telefone(tel):
    import re
    t = re.sub(r'\D', '', str(tel or ''))
    if len(t) in (10, 11):
        t = '55' + t
    return t if len(t) in (12, 13) else None


def enviar_wpp(telefone, texto):
    numero = _limpar_telefone(telefone)
    if not numero:
        return False, 'telefone inválido'
    if not EVOLUTION_BASE_URL or not EVOLUTION_API_KEY or not EVOLUTION_INSTANCE:
        return False, 'Evolution API não configurada no .env'
    try:
        from urllib import request as _req
        url = f'{EVOLUTION_BASE_URL}/message/sendText/{EVOLUTION_INSTANCE}'
        payload = json.dumps({'number': numero, 'text': texto}).encode('utf-8')
        req = _req.Request(url, data=payload, method='POST', headers={
            'apikey': EVOLUTION_API_KEY,
            'Content-Type': 'application/json',
        })
        with _req.urlopen(req, timeout=15):
            pass
        return True, 'ok'
    except Exception as e:
        return False, str(e)[:200]


def enviar_email(para, assunto, corpo_html):
    if not EMAIL_USER or not EMAIL_PASSWORD:
        return False, 'credenciais de e-mail não configuradas no .env'
    try:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = assunto
        msg['From']    = EMAIL_FROM or EMAIL_USER
        msg['To']      = para
        msg.attach(MIMEText(corpo_html, 'html', 'utf-8'))
        if EMAIL_PORT == 465:
            with smtplib.SMTP_SSL(EMAIL_HOST, EMAIL_PORT) as s:
                s.login(EMAIL_USER, EMAIL_PASSWORD)
                s.sendmail(msg['From'], [para], msg.as_string())
        else:
            with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT) as s:
                s.ehlo()
                s.starttls()
                s.login(EMAIL_USER, EMAIL_PASSWORD)
                s.sendmail(msg['From'], [para], msg.as_string())
        return True, 'ok'
    except Exception as e:
        return False, str(e)[:200]


def _corpo_email(alarme, status_atual):
    sname         = adb.STATUS_LABELS.get(status_atual, status_atual)
    sname_gatilho = adb.STATUS_LABELS.get(alarme['status_gatilho'], alarme['status_gatilho'])
    servico_nome  = adb.SERVICOS.get(alarme['servico'], alarme['servico'])
    posto_nome    = adb.POSTOS_NOMES.get(alarme['posto'], alarme['posto'])
    return f"""<!doctype html>
<html lang="pt-br"><body style="margin:0;padding:0;background:#f4f4f4;font-family:sans-serif">
<table width="100%" cellpadding="0" cellspacing="0" style="padding:32px 16px">
<tr><td align="center">
<table width="600" cellpadding="0" cellspacing="0"
       style="background:#fff;border-radius:10px;overflow:hidden;
              box-shadow:0 2px 12px rgba(0,0,0,.10)">
  <tr>
    <td style="background:#c62828;padding:20px 32px">
      <h2 style="margin:0;color:#fff;font-size:20px">
        &#9888; Alerta CAMIM &mdash; {alarme['nome']}
      </h2>
    </td>
  </tr>
  <tr>
    <td style="padding:24px 32px">
      <table width="100%" style="border-collapse:collapse;margin-bottom:20px">
        <tr>
          <td style="color:#666;padding:6px 0;width:130px">Posto</td>
          <td><strong>{alarme['posto']} &mdash; {posto_nome}</strong></td>
        </tr>
        <tr>
          <td style="color:#666;padding:6px 0">Servi&ccedil;o</td>
          <td><strong>{servico_nome}</strong></td>
        </tr>
        <tr>
          <td style="color:#666;padding:6px 0">Status atual</td>
          <td><strong style="color:#c62828">{sname}</strong></td>
        </tr>
        <tr>
          <td style="color:#666;padding:6px 0">Gatilho</td>
          <td>{sname_gatilho} ou pior</td>
        </tr>
      </table>
      <div style="background:#fff8e1;border-left:4px solid #f9a825;
                  border-radius:0 6px 6px 0;padding:14px 16px;margin-bottom:20px">
        <strong style="display:block;margin-bottom:6px">Mensagem</strong>
        {alarme['mensagem']}
      </div>
      <p style="margin-bottom:20px">
        <a href="{APP_URL}/monitorarrobos.html"
           style="background:#1565c0;color:#fff;padding:11px 22px;border-radius:6px;
                  text-decoration:none;font-weight:600;display:inline-block">
          Acessar Painel de Monitoramento
        </a>
      </p>
      <p style="color:#999;font-size:12px;margin:0">
        Disparado em {datetime.now().strftime('%d/%m/%Y %H:%M')}
      </p>
    </td>
  </tr>
  <tr>
    <td style="background:#f8f8f8;padding:12px 32px;border-top:1px solid #eee">
      <p style="margin:0;color:#aaa;font-size:11px">
        CAMIM &mdash; Sistema de Alertas Operacionais
      </p>
    </td>
  </tr>
</table>
</td></tr></table>
</body></html>"""


# ── Lógica de disparo ─────────────────────────────────────────────────────────

def deve_disparar_agora(alarme):
    agora = datetime.now()
    hora_atual = agora.strftime('%H:%M')
    dia_atual  = str(agora.weekday())  # 0=Seg, 6=Dom
    dias_conf  = [d.strip() for d in (alarme.get('dias_semana') or '0,1,2,3,4').split(',')]
    return hora_atual == (alarme.get('hora_disparo') or '08:00') and dia_atual in dias_conf


def _enviar_para(tipo, canal, dest, texto_wpp, assunto_email, corpo_email_html, detalhes):
    """Tenta enviar WPP e/ou email para um destinatário e registra no detalhes."""
    if canal in ('wpp', 'ambos') and dest.get('telefone'):
        ok, msg = enviar_wpp(dest['telefone'], texto_wpp)
        detalhes.append({'tipo': 'wpp', 'para': tipo, 'dest': dest.get('telefone'), 'ok': ok, 'msg': msg})
        log.info('WPP %s %s: ok=%s msg=%s', tipo, dest.get('telefone'), ok, msg)
    if canal in ('email', 'ambos') and dest.get('email'):
        ok, msg = enviar_email(dest['email'], assunto_email, corpo_email_html)
        detalhes.append({'tipo': 'email', 'para': tipo, 'dest': dest.get('email'), 'ok': ok, 'msg': msg})
        log.info('Email %s %s: ok=%s msg=%s', tipo, dest.get('email'), ok, msg)


def disparar(alarme):
    posto        = alarme['posto']
    status_atual = get_status(alarme['servico'], posto)

    if not adb.status_igual_ou_pior(status_atual, alarme['status_gatilho']):
        log.info('Alarme %d: status=%s gatilho=%s — não dispara',
                 alarme['id'], status_atual, alarme['status_gatilho'])
        return

    if adb.esta_silenciado(alarme['id']):
        log.info('Alarme %d: silenciado — pulando', alarme['id'])
        return

    numero_ciclo = adb.get_numero_ciclo(alarme['id'])
    sname        = adb.STATUS_LABELS.get(status_atual, status_atual)
    servico_nome = adb.SERVICOS.get(alarme['servico'], alarme['servico'])
    assunto      = f"[ALERTA CAMIM] {alarme['nome']} — Posto {posto} — {sname}"
    texto_wpp_base = (
        f"⚠️ ALERTA CAMIM\n"
        f"*{alarme['nome']}*\n"
        f"Posto: {posto} — {adb.POSTOS_NOMES.get(posto, posto)}\n"
        f"Serviço: {servico_nome}\n"
        f"Status: *{sname}*\n\n"
        f"{alarme['mensagem']}\n\n"
        f"Acesse: {APP_URL}/monitorarrobos.html"
    )
    corpo_email_html = _corpo_email(alarme, status_atual)
    detalhes = {'status_atual': status_atual, 'numero_ciclo': numero_ciclo, 'envios': []}
    wpp_ger_ok   = False
    email_ger_ok = False

    # ─ Gerente do posto (banco, não removível)
    gerente = adb.get_gerente(posto)
    if gerente:
        canal = 'ambos'
        if alarme['via_whatsapp'] and not alarme['via_email']:
            canal = 'wpp'
        elif alarme['via_email'] and not alarme['via_whatsapp']:
            canal = 'email'
        if alarme['via_whatsapp'] and gerente.get('telefone'):
            ok, msg = enviar_wpp(gerente['telefone'], texto_wpp_base)
            if ok:
                wpp_ger_ok = True
            detalhes['envios'].append({'tipo': 'wpp', 'para': 'gerente', 'ok': ok, 'msg': msg})
        if alarme['via_email'] and gerente.get('email'):
            ok, msg = enviar_email(gerente['email'], assunto, corpo_email_html)
            if ok:
                email_ger_ok = True
            detalhes['envios'].append({'tipo': 'email', 'para': 'gerente', 'ok': ok, 'msg': msg})

    # ─ Gerentes extras
    for extra in (alarme.get('extras') or []):
        if extra.get('via_whatsapp') and extra.get('telefone') and alarme['via_whatsapp']:
            ok, msg = enviar_wpp(extra['telefone'],
                                 f"[EXTRA] {texto_wpp_base}")
            detalhes['envios'].append({'tipo': 'wpp', 'para': f'extra:{extra["id"]}', 'ok': ok})
        if extra.get('via_email') and extra.get('email') and alarme['via_email']:
            ok, msg = enviar_email(extra['email'], assunto, corpo_email_html)
            detalhes['envios'].append({'tipo': 'email', 'para': f'extra:{extra["id"]}', 'ok': ok})

    # ─ Auditores (ciclo 1 = prefs deles; ciclo 2+ = ambos os canais)
    for aid in (alarme.get('auditores') or []):
        aud = adb.get_auditor(aid)
        if not aud:
            continue
        if numero_ciclo == 1:
            send_wpp   = bool(aud.get('recebe_1_wpp'))
            send_email = bool(aud.get('recebe_1_email'))
        else:
            send_wpp   = True
            send_email = True
        if send_wpp and aud.get('telefone'):
            ok, msg = enviar_wpp(aud['telefone'],
                                 f"[AUDITORIA] {texto_wpp_base}")
            detalhes['envios'].append({'tipo': 'wpp', 'para': f'auditor:{aid}', 'ok': ok})
        if send_email and aud.get('email'):
            ok, msg = enviar_email(aud['email'], f"[AUDITORIA] {assunto}", corpo_email_html)
            detalhes['envios'].append({'tipo': 'email', 'para': f'auditor:{aid}', 'ok': ok})

    # ─ Diretores (mesma lógica de ciclos)
    for did in (alarme.get('diretores') or []):
        dire = adb.get_diretor(did)
        if not dire:
            continue
        if numero_ciclo == 1:
            send_wpp   = bool(dire.get('recebe_1_wpp'))
            send_email = bool(dire.get('recebe_1_email'))
        else:
            send_wpp   = True
            send_email = True
        if send_wpp and dire.get('telefone'):
            ok, msg = enviar_wpp(dire['telefone'],
                                 f"[DIRETORIA] {texto_wpp_base}")
            detalhes['envios'].append({'tipo': 'wpp', 'para': f'diretor:{did}', 'ok': ok})
        if send_email and dire.get('email'):
            ok, msg = enviar_email(dire['email'], f"[DIRETORIA] {assunto}", corpo_email_html)
            detalhes['envios'].append({'tipo': 'email', 'para': f'diretor:{did}', 'ok': ok})

    adb.registrar_disparo(alarme['id'], numero_ciclo, status_atual,
                          wpp_ger_ok, email_ger_ok, detalhes)
    adb.registrar_auditoria(None, 'DISPARO', 'alarme', alarme['id'], detalhes)
    log.info('Alarme %d "%s" disparado: status=%s ciclo=%d envios=%d',
             alarme['id'], alarme['nome'], status_atual, numero_ciclo, len(detalhes['envios']))


def main():
    adb.init_db()
    alarmes = adb.listar_alarmes(ativo=True)
    log.info('%d alarme(s) ativo(s) para verificar', len(alarmes))
    for a in alarmes:
        if deve_disparar_agora(a):
            a_full = adb.get_alarme(a['id'])
            if a_full:
                try:
                    disparar(a_full)
                except Exception as exc:
                    log.error('Erro ao disparar alarme %d: %s', a['id'], exc)
        else:
            log.debug('Alarme %d: não é hora (%s / dias %s)',
                      a['id'], a.get('hora_disparo'), a.get('dias_semana'))


if __name__ == '__main__':
    main()
