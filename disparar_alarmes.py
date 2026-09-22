#!/usr/bin/env python3
"""
disparar_alarmes.py
Dispatcher de alarmes CAMIM — verificação e disparo de notificações.

Executado pelo cron a cada minuto (verifica se algum alarme está agendado para o minuto atual).

Cron (cada minuto):
  * * * * * /opt/relatorio_h_t/.venv/bin/python /opt/relatorio_h_t/disparar_alarmes.py \
    >> /opt/relatorio_h_t/logs/alarmes.log 2>&1

Mensagem (reescrita em 2026-09-22, caso do posto D): chama o gestor pelo nome,
diz QUAL campanha/serviço, há QUANTOS dias, desde QUANDO, e — para o serviço
wpp_campanha — o que o robô ENCONTROU (rodou? achou clientes? por que não
enviou?), as CONDIÇÕES de disparo para conferir no contas a receber e O QUE
FAZER. Diagnóstico em wpp_diagnostico.py, lido do batimento que o próprio
cron grava (robo_rodadas). Se a rodada de hoje ainda não chegou ao posto, o
alarme ESPERA (alarme_espera) até ela passar, em vez de acusar em falso.
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

try:
    import wpp_diagnostico as wd
except Exception as _e:  # sem o módulo o alarme continua saindo, só genérico
    wd = None
    logging.getLogger(__name__).error('wpp_diagnostico indisponível: %s', _e)

# Quanto tempo o alarme wpp_campanha espera a rodada do robô chegar ao posto
# antes de disparar assim mesmo (a rodada das 08:00 leva ~1h até o Couto).
ESPERA_RODADA_MAX_MIN = int(os.getenv('ALARME_ESPERA_RODADA_MIN', '150'))

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


def ultimo_push(posto):
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
            return None
        cols = [r[1] for r in conn.execute(f'PRAGMA table_info({tabela})').fetchall()]
        col_data  = next((c for c in cols if any(k in c.lower() for k in ('data', 'hora', 'created', 'time', 'sent'))), None)
        col_posto = next((c for c in cols if 'posto' in c.lower()), None)
        col_modo  = next((c for c in cols if 'modo' in c.lower()), None)
        if not col_data or not col_posto:
            conn.close()
            return None
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
        return row[0] if row else None
    except Exception as e:
        log.warning('ultimo_push(%s): %s', posto, e)
        raise


def status_push(posto):
    try:
        return _dias_para_status(_ultimo_para_dias(ultimo_push(posto)))
    except Exception:
        return 'horrivel'


def ultimo_email(posto):
    kpi_db = os.getenv('KPI_DB_PATH', '/opt/relatorio_h_t/camim_kpi.db')
    conn = sqlite3.connect(f'file:{kpi_db}?mode=ro', uri=True)
    try:
        row = conn.execute(
            "SELECT MAX(datahora) FROM ind_email WHERE posto=? AND titulo_categoria='Boleto'",
            (posto,)
        ).fetchone()
    finally:
        conn.close()
    return row[0] if row else None


def status_email(posto):
    try:
        return _dias_para_status(_ultimo_para_dias(ultimo_email(posto)))
    except Exception as e:
        log.warning('status_email(%s): %s', posto, e)
        return 'horrivel'


def ultimo_tef(posto):
    kpi_db = os.getenv('KPI_DB_PATH', '/opt/relatorio_h_t/camim_kpi.db')
    conn = sqlite3.connect(f'file:{kpi_db}?mode=ro', uri=True)
    try:
        row = conn.execute(
            "SELECT MAX(datahora) FROM ind_tef WHERE posto=?", (posto,)
        ).fetchone()
    finally:
        conn.close()
    return row[0] if row else None


def status_tef(posto):
    try:
        return _dias_para_status(_ultimo_para_dias(ultimo_tef(posto)))
    except Exception as e:
        log.warning('status_tef(%s): %s', posto, e)
        return 'horrivel'


def ultimo_wpp(posto):
    wpp_db = os.getenv('WAPP_CTRL_DB', '/opt/camim-auth/whatsapp_cobranca.db')
    conn = sqlite3.connect(f'file:{wpp_db}?mode=ro', uri=True)
    try:
        row = conn.execute("""
            SELECT MAX(e.enviado_em)
            FROM envios e
            JOIN campanhas c ON c.id = e.campanha_id
            WHERE c.ativa=1 AND e.posto=? AND e.status='accepted'
        """, (posto,)).fetchone()
    finally:
        conn.close()
    return row[0] if row else None


def status_wpp(posto):
    try:
        return _dias_para_status(_ultimo_para_dias(ultimo_wpp(posto)))
    except Exception as e:
        log.warning('status_wpp(%s): %s', posto, e)
        return 'horrivel'


def ultimo_registro(servico, posto):
    """Última atividade registrada do serviço no posto (string ISO) ou None.
    Só para a mensagem dizer 'desde quando'; erro aqui vira None."""
    fn = {'push': ultimo_push, 'email': ultimo_email, 'tef': ultimo_tef, 'wpp': ultimo_wpp}
    try:
        return fn[servico](posto) if servico in fn else None
    except Exception:
        return None


def status_wpp_campanha(posto):
    """Pior campanha ATIVA de cobrança que inclui o posto — staleness do
    último envio accepted DAQUELA campanha NAQUELE posto.

    Diferença para status_wpp (que olha o último envio do posto em QUALQUER
    campanha): pega o padrão do incidente de jun-ago/2026, quando a campanha
    1 enviava todo dia (posto parecia saudável) e as demais ficaram 59 dias
    mudas. Campanha falta_medico fica fora (dispara via API, não pelo cron).
    Posto fora de toda campanha ativa → 'otimo' (não há o que cobrar)."""
    wpp_db = os.getenv('WAPP_CTRL_DB', '/opt/camim-auth/whatsapp_cobranca.db')
    try:
        conn = sqlite3.connect(f'file:{wpp_db}?mode=ro', uri=True)
        try:
            camps = conn.execute(
                "SELECT id, postos, modo_envio FROM campanhas WHERE ativa=1"
            ).fetchall()
            pior = None
            for cid, postos_json, modo in camps:
                if (modo or '').strip().lower() == 'falta_medico':
                    continue
                try:
                    postos_c = json.loads(postos_json or '[]')
                except Exception:
                    postos_c = []
                if posto not in postos_c:
                    continue
                row = conn.execute(
                    "SELECT MAX(enviado_em) FROM envios "
                    "WHERE campanha_id=? AND posto=? AND status LIKE 'accepted%'",
                    (cid, posto)).fetchone()
                dias = _ultimo_para_dias(row[0] if row else None)
                dias = 999 if dias is None else dias
                pior = dias if pior is None else max(pior, dias)
        finally:
            conn.close()
        if pior is None:
            return 'otimo'
        return _dias_para_status(pior)
    except Exception as e:
        log.warning('status_wpp_campanha(%s): %s', posto, e)
        return 'horrivel'


def get_status(servico, posto):
    fn = {'push': status_push, 'email': status_email, 'tef': status_tef,
          'wpp': status_wpp, 'wpp_campanha': status_wpp_campanha}
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


def _quando(ultimo_str):
    """'quinta 17/09 às 08:59' / 'hoje às 08:46' / 'nunca' a partir da string ISO."""
    if not ultimo_str:
        return 'nunca'
    try:
        dt = datetime.fromisoformat(str(ultimo_str)).replace(tzinfo=None)
    except Exception:
        return str(ultimo_str)
    hoje = date.today()
    nomes = ['segunda', 'terça', 'quarta', 'quinta', 'sexta', 'sábado', 'domingo']
    if dt.date() == hoje:
        return f'hoje às {dt:%H:%M}'
    if dt.date() == hoje - timedelta(days=1):
        return f'ontem ({nomes[dt.weekday()]} {dt:%d/%m}) às {dt:%H:%M}'
    return f'{nomes[dt.weekday()]} {dt:%d/%m/%Y} às {dt:%H:%M}'


def _saudacao(gerente, posto):
    nome = ((gerente or {}).get('nome') or '').strip()
    return nome.split()[0].capitalize() if nome else f'Gestor(a) do posto {posto}'


# "O que fazer" por serviço quando não há diagnóstico automático. Frases de
# balcão: quem lê é o gestor do posto, não o TI.
ACOES_GENERICAS = {
    'push':  'Acione o TI para verificar se o programa de push de cobrança do posto está rodando '
             'no servidor. Enquanto isso, os clientes não recebem a notificação de cobrança no app.',
    'email': 'Acione o TI para verificar o robô de boleto por e-mail do posto (servidor/serviço de '
             'e-mail). Enquanto isso, os clientes não recebem o boleto por e-mail.',
    'tef':   'Acione o TI e o financeiro: a cobrança automática no cartão (TEF recorrente) pode ter '
             'parado. Confira no contas a receber se as recorrências do posto estão sendo lançadas.',
    'wpp':   'Acione o TI para verificar o robô de WhatsApp. Confira também no painel de campanhas '
             'se as campanhas do posto estão ativas.',
}

_DIAS_LONGO = ['segunda-feira', 'terça-feira', 'quarta-feira', 'quinta-feira',
               'sexta-feira', 'sábado', 'domingo']


def _mensagens_genericas(alarme, status_atual, gerente):
    """Texto (WhatsApp) e HTML (e-mail) para push/email/tef/wpp — e fallback
    do wpp_campanha quando o diagnóstico não está disponível."""
    import html as _h
    posto = alarme['posto']
    posto_nome = adb.POSTOS_NOMES.get(posto, posto)
    servico_nome = adb.SERVICOS.get(alarme['servico'], alarme['servico'])
    sname = adb.STATUS_LABELS.get(status_atual, status_atual)
    ultimo = ultimo_registro(alarme['servico'], posto)
    dias = _ultimo_para_dias(ultimo)
    agora = datetime.now()
    quem = _saudacao(gerente, posto)
    if ultimo and dias < 999:
        fato = (f'{quem}, o serviço *{servico_nome}* do posto {posto} não registra atividade há '
                f'*{dias} dia{"s" if dias != 1 else ""}*. A última foi {_quando(ultimo)}. '
                f'No painel isso aparece como *{sname}*.')
    else:
        fato = (f'{quem}, o serviço *{servico_nome}* do posto {posto} não tem NENHUMA atividade '
                f'registrada. No painel isso aparece como *{sname}*.')
    explica = (alarme.get('mensagem') or '').strip()
    acao = ACOES_GENERICAS.get(alarme['servico'], 'Acione o TI para verificar o serviço.')
    cab = f'⚠️ ALERTA CAMIM — Posto {posto} ({posto_nome})'
    data = f'{_DIAS_LONGO[agora.weekday()]}, {agora:%d/%m/%Y} às {agora:%H:%M}'

    def texto(papel='gerente'):
        out = [cab, data]
        if papel != 'gerente':
            rot = {'diretor': 'DIRETORIA', 'auditor': 'AUDITORIA', 'extra': 'CÓPIA'}.get(papel, papel.upper())
            ger = ((gerente or {}).get('nome') or '').strip() or f'gestor(a) do posto {posto}'
            out.append(f'[{rot}] Cópia do aviso enviado a {ger}.')
        out += ['', fato, '']
        if explica:
            out += ['*O que isso significa:*', explica, '']
        out += ['👉 *O que fazer:*', acao, '',
                f'🔎 Painel: {APP_URL}/monitorarrobos.html?posto={posto}']
        return '\n'.join(out)

    def html(papel='gerente'):
        import re
        def f(t):
            return re.sub(r'\*([^*]+)\*', r'<b>\1</b>', _h.escape(t))
        copia = ''
        if papel != 'gerente':
            ger = _h.escape(((gerente or {}).get('nome') or '').strip() or f'gestor(a) do posto {posto}')
            copia = f'<p style="color:#666;font-size:13px;margin:0 0 14px">Cópia do aviso enviado a <b>{ger}</b>.</p>'
        bloco_explica = (f'<h3 style="font-size:15px;margin:18px 0 6px">O que isso significa</h3>'
                         f'<p style="margin:0 0 8px;line-height:1.45">{f(explica)}</p>') if explica else ''
        return f"""<!doctype html>
<html lang="pt-br"><body style="margin:0;padding:0;background:#f4f4f4;font-family:sans-serif">
<table width="100%" cellpadding="0" cellspacing="0" style="padding:32px 16px"><tr><td align="center">
<table width="620" cellpadding="0" cellspacing="0"
       style="background:#fff;border-radius:10px;overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,.10)">
  <tr><td style="background:#c62828;padding:18px 32px">
    <h2 style="margin:0;color:#fff;font-size:19px">&#9888; Posto {posto} &mdash; {_h.escape(posto_nome)}</h2>
    <div style="color:#fff;font-size:12px;opacity:.9;margin-top:4px">{_h.escape(alarme['nome'])} &middot; {_h.escape(data)}</div>
  </td></tr>
  <tr><td style="padding:22px 32px;font-size:14px;color:#222">
    {copia}
    <p style="margin:0 0 8px;line-height:1.45">{f(fato)}</p>
    {bloco_explica}
    <h3 style="font-size:15px;margin:18px 0 6px">O que fazer</h3>
    <p style="margin:0 0 8px;line-height:1.45">{f(acao)}</p>
    <p style="margin:22px 0 0">
      <a href="{APP_URL}/monitorarrobos.html?posto={posto}"
         style="background:#1565c0;color:#fff;padding:11px 22px;border-radius:6px;text-decoration:none;font-weight:600;display:inline-block">
        Abrir o painel Monitorar Rob&ocirc;s</a></p>
  </td></tr>
  <tr><td style="background:#f8f8f8;padding:12px 32px;border-top:1px solid #eee">
    <p style="margin:0;color:#aaa;font-size:11px">CAMIM &mdash; Sistema de Alertas Operacionais</p>
  </td></tr>
</table></td></tr></table></body></html>"""

    resumo = (f'{servico_nome}: {dias} dias sem atividade' if dias < 999
              else f'{servico_nome}: sem atividade registrada')
    return {'texto': texto, 'html': html, 'resumo': resumo, 'diagnostico': None}


def _mensagens_wpp_campanha(alarme, gerente, dp):
    """Mensagens do wpp_campanha a partir do diagnóstico (wpp_diagnostico)."""
    posto = alarme['posto']
    posto_nome = adb.POSTOS_NOMES.get(posto, posto)
    nome_ger = (gerente or {}).get('nome')

    def texto(papel='gerente'):
        return wd.texto_whatsapp(dp, nome_ger, posto_nome, APP_URL, papel=papel)

    def html(papel='gerente'):
        return wd.html_email(dp, nome_ger, posto_nome, APP_URL, alarme['nome'], papel=papel)

    return {'texto': texto, 'html': html, 'resumo': wd.resumo_curto(dp),
            'diagnostico': wd.compactar(dp)}


# ── Lógica de disparo ─────────────────────────────────────────────────────────

def deve_disparar_agora(alarme):
    agora = datetime.now()
    hora_atual = agora.strftime('%H:%M')
    dia_atual  = str(agora.weekday())  # 0=Seg, 6=Dom
    dias_conf  = [d.strip() for d in (alarme.get('dias_semana') or '0,1,2,3,4').split(',')]
    return hora_atual == (alarme.get('hora_disparo') or '08:00') and dia_atual in dias_conf


def disparar(alarme, origem='hora'):
    """origem='hora': chamado no minuto de hora_disparo. origem='espera':
    reavaliação de um alarme que ficou aguardando a rodada do robô."""
    posto        = alarme['posto']
    status_atual = get_status(alarme['servico'], posto)
    espera       = adb.get_espera(alarme['id'])

    if not adb.status_igual_ou_pior(status_atual, alarme['status_gatilho']):
        if espera:
            adb.limpar_espera(alarme['id'])
            log.info('Alarme %d: a rodada do robô resolveu (status=%s) — espera encerrada sem disparo',
                     alarme['id'], status_atual)
        else:
            log.info('Alarme %d: status=%s gatilho=%s — não dispara',
                     alarme['id'], status_atual, alarme['status_gatilho'])
        return

    if adb.esta_silenciado(alarme['id']):
        if espera:
            adb.limpar_espera(alarme['id'])
        log.info('Alarme %d: silenciado — pulando', alarme['id'])
        return

    gerente = adb.get_gerente(posto)

    # ─ Diagnóstico + espera pela rodada (só wpp_campanha)
    dp = None
    if alarme['servico'] == 'wpp_campanha' and wd is not None:
        try:
            dp_leve = wd.diagnostico_posto(posto, consultar_sql=False)
        except Exception as e:
            log.error('Alarme %d: diagnóstico falhou (%s) — mensagem genérica', alarme['id'], e)
            dp_leve = None
        if dp_leve and dp_leve.get('aguardar_rodada'):
            agora = datetime.now()
            if not espera:
                ate = agora + timedelta(minutes=ESPERA_RODADA_MAX_MIN)
                adb.registrar_espera(alarme['id'], ate,
                                     'rodada do robô em andamento ainda não chegou ao posto')
                log.info('Alarme %d: rodada em andamento ainda não chegou ao posto %s — '
                         'aguardando até %s', alarme['id'], posto, ate.strftime('%H:%M'))
                return
            try:
                ate = datetime.fromisoformat(str(espera['ate']))
            except Exception:
                ate = agora
            if agora < ate:
                log.debug('Alarme %d: ainda aguardando a rodada (até %s)', alarme['id'], espera['ate'])
                return
            log.warning('Alarme %d: rodada não chegou ao posto %s em %d min — disparando assim mesmo',
                        alarme['id'], posto, ESPERA_RODADA_MAX_MIN)
        if dp_leve is not None:
            try:
                dp = wd.diagnostico_posto(posto, consultar_sql=True)
            except Exception as e:
                log.error('Alarme %d: diagnóstico completo falhou (%s) — usando o leve', alarme['id'], e)
                dp = dp_leve
    if espera:
        adb.limpar_espera(alarme['id'])

    if dp is not None and dp.get('paradas'):
        msgs = _mensagens_wpp_campanha(alarme, gerente, dp)
    else:
        msgs = _mensagens_genericas(alarme, status_atual, gerente)

    numero_ciclo = adb.get_numero_ciclo(alarme['id'])
    sname        = adb.STATUS_LABELS.get(status_atual, status_atual)
    if dp is not None and dp.get('paradas'):
        grav = {'critico': 'DEFEITO', 'atencao': 'ATENÇÃO', 'normal': 'robô ok, sem clientes'}[dp['gravidade']]
        assunto = f"[ALERTA CAMIM] Posto {posto} — WhatsApp {sname} — {grav}"
    else:
        assunto = f"[ALERTA CAMIM] {alarme['nome']} — Posto {posto} — {sname}"

    texto_gerente = msgs['texto']('gerente')
    detalhes = {'status_atual': status_atual, 'numero_ciclo': numero_ciclo, 'origem': origem,
                'resumo': msgs['resumo'], 'diagnostico': msgs['diagnostico'],
                'texto_enviado': texto_gerente, 'envios': []}
    wpp_ger_ok   = False
    email_ger_ok = False

    # Pré-registra o disparo para amarrar as ciências (1 token por
    # destinatário×canal). Central de Notificação de Problemas, 2026-08-10.
    disparo_id = adb.registrar_disparo(alarme['id'], numero_ciclo, status_atual,
                                       False, False, {'status': 'enviando'})

    def _ciencia_wpp(dest_tipo, nome, email_d, tel):
        tok = adb.criar_ciencia(disparo_id, alarme['id'], dest_tipo, nome,
                                email_d, tel, 'wpp')
        return (f"\n\n✅ *Confirme que está ciente deste aviso:*\n"
                f"{APP_URL}/ciencia/{tok}")

    def _ciencia_email(dest_tipo, nome, email_d, tel):
        tok = adb.criar_ciencia(disparo_id, alarme['id'], dest_tipo, nome,
                                email_d, tel, 'email')
        return (f'<p style="margin:18px 0"><a href="{APP_URL}/ciencia/{tok}" '
                f'style="background:#28a745;color:#fff;padding:10px 18px;'
                f'border-radius:6px;text-decoration:none;font-weight:bold">'
                f'✅ Confirmar ciência do aviso</a></p>')

    def _enviar(papel, nome, email_d, tel, mandar_wpp, mandar_email, rotulo):
        """Envia a versão do texto para o papel e registra no detalhes."""
        okw = oke = False
        if mandar_wpp and tel:
            okw, msg = enviar_wpp(tel, msgs['texto'](papel) + _ciencia_wpp(papel, nome, email_d, tel))
            detalhes['envios'].append({'tipo': 'wpp', 'para': rotulo, 'ok': okw, 'msg': msg})
            log.info('WPP %s %s: ok=%s msg=%s', rotulo, tel, okw, msg)
        if mandar_email and email_d:
            pref = '' if papel == 'gerente' else f"[{ {'diretor': 'DIRETORIA', 'auditor': 'AUDITORIA'}.get(papel, 'CÓPIA') }] "
            oke, msg = enviar_email(email_d, pref + assunto,
                                    msgs['html'](papel) + _ciencia_email(papel, nome, email_d, tel))
            detalhes['envios'].append({'tipo': 'email', 'para': rotulo, 'ok': oke, 'msg': msg})
            log.info('Email %s %s: ok=%s msg=%s', rotulo, email_d, oke, msg)
        return okw, oke

    # ─ Gerente do posto (banco, não removível)
    if gerente:
        nome_ger = (gerente.get('nome') or '').strip() or f"Gerente posto {posto}"
        wpp_ger_ok, email_ger_ok = _enviar(
            'gerente', nome_ger, gerente.get('email'), gerente.get('telefone'),
            bool(alarme['via_whatsapp']), bool(alarme['via_email']), 'gerente')

    # ─ Gerentes extras
    for extra in (alarme.get('extras') or []):
        nome_ex = extra.get('nome') or f"extra:{extra['id']}"
        _enviar('extra', nome_ex, extra.get('email'), extra.get('telefone'),
                bool(extra.get('via_whatsapp') and alarme['via_whatsapp']),
                bool(extra.get('via_email') and alarme['via_email']), f'extra:{extra["id"]}')

    # ─ Auditores (ciclo 1 = prefs deles; ciclo 2+ = ambos os canais)
    for aid in (alarme.get('auditores') or []):
        aud = adb.get_auditor(aid)
        if not aud:
            continue
        if numero_ciclo == 1:
            send_wpp, send_email = bool(aud.get('recebe_1_wpp')), bool(aud.get('recebe_1_email'))
        else:
            send_wpp = send_email = True
        _enviar('auditor', aud.get('nome') or f"auditor:{aid}", aud.get('email'), aud.get('telefone'),
                send_wpp, send_email, f'auditor:{aid}')

    # ─ Diretores (mesma lógica de ciclos)
    for did in (alarme.get('diretores') or []):
        dire = adb.get_diretor(did)
        if not dire:
            continue
        if numero_ciclo == 1:
            send_wpp, send_email = bool(dire.get('recebe_1_wpp')), bool(dire.get('recebe_1_email'))
        else:
            send_wpp = send_email = True
        _enviar('diretor', dire.get('nome') or f"diretor:{did}", dire.get('email'), dire.get('telefone'),
                send_wpp, send_email, f'diretor:{did}')

    adb.atualizar_disparo(disparo_id, wpp_ger_ok, email_ger_ok, detalhes)
    adb.registrar_auditoria(None, 'DISPARO', 'alarme', alarme['id'],
                            {k: v for k, v in detalhes.items() if k not in ('texto_enviado', 'diagnostico')})
    log.info('Alarme %d "%s" disparado (%s): status=%s ciclo=%d envios=%d — %s',
             alarme['id'], alarme['nome'], origem, status_atual, numero_ciclo,
             len(detalhes['envios']), msgs['resumo'])


def main():
    adb.init_db()
    alarmes = adb.listar_alarmes(ativo=True)
    log.info('%d alarme(s) ativo(s) para verificar', len(alarmes))
    em_espera = {e['alarme_id'] for e in adb.listar_esperas()}
    for a in alarmes:
        if deve_disparar_agora(a):
            origem = 'hora'
        elif a['id'] in em_espera:
            origem = 'espera'   # aguardando a rodada do robô chegar ao posto
        else:
            log.debug('Alarme %d: não é hora (%s / dias %s)',
                      a['id'], a.get('hora_disparo'), a.get('dias_semana'))
            continue
        a_full = adb.get_alarme(a['id'])
        if a_full:
            try:
                disparar(a_full, origem)
            except Exception as exc:
                log.error('Erro ao disparar alarme %d: %s', a['id'], exc)


if __name__ == '__main__':
    main()
