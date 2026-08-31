#!/usr/bin/env python3
"""
relatorio_nf.py — "NF emitidas × meta" em IMAGEM para o celular (Notas x RPS)

Gera um PNG no formato de tela de celular com a tabela de notas emitidas por
posto/empresa (grupo Clínicas, mês corrente) colorida contra a meta mensal de
NF de cada CNPJ (`objetivos` do JSON do export_notas_rps):

    ≤ 80 % da meta  → vermelho
    81 % – 99 %     → amarelo
    > 99 %          → verde

e envia por WhatsApp (Evolution API, texto livre — custo zero, mesma instância
dos alarmes) e por e-mail (ALARM_EMAIL_*) para os destinatários configurados.

Toda mensagem leva um LINK assinado (itsdangerous + SECRET_KEY). Quem toca no
link recebe na hora "⏳ Espere, atualizando os dados…", o pedido cai numa FILA
(spool) e o cron de root processa em até 1 min: roda o ETL, gera a imagem nova
e manda mensagem nova — com link novo.

Por que fila e não envio direto pelo Flask: o camim-auth roda como www-data e
não pode escrever em json_notas_rps (root:deploy); o ETL precisa de root.

Fonte dos dados: json_notas_rps/<posto>_notas_rps_<AAAA-MM>.json (o mesmo que
a página kpi_notas_rps.html lê). A agregação replica o modo "Clínicas" da
página: tudo que NÃO é operadora, agrupado por posto+empresa, ordenado por
valor emitido desc.

% da meta = NF CONTABILIZADAS (emitidas − canceladas) ÷ meta. Nota cancelada
não conta para o teto do CNPJ.

Uso:
    python relatorio_nf.py --preview /tmp/nf.png          # só gera a imagem
    python relatorio_nf.py --enviar --para all            # DRY-RUN (não envia)
    python relatorio_nf.py --enviar --para all --run      # envia de verdade
    python relatorio_nf.py --enviar --para all --atualizar --run   # roda o ETL antes
    python relatorio_nf.py --spool --run                  # processa a fila (cron 1/min)

Kill-switches (explícitos, de propósito):
    - sem --run nada é enviado (só imprime o que faria);
    - RELATORIO_NF_ENVIO=0 no .env desliga o envio mesmo com --run.
"""
from __future__ import annotations

import argparse
import base64
import glob
import io
import json
import logging
import os
import re
import smtplib
import subprocess
import sys
import time
import uuid
from datetime import date, datetime
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

try:
    from dotenv import load_dotenv
    if os.path.exists('/opt/relatorio_h_t/.env'):
        load_dotenv('/opt/relatorio_h_t/.env')
except Exception:  # pragma: no cover
    pass

log = logging.getLogger('relatorio_nf')

# ── Configuração ─────────────────────────────────────────────────────────────

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_JSON_DIR_PADRAO = '/opt/relatorio_h_t/json_notas_rps'
JSON_DIR = os.getenv('NOTAS_RPS_JSON_DIR') or (
    _JSON_DIR_PADRAO if os.path.isdir(_JSON_DIR_PADRAO) else os.path.join(BASE_DIR, 'json_notas_rps'))

# Fila de pedidos. /opt/camim-auth é deploy:deploy 775 e o www-data está no
# grupo deploy — o Flask consegue criar o subdiretório e gravar; o root lê.
SPOOL_DIR = os.getenv('RELATORIO_NF_SPOOL', '/opt/camim-auth/relatorio_nf_spool')

ETL_SH   = '/opt/relatorio_h_t/export_notas_rps.sh'
ETL_LOCK = '/opt/relatorio_h_t/locks/export_notas_rps.lock'

APP_URL    = (os.getenv('APP_BASE_URL') or 'https://kpi.camim.com.br').rstrip('/')
PAGINA_URL = f'{APP_URL}/kpi_notas_rps.html'
# Segredo do link. Dedicado (RELATORIO_NF_SECRET no .env de /opt/relatorio_h_t,
# que o camim-auth também carrega) — o SECRET_KEY "do Flask" não existe em
# .env nenhum da VM (medido 2026-08-31). Cron e Flask PRECISAM ler o mesmo valor.
SECRET_KEY = os.getenv('RELATORIO_NF_SECRET') or os.getenv('SECRET_KEY', '')

EVOLUTION_BASE_URL = (os.getenv('EVOLUTION_BASE_URL') or '').rstrip('/')
EVOLUTION_API_KEY  = os.getenv('EVOLUTION_API_KEY', '')
EVOLUTION_INSTANCE = os.getenv('EVOLUTION_INSTANCE', '')

EMAIL_HOST     = os.getenv('ALARM_EMAIL_HOST', 'smtp.gmail.com')
EMAIL_PORT     = int(os.getenv('ALARM_EMAIL_PORT', '465') or 465)
EMAIL_USER     = os.getenv('ALARM_EMAIL_USER', '')
EMAIL_PASSWORD = os.getenv('ALARM_EMAIL_PASSWORD', '')
EMAIL_FROM     = os.getenv('ALARM_EMAIL_FROM', '') or EMAIL_USER

ENVIO_LIGADO = (os.getenv('RELATORIO_NF_ENVIO', '1').strip() not in ('0', 'false', 'off', 'nao', 'não'))

# Validade do link (dias). Link velho continua abrindo até expirar — quem toca
# recebe o dado de HOJE, não o de quando o link foi gerado.
TOKEN_MAX_AGE = int(os.getenv('RELATORIO_NF_TOKEN_DIAS', '90')) * 86400

# Anti-loop / anti-abuso do link público: cooldown entre pedidos do mesmo
# destinatário e teto diário. Não custa dinheiro (Evolution é texto livre),
# mas um scanner de link ou um toque duplo não pode virar rajada.
COOLDOWN_SEG = 90
TETO_DIA_POR_DEST = 20

# Destinatários. Formato do env RELATORIO_NF_DESTINATARIOS:
#   id:Nome:email:telefone;id2:Nome 2:email2:telefone2
# Telefone vazio = só e-mail (o zap é pulado e o log avisa).
_DEST_PADRAO = (
    'cristiano:Cristiano:cristiano@camim.com.br:5521994317573;'
    'vinicius:Vinicius Gomes:viniciusgomes@camim.com.br:'
)

FAIXAS = {
    # nome: (hex, rótulo)
    'vermelho': ('#dc2626', 'até 80 %'),
    'amarelo':  ('#d97706', '81 % a 99 %'),
    'verde':    ('#16a34a', 'acima de 99 %'),
    'sem_meta': ('#9ca3af', 'sem meta cadastrada'),
}

MESES = ['', 'Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho', 'Julho',
         'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro']

ORIGEM_TO_OBJ = {
    'Clinica': 'clinica', 'OperadoraCamim': 'camim',
    'OperadoraSDM': 'sdm', 'Laboratorio': 'laboratorio',
}


# ── Destinatários ────────────────────────────────────────────────────────────

def destinatarios() -> dict:
    """{id: {id, nome, email, telefone}} a partir do env (ou padrão)."""
    raw = os.getenv('RELATORIO_NF_DESTINATARIOS', '').strip() or _DEST_PADRAO
    out = {}
    for bloco in raw.split(';'):
        bloco = bloco.strip()
        if not bloco:
            continue
        partes = [p.strip() for p in bloco.split(':')]
        while len(partes) < 4:
            partes.append('')
        did, nome, email, tel = partes[:4]
        if not did:
            continue
        out[did] = {'id': did, 'nome': nome or did, 'email': email,
                    'telefone': _limpar_telefone(tel)}
    return out


def _limpar_telefone(tel: str) -> str:
    d = re.sub(r'\D', '', tel or '')
    if not d:
        return ''
    if len(d) in (10, 11):        # DDD + número, sem DDI
        d = '55' + d
    return d if 12 <= len(d) <= 13 else ''


# ── Dados ────────────────────────────────────────────────────────────────────

def ym_atual() -> str:
    return date.today().strftime('%Y-%m')


def _num(x) -> float:
    try:
        return float(x or 0)
    except (TypeError, ValueError):
        return 0.0


def _eh_clinica(row: dict) -> bool:
    # Mesmo critério do notasFilter() da página no modo "Clínicas":
    # tudo que NÃO é operadora (inclui Laboratório, como lá).
    emp = str(row.get('Empresa') or row.get('empresa') or '')
    return 'OPERADORA' not in emp.upper()


def faixa(pct) -> str:
    if pct is None:
        return 'sem_meta'
    if pct <= 80:
        return 'vermelho'
    if pct <= 99:
        return 'amarelo'
    return 'verde'


def carregar_dados(ym: str | None = None) -> dict:
    """Lê os JSONs de todos os postos do mês e agrega no modo Clínicas."""
    ym = ym or ym_atual()
    linhas = []
    rps_qtd = 0.0
    rps_total = 0.0
    gerados = []
    postos = []
    for path in sorted(glob.glob(os.path.join(JSON_DIR, f'*_notas_rps_{ym}.json'))):
        posto = os.path.basename(path).split('_')[0]
        try:
            with open(path, encoding='utf-8') as f:
                d = json.load(f)
        except Exception as e:
            log.warning('JSON ilegível %s: %s', path, e)
            continue
        postos.append(posto)
        if d.get('gerado_em'):
            try:
                gerados.append(datetime.fromisoformat(d['gerado_em']))
            except ValueError:
                pass
        objetivos = d.get('objetivos') or {}
        por_emp: dict[str, dict] = {}
        for row in d.get('notas_emitidas') or []:
            if not _eh_clinica(row):
                continue
            emp = str(row.get('Empresa') or row.get('empresa') or '')
            cur = por_emp.setdefault(emp, {
                'posto': posto, 'empresa': emp, 'origem': str(row.get('origem') or ''),
                'qtd': 0.0, 'emitidas': 0.0, 'canc_qtd': 0.0, 'canc_val': 0.0,
                'cont_qtd': 0.0, 'cont_val': 0.0, 'objetivo': None,
            })
            cur['qtd']      += _num(row.get('qtd_notas'))
            cur['emitidas'] += _num(row.get('valor_notas_emitidas'))
            cur['canc_qtd'] += _num(row.get('qtd_notas_canceladas'))
            cur['canc_val'] += _num(row.get('valor_notas_canceladas'))
            cur['cont_qtd'] += _num(row.get('qtd_notas_contabilizadas'))
            cur['cont_val'] += _num(row.get('valor_notas_contabilizadas'))
            k = ORIGEM_TO_OBJ.get(str(row.get('origem') or ''))
            obj = objetivos.get(k) if k else None
            if obj is not None:
                cur['objetivo'] = _num(obj)   # teto é constante por (posto, categoria): fixa, não soma
        for cur in por_emp.values():
            obj = cur['objetivo']
            cur['pct'] = (cur['cont_val'] / obj * 100.0) if obj else None
            cur['faixa'] = faixa(cur['pct'])
            linhas.append(cur)
        for row in d.get('rps_pendentes') or []:
            if str(row.get('origem') or '') == 'Clinica':
                rps_qtd   += _num(row.get('qtd'))
                rps_total += _num(row.get('total'))

    linhas.sort(key=lambda r: (-r['emitidas'], r['posto']))
    tot_cont = sum(r['cont_val'] for r in linhas)
    tot_meta = sum(r['objetivo'] or 0 for r in linhas if r['objetivo'])
    tot_emit = sum(r['emitidas'] for r in linhas)
    contagem = {k: 0 for k in FAIXAS}
    for r in linhas:
        contagem[r['faixa']] += 1
    ano, mes = ym.split('-')
    return {
        'ym': ym,
        'mes_nome': f'{MESES[int(mes)]}/{ano}',
        'linhas': linhas,
        'postos': postos,
        'gerado_em': min(gerados) if gerados else None,   # coleta mais ANTIGA (igual à página)
        'totais': {
            'emitidas': tot_emit, 'cont_val': tot_cont, 'meta': tot_meta,
            'pct': (tot_cont / tot_meta * 100.0) if tot_meta else None,
            'rps_qtd': rps_qtd, 'rps_total': rps_total,
        },
        'contagem': contagem,
    }


# ── Formatação ───────────────────────────────────────────────────────────────

def fmt_brl(v: float, centavos: bool = False) -> str:
    if v is None:
        return '—'
    s = f'{v:,.2f}' if centavos else f'{round(v):,.0f}'
    s = s.replace(',', '\x00').replace('.', ',').replace('\x00', '.')
    return f'R$ {s}'


def fmt_int(v: float) -> str:
    return f'{round(v):,.0f}'.replace(',', '.')


def fmt_pct(p) -> str:
    return '—' if p is None else f'{p:.0f}%'


def nome_curto(emp: str) -> str:
    """Tira sufixo societário para caber na largura do celular."""
    s = re.sub(r'\b(LTDA\.?|EIRELI|EIRELE|ME|EPP|S/?A)\b\.?', '', emp, flags=re.I)
    s = re.sub(r'\s{2,}', ' ', s).strip(' -')
    return s or emp


def _quando(dados: dict) -> str:
    g = dados.get('gerado_em')
    return g.strftime('%d/%m às %H:%M') if g else 'horário não informado'


# ── Imagem (Pillow) ──────────────────────────────────────────────────────────

_FONT_DIRS = ['/usr/share/fonts/truetype/dejavu', '/usr/share/fonts/dejavu',
              '/usr/share/fonts/TTF', os.path.join(BASE_DIR, 'fonts')]


def _font(bold: bool, size: int):
    from PIL import ImageFont
    nome = 'DejaVuSans-Bold.ttf' if bold else 'DejaVuSans.ttf'
    for d in _FONT_DIRS:
        p = os.path.join(d, nome)
        if os.path.exists(p):
            return ImageFont.truetype(p, size)
    return ImageFont.load_default()


def _hex(h: str):
    h = h.lstrip('#')
    return tuple(int(h[i:i + 2], 16) for i in (0, 2, 4))


def _cortar(draw, texto: str, font, max_w: int) -> str:
    if draw.textlength(texto, font=font) <= max_w:
        return texto
    while texto and draw.textlength(texto + '…', font=font) > max_w:
        texto = texto[:-1]
    return texto.rstrip() + '…'


def _selo(dr, x, y, w, h, texto, font, cor):
    """Pílula colorida com o texto centralizado; encolhe a fonte se não couber."""
    dr.rounded_rectangle([x, y, x + w, y + h], radius=h // 2, fill=cor)
    f = font
    while dr.textlength(texto, font=f) > w - 16 and f.size > 16:
        f = _font(True, f.size - 2)
    tw = dr.textlength(texto, font=f)
    dr.text((x + (w - tw) / 2, y + (h - f.size) / 2 - 3), texto, font=f, fill=(255, 255, 255))


def render_png(dados: dict) -> bytes:
    """Tabela em retrato, 1080 px de largura — cabe na tela do celular sem zoom."""
    from PIL import Image, ImageDraw

    W = 1080
    PAD = 36
    ROW_H = 84
    linhas = dados['linhas']
    tot = dados['totais']

    f_tit  = _font(True, 46)
    f_sub  = _font(False, 27)
    f_hdr  = _font(True, 21)
    f_post = _font(True, 34)
    f_emp  = _font(False, 25)
    f_val  = _font(True, 29)
    f_meta = _font(False, 23)
    f_pct  = _font(True, 27)
    f_rod  = _font(False, 23)
    f_leg  = _font(True, 23)

    top_h = 200            # título + subtítulo + chips
    hdr_h = 56
    tot_h = 96
    foot_h = 170
    H = PAD + top_h + hdr_h + ROW_H * max(1, len(linhas)) + tot_h + foot_h + PAD

    img = Image.new('RGB', (W, H), _hex('#eef1f5'))
    dr = ImageDraw.Draw(img)

    # cartão
    dr.rounded_rectangle([PAD // 2, PAD // 2, W - PAD // 2, H - PAD // 2], radius=28, fill=(255, 255, 255))

    y = PAD + 10
    dr.text((PAD, y), 'NF emitidas × meta', font=f_tit, fill=_hex('#111827'))
    y += 60
    dr.text((PAD, y), f"{dados['mes_nome']} · Clínicas · dados de {_quando(dados)}", font=f_sub, fill=_hex('#6b7280'))
    y += 48

    # chips de contagem por faixa
    x = PAD
    for k in ('verde', 'amarelo', 'vermelho', 'sem_meta'):
        n = dados['contagem'].get(k, 0)
        if k == 'sem_meta' and n == 0:
            continue
        cor = _hex(FAIXAS[k][0])
        txt = f'{n} {FAIXAS[k][1]}' if k != 'sem_meta' else f'{n} sem meta'
        tw = dr.textlength(txt, font=f_leg)
        dr.rounded_rectangle([x, y, x + tw + 56, y + 44], radius=22, fill=_hex('#f3f4f6'))
        dr.ellipse([x + 14, y + 12, x + 34, y + 32], fill=cor)
        dr.text((x + 44, y + 9), txt, font=f_leg, fill=_hex('#374151'))
        x += tw + 70
    y += 70

    # cabeçalho da tabela
    X_POSTO = PAD + 20
    X_EMP   = PAD + 104
    X_VAL_R = 782          # borda direita "contabilizado"
    X_META_R = 926         # borda direita "meta"
    X_PCT   = 940          # início do selo
    PCT_W   = W - PAD - X_PCT
    EMP_W   = X_VAL_R - 190 - X_EMP   # valor por linha fica < R$ 1 mi (~170 px)

    dr.rectangle([PAD, y, W - PAD, y + hdr_h], fill=_hex('#f9fafb'))
    hy = y + 18
    dr.text((X_POSTO, hy), 'POSTO', font=f_hdr, fill=_hex('#6b7280'))
    dr.text((X_EMP, hy), 'EMPRESA', font=f_hdr, fill=_hex('#6b7280'))
    for txt, xr in (('CONTABILIZADO', X_VAL_R), ('META', X_META_R)):
        dr.text((xr - dr.textlength(txt, font=f_hdr), hy), txt, font=f_hdr, fill=_hex('#6b7280'))
    dr.text((X_PCT + (PCT_W - dr.textlength('% META', font=f_hdr)) / 2, hy), '% META', font=f_hdr, fill=_hex('#6b7280'))
    y += hdr_h

    if not linhas:
        dr.text((X_EMP, y + 24), 'Nenhuma nota encontrada para o mês.', font=f_emp, fill=_hex('#6b7280'))
        y += ROW_H

    for i, r in enumerate(linhas):
        cor = _hex(FAIXAS[r['faixa']][0])
        if i % 2:
            dr.rectangle([PAD, y, W - PAD, y + ROW_H], fill=_hex('#fafafa'))
        dr.rectangle([PAD, y + 8, PAD + 10, y + ROW_H - 8], fill=cor)      # faixa lateral
        dr.text((X_POSTO, y + 20), r['posto'], font=f_post, fill=_hex('#111827'))
        dr.text((X_EMP, y + 14), _cortar(dr, nome_curto(r['empresa']), f_emp, EMP_W), font=f_emp, fill=_hex('#111827'))
        sub = f"{fmt_int(r['cont_qtd'])} NF · emit. {fmt_brl(r['emitidas'])}"
        if r['canc_qtd']:
            sub += f" · {fmt_int(r['canc_qtd'])} canc."
        dr.text((X_EMP, y + 49), _cortar(dr, sub, f_meta, EMP_W), font=f_meta, fill=_hex('#9ca3af'))
        v = fmt_brl(r['cont_val'])
        dr.text((X_VAL_R - dr.textlength(v, font=f_val), y + 27), v, font=f_val, fill=_hex('#111827'))
        m = fmt_brl(r['objetivo']) if r['objetivo'] else '—'
        dr.text((X_META_R - dr.textlength(m, font=f_meta), y + 30), m, font=f_meta, fill=_hex('#6b7280'))
        _selo(dr, X_PCT, y + 20, PCT_W, ROW_H - 40, fmt_pct(r['pct']), f_pct, cor)
        y += ROW_H

    # totais
    dr.rectangle([PAD, y, W - PAD, y + 2], fill=_hex('#e5e7eb'))
    y += 14
    cor_t = _hex(FAIXAS[faixa(tot['pct'])][0])
    dr.text((X_POSTO, y + 24), 'TOTAL', font=f_hdr, fill=_hex('#374151'))
    dr.text((X_EMP, y + 14), f"{len(linhas)} empresas · {len(dados['postos'])} postos", font=f_emp, fill=_hex('#374151'))
    dr.text((X_EMP, y + 49), _cortar(dr, f"RPS pendentes: {fmt_int(tot['rps_qtd'])} · {fmt_brl(tot['rps_total'])}", f_meta, EMP_W + 40),
            font=f_meta, fill=_hex('#9ca3af'))
    # no total o valor passa de R$ 1 mi e não cabe ao lado da meta: meta vai embaixo
    v = fmt_brl(tot['cont_val'])
    dr.text((X_META_R - dr.textlength(v, font=f_val), y + 12), v, font=f_val, fill=_hex('#111827'))
    m = f"meta {fmt_brl(tot['meta'])}" if tot['meta'] else 'sem meta'
    dr.text((X_META_R - dr.textlength(m, font=f_meta), y + 50), m, font=f_meta, fill=_hex('#6b7280'))
    _selo(dr, X_PCT, y + 20, PCT_W, ROW_H - 40, fmt_pct(tot['pct']), f_pct, cor_t)
    y += tot_h

    # rodapé
    dr.text((PAD, y), '% META = NF contabilizadas (emitidas − canceladas) ÷ meta mensal de NF do CNPJ.', font=f_rod, fill=_hex('#6b7280'))
    y += 36
    x = PAD
    for k in ('vermelho', 'amarelo', 'verde'):
        dr.ellipse([x, y + 4, x + 20, y + 24], fill=_hex(FAIXAS[k][0]))
        txt = FAIXAS[k][1]
        dr.text((x + 30, y), txt, font=f_rod, fill=_hex('#6b7280'))
        x += dr.textlength(txt, font=f_rod) + 70
    y += 40
    dr.text((PAD, y), f'Gerado em {datetime.now():%d/%m/%Y %H:%M} · {PAGINA_URL.replace("https://", "")}', font=f_rod, fill=_hex('#9ca3af'))

    buf = io.BytesIO()
    img.save(buf, format='PNG', optimize=True)
    return buf.getvalue()


# ── Texto das mensagens ──────────────────────────────────────────────────────

def texto_resumo(dados: dict) -> str:
    c = dados['contagem']
    t = dados['totais']
    partes = [f"🟢 {c['verde']} acima de 99%", f"🟡 {c['amarelo']} entre 81 e 99%", f"🔴 {c['vermelho']} até 80%"]
    if c['sem_meta']:
        partes.append(f"⚪ {c['sem_meta']} sem meta")
    return (f"📊 *NF emitidas × meta — {dados['mes_nome']}* (Clínicas)\n"
            f"Dados coletados {_quando(dados)}\n"
            f"{' · '.join(partes)}\n"
            f"Contabilizado: {fmt_brl(t['cont_val'])} de {fmt_brl(t['meta'])} ({fmt_pct(t['pct'])})")


def caption_zap(dados: dict, link: str) -> str:
    return (f"{texto_resumo(dados)}\n\n"
            f"🔄 *Atualizar e receber de novo:*\n{link}\n\n"
            f"📈 Painel completo: {PAGINA_URL}")


def html_email(dados: dict, link: str, nome: str) -> str:
    t = dados['totais']
    trs = []
    for r in dados['linhas']:
        cor = FAIXAS[r['faixa']][0]
        trs.append(
            f"<tr><td style='padding:6px 8px;font-weight:700'>{r['posto']}</td>"
            f"<td style='padding:6px 8px'>{nome_curto(r['empresa'])}</td>"
            f"<td style='padding:6px 8px;text-align:right'>{fmt_brl(r['cont_val'], True)}</td>"
            f"<td style='padding:6px 8px;text-align:right;color:#6b7280'>{fmt_brl(r['objetivo'], True) if r['objetivo'] else '—'}</td>"
            f"<td style='padding:6px 8px;text-align:center'><span style='background:{cor};color:#fff;"
            f"border-radius:12px;padding:2px 10px;font-weight:700'>{fmt_pct(r['pct'])}</span></td></tr>")
    return f"""<!doctype html><html><body style="font-family:Segoe UI,Arial,sans-serif;background:#f4f6f9;margin:0;padding:16px">
<div style="max-width:640px;margin:0 auto;background:#fff;border-radius:12px;padding:20px">
  <h2 style="margin:0 0 4px">📊 NF emitidas × meta — {dados['mes_nome']}</h2>
  <p style="margin:0 0 14px;color:#6b7280">Clínicas · dados coletados {_quando(dados)} · olá, {nome}</p>
  <p><a href="{link}" style="background:#2563eb;color:#fff;text-decoration:none;padding:12px 18px;border-radius:8px;font-weight:700;display:inline-block">🔄 Atualizar e receber de novo</a>
     &nbsp; <a href="{PAGINA_URL}" style="color:#2563eb">Abrir o painel</a></p>
  <img src="cid:relatorio_nf" alt="Relatório NF x meta" style="width:100%;max-width:600px;border-radius:8px;border:1px solid #e5e7eb">
  <h3 style="margin:20px 0 6px;font-size:16px">Mesma tabela em texto</h3>
  <table style="border-collapse:collapse;width:100%;font-size:14px">
    <thead><tr style="background:#f3f4f6;color:#6b7280;font-size:12px">
      <th style="padding:6px 8px;text-align:left">POSTO</th><th style="padding:6px 8px;text-align:left">EMPRESA</th>
      <th style="padding:6px 8px;text-align:right">CONTABILIZADO</th><th style="padding:6px 8px;text-align:right">META</th>
      <th style="padding:6px 8px">% META</th></tr></thead>
    <tbody>{''.join(trs)}
    <tr style="border-top:2px solid #e5e7eb;font-weight:700"><td style="padding:6px 8px" colspan="2">TOTAL · RPS pendentes {fmt_int(t['rps_qtd'])} ({fmt_brl(t['rps_total'], True)})</td>
      <td style="padding:6px 8px;text-align:right">{fmt_brl(t['cont_val'], True)}</td>
      <td style="padding:6px 8px;text-align:right">{fmt_brl(t['meta'], True) if t['meta'] else '—'}</td>
      <td style="padding:6px 8px;text-align:center">{fmt_pct(t['pct'])}</td></tr></tbody>
  </table>
  <p style="color:#9ca3af;font-size:12px;margin-top:16px">% META = NF contabilizadas (emitidas − canceladas) ÷ meta mensal de NF do CNPJ.
  🔴 até 80 % · 🟡 81 % a 99 % · 🟢 acima de 99 %.<br>
  O link acima é pessoal: ao tocar, o robô atualiza os dados e manda uma mensagem nova (WhatsApp e e-mail) com um link novo.</p>
</div></body></html>"""


# ── Token / link ─────────────────────────────────────────────────────────────

def _serializer():
    from itsdangerous import URLSafeTimedSerializer
    if not SECRET_KEY:
        raise RuntimeError('RELATORIO_NF_SECRET ausente em /opt/relatorio_h_t/.env — não dá para assinar o link')
    return URLSafeTimedSerializer(SECRET_KEY, salt='relatorio-nf-v1')


def gerar_link(dest_id: str) -> str:
    # `n` muda a cada emissão → todo envio leva um link diferente do anterior.
    token = _serializer().dumps({'d': dest_id, 'n': uuid.uuid4().hex[:10]})
    return f'{APP_URL}/relatorio_nf/{token}'


def ler_token(token: str) -> str | None:
    """id do destinatário, ou None se inválido/expirado/desconhecido."""
    try:
        from itsdangerous import BadData
        try:
            data = _serializer().loads(token, max_age=TOKEN_MAX_AGE)
        except BadData:
            return None
    except Exception as e:
        log.error('ler_token: %s', e)
        return None
    did = (data or {}).get('d')
    return did if did in destinatarios() else None


# ── Envio ────────────────────────────────────────────────────────────────────

def _evolution_post(rota: str, payload: dict) -> tuple[bool, str]:
    if not (EVOLUTION_BASE_URL and EVOLUTION_API_KEY and EVOLUTION_INSTANCE):
        return False, 'Evolution API não configurada no .env'
    from urllib import request as _req, error as _err
    body = json.dumps(payload).encode('utf-8')
    req = _req.Request(f'{EVOLUTION_BASE_URL}/message/{rota}/{EVOLUTION_INSTANCE}', data=body, method='POST',
                       headers={'apikey': EVOLUTION_API_KEY, 'Content-Type': 'application/json'})
    try:
        with _req.urlopen(req, timeout=60) as r:
            return True, f'HTTP {r.status}'
    except _err.HTTPError as e:
        return False, f'HTTP {e.code} {e.read()[:200]!r}'
    except Exception as e:
        return False, str(e)[:200]


def enviar_zap_texto(telefone: str, texto: str) -> tuple[bool, str]:
    numero = _limpar_telefone(telefone)
    if not numero:
        return False, 'sem telefone'
    # linkPreview=False: o preview do link faria o próprio Evolution dar GET na
    # rota pública — e a página do link é justamente o gatilho de reenvio.
    return _evolution_post('sendText', {'number': numero, 'text': texto, 'linkPreview': False})


def enviar_zap_imagem(telefone: str, png: bytes, caption: str, nome_arquivo: str) -> tuple[bool, str]:
    numero = _limpar_telefone(telefone)
    if not numero:
        return False, 'sem telefone'
    return _evolution_post('sendMedia', {
        'number': numero, 'mediatype': 'image', 'mimetype': 'image/png',
        'fileName': nome_arquivo, 'caption': caption,
        'media': base64.b64encode(png).decode('ascii'), 'linkPreview': False,
    })


def enviar_email(para: str, assunto: str, html: str, png: bytes, nome_arquivo: str) -> tuple[bool, str]:
    if not (EMAIL_USER and EMAIL_PASSWORD):
        return False, 'credenciais de e-mail não configuradas no .env'
    if not para:
        return False, 'sem e-mail'
    try:
        msg = MIMEMultipart('related')
        msg['Subject'] = assunto
        msg['From'] = EMAIL_FROM
        msg['To'] = para
        alt = MIMEMultipart('alternative')
        alt.attach(MIMEText(re.sub(r'<[^>]+>', ' ', html), 'plain', 'utf-8'))
        alt.attach(MIMEText(html, 'html', 'utf-8'))
        msg.attach(alt)
        im = MIMEImage(png, _subtype='png')
        im.add_header('Content-ID', '<relatorio_nf>')
        im.add_header('Content-Disposition', 'inline', filename=nome_arquivo)
        msg.attach(im)
        if EMAIL_PORT == 465:
            with smtplib.SMTP_SSL(EMAIL_HOST, EMAIL_PORT, timeout=60) as s:
                s.login(EMAIL_USER, EMAIL_PASSWORD)
                s.sendmail(EMAIL_FROM, [para], msg.as_string())
        else:
            with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT, timeout=60) as s:
                s.ehlo(); s.starttls()
                s.login(EMAIL_USER, EMAIL_PASSWORD)
                s.sendmail(EMAIL_FROM, [para], msg.as_string())
        return True, 'ok'
    except Exception as e:
        return False, str(e)[:200]


# ── ETL ──────────────────────────────────────────────────────────────────────

def atualizar_etl(timeout: int = 900) -> tuple[bool, str]:
    """Roda o export_notas_rps.sh (que também publica no /var/www) esperando o
    lock do cron horário — se a coleta da hora estiver no meio, espera acabar
    e roda de novo em vez de mandar dado velho."""
    if not os.path.exists(ETL_SH):
        return False, f'{ETL_SH} não existe'
    os.makedirs(os.path.dirname(ETL_LOCK), exist_ok=True)
    t0 = time.time()
    try:
        r = subprocess.run(['flock', '-w', '600', ETL_LOCK, '/bin/bash', ETL_SH],
                           capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return False, f'ETL estourou {timeout}s'
    dur = time.time() - t0
    if r.returncode != 0:
        return False, f'ETL rc={r.returncode} em {dur:.0f}s: {(r.stderr or r.stdout)[-300:]}'
    return True, f'ETL ok em {dur:.0f}s'


# ── Fila (spool) ─────────────────────────────────────────────────────────────

def _spool_dir() -> str:
    try:
        os.makedirs(SPOOL_DIR, exist_ok=True)
        if os.geteuid() == 0:
            # Se foi o root (cron) quem criou, o www-data (Flask) precisa gravar aqui.
            os.chmod(SPOOL_DIR, 0o2777)
    except OSError as e:
        log.warning('spool %s: %s', SPOOL_DIR, e)
    return SPOOL_DIR


def _ultimo_path(dest_id: str) -> str:
    return os.path.join(_spool_dir(), f'ultimo_{dest_id}.json')


def _ler_json(path: str) -> dict:
    try:
        with open(path, encoding='utf-8') as f:
            return json.load(f) or {}
    except Exception:
        return {}


def pedidos_pendentes() -> list[dict]:
    out = []
    for p in sorted(glob.glob(os.path.join(_spool_dir(), 'pedido_*.json'))):
        d = _ler_json(p)
        if d:
            d['_path'] = p
            out.append(d)
    return out


def pedir_envio(dest_ids: list[str], origem: str, solicitante: str = '') -> tuple[bool, str]:
    """Deixa um pedido na fila. Devolve (criado, motivo).
    Não cria se já há pedido pendente para o mesmo destinatário (toque duplo),
    se o último envio por link foi há menos de COOLDOWN_SEG, ou se estourou o
    teto diário — proteção contra scanner de link virar rajada."""
    dests = destinatarios()
    dest_ids = [d for d in dest_ids if d in dests]
    if not dest_ids:
        return False, 'destinatário desconhecido'
    pend = {d for p in pedidos_pendentes() for d in p.get('para', [])}
    if all(d in pend for d in dest_ids):
        return False, 'já existe pedido pendente — aguarde a mensagem'
    if origem == 'link':
        agora = time.time()
        for d in dest_ids:
            u = _ler_json(_ultimo_path(d))
            if agora - float(u.get('ts', 0)) < COOLDOWN_SEG:
                return False, f'último envio há menos de {COOLDOWN_SEG}s — aguarde'
            if u.get('dia') == date.today().isoformat() and int(u.get('n_dia', 0)) >= TETO_DIA_POR_DEST:
                return False, f'teto de {TETO_DIA_POR_DEST} reenvios por dia atingido'
    pedido = {'id': uuid.uuid4().hex[:12], 'ts': time.time(), 'quando': datetime.now().isoformat(timespec='seconds'),
              'para': dest_ids, 'origem': origem, 'solicitante': solicitante, 'atualizar': True}
    path = os.path.join(_spool_dir(), f"pedido_{int(pedido['ts'])}_{pedido['id']}.json")
    tmp = path + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(pedido, f, ensure_ascii=False)
    os.replace(tmp, path)
    try:
        os.chmod(path, 0o666)
    except OSError:
        pass
    return True, pedido['id']


def _registrar_ultimo(dest_id: str, origem: str):
    u = _ler_json(_ultimo_path(dest_id))
    hoje = date.today().isoformat()
    n = int(u.get('n_dia', 0)) + 1 if u.get('dia') == hoje else 1
    try:
        with open(_ultimo_path(dest_id), 'w', encoding='utf-8') as f:
            json.dump({'ts': time.time(), 'dia': hoje, 'n_dia': n, 'origem': origem}, f)
    except OSError as e:
        log.warning('ultimo_%s: %s', dest_id, e)


# ── Orquestração ─────────────────────────────────────────────────────────────

def executar_envio(dest_ids: list[str], atualizar: bool, run: bool, origem: str = 'manual',
                   ym: str | None = None, salvar_png: str | None = None) -> dict:
    dests = destinatarios()
    alvo = [dests[d] for d in dest_ids if d in dests]
    res = {'origem': origem, 'run': run, 'etl': None, 'envios': [], 'png': None}
    if not alvo:
        res['erro'] = f'nenhum destinatário válido em {dest_ids}'
        log.error(res['erro'])
        return res

    if atualizar:
        if run:
            ok, msg = atualizar_etl()
            res['etl'] = msg
            (log.info if ok else log.error)('atualizar: %s', msg)
        else:
            res['etl'] = 'DRY-RUN: não rodou o ETL'
            log.info('DRY-RUN: pularia o ETL (export_notas_rps.sh)')

    dados = carregar_dados(ym)
    if not dados['postos']:
        res['erro'] = f"nenhum JSON de {dados['ym']} em {JSON_DIR}"
        log.error(res['erro'])
        return res
    png = render_png(dados)
    nome_png = f"nf_x_meta_{dados['ym']}_{datetime.now():%d_%H%M}.png"
    if salvar_png:
        with open(salvar_png, 'wb') as f:
            f.write(png)
        res['png'] = salvar_png
    assunto = f"NF emitidas × meta — {dados['mes_nome']} — {datetime.now():%d/%m %H:%M}"

    envio_ok = run and ENVIO_LIGADO
    if run and not ENVIO_LIGADO:
        log.error('RELATORIO_NF_ENVIO=0 no .env — kill-switch LIGADO, nada foi enviado')

    for d in alvo:
        link = gerar_link(d['id'])
        item = {'para': d['id'], 'link': link, 'zap': None, 'email': None}
        if envio_ok:
            if d['telefone']:
                ok, msg = enviar_zap_imagem(d['telefone'], png, caption_zap(dados, link), nome_png)
                item['zap'] = f"{'ok' if ok else 'ERRO'}: {msg}"
            else:
                item['zap'] = 'pulado: destinatário sem telefone cadastrado (RELATORIO_NF_DESTINATARIOS)'
            ok, msg = enviar_email(d['email'], assunto, html_email(dados, link, d['nome']), png, nome_png)
            item['email'] = f"{'ok' if ok else 'ERRO'}: {msg}"
            _registrar_ultimo(d['id'], origem)
        else:
            item['zap'] = f"DRY-RUN → {d['telefone'] or 'sem telefone'}"
            item['email'] = f"DRY-RUN → {d['email'] or 'sem e-mail'}"
        log.info('envio %s: zap=%s | email=%s', d['id'], item['zap'], item['email'])
        res['envios'].append(item)
    res['resumo'] = texto_resumo(dados)
    return res


def processar_spool(run: bool) -> int:
    """Consome a fila. Agrupa os pedidos pendentes numa execução só (um ETL para
    todos), remove os arquivos ANTES de enviar — pedido que estourar não fica
    em loop reenviando a cada minuto."""
    pend = pedidos_pendentes()
    if not pend:
        return 0
    dest_ids, origens = [], set()
    for p in pend:
        for d in p.get('para', []):
            if d not in dest_ids:
                dest_ids.append(d)
        origens.add(p.get('origem') or '?')
        try:
            os.remove(p['_path'])
        except OSError as e:
            log.warning('remover %s: %s', p['_path'], e)
    log.info('spool: %d pedido(s) → %s (origem %s)', len(pend), dest_ids, ','.join(sorted(origens)))
    atualizar = any(p.get('atualizar', True) for p in pend)
    origem = 'link' if 'link' in origens else ','.join(sorted(origens))
    executar_envio(dest_ids, atualizar=atualizar, run=run, origem=origem)
    return len(pend)


# ── CLI ──────────────────────────────────────────────────────────────────────

def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--preview', metavar='PNG', help='só gera a imagem no caminho dado e sai')
    ap.add_argument('--enviar', action='store_true', help='gera e envia (DRY-RUN sem --run)')
    ap.add_argument('--para', default='all', help='ids separados por vírgula, ou all')
    ap.add_argument('--atualizar', action='store_true', help='roda o export_notas_rps.sh antes')
    ap.add_argument('--spool', action='store_true', help='processa a fila de pedidos (cron 1×/min)')
    ap.add_argument('--run', action='store_true', help='ENVIA de verdade (sem isso é dry-run)')
    ap.add_argument('--ym', help='mês AAAA-MM (padrão: atual)')
    ap.add_argument('--png', help='com --enviar: também salva o PNG neste caminho')
    a = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

    if a.preview:
        dados = carregar_dados(a.ym)
        if not dados['postos']:
            print(f"nenhum JSON de {dados['ym']} em {JSON_DIR}", file=sys.stderr)
            return 2
        with open(a.preview, 'wb') as f:
            f.write(render_png(dados))
        print(texto_resumo(dados))
        print(f'PNG: {a.preview} ({len(dados["linhas"])} linhas)')
        return 0

    if a.spool:
        n = processar_spool(run=a.run)
        if n:
            log.info('spool: %d processado(s) (run=%s)', n, a.run)
        return 0

    if a.enviar:
        ids = list(destinatarios()) if a.para == 'all' else [x.strip() for x in a.para.split(',') if x.strip()]
        res = executar_envio(ids, atualizar=a.atualizar, run=a.run, origem='cron' if a.run else 'dry-run',
                             ym=a.ym, salvar_png=a.png)
        print(json.dumps(res, ensure_ascii=False, indent=2, default=str))
        return 1 if res.get('erro') else 0

    ap.print_help()
    return 0


if __name__ == '__main__':
    sys.exit(main())
