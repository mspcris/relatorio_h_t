"""
relatorio_nf_routes.py — rotas do relatório "NF emitidas × meta" (Notas x RPS)

Duas portas de entrada para o MESMO pedido ("gera e manda o relatório
atualizado"), que cai na fila lida pelo cron de root (`relatorio_nf.py --spool`):

1. /relatorio_nf/<token>  — PÚBLICA de propósito (nginx sem auth_request, mesmo
   modelo do /ciencia/): é o link que vai dentro do WhatsApp/e-mail. O token é
   assinado com o SECRET_KEY e identifica o destinatário.
   - GET  → só devolve a página (sem efeito colateral). Scanner de link de
            e-mail e preview do WhatsApp fazem GET — não podem disparar envio.
   - POST /ir → a página faz sozinha ao abrir (JS). Cria o pedido, manda na
            hora o zap "⏳ Espere, atualizando os dados…" e devolve a frase.
2. /api/relatorio_nf/enviar — botão no rodapé da página kpi_notas_rps.html,
   exige sessão (cookie). Pede para TODOS os destinatários configurados.

Este módulo NÃO gera imagem nem roda ETL: o camim-auth é www-data e não escreve
em json_notas_rps. Ele só grava o pedido; quem executa é o cron (≤ 1 min).
"""
from __future__ import annotations

import logging
import threading

from flask import Blueprint, jsonify, request

log = logging.getLogger(__name__)

relatorio_nf_bp = Blueprint('relatorio_nf', __name__)

FRASE_ESPERE = '⏳ Espere, atualizando os dados…'


def _mod():
    # import lazy: problema no módulo (Pillow, dotenv…) não derruba o serviço
    import relatorio_nf
    return relatorio_nf


def _email_logado() -> str | None:
    try:
        from auth_routes import decode_user
        email, _postos = decode_user()
        return email or None
    except Exception:
        return None


_PAGE = """<!doctype html><html lang="pt-br"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="robots" content="noindex,nofollow">
<title>Relatório NF × meta — CAMIM</title>
<style>
 body{{font-family:system-ui,-apple-system,"Segoe UI",Roboto,Arial,sans-serif;background:#f4f6f9;margin:0;padding:24px 16px;color:#111827}}
 .card{{max-width:520px;margin:32px auto;background:#fff;border-radius:16px;padding:28px 24px;box-shadow:0 4px 18px rgba(0,0,0,.08);text-align:center}}
 h1{{font-size:1.35rem;margin:0 0 10px}} p{{color:#4b5563;line-height:1.5;margin:8px 0}}
 .big{{font-size:1.25rem;font-weight:700;color:#111827}}
 .ok{{color:#15803d}} .warn{{color:#b45309}} .err{{color:#b91c1c}}
 button{{margin-top:14px;background:#2563eb;color:#fff;border:0;border-radius:10px;padding:14px 22px;font-size:1.05rem;font-weight:700;width:100%}}
 a{{color:#2563eb}} .mini{{font-size:.85rem;color:#9ca3af;margin-top:18px}}
</style></head><body>
<div class="card">
  <h1>📊 NF emitidas × meta</h1>
  <p>Olá, <b>{nome}</b>.</p>
  <p id="msg" class="big">{espere}</p>
  <p id="det">Em alguns minutos (o robô recoleta os 13 postos antes) chega no seu {canais} uma mensagem <b>nova</b>, com a imagem atualizada e um link novo.</p>
  <noscript><form method="POST" action="/relatorio_nf/{token}/ir"><button type="submit">🔄 Atualizar e me enviar</button></form></noscript>
  <p class="mini"><a href="{pagina}">Abrir o painel completo</a></p>
</div>
<script>
(function(){{
  var msg=document.getElementById('msg'), det=document.getElementById('det');
  fetch('/relatorio_nf/{token}/ir',{{method:'POST',headers:{{'Content-Type':'application/json'}},body:'{{}}'}})
    .then(function(r){{return r.json();}})
    .then(function(j){{
      if(j.criado){{ msg.textContent='{espere}'; msg.className='big ok'; }}
      else {{ msg.textContent='⏳ '+(j.motivo||'Já estou atualizando.'); msg.className='big warn'; }}
      if(j.detalhe) det.textContent=j.detalhe;
    }})
    .catch(function(){{ msg.textContent='Não consegui registrar o pedido. Tente de novo.'; msg.className='big err'; }});
}})();
</script>
</body></html>"""

_PAGE_404 = ('<h3 style="font-family:sans-serif;text-align:center;margin-top:60px">'
             'Link inválido ou expirado.<br><small>Peça um relatório novo pelo painel.</small></h3>')


def _canais(d: dict) -> str:
    c = []
    if d.get('telefone'):
        c.append('WhatsApp')
    if d.get('email'):
        c.append('e-mail')
    return ' e '.join(c) or 'e-mail'


@relatorio_nf_bp.get('/relatorio_nf/<token>')
def abrir(token):
    """Página do link. SEM efeito colateral — o disparo é o POST /ir feito pelo JS."""
    try:
        rn = _mod()
        dest_id = rn.ler_token(token)
    except Exception as e:
        log.error('relatorio_nf abrir: %s', e)
        dest_id = None
    if not dest_id:
        return _PAGE_404, 404
    d = rn.destinatarios()[dest_id]
    return _PAGE.format(nome=d['nome'], espere=FRASE_ESPERE, canais=_canais(d),
                        token=token, pagina=rn.PAGINA_URL)


@relatorio_nf_bp.post('/relatorio_nf/<token>/ir')
def ir(token):
    try:
        rn = _mod()
    except Exception as e:
        log.error('relatorio_nf módulo: %s', e)
        return jsonify(ok=False, criado=False, motivo='serviço indisponível'), 500
    dest_id = rn.ler_token(token)
    if not dest_id:
        return jsonify(ok=False, criado=False, motivo='link inválido ou expirado'), 404
    d = rn.destinatarios()[dest_id]
    try:
        criado, motivo = rn.pedir_envio([dest_id], origem='link', solicitante=dest_id)
    except Exception as e:
        log.error('relatorio_nf pedir_envio: %s', e)
        return jsonify(ok=False, criado=False, motivo='não consegui registrar o pedido'), 500
    log.info('relatorio_nf link: dest=%s criado=%s motivo=%s ip=%s', dest_id, criado, motivo,
             request.headers.get('X-Real-IP') or request.remote_addr)
    if criado and d.get('telefone'):
        # zap imediato, fora do request: o usuário vê "Espere…" no celular na hora
        threading.Thread(target=rn.enviar_zap_texto, args=(d['telefone'], FRASE_ESPERE), daemon=True).start()
    detalhe = (f"Em alguns minutos (o robô recoleta os 13 postos antes) chega no seu {_canais(d)} uma mensagem nova, com a imagem atualizada e um link novo."
               if criado else 'Um pedido já está em andamento — a mensagem nova está a caminho.')
    return jsonify(ok=True, criado=criado, motivo=motivo, mensagem=FRASE_ESPERE, detalhe=detalhe)


@relatorio_nf_bp.get('/api/relatorio_nf/info')
def info():
    if not _email_logado():
        return jsonify(erro='não autenticado'), 401
    try:
        rn = _mod()
        dests = [{'id': d['id'], 'nome': d['nome'], 'canais': _canais(d)} for d in rn.destinatarios().values()]
        pend = len(rn.pedidos_pendentes())
        return jsonify(destinatarios=dests, pendentes=pend, envio_ligado=rn.ENVIO_LIGADO)
    except Exception as e:
        log.error('relatorio_nf info: %s', e)
        return jsonify(erro=str(e)[:200]), 500


@relatorio_nf_bp.post('/api/relatorio_nf/enviar')
def enviar():
    email = _email_logado()
    if not email:
        return jsonify(erro='não autenticado'), 401
    try:
        rn = _mod()
        ids = list(rn.destinatarios())
        criado, motivo = rn.pedir_envio(ids, origem='pagina', solicitante=email)
    except Exception as e:
        log.error('relatorio_nf enviar: %s', e)
        return jsonify(ok=False, erro=str(e)[:200]), 500
    log.info('relatorio_nf página: por=%s criado=%s motivo=%s', email, criado, motivo)
    nomes = ' e '.join(d['nome'] for d in rn.destinatarios().values())
    msg = (f'{FRASE_ESPERE} Em alguns minutos (o robô recoleta os 13 postos antes) {nomes} recebem a imagem nova por WhatsApp/e-mail.'
           if criado else f'Já existe um pedido em andamento ({motivo}).')
    return jsonify(ok=True, criado=criado, motivo=motivo, mensagem=msg)
