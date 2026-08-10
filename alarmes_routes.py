"""
alarmes_routes.py
Flask Blueprint — Sistema de Alarmes CAMIM.

Rotas:
  GET  /alarmes                       — página admin
  GET  /alarmes/api/alarmes           — lista alarmes
  POST /alarmes/api/alarmes           — cria alarme
  GET  /alarmes/api/alarmes/<id>      — detalhe alarme
  PUT  /alarmes/api/alarmes/<id>      — edita alarme
  DELETE /alarmes/api/alarmes/<id>    — exclui alarme
  POST /alarmes/api/alarmes/<id>/toggle — ativa/desativa
  GET  /alarmes/api/gerentes          — lista gerentes do banco
  POST /alarmes/api/extras            — adiciona gerente extra
  DELETE /alarmes/api/extras/<id>     — remove gerente extra
  GET  /alarmes/api/auditores         — lista auditores
  POST /alarmes/api/auditores         — cria auditor
  PUT  /alarmes/api/auditores/<id>    — edita auditor
  DELETE /alarmes/api/auditores/<id>  — desativa auditor
  GET  /alarmes/api/diretores         — lista diretores
  POST /alarmes/api/diretores         — cria diretor
  PUT  /alarmes/api/diretores/<id>    — edita diretor
  DELETE /alarmes/api/diretores/<id>  — desativa diretor
  POST /alarmes/api/silenciar         — silencia alarme (gerente/usuário do posto)
  GET  /alarmes/api/silenciamentos    — silenciamentos ativos
  GET  /alarmes/api/disparos          — histórico de disparos
  GET  /alarmes/api/auditoria         — log de auditoria
  GET  /alarmes/api/resumo            — alarmes ativos por posto/serviço (para monitorarrobos)
"""

import os
import sys
import json
from flask import Blueprint, request, jsonify, render_template

sys.path.insert(0, '/opt/relatorio_h_t')
sys.path.insert(0, '/opt/camim-auth')

import alarmes_db as db

from dotenv import load_dotenv
load_dotenv('/opt/relatorio_h_t/.env')

alarmes_bp = Blueprint('alarmes', __name__, url_prefix='/alarmes')

# ── Auth helpers ──────────────────────────────────────────────────────────────

def _check_auth():
    try:
        from auth_routes import decode_user
        from auth_db import SessionLocal, get_user_by_email
        email, postos = decode_user()
        if not email:
            return None, None, None
        sess = SessionLocal()
        try:
            u = get_user_by_email(sess, email)
            is_admin = u.is_admin if u else False
        finally:
            sess.close()
        return email, is_admin, postos
    except Exception:
        return None, None, None


def _require_admin():
    email, is_admin, postos = _check_auth()
    if not email:
        return None, None, None, jsonify({'ok': False, 'error': 'não autenticado'}), 401
    if not is_admin:
        return None, None, None, jsonify({'ok': False, 'error': 'acesso negado'}), 403
    return email, is_admin, postos, None, None


def _require_auth():
    email, is_admin, postos = _check_auth()
    if not email:
        return None, None, None, jsonify({'ok': False, 'error': 'não autenticado'}), 401
    return email, is_admin, postos, None, None


# ── Página admin ──────────────────────────────────────────────────────────────

@alarmes_bp.get('')
@alarmes_bp.get('')
@alarmes_bp.get('/')
def page_alarmes():
    email, is_admin, postos = _check_auth()
    if not email:
        return ('', 401)
    if not is_admin:
        return ('Acesso negado — somente administradores', 403)
    db.init_db()
    return render_template(
        'alarmes_admin.html',
        USER_EMAIL=email,
        USER_IS_ADMIN=is_admin,
        USER_POSTOS=json.dumps(postos or []),
    )


# ── Alarmes ───────────────────────────────────────────────────────────────────

@alarmes_bp.get('/api/alarmes')
def api_list_alarmes():
    email, is_admin, postos, err, code = _require_admin()
    if err:
        return err, code
    db.init_db()
    posto = request.args.get('posto') or None
    ativo = request.args.get('ativo')
    ativo_bool = None if ativo is None else (ativo == '1')
    alarmes = db.listar_alarmes(posto=posto, ativo=ativo_bool)
    # enrich with gerente info and silence status
    for a in alarmes:
        g = db.get_gerente(a['posto'])
        a['gerente_email']    = g['email'] if g else None
        a['gerente_telefone'] = g['telefone'] if g else None
        a['gerente_atualizado_em'] = g['atualizado_em'] if g else None
        a['silenciado'] = db.esta_silenciado(a['id'])
    return jsonify({'ok': True, 'alarmes': alarmes})


@alarmes_bp.post('/api/alarmes')
def api_create_alarme():
    email, is_admin, postos, err, code = _require_admin()
    if err:
        return err, code
    data = request.get_json(force=True) or {}
    if not data.get('nome') or not data.get('posto') or not data.get('servico'):
        return jsonify({'ok': False, 'error': 'nome, posto e servico são obrigatórios'}), 400
    if not data.get('status_gatilho'):
        return jsonify({'ok': False, 'error': 'status_gatilho é obrigatório'}), 400
    if not data.get('mensagem', '').strip():
        return jsonify({'ok': False, 'error': 'mensagem é obrigatória'}), 400
    if not data.get('via_whatsapp') and not data.get('via_email'):
        return jsonify({'ok': False, 'error': 'pelo menos um canal (WhatsApp ou Email) é obrigatório'}), 400
    db.init_db()
    alarme_id = db.criar_alarme(data, criado_por=email)
    db.registrar_auditoria(email, 'CRIAR_ALARME', 'alarme', alarme_id, data,
                           ip=request.remote_addr)
    return jsonify({'ok': True, 'id': alarme_id})


@alarmes_bp.get('/api/alarmes/<int:aid>')
def api_get_alarme(aid):
    email, is_admin, postos, err, code = _require_admin()
    if err:
        return err, code
    db.init_db()
    a = db.get_alarme(aid)
    if not a:
        return jsonify({'ok': False, 'error': 'não encontrado'}), 404
    g = db.get_gerente(a['posto'])
    a['gerente_email']    = g['email'] if g else None
    a['gerente_telefone'] = g['telefone'] if g else None
    a['silenciado'] = db.esta_silenciado(aid)
    return jsonify({'ok': True, 'alarme': a})


@alarmes_bp.put('/api/alarmes/<int:aid>')
def api_update_alarme(aid):
    email, is_admin, postos, err, code = _require_admin()
    if err:
        return err, code
    data = request.get_json(force=True) or {}
    if not data.get('nome') or not data.get('posto') or not data.get('servico'):
        return jsonify({'ok': False, 'error': 'nome, posto e servico são obrigatórios'}), 400
    if not data.get('via_whatsapp') and not data.get('via_email'):
        return jsonify({'ok': False, 'error': 'pelo menos um canal é obrigatório'}), 400
    db.init_db()
    antes = db.get_alarme(aid)
    db.atualizar_alarme(aid, data)
    db.registrar_auditoria(email, 'EDITAR_ALARME', 'alarme', aid,
                           {'antes': antes, 'depois': data}, ip=request.remote_addr)
    return jsonify({'ok': True})


@alarmes_bp.delete('/api/alarmes/<int:aid>')
def api_delete_alarme(aid):
    email, is_admin, postos, err, code = _require_admin()
    if err:
        return err, code
    db.init_db()
    antes = db.get_alarme(aid)
    db.excluir_alarme(aid)
    db.registrar_auditoria(email, 'EXCLUIR_ALARME', 'alarme', aid, antes,
                           ip=request.remote_addr)
    return jsonify({'ok': True})


@alarmes_bp.post('/api/alarmes/<int:aid>/toggle')
def api_toggle_alarme(aid):
    email, is_admin, postos, err, code = _require_admin()
    if err:
        return err, code
    db.init_db()
    novo = db.toggle_alarme(aid)
    acao = 'ATIVAR_ALARME' if novo else 'DESATIVAR_ALARME'
    db.registrar_auditoria(email, acao, 'alarme', aid, None, ip=request.remote_addr)
    return jsonify({'ok': True, 'ativo': novo})


# ── Gerentes ──────────────────────────────────────────────────────────────────

@alarmes_bp.get('/api/gerentes')
def api_list_gerentes():
    email, is_admin, postos, err, code = _require_admin()
    if err:
        return err, code
    db.init_db()
    return jsonify({'ok': True, 'gerentes': db.listar_gerentes()})


@alarmes_bp.post('/api/extras')
def api_add_extra():
    email, is_admin, postos, err, code = _require_admin()
    if err:
        return err, code
    data = request.get_json(force=True) or {}
    alarme_id = data.get('alarme_id')
    if not alarme_id:
        return jsonify({'ok': False, 'error': 'alarme_id obrigatório'}), 400
    db.init_db()
    eid = db.add_gerente_extra(
        alarme_id,
        data.get('nome'), data.get('email'), data.get('telefone'),
        data.get('via_whatsapp', True), data.get('via_email', True),
    )
    db.registrar_auditoria(email, 'ADD_EXTRA', 'gerente_extra', eid, data,
                           ip=request.remote_addr)
    return jsonify({'ok': True, 'id': eid})


@alarmes_bp.delete('/api/extras/<int:eid>')
def api_del_extra(eid):
    email, is_admin, postos, err, code = _require_admin()
    if err:
        return err, code
    db.init_db()
    db.del_gerente_extra(eid)
    db.registrar_auditoria(email, 'DEL_EXTRA', 'gerente_extra', eid, None,
                           ip=request.remote_addr)
    return jsonify({'ok': True})


# ── Auditores ─────────────────────────────────────────────────────────────────

@alarmes_bp.get('/api/auditores')
def api_list_auditores():
    email, is_admin, postos, err, code = _require_admin()
    if err:
        return err, code
    db.init_db()
    return jsonify({'ok': True, 'auditores': db.listar_auditores(ativo=True)})


@alarmes_bp.post('/api/auditores')
def api_create_auditor():
    email, is_admin, postos, err, code = _require_admin()
    if err:
        return err, code
    data = request.get_json(force=True) or {}
    if not data.get('nome', '').strip():
        return jsonify({'ok': False, 'error': 'nome é obrigatório'}), 400
    db.init_db()
    aid = db.criar_auditor(data)
    db.registrar_auditoria(email, 'CRIAR_AUDITOR', 'auditor', aid, data,
                           ip=request.remote_addr)
    return jsonify({'ok': True, 'id': aid})


@alarmes_bp.put('/api/auditores/<int:aid>')
def api_update_auditor(aid):
    email, is_admin, postos, err, code = _require_admin()
    if err:
        return err, code
    data = request.get_json(force=True) or {}
    if not data.get('nome', '').strip():
        return jsonify({'ok': False, 'error': 'nome é obrigatório'}), 400
    db.init_db()
    db.atualizar_auditor(aid, data)
    db.registrar_auditoria(email, 'EDITAR_AUDITOR', 'auditor', aid, data,
                           ip=request.remote_addr)
    return jsonify({'ok': True})


@alarmes_bp.delete('/api/auditores/<int:aid>')
def api_delete_auditor(aid):
    email, is_admin, postos, err, code = _require_admin()
    if err:
        return err, code
    db.init_db()
    db.desativar_auditor(aid)
    db.registrar_auditoria(email, 'DESATIVAR_AUDITOR', 'auditor', aid, None,
                           ip=request.remote_addr)
    return jsonify({'ok': True})


# ── Diretores ─────────────────────────────────────────────────────────────────

@alarmes_bp.get('/api/diretores')
def api_list_diretores():
    email, is_admin, postos, err, code = _require_admin()
    if err:
        return err, code
    db.init_db()
    return jsonify({'ok': True, 'diretores': db.listar_diretores(ativo=True)})


@alarmes_bp.post('/api/diretores')
def api_create_diretor():
    email, is_admin, postos, err, code = _require_admin()
    if err:
        return err, code
    data = request.get_json(force=True) or {}
    if not data.get('nome', '').strip():
        return jsonify({'ok': False, 'error': 'nome é obrigatório'}), 400
    db.init_db()
    did = db.criar_diretor(data)
    db.registrar_auditoria(email, 'CRIAR_DIRETOR', 'diretor', did, data,
                           ip=request.remote_addr)
    return jsonify({'ok': True, 'id': did})


@alarmes_bp.put('/api/diretores/<int:did>')
def api_update_diretor(did):
    email, is_admin, postos, err, code = _require_admin()
    if err:
        return err, code
    data = request.get_json(force=True) or {}
    if not data.get('nome', '').strip():
        return jsonify({'ok': False, 'error': 'nome é obrigatório'}), 400
    db.init_db()
    db.atualizar_diretor(did, data)
    db.registrar_auditoria(email, 'EDITAR_DIRETOR', 'diretor', did, data,
                           ip=request.remote_addr)
    return jsonify({'ok': True})


@alarmes_bp.delete('/api/diretores/<int:did>')
def api_delete_diretor(did):
    email, is_admin, postos, err, code = _require_admin()
    if err:
        return err, code
    db.init_db()
    db.desativar_diretor(did)
    db.registrar_auditoria(email, 'DESATIVAR_DIRETOR', 'diretor', did, None,
                           ip=request.remote_addr)
    return jsonify({'ok': True})


# ── Silenciamento ─────────────────────────────────────────────────────────────

@alarmes_bp.post('/api/silenciar')
def api_silenciar():
    email, is_admin, postos, err, code = _require_auth()
    if err:
        return err, code
    data = request.get_json(force=True) or {}
    alarme_id = data.get('alarme_id')
    dias = int(data.get('dias', 1))
    motivo = data.get('motivo', '').strip() or None

    if not alarme_id:
        return jsonify({'ok': False, 'error': 'alarme_id obrigatório'}), 400
    if dias < 1 or dias > 3:
        return jsonify({'ok': False, 'error': 'dias deve ser entre 1 e 3'}), 400

    db.init_db()
    alarme = db.get_alarme(int(alarme_id))
    if not alarme:
        return jsonify({'ok': False, 'error': 'alarme não encontrado'}), 404

    # Verifica se o usuário tem acesso ao posto (ou é admin)
    if not is_admin and postos and alarme['posto'] not in postos:
        return jsonify({'ok': False, 'error': 'sem acesso a este posto'}), 403

    sil_id = db.criar_silenciamento(int(alarme_id), email, dias, motivo)
    db.registrar_auditoria(
        email, 'SILENCIAR_ALARME', 'silenciamento', sil_id,
        {'alarme_id': alarme_id, 'dias': dias, 'motivo': motivo},
        ip=request.remote_addr
    )
    return jsonify({'ok': True, 'silenciamento_id': sil_id})


@alarmes_bp.get('/api/silenciamentos')
def api_silenciamentos():
    email, is_admin, postos, err, code = _require_admin()
    if err:
        return err, code
    db.init_db()
    return jsonify({'ok': True, 'silenciamentos': db.listar_silenciamentos(apenas_ativos=True)})


# ── Histórico ─────────────────────────────────────────────────────────────────

@alarmes_bp.get('/api/disparos')
def api_disparos():
    email, is_admin, postos, err, code = _require_admin()
    if err:
        return err, code
    db.init_db()
    alarme_id = request.args.get('alarme_id')
    limit = int(request.args.get('limit', 100))
    disparos = db.listar_disparos(alarme_id=int(alarme_id) if alarme_id else None, limit=limit)
    return jsonify({'ok': True, 'disparos': disparos})


@alarmes_bp.get('/api/auditoria')
def api_auditoria():
    email, is_admin, postos, err, code = _require_admin()
    if err:
        return err, code
    db.init_db()
    return jsonify({'ok': True, 'registros': db.listar_auditoria(limit=300)})


# ── Resumo para página de monitoramento ──────────────────────────────────────

@alarmes_bp.get('/api/resumo')
def api_resumo():
    email, is_admin, postos, err, code = _require_auth()
    if err:
        return err, code
    db.init_db()
    resumo = db.alarmes_ativos_por_posto_servico(postos=postos if not is_admin else None)
    return jsonify({'ok': True, 'resumo': resumo})


# ── Central de Notificação de Problemas (2026-08-10) ─────────────────────────

@alarmes_bp.get('/api/central')
def api_central():
    """Registro de cada notificação enviada + estado de ciência por
    destinatário — alimenta a página central_problemas.html."""
    email, is_admin, postos, err, code = _require_auth()
    if err:
        return err, code
    db.init_db()
    return jsonify({'ok': True, 'itens': db.listar_central(limite=300),
                    'servicos': db.SERVICOS, 'status_labels': db.STATUS_LABELS})


# ── Ciência pública por token (link enviado no zap/e-mail) ───────────────────
# Blueprint SEPARADO e SEM auth: o gestor clica do WhatsApp sem ter conta no
# KPI. O token (uuid4 hex) é a credencial — identifica destinatário+disparo.
# nginx: location ^~ /ciencia/ SEM auth_request (nginx/teste-ia.conf).

from flask import Blueprint as _Blueprint

ciencia_bp = _Blueprint('ciencia', __name__, url_prefix='/ciencia')

_CIENCIA_PAGE = """<!doctype html><html lang="pt-br"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Ciência de Problema — CAMIM</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@4.6.2/dist/css/bootstrap.min.css">
</head><body style="background:#f4f6f9">
<div class="container" style="max-width:560px;margin-top:40px">
  <div class="card shadow"><div class="card-body p-4">
    <h5 class="mb-3">⚠️ Notificação de problema — CAMIM</h5>
    <table class="table table-sm">
      <tr><th>Alerta</th><td>{alarme_nome}</td></tr>
      <tr><th>Posto</th><td>{posto}</td></tr>
      <tr><th>Serviço</th><td>{servico}</td></tr>
      <tr><th>Status</th><td><b>{status}</b></td></tr>
      <tr><th>Enviado em</th><td>{quando}</td></tr>
      <tr><th>Destinatário</th><td>{dest}</td></tr>
    </table>
    {miolo}
  </div></div>
  <p class="text-muted text-center mt-3" style="font-size:.8rem">
    Central de Notificação de Problemas — CAMIM</p>
</div></body></html>"""


def _render_ciencia(c, miolo):
    return _CIENCIA_PAGE.format(
        alarme_nome=c.get('alarme_nome') or '?',
        posto=c.get('posto') or '?',
        servico=db.SERVICOS.get(c.get('servico'), c.get('servico') or '?'),
        status=db.STATUS_LABELS.get(c.get('status_registrado'),
                                    c.get('status_registrado') or '?'),
        quando=(c.get('disparado_em') or c.get('criado_em') or '?'),
        dest=(c.get('dest_nome') or c.get('dest_email')
              or c.get('dest_telefone') or '?'),
        miolo=miolo,
    )


@ciencia_bp.get('/<token>')
def ciencia_ver(token):
    c = db.get_ciencia(token)
    if not c:
        return ('<h3 style="font-family:sans-serif;text-align:center;margin-top:60px">'
                'Link inválido ou expirado.</h3>'), 404
    if c.get('ciente_em'):
        miolo = (f'<div class="alert alert-success">✅ Ciência já registrada em '
                 f'<b>{c["ciente_em"]}</b>. Obrigado!</div>')
    else:
        miolo = (f'<form method="POST" action="/ciencia/{token}/confirmar">'
                 f'<button type="submit" class="btn btn-success btn-lg btn-block">'
                 f'✅ Estou ciente deste problema</button></form>'
                 f'<p class="text-muted mt-2" style="font-size:.85rem">Ao confirmar, '
                 f'fica registrado dia e hora da sua ciência na Central de '
                 f'Notificação de Problemas.</p>')
    return _render_ciencia(c, miolo)


@ciencia_bp.post('/<token>/confirmar')
def ciencia_confirmar(token):
    ip = (request.headers.get('X-Real-IP')
          or request.headers.get('X-Forwarded-For', '').split(',')[0].strip()
          or request.remote_addr)
    c = db.marcar_ciencia(token, ip=ip,
                          agente=request.headers.get('User-Agent'))
    if not c:
        return ('<h3 style="font-family:sans-serif;text-align:center;margin-top:60px">'
                'Link inválido.</h3>'), 404
    db.registrar_auditoria(c.get('dest_email') or c.get('dest_telefone') or 'ciencia',
                           'CIENCIA', 'ciencia', c.get('id'),
                           {'token': token, 'ip': ip})
    miolo = (f'<div class="alert alert-success">✅ Ciência registrada em '
             f'<b>{c["ciente_em"]}</b>. Obrigado!</div>')
    return _render_ciencia(c, miolo)
