"""
agenda_f3/app.py — Flask app pra camila2.ia.camim.com.br.

Auth: IDCAMIM OpenID Connect (sem whitelist — qualquer login válido entra).
Dados: lê de Postgres f3 (fonte primária). Fallback pra JSON em disco se
       o Postgres estiver indisponível.

Rotas:
  GET  /                  → redirect /agenda_dia
  GET  /agenda_dia        → template agenda_dia.html
  GET  /api/agenda_dia    → JSON {ok, posto, data, pacientes, gerado_em, meta}
  GET  /api/agenda_dia/meta → status por posto (gerado_em, sucesso, n_registros)
  GET  /session/me        → {email, postos} (postos = todos os 13)
  GET  /auth/login        → inicia OIDC
  GET  /auth/callback     → callback OIDC
  POST /auth/logout       → encerra sessão + redirect end_session do IDCAMIM
  POST /session/logout    → alias do logout (compat com template)
"""
import os, json, secrets, time
from urllib.parse import urlencode
from functools import wraps
from datetime import datetime, timezone

import requests as http
from flask import (Flask, request, redirect, session, render_template,
                   url_for, jsonify, render_template_string)
from dotenv import load_dotenv

load_dotenv('/opt/agenda_f3/.env')

# ── Flask app ────────────────────────────────────────────────────────────────

app = Flask(__name__, template_folder='/opt/agenda_f3/templates')
app.secret_key = os.environ.get('SESSION_SECRET', secrets.token_hex(32))
app.config['SESSION_COOKIE_SECURE']   = True
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
app.config['PERMANENT_SESSION_LIFETIME'] = 3600 * 8

# ── Constantes ───────────────────────────────────────────────────────────────

POSTOS_TODOS = list("ABCDGIJMNPRXY")  # todos os 13, exibidos a qualquer usuário
JSON_FALLBACK = '/opt/agenda_f3/json_consolidado/agenda_dia.json'

# ── OIDC config (lazy) ───────────────────────────────────────────────────────

IDCAMIM_CLIENT_ID     = os.environ['IDCAMIM_CLIENT_ID']
IDCAMIM_CLIENT_SECRET = os.environ['IDCAMIM_CLIENT_SECRET']
IDCAMIM_REDIRECT_URI  = os.environ['IDCAMIM_REDIRECT_URI']
IDCAMIM_DISCOVERY     = 'https://idcamim.camim.com.br/.well-known/openid-configuration'

_oidc_cfg = None

def _oidc_config():
    global _oidc_cfg
    if _oidc_cfg is None:
        _oidc_cfg = http.get(IDCAMIM_DISCOVERY, timeout=10).json()
    return _oidc_cfg


# ── Decorador ────────────────────────────────────────────────────────────────

def login_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if 'sub' not in session:
            session['next'] = request.url
            return redirect(url_for('auth_login'))
        return f(*args, **kwargs)
    return wrapper


# ── Auth ─────────────────────────────────────────────────────────────────────

@app.get('/auth/login')
def auth_login():
    state = secrets.token_urlsafe(16)
    session['oidc_state'] = state
    cfg = _oidc_config()
    params = urlencode({
        'response_type': 'code',
        'client_id':     IDCAMIM_CLIENT_ID,
        'redirect_uri':  IDCAMIM_REDIRECT_URI,
        'scope':         'openid email profile',
        'state':         state,
    })
    return redirect(f"{cfg['authorization_endpoint']}?{params}")


@app.get('/auth/callback')
def auth_callback():
    err = request.args.get('error')
    if err:
        return render_template_string(TMPL_ERRO, titulo='Erro', msg=err), 400

    state    = request.args.get('state', '')
    expected = session.pop('oidc_state', None)
    if not expected or state != expected:
        return render_template_string(TMPL_ERRO, titulo='Erro de segurança',
                                      msg='State inválido.'), 400

    code = request.args.get('code')
    if not code:
        return render_template_string(TMPL_ERRO, titulo='Erro',
                                      msg='Código ausente.'), 400

    cfg = _oidc_config()
    resp = http.post(cfg['token_endpoint'], data={
        'grant_type':    'authorization_code',
        'code':          code,
        'redirect_uri':  IDCAMIM_REDIRECT_URI,
        'client_id':     IDCAMIM_CLIENT_ID,
        'client_secret': IDCAMIM_CLIENT_SECRET,
    }, timeout=15)
    if not resp.ok:
        return render_template_string(TMPL_ERRO, titulo='Erro',
                                      msg='Falha ao trocar code por token.'), 502

    tokens = resp.json()
    ui = http.get(cfg['userinfo_endpoint'],
                  headers={'Authorization': f"Bearer {tokens.get('access_token')}"},
                  timeout=10)
    if not ui.ok:
        return render_template_string(TMPL_ERRO, titulo='Erro',
                                      msg='Falha ao obter userinfo.'), 502

    userinfo = ui.json()
    sub = userinfo.get('sub')
    if not sub:
        return render_template_string(TMPL_ERRO, titulo='Erro',
                                      msg='sub ausente.'), 502

    session.permanent = True
    session['sub']   = sub
    session['email'] = userinfo.get('email', '')
    session['nome']  = userinfo.get('name') or userinfo.get('preferred_username', '')

    return redirect(session.pop('next', '/'))


@app.route('/auth/logout',    methods=['GET', 'POST'])
@app.route('/session/logout', methods=['GET', 'POST'])
def auth_logout():
    session.clear()
    try:
        cfg = _oidc_config()
        end = cfg.get('end_session_endpoint', '')
        if end:
            return redirect(end)
    except Exception:
        pass
    return render_template_string(TMPL_LOGOUT)


# ── Session API ──────────────────────────────────────────────────────────────

@app.get('/session/me')
def session_me():
    if 'sub' not in session:
        return jsonify({'email': None, 'postos': []}), 401
    # Decisão de produto: qualquer login IDCamim vê todos os 13 postos.
    return jsonify({'email': session.get('email'), 'postos': POSTOS_TODOS})


# ── Rota raiz / template ─────────────────────────────────────────────────────

@app.get('/')
@login_required
def index():
    return redirect('/agenda_dia')


@app.get('/agenda_dia')
@login_required
def agenda_dia_page():
    return render_template('agenda_dia.html',
                           USER_EMAIL=session.get('email'),
                           USER_POSTOS=json.dumps(POSTOS_TODOS))


# ── API: agenda do dia ───────────────────────────────────────────────────────

@app.get('/api/agenda_dia')
@login_required
def api_agenda_dia():
    posto    = (request.args.get('posto') or '').strip().upper()
    data_str = (request.args.get('data')  or '').strip()
    if not posto or not data_str:
        return jsonify({'ok': False, 'error': 'posto e data são obrigatórios'}), 400
    if posto not in POSTOS_TODOS:
        return jsonify({'ok': False, 'error': 'posto inválido'}), 400

    # Tenta Postgres primeiro
    try:
        from f3_db import fetch_agenda
        pacientes, gerado_em = fetch_agenda(posto, data_str)
        return jsonify({
            'ok':         True,
            'posto':      posto,
            'data':       data_str,
            'total':      len(pacientes),
            'pacientes':  pacientes,
            'gerado_em':  gerado_em.isoformat() if gerado_em else None,
            'fonte':      'postgres',
        })
    except Exception as e:
        app.logger.warning('Postgres indisponível, usando JSON fallback: %s', e)

    # Fallback: JSON em disco
    if not os.path.exists(JSON_FALLBACK):
        return jsonify({'ok': False,
                        'error': 'Postgres indisponível e JSON fallback ausente'}), 503
    try:
        with open(JSON_FALLBACK, 'r', encoding='utf-8') as f:
            payload = json.load(f)
    except Exception as e:
        return jsonify({'ok': False, 'error': f'Falha ao ler JSON: {e}'}), 500

    dados_posto = payload.get('dados', {}).get(posto, {})
    pacientes = dados_posto.get(data_str, [])
    return jsonify({
        'ok':        True,
        'posto':     posto,
        'data':      data_str,
        'total':     len(pacientes),
        'pacientes': pacientes,
        'gerado_em': payload.get('meta', {}).get('gerado_em', ''),
        'fonte':     'json_fallback',
    })


@app.get('/api/agenda_dia/meta')
@login_required
def api_agenda_dia_meta():
    """Status da última rodada do ETL por posto. Usado pelo banner do topo."""
    try:
        from f3_db import fetch_meta_all
        meta = fetch_meta_all()
        return jsonify({
            'ok':         True,
            'postos':     {p: {
                'gerado_em':   m['gerado_em'].isoformat() if m['gerado_em'] else None,
                'sucesso':     m['sucesso'],
                'erro':        m['erro'],
                'n_registros': m['n_registros'],
            } for p, m in meta.items()},
            'fonte':      'postgres',
        })
    except Exception as e:
        app.logger.warning('Postgres indisponível para meta: %s', e)

    if not os.path.exists(JSON_FALLBACK):
        return jsonify({'ok': False, 'error': 'fontes indisponíveis'}), 503
    try:
        with open(JSON_FALLBACK, 'r', encoding='utf-8') as f:
            payload = json.load(f)
        gerado_em = payload.get('meta', {}).get('gerado_em', '')
        postos_meta = {p: {'gerado_em': gerado_em, 'sucesso': True,
                           'erro': None, 'n_registros': 0}
                       for p in payload.get('meta', {}).get('postos_no_json', [])}
        return jsonify({'ok': True, 'postos': postos_meta, 'fonte': 'json_fallback'})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500


# ── Templates inline (erro/logout) ───────────────────────────────────────────

TMPL_ERRO = '''<!doctype html><html lang="pt-br"><head><meta charset="UTF-8">
<title>Erro — Agenda F3</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@4.6.2/dist/css/bootstrap.min.css">
</head><body class="d-flex align-items-center justify-content-center" style="min-height:100vh;background:#f4f6f9">
<div class="card shadow" style="max-width:420px;width:100%">
  <div class="card-body text-center p-5">
    <h4 class="text-danger mb-3">{{ titulo }}</h4>
    <p class="text-muted">{{ msg }}</p>
    <a href="/auth/login" class="btn btn-primary mt-3">Tentar novamente</a>
  </div>
</div></body></html>'''

TMPL_LOGOUT = '''<!doctype html><html lang="pt-br"><head><meta charset="UTF-8">
<title>Desconectado — Agenda F3</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@4.6.2/dist/css/bootstrap.min.css">
</head><body class="d-flex align-items-center justify-content-center" style="min-height:100vh;background:#f4f6f9">
<div class="card shadow" style="max-width:420px;width:100%">
  <div class="card-body text-center p-5">
    <h4 class="mb-3">Você saiu</h4>
    <p class="text-muted">Sessão encerrada.</p>
    <a href="/auth/login" class="btn btn-primary mt-3">Entrar novamente</a>
  </div>
</div></body></html>'''


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8024, debug=False)
