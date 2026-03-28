"""
wpp-campanhas — Flask app para a plataforma WhatsApp Campanhas.
Autenticação: IDCAMIM OpenID Connect + whitelist local de usuários.
Rotas WPP   : reutilizadas de wpp_cobranca_routes.py via monkey-patch de _check_auth.
Porta       : 8023
"""
import os, sys, json, secrets, sqlite3
from urllib.parse import urlencode
from functools import wraps
from datetime import datetime

import requests as http
from flask import (Flask, request, redirect, session,
                   render_template_string, url_for, abort, jsonify)
from jinja2 import ChoiceLoader, FileSystemLoader
from dotenv import load_dotenv

load_dotenv('/opt/wpp-campanhas/.env')

# Adiciona relatorio_h_t e camim-auth ao path para importar módulos WPP
sys.path.insert(0, '/opt/relatorio_h_t')
sys.path.insert(0, '/opt/camim-auth')

# ── Flask app ─────────────────────────────────────────────────────────────────

app = Flask(__name__, template_folder='/opt/wpp-campanhas/templates')
app.jinja_loader = ChoiceLoader([
    FileSystemLoader('/opt/wpp-campanhas/templates'),
    FileSystemLoader('/opt/camim-auth/templates'),
])
app.secret_key = os.environ.get('WPP_SECRET_KEY', secrets.token_hex(32))
app.config['SESSION_COOKIE_SECURE'] = True
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
app.config['PERMANENT_SESSION_LIFETIME'] = 3600 * 8  # 8h

# ── IDCAMIM OIDC ──────────────────────────────────────────────────────────────

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


# ── Banco whitelist local ──────────────────────────────────────────────────────

WPP_DB = os.getenv('WPP_USERS_DB', '/opt/wpp-campanhas/wpp_users.db')


def _db():
    conn = sqlite3.connect(WPP_DB)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS wpp_users (
            id         INTEGER PRIMARY KEY,
            sub        TEXT UNIQUE NOT NULL,
            email      TEXT NOT NULL,
            nome       TEXT DEFAULT "",
            is_admin   INTEGER NOT NULL DEFAULT 0,
            ativo      INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT (datetime("now"))
        )
    ''')
    return conn


def _find_user(sub):
    with _db() as conn:
        return conn.execute(
            'SELECT * FROM wpp_users WHERE sub=?', (sub,)
        ).fetchone()


def _is_allowed(sub):
    u = _find_user(sub)
    return bool(u and u['ativo'])


def _is_admin(sub):
    u = _find_user(sub)
    return bool(u and u['ativo'] and u['is_admin'])


# ── Decoradores ───────────────────────────────────────────────────────────────

def login_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if 'sub' not in session:
            session['next'] = request.url
            return redirect(url_for('auth_login'))
        return f(*args, **kwargs)
    return wrapper


def admin_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if 'sub' not in session:
            session['next'] = request.url
            return redirect(url_for('auth_login'))
        if not _is_admin(session.get('sub', '')):
            abort(403)
        return f(*args, **kwargs)
    return wrapper


# ── Rotas de autenticação OIDC ────────────────────────────────────────────────

@app.get('/auth/login')
def auth_login():
    state = secrets.token_urlsafe(16)
    session['oidc_state'] = state
    cfg = _oidc_config()
    params = urlencode({
        'response_type': 'code',
        'client_id': IDCAMIM_CLIENT_ID,
        'redirect_uri': IDCAMIM_REDIRECT_URI,
        'scope': 'openid email profile',
        'state': state,
    })
    return redirect(f"{cfg['authorization_endpoint']}?{params}")


@app.get('/auth/callback')
def auth_callback():
    err = request.args.get('error')
    if err:
        return render_template_string(
            TMPL_ERRO, titulo='Erro de autenticação', msg=err
        ), 400

    state    = request.args.get('state', '')
    expected = session.pop('oidc_state', None)
    if not expected or state != expected:
        return render_template_string(
            TMPL_ERRO, titulo='Erro de segurança', msg='State inválido. Tente novamente.'
        ), 400

    code = request.args.get('code')
    if not code:
        return render_template_string(
            TMPL_ERRO, titulo='Erro', msg='Código de autorização ausente.'
        ), 400

    cfg = _oidc_config()

    # Troca code → tokens
    resp = http.post(cfg['token_endpoint'], data={
        'grant_type':   'authorization_code',
        'code':         code,
        'redirect_uri': IDCAMIM_REDIRECT_URI,
        'client_id':    IDCAMIM_CLIENT_ID,
        'client_secret': IDCAMIM_CLIENT_SECRET,
    }, timeout=15)
    if not resp.ok:
        return render_template_string(
            TMPL_ERRO, titulo='Erro', msg='Falha ao trocar código por token.'
        ), 502

    tokens       = resp.json()
    access_token = tokens.get('access_token')

    # Obtém userinfo
    ui = http.get(
        cfg['userinfo_endpoint'],
        headers={'Authorization': f'Bearer {access_token}'},
        timeout=10,
    )
    if not ui.ok:
        return render_template_string(
            TMPL_ERRO, titulo='Erro', msg='Falha ao obter informações do usuário.'
        ), 502

    userinfo = ui.json()
    sub   = userinfo.get('sub')
    email = userinfo.get('email', '')
    nome  = userinfo.get('name') or userinfo.get('preferred_username', '')

    if not sub:
        return render_template_string(
            TMPL_ERRO, titulo='Erro', msg='Identificador de usuário ausente.'
        ), 502

    if not _is_allowed(sub):
        return render_template_string(TMPL_NEGADO, email=email, nome=nome), 403

    session.permanent = True
    session['sub']   = sub
    session['email'] = email
    session['nome']  = nome

    next_url = session.pop('next', '/')
    return redirect(next_url)


@app.route('/auth/logout', methods=['GET', 'POST'])
def auth_logout():
    session.clear()
    return redirect('/auth/login')


# ── Patch auth no wpp_cobranca_routes ─────────────────────────────────────────

def _wpp_check_auth():
    """Substitui _check_auth do wpp_cobranca_routes para usar sessão IDCAMIM."""
    if 'sub' not in session:
        return None, None
    return session.get('email'), _is_admin(session.get('sub', ''))


try:
    import wpp_cobranca_routes as _wpp_mod
    _wpp_mod._check_auth = _wpp_check_auth
    from wpp_cobranca_routes import wpp_bp
    app.register_blueprint(wpp_bp)
except Exception as _e:
    import logging
    logging.getLogger(__name__).error('wpp_bp não carregado: %s', _e)


# ── Rota raiz ─────────────────────────────────────────────────────────────────

@app.get('/')
@login_required
def index():
    return redirect('/wpp')


# ── Before request: garante login para /wpp/* ─────────────────────────────────

@app.before_request
def check_wpp_auth():
    if request.path.startswith('/wpp') and 'sub' not in session:
        session['next'] = request.url
        return redirect(url_for('auth_login'))


# ── Admin whitelist ────────────────────────────────────────────────────────────

@app.get('/admin')
@admin_required
def admin_index():
    with _db() as conn:
        users = conn.execute(
            'SELECT * FROM wpp_users ORDER BY created_at DESC'
        ).fetchall()
    return render_template_string(TMPL_ADMIN, users=users)


@app.post('/admin/usuarios')
@admin_required
def admin_add_user():
    data     = request.get_json(force=True, silent=True) or {}
    sub      = (data.get('sub') or '').strip()
    email    = (data.get('email') or '').strip()
    nome     = (data.get('nome') or '').strip()
    is_admin = int(bool(data.get('is_admin')))
    if not sub or not email:
        return jsonify({'ok': False, 'msg': 'sub e email obrigatórios'}), 400
    with _db() as conn:
        conn.execute('''
            INSERT INTO wpp_users(sub, email, nome, is_admin, ativo)
            VALUES(?, ?, ?, ?, 1)
            ON CONFLICT(sub) DO UPDATE SET
              email=excluded.email, nome=excluded.nome,
              is_admin=excluded.is_admin, ativo=1
        ''', (sub, email, nome, is_admin))
    return jsonify({'ok': True})


@app.post('/admin/usuarios/<int:uid>/toggle')
@admin_required
def admin_toggle(uid):
    with _db() as conn:
        conn.execute('UPDATE wpp_users SET ativo = 1-ativo WHERE id=?', (uid,))
    return jsonify({'ok': True})


@app.post('/admin/usuarios/<int:uid>/delete')
@admin_required
def admin_delete(uid):
    with _db() as conn:
        conn.execute('DELETE FROM wpp_users WHERE id=?', (uid,))
    return jsonify({'ok': True})


# ── Templates inline ──────────────────────────────────────────────────────────

TMPL_ERRO = '''<!doctype html><html lang="pt-br"><head><meta charset="UTF-8">
<title>Erro — WPP Campanhas</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@4.6.2/dist/css/bootstrap.min.css">
</head><body class="d-flex align-items-center justify-content-center" style="min-height:100vh;background:#f4f6f9">
<div class="card shadow" style="max-width:420px;width:100%">
  <div class="card-body text-center p-5">
    <h4 class="text-danger mb-3">{{ titulo }}</h4>
    <p class="text-muted">{{ msg }}</p>
    <a href="/auth/login" class="btn btn-primary mt-3">Tentar novamente</a>
  </div>
</div></body></html>'''

TMPL_NEGADO = '''<!doctype html><html lang="pt-br"><head><meta charset="UTF-8">
<title>Acesso Negado — WPP Campanhas</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@4.6.2/dist/css/bootstrap.min.css">
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@fortawesome/fontawesome-free@5.15.4/css/all.min.css">
</head><body class="d-flex align-items-center justify-content-center" style="min-height:100vh;background:#f4f6f9">
<div class="card shadow" style="max-width:480px;width:100%">
  <div class="card-body text-center p-5">
    <i class="fas fa-ban fa-3x text-danger mb-3"></i>
    <h4 class="mb-2">Acesso não autorizado</h4>
    <p class="text-muted mb-1">Você se autenticou com IDCAMIM mas não possui acesso à plataforma WPP Campanhas.</p>
    {% if email %}<p class="text-muted small">Conta: {{ email }}</p>{% endif %}
    <p class="text-muted">Solicite acesso ao administrador do sistema.</p>
    <a href="/auth/logout" class="btn btn-outline-secondary mt-3">Sair</a>
  </div>
</div></body></html>'''

TMPL_ADMIN = '''<!doctype html><html lang="pt-br"><head><meta charset="UTF-8">
<title>Admin — WPP Campanhas</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@4.6.2/dist/css/bootstrap.min.css">
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@fortawesome/fontawesome-free@5.15.4/css/all.min.css">
<style>.btn-xs{padding:.15rem .4rem;font-size:.75rem}</style>
</head><body style="background:#f4f6f9">
<div class="container mt-4">
  <div class="d-flex justify-content-between align-items-center mb-3">
    <h4><i class="fas fa-users-cog mr-2"></i>Usuários WPP Campanhas</h4>
    <a href="/wpp" class="btn btn-sm btn-outline-secondary">← Plataforma</a>
  </div>
  <div class="card mb-4">
    <div class="card-header font-weight-bold">Adicionar usuário</div>
    <div class="card-body">
      <div class="form-row align-items-end">
        <div class="col-md-3 mb-2"><label class="small">sub (IDCAMIM)</label>
          <input id="f_sub" class="form-control form-control-sm" placeholder="sub"></div>
        <div class="col-md-3 mb-2"><label class="small">email</label>
          <input id="f_email" class="form-control form-control-sm" placeholder="email"></div>
        <div class="col-md-3 mb-2"><label class="small">nome</label>
          <input id="f_nome" class="form-control form-control-sm" placeholder="nome"></div>
        <div class="col-md-1 mb-2">
          <div class="form-check mt-4">
            <input class="form-check-input" type="checkbox" id="f_admin">
            <label class="form-check-label small" for="f_admin">Admin</label>
          </div>
        </div>
        <div class="col-md-2 mb-2 mt-4">
          <button class="btn btn-primary btn-sm btn-block" onclick="addUser()">Adicionar</button>
        </div>
      </div>
    </div>
  </div>
  <div class="card">
    <table class="table table-sm table-bordered mb-0">
      <thead class="thead-light">
        <tr><th>sub</th><th>email</th><th>nome</th><th>admin</th><th>status</th><th>criado em</th><th></th></tr>
      </thead>
      <tbody>
        {% for u in users %}
        <tr id="row-{{ u.id }}">
          <td class="text-monospace small" title="{{ u.sub }}">{{ u.sub[:24] }}…</td>
          <td>{{ u.email }}</td>
          <td>{{ u.nome }}</td>
          <td class="text-center">{% if u.is_admin %}<span class="badge badge-warning">admin</span>{% endif %}</td>
          <td class="text-center">
            <span class="badge badge-{{ "success" if u.ativo else "secondary" }}">
              {{ "ativo" if u.ativo else "inativo" }}
            </span>
          </td>
          <td class="small text-muted">{{ u.created_at[:16] }}</td>
          <td class="text-nowrap">
            <button class="btn btn-xs btn-outline-{{ "warning" if u.ativo else "success" }}"
              onclick="toggle({{ u.id }})">{{ "Desativar" if u.ativo else "Ativar" }}</button>
            <button class="btn btn-xs btn-outline-danger ml-1"
              onclick="del({{ u.id }})">Excluir</button>
          </td>
        </tr>
        {% else %}
        <tr><td colspan="7" class="text-center text-muted py-3">Nenhum usuário cadastrado.</td></tr>
        {% endfor %}
      </tbody>
    </table>
  </div>
</div>
<script>
async function addUser() {
  const body = {sub:f_sub.value.trim(), email:f_email.value.trim(),
                nome:f_nome.value.trim(), is_admin:f_admin.checked};
  const r = await fetch('/admin/usuarios', {method:'POST',
    headers:{'Content-Type':'application/json'}, body:JSON.stringify(body)});
  const d = await r.json();
  if (d.ok) location.reload(); else alert(d.msg);
}
async function toggle(id) {
  await fetch('/admin/usuarios/'+id+'/toggle', {method:'POST'});
  location.reload();
}
async function del(id) {
  if (!confirm('Excluir usuário?')) return;
  await fetch('/admin/usuarios/'+id+'/delete', {method:'POST'});
  document.getElementById('row-'+id).remove();
}
</script></body></html>'''


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8023, debug=False)
