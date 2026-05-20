"""
camila2 — Flask app servindo camila2.ia.camim.com.br.

Auth: cookie de sessão COMPARTILHADO com kpi.camim.com.br (mesmo SECRET_KEY,
SESS_NAME=appsess, cookie domain=.camim.com.br). Quem está logado no KPI
fica logado aqui também — sem fazer login 2x.

Banco: compartilha o SQLite camim_auth.db usado pelo camim-auth (KPI), via
sys.path.insert pra importar `auth_db` + `auth_routes.decode_user`.

Rotas iniciais:
  GET /                    health/landing
  GET /agenda_dia          dashboard agenda do dia (espelha o do KPI)
  GET /agenda_dia.html     alias
  GET /admin               redirect pro admin do KPI (mesmo banco, mesma sessão)
  GET /session/me          {email,is_admin} pra debug
  GET /session/logout      redirect pro logout do KPI

Porta: 8025 (systemd unit camila2.service).

NÃO duplica /api/agenda_dia (que vive no KPI). Em vez disso, o template
de agenda_dia faz fetch para https://kpi.camim.com.br/api/agenda_dia, que
respeita a sessão compartilhada via cookie domain .camim.com.br.
"""
import os
import sys
from datetime import timedelta
from pathlib import Path

from flask import Flask, jsonify, redirect, render_template, request
from itsdangerous import BadSignature, TimestampSigner
from jinja2 import ChoiceLoader, FileSystemLoader
from dotenv import load_dotenv

# Carrega .env: prefere /opt/camila2/.env, cai pra .env local
_env_paths = ['/opt/camila2/.env', str(Path(__file__).parent / '.env')]
for _p in _env_paths:
    if os.path.exists(_p):
        load_dotenv(_p)
        break

# Importa do KPI (camim-auth) — onde está auth_db e auth_routes
# /opt/camim-auth: SessionLocal, User, get_user_by_email
# /opt/relatorio_h_t: bibliotecas auxiliares
for _path in ('/opt/camim-auth', '/opt/relatorio_h_t'):
    if os.path.isdir(_path) and _path not in sys.path:
        sys.path.insert(0, _path)

try:
    from auth_db import SessionLocal, get_user_by_email
except Exception as e:
    raise SystemExit(f"camila2: não consegue importar auth_db do camim-auth: {e}")


# ── Config sessão (alinhada com camim-auth) ────────────────────────────────
# camim-auth usa env SESSION_SECRET (visto em app.py:25). Camila2 LÊ A MESMA
# var pra que o signer compartilhe semente — único jeito de validar o mesmo
# cookie appsess emitido pelo KPI.
SECRET_KEY  = os.environ.get('SESSION_SECRET') or os.environ.get('SECRET_KEY')
if not SECRET_KEY:
    raise SystemExit("camila2: SESSION_SECRET ausente no .env (precisa ser IGUAL ao do camim-auth)")

SESS_NAME   = os.environ.get('SESS_NAME', 'appsess')
TTL_SECONDS = int(os.environ.get('SESS_TTL_SECONDS', 3600 * 8))
_signer     = TimestampSigner(SECRET_KEY)

KPI_BASE = os.environ.get('KPI_BASE_URL', 'https://kpi.camim.com.br')


def decode_user():
    """Lê o cookie appsess (formato do KPI: 'email:rand_hex' assinado) e
    retorna (email, postos_do_banco) ou (None, None).

    Tolerância: usa max_age + 1h igual auth_routes do KPI."""
    c = request.cookies.get(SESS_NAME)
    if not c:
        return (None, None)
    try:
        raw = _signer.unsign(c, max_age=TTL_SECONDS + 3600).decode()
    except BadSignature:
        return (None, None)
    email = raw.split(':', 1)[0]
    db = SessionLocal()
    try:
        user = get_user_by_email(db, email)
        if not user or not user.ativo:
            return (None, None)
        return (email, user.lista_postos())
    finally:
        db.close()


# ── Flask app ───────────────────────────────────────────────────────────────
TEMPLATES_LOCAL = str(Path(__file__).parent / 'templates')
TEMPLATES_KPI   = '/opt/camim-auth/templates'

app = Flask(__name__, template_folder=TEMPLATES_LOCAL)
app.jinja_loader = ChoiceLoader([
    FileSystemLoader(TEMPLATES_LOCAL),
    FileSystemLoader(TEMPLATES_KPI),
])
app.config['SESSION_COOKIE_SECURE']   = True
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(seconds=TTL_SECONDS)


# ── Helpers ──────────────────────────────────────────────────────────────────

def _login_redirect():
    """Sem sessão → manda pro login do KPI preservando o destino original."""
    next_url = request.full_path or '/'
    if next_url.endswith('?'):
        next_url = next_url[:-1]
    return redirect(f"{KPI_BASE}/login?next=https://{request.host}{next_url}")


def _require_user():
    """Decorator-style: se logado retorna User (SQLAlchemy); senão redirect.
    Uso: `user_or_redir = _require_user(); if not isinstance(user_or_redir, User): return user_or_redir`"""
    email, _ = decode_user()
    if not email:
        return _login_redirect()
    db = SessionLocal()
    try:
        u = get_user_by_email(db, email)
        if not u or not u.ativo:
            return _login_redirect()
        return u
    finally:
        db.close()


# ── Rotas ────────────────────────────────────────────────────────────────────

@app.get('/')
def root():
    user_or_redir = _require_user()
    if not hasattr(user_or_redir, 'email'):
        return user_or_redir
    # Landing simples por enquanto: lista o que existe em camila2.
    return (
        '<!doctype html><html lang="pt-br"><head><meta charset="utf-8">'
        '<title>camila2.ia.camim.com.br</title>'
        '<style>body{font-family:system-ui;margin:40px;background:#0b1220;color:#e2e8f0;}'
        'a{color:#60a5fa;text-decoration:none;font-size:1.1rem;display:inline-block;margin-right:18px;}'
        'a:hover{text-decoration:underline;}</style></head><body>'
        f'<h1>camila2.ia.camim.com.br</h1>'
        f'<p style="color:#94a3b8">Logado como <b>{user_or_redir.email}</b></p>'
        '<p><a href="/agenda_dia">/agenda_dia</a>'
        f'<a href="{KPI_BASE}/admin">/admin (KPI)</a>'
        f'<a href="{KPI_BASE}/mais_servicos">Mais Serviços</a>'
        '<a href="/session/logout">Sair</a></p>'
        '</body></html>'
    )


@app.get('/agenda_dia')
@app.get('/agenda_dia.html')
def r_agenda_dia():
    user_or_redir = _require_user()
    if not hasattr(user_or_redir, 'email'):
        return user_or_redir
    u = user_or_redir
    return render_template(
        'agenda_dia.html',
        USER_EMAIL=u.email,
        USER_POSTOS=str(u.lista_postos() if hasattr(u, 'lista_postos') else []),
        USER_IS_ADMIN=bool(u.is_admin),
        USER_HAS_OPENAI=False,
        # Aponta as APIs do agenda_dia pro backend do KPI (que tem a lógica).
        # O template chama /api/agenda_dia no host atual; em camila2 redirecionamos
        # via nginx (location /api/agenda_dia proxy_pass http://kpi).
        KPI_BASE_URL=KPI_BASE,
    )


@app.get('/admin')
def r_admin():
    # Sem replicação local: o admin vive no KPI (mesmo banco e sessão).
    return redirect(f"{KPI_BASE}/admin", code=302)


@app.get('/session/me')
def session_me():
    email, postos = decode_user()
    return jsonify({"email": email, "postos": postos})


@app.get('/session/logout')
def session_logout():
    return redirect(f"{KPI_BASE}/session/logout?next=https://{request.host}/", code=302)


@app.get('/healthz')
def healthz():
    return jsonify({"ok": True, "service": "camila2"})


# WSGI entry point (gunicorn app:app)
if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8025, debug=False)
