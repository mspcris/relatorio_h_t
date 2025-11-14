from flask import Flask, request, make_response, jsonify, render_template
from itsdangerous import TimestampSigner, BadSignature
from passlib.apache import HtpasswdFile
import os, secrets, json

# ===============================
# Configurações
# ===============================

HTPASS_PATH = '/etc/nginx/.htpasswd'           # usuários
ACL_PATH    = '/etc/nginx/postos_acl.json'     # postos permitidos

SESS_NAME   = 'appsess'
SECRET      = os.environ.get('SESSION_SECRET', secrets.token_hex(32))
TTL_SECONDS = 3600 * 8   # 8h

ht = HtpasswdFile(HTPASS_PATH)
signer = TimestampSigner(SECRET)
app = Flask(__name__, template_folder="/opt/camim-auth/templates")


# ===============================
# Funções auxiliares
# ===============================

def set_cookie(resp, value, max_age=TTL_SECONDS):
    resp.set_cookie(
        SESS_NAME,
        value,
        max_age=max_age,
        httponly=True,
        secure=True,
        samesite='Lax',
        path='/'
    )

def load_acl():
    try:
        with open(ACL_PATH) as f:
            return json.load(f)
    except:
        return {}

def decode_user():
    """Retorna email e postos autorizados (None se não autenticado)."""
    c = request.cookies.get(SESS_NAME)
    if not c:
        return None, None

    try:
        raw = signer.unsign(c, max_age=TTL_SECONDS + 3600).decode()
        email = raw.split(':', 1)[0]
        acl = load_acl()
        postos = acl.get(email, [])
        return email, postos
    except BadSignature:
        return None, None


# ===============================
# LOGIN / LOGOUT
# ===============================

@app.post('/session/login')
def login():
    ht.load()
    email = request.form.get('email', '').strip()
    senha = request.form.get('senha', '')

    if not (email and senha) or not ht.check_password(email, senha):
        r = make_response('', 302)
        r.headers['Location'] = '/login?e=1'
        return r

    token = signer.sign(f"{email}:{secrets.token_hex(8)}").decode()
    r = make_response('', 302)
    set_cookie(r, token)
    r.headers['Location'] = '/'
    return r


@app.post('/session/logout')
def logout():
    r = make_response('', 302)
    r.delete_cookie(SESS_NAME, path='/')
    r.headers['Location'] = '/login'
    return r


# ===============================
# /auth — NGINX usa isso
# ===============================

@app.get('/auth')
def auth():
    c = request.cookies.get(SESS_NAME)
    if not c:
        return ('', 401)

    try:
        signer.unsign(c, max_age=TTL_SECONDS + 3600)
        return ('', 200)
    except BadSignature:
        return ('', 401)


# ===============================
# /session/me — usado pelo frontend
# ===============================

@app.get('/session/me')
def session_me():
    email, postos = decode_user()
    if not email:
        return ('', 401)

    return jsonify({
        "email": email,
        "postos": postos
    })


# ======================================================
#  PÁGINAS RENDERIZADAS (COM USER_EMAIL + USER_POSTOS)
# ======================================================

def render_protected_page(page_name):
    email, postos = decode_user()
    if not email:
        return ('', 401)

    # Renderiza template e injeta variáveis
    return render_template(
        page_name,
        USER_EMAIL=email,
        USER_POSTOS=json.dumps(postos)
    )

@app.get('/kpi_v2')
def page_kpi_v2():
    return render_protected_page("kpi_v2.html")

@app.get('/alimentacao')
def page_alimentacao():
    return render_protected_page("alimentacao.html")

@app.get('/medicos')
def page_medicos():
    return render_protected_page("medicos.html")


# ======================================================
# OPCIONAL — fallback para qualquer .html autenticado
# ======================================================
@app.get('/<path:filename>')
def any_html(filename):
    if not filename.endswith(".html"):
        return ('', 404)

    email, postos = decode_user()
    if not email:
        return ('', 401)

    try:
        return render_template(
            filename,
            USER_EMAIL=email,
            USER_POSTOS=json.dumps(postos)
        )
    except:
        return ('', 404)


# ===============================
# Execução manual
# ===============================

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8020)