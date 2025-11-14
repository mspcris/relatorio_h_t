#/opt/camim-auth/app.py
from flask import Flask, request, make_response, jsonify
from itsdangerous import TimestampSigner, BadSignature
from passlib.apache import HtpasswdFile
import os, secrets, json

# ===============================
# Configurações
# ===============================

HTPASS_PATH = '/etc/nginx/.htpasswd'           # usuários existentes
ACL_PATH    = '/etc/nginx/postos_acl.json'     # postos permitidos por usuário

SESS_NAME   = 'appsess'
SECRET      = os.environ.get('SESSION_SECRET', secrets.token_hex(32))
TTL_SECONDS = 3600 * 8   # 8 horas

ht = HtpasswdFile(HTPASS_PATH)
signer = TimestampSigner(SECRET)
app = Flask(__name__)


# ===============================
# Funções auxiliares
# ===============================

def set_cookie(resp, value, max_age=TTL_SECONDS):
    """Define cookie seguro da sessão."""
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
    """Carrega permissões de postos por usuário."""
    try:
        with open(ACL_PATH) as f:
            return json.load(f)
    except:
        return {}


# ===============================
# Endpoints de sessão / login
# ===============================

@app.post('/session/login')
def login():
    ht.load()  # recarrega htpasswd sempre
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
# /auth — usado pelo NGINX (NÃO ALTERAR)
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
# /session/me — usado pelo FRONTEND
# ===============================

@app.get('/session/me')
def session_me():
    """Retorna email + lista de postos permitidos do usuário logado."""
    c = request.cookies.get(SESS_NAME)
    if not c:
        return ('', 401)

    try:
        raw = signer.unsign(c, max_age=TTL_SECONDS + 3600).decode()
        email = raw.split(':', 1)[0]

        acl = load_acl()
        postos = acl.get(email, [])

        return jsonify({
            "email": email,
            "postos": postos
        })

    except BadSignature:
        return ('', 401)


# ===============================
# Execução manual
# ===============================

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8020)
