from flask import Flask, request, make_response, jsonify, render_template
from itsdangerous import TimestampSigner, BadSignature
from passlib.apache import HtpasswdFile
import os, secrets, json

# ===============================
# Configurações
# ===============================

HTPASS_PATH = '/etc/nginx/.htpasswd'
ACL_PATH    = '/etc/nginx/postos_acl.json'

SESS_NAME   = 'appsess'
SECRET      = os.environ.get('SESSION_SECRET', secrets.token_hex(32))
TTL_SECONDS = 3600 * 8   # 8h

ht = HtpasswdFile(HTPASS_PATH)
signer = TimestampSigner(SECRET)

# Pasta única de templates
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
# /auth — usado pelo NGINX
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
# /session/me
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


# ============================================
# RENDERIZAÇÃO PROTEGIDA PADRÃO
# ============================================

def render_protected_page(page_name):
    email, postos = decode_user()
    if not email:
        return ('', 401)

    return render_template(
        page_name,
        USER_EMAIL=email,
        USER_POSTOS=json.dumps(postos)
    )


# ===============================
# ROTAS SEM .html
# ===============================

@app.get('/')
def home():
    return render_protected_page("kpi_home.html")

@app.get('/alimentacao')
def r1(): return render_protected_page("alimentacao.html")

@app.get('/clientes')
def r2(): return render_protected_page("clientes.html")

@app.get('/kpi_governo')
def r3(): return render_protected_page("kpi_governo.html")

@app.get('/kpi_home')
def r4(): return render_protected_page("kpi_home.html")

@app.get('/kpi_prescricao')
def r5(): return render_protected_page("kpi_prescricao.html")

@app.get('/kpi_receita_despesa')
def r6(): return render_protected_page("kpi_receita_despesa.html")

@app.get('/kpi_v2')
def r7(): return render_protected_page("kpi_v2.html")

@app.get('/kpi_vendas')
def r8(): return render_protected_page("kpi_vendas.html")

@app.get('/medicos')
def r9(): return render_protected_page("medicos.html")

@app.get('/carregando')
def r10(): return render_protected_page("carregando.html")

@app.get('/teste')
def r11(): return render_protected_page("teste.html")

@app.get('/trello_harvest')
def r12(): return render_protected_page("trello_harvest.html")


# ===============================
# ROTAS COM .html
# ===============================

@app.get('/alimentacao.html')
def h1(): return render_protected_page("alimentacao.html")

@app.get('/clientes.html')
def h2(): return render_protected_page("clientes.html")

@app.get('/kpi_governo.html')
def h3(): return render_protected_page("kpi_governo.html")

@app.get('/kpi_home.html')
def h4(): return render_protected_page("kpi_home.html")

@app.get('/kpi_prescricao.html')
def h5(): return render_protected_page("kpi_prescricao.html")

@app.get('/kpi_receita_despesa.html')
def h6(): return render_protected_page("kpi_receita_despesa.html")

@app.get('/kpi_v2.html')
def h7(): return render_protected_page("kpi_v2.html")

@app.get('/kpi_vendas.html')
def h8(): return render_protected_page("kpi_vendas.html")

@app.get('/medicos.html')
def h9(): return render_protected_page("medicos.html")

@app.get('/carregando.html')
def h10(): return render_protected_page("carregando.html")

@app.get('/teste.html')
def h11(): return render_protected_page("teste.html")

@app.get('/trello_harvest.html')
def h12(): return render_protected_page("trello_harvest.html")

@app.get('/index.html')
def h13(): return render_protected_page("index.html")

@app.get('/overlay.html')
def h14(): return render_protected_page("overlay.html")


# ===============================
# ACL JSON direto
# ===============================

@app.get('/postos_acl.json')
def postos_acl_json():
    email, postos = decode_user()
    if not email:
        return ('', 401)
    return jsonify(load_acl())


# ===============================
# Fallback final para html
# ===============================

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
