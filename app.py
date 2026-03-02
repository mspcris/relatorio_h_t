from flask import Flask, request, make_response, jsonify, render_template
from itsdangerous import TimestampSigner, BadSignature
from passlib.apache import HtpasswdFile
import os, secrets, json
from datetime import date, datetime
import re
import time
from urllib.parse import quote_plus

from sqlalchemy import create_engine, text

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
# Metas API (SQL Server por posto)
# ===============================

ODBC_DRIVER = os.getenv("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")
JSON_METAS_DIR = os.getenv("JSON_METAS_DIR", "/var/www/json_metas").strip()  # caminho físico dos JSONs servidos em /json_metas pelo nginx para uptade imediato na tela

def _env(key, default=""):
    v = os.getenv(key, default)
    return v.strip() if isinstance(v, str) else v

def _sleep_backoff(attempt: int):
    time.sleep(min(0.5 * (2 ** (attempt - 1)), 5.0))

def _month_bounds_from_ym(ym: str):
    m = re.match(r"^(\d{4})-(\d{2})$", (ym or "").strip())
    if not m:
        raise ValueError("ym inválido; esperado YYYY-MM")
    y = int(m.group(1))
    mo = int(m.group(2))
    if mo < 1 or mo > 12:
        raise ValueError("mês inválido")
    ini = date(y, mo, 1)
    # próximo mês
    if mo == 12:
        fim = date(y + 1, 1, 1)
    else:
        fim = date(y, mo + 1, 1)
    return y, mo, ini, fim

def _build_conn_str_for_posto(posto: str):
    p = (posto or "").strip()
    if not p:
        raise ValueError("posto obrigatório")

    host = _env(f"DB_HOST_{p}") or _env(f"DB_HOST_{p.lower()}")
    base = _env(f"DB_BASE_{p}") or _env(f"DB_BASE_{p.lower()}")
    if not host or not base:
        raise ValueError(f"posto sem configuração no env (DB_HOST_{p} / DB_BASE_{p})")

    user = _env(f"DB_USER_{p}") or _env(f"DB_USER_{p.lower()}")
    pwd  = _env(f"DB_PASSWORD_{p}") or _env(f"DB_PASSWORD_{p.lower()}")
    port = _env(f"DB_PORT_{p}", "1433") or _env(f"DB_PORT_{p.lower()}", "1433")

    encrypt    = _env("DB_ENCRYPT", "yes")
    trust_cert = _env("DB_TRUST_CERT", "yes")
    timeout    = _env("DB_TIMEOUT", "20")

    server = f"tcp:{host},{port or '1433'}"
    common = (
        f"DRIVER={{{ODBC_DRIVER}}};"
        f"SERVER={server};DATABASE={base};"
        f"Encrypt={encrypt};TrustServerCertificate={trust_cert};"
        f"Connection Timeout={timeout or '5'};"
    )
    if user:
        return common + f"UID={user};PWD={pwd}"
    return common + "Trusted_Connection=yes"

def _make_engine(odbc_conn_str: str):
    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_conn_str)}",
        pool_pre_ping=True,
        pool_recycle=300,
        future=True,
    )

def _try_build_engine_for_posto(posto: str, retries: int = 4):
    last_err = None
    odbc = _build_conn_str_for_posto(posto)
    for attempt in range(1, retries + 1):
        try:
            eng = _make_engine(odbc)
            with eng.connect() as con:
                con.execute(text("SELECT 1"))
            return eng
        except Exception as e:
            last_err = e
            if attempt < retries:
                _sleep_backoff(attempt)
    raise last_err

def _json_path(posto: str, ym: str):
    return os.path.join(JSON_METAS_DIR, f"{posto}_metas_{ym}.json")

def _patch_json_meta(posto: str, ym: str, meta_obj: dict):
    """
    Atualiza APENAS o bloco 'meta' e 'gerado_em' do JSON já existente.
    Se o arquivo não existir, não cria (para não inventar payload parcial).
    """
    if not JSON_METAS_DIR:
        return {"patched": False, "reason": "JSON_METAS_DIR não definido"}

    path = _json_path(posto, ym)
    if not os.path.exists(path):
        return {"patched": False, "reason": "arquivo JSON do mês não existe"}

    with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    payload["meta"] = meta_obj
    payload["gerado_em"] = datetime.now().astimezone().isoformat(timespec="seconds")

    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

    return {"patched": True}

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

@app.get('/kpi_receita_despesa_rateio')
def r_rateio():
    return render_protected_page("kpi_receita_despesa_rateio.html")


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

@app.get('/kpi_receita_despesa_rateio.html')
def h_rateio():
    return render_protected_page("kpi_receita_despesa_rateio.html")


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
# ROTAS PÚBLICAS (SEM LOGIN)
# ===============================

@app.get("/kpi_prescricao_aberta.html")
def kpi_prescricao_aberta():
    return render_template("kpi_prescricao_aberta.html")


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
# API Metas
# ===============================

@app.get("/api/metas")
def api_get_metas():
    email, postos_acl = decode_user()
    if not email:
        return ("", 401)

    posto = (request.args.get("posto") or "").strip()
    ym = (request.args.get("ym") or "").strip()

    if not posto or not ym:
        return jsonify({"ok": False, "code": "BAD_REQUEST", "message": "posto e ym são obrigatórios"}), 400

    if posto not in (postos_acl or []):
        return jsonify({"ok": False, "code": "FORBIDDEN", "message": "posto não autorizado"}), 403

    try:
        y, mo, ini, fim = _month_bounds_from_ym(ym)
    except Exception as e:
        return jsonify({"ok": False, "code": "BAD_REQUEST", "message": str(e)}), 400

    try:
        eng = _try_build_engine_for_posto(posto, retries=4)
    except Exception as e:
        return jsonify({
            "ok": False,
            "code": "POSTO_OFFLINE",
            "message": f"Não foi possível conectar ao posto {posto}",
            "detail": str(e)
        }), 503

    sql = """
    SET NOCOUNT ON;
    SELECT TOP 1
        idMetaFilial, ano, mes, DataReferencia,
        Meta1Venda, Meta2Venda, Meta1Mensalidade, Meta2Mensalidade, desativado
    FROM Cad_MetaFilial
    WHERE desativado = 0
      AND ano = :ano
      AND mes = :mes
    ORDER BY idMetaFilial DESC;
    """

    with eng.connect() as con:
        row = con.execute(text(sql), {"ano": y, "mes": mo}).mappings().first()

    if not row:
        return jsonify({
            "ok": True,
            "exists": False,
            "posto": posto,
            "ym": ym,
            "meta": None
        })

    meta = {
        "idMetaFilial": row.get("idMetaFilial"),
        "ano": int(row.get("ano")),
        "mes": int(row.get("mes")),
        "DataReferencia": str(row.get("DataReferencia")),
        "Meta1Venda": float(row.get("Meta1Venda") or 0),
        "Meta2Venda": float(row.get("Meta2Venda") or 0),
        "Meta1Mensalidade": float(row.get("Meta1Mensalidade") or 0),
        "Meta2Mensalidade": float(row.get("Meta2Mensalidade") or 0),
        "desativado": int(row.get("desativado") or 0),
    }

    return jsonify({
        "ok": True,
        "exists": True,
        "posto": posto,
        "ym": ym,
        "meta": meta
    })


@app.post("/api/metas")
def api_upsert_metas():
    email, postos_acl = decode_user()
    if not email:
        return ("", 401)

    try:
        payload = request.get_json(force=True, silent=False) or {}
    except Exception:
        return jsonify({"ok": False, "code": "BAD_REQUEST", "message": "JSON inválido"}), 400

    posto = (payload.get("posto") or "").strip()
    ym = (payload.get("ym") or "").strip()

    if not posto or not ym:
        return jsonify({"ok": False, "code": "BAD_REQUEST", "message": "posto e ym são obrigatórios"}), 400

    if posto not in (postos_acl or []):
        return jsonify({"ok": False, "code": "FORBIDDEN", "message": "posto não autorizado"}), 403

    try:
        y, mo, ini, fim = _month_bounds_from_ym(ym)
    except Exception as e:
        return jsonify({"ok": False, "code": "BAD_REQUEST", "message": str(e)}), 400

    def _fnum(x):
        try:
            n = float(x)
            return n if n >= 0 else 0.0
        except Exception:
            return 0.0

    meta2_mens = _fnum(payload.get("meta2_mens"))
    meta2_venda = _fnum(payload.get("meta2_venda"))

    # regra: meta1 sugere 90% da meta2, mas pode editar
    meta1_mens = payload.get("meta1_mens", None)
    meta1_venda = payload.get("meta1_venda", None)

    meta1_mens = _fnum(meta1_mens) if meta1_mens is not None else round(meta2_mens * 0.90, 2)
    meta1_venda = _fnum(meta1_venda) if meta1_venda is not None else round(meta2_venda * 0.90, 2)

    try:
        eng = _try_build_engine_for_posto(posto, retries=4)
    except Exception as e:
        return jsonify({
            "ok": False,
            "code": "POSTO_OFFLINE",
            "message": f"Não foi possível conectar ao posto {posto}",
            "detail": str(e)
        }), 503

    # UPSERT por (ano, mes, desativado=0)
    merge_sql = """
    SET NOCOUNT ON;

    MERGE Cad_MetaFilial AS tgt
    USING (SELECT :ano AS ano, :mes AS mes) AS src
      ON (tgt.ano = src.ano AND tgt.mes = src.mes AND tgt.desativado = 0)
    WHEN MATCHED THEN
      UPDATE SET
        DataReferencia = :dataref,
        Meta1Venda = :m1v,
        Meta2Venda = :m2v,
        Meta1Mensalidade = :m1m,
        Meta2Mensalidade = :m2m,
        desativado = 0
    WHEN NOT MATCHED THEN
      INSERT (ano, mes, DataReferencia, Meta1Venda, Meta2Venda, Meta1Mensalidade, Meta2Mensalidade, desativado)
      VALUES (:ano, :mes, :dataref, :m1v, :m2v, :m1m, :m2m, 0);

    SELECT TOP 1 idMetaFilial
    FROM Cad_MetaFilial
    WHERE desativado = 0 AND ano = :ano AND mes = :mes
    ORDER BY idMetaFilial DESC;
    """

    params = {
        "ano": y,
        "mes": mo,
        "dataref": ini,
        "m1v": meta1_venda,
        "m2v": meta2_venda,
        "m1m": meta1_mens,
        "m2m": meta2_mens,
    }

    with eng.begin() as con:
        new_id = con.execute(text(merge_sql), params).scalar()

    # Patch do JSON (se possível) para refletir imediato na tela
    meta_obj_json = {
        "codigo": None,
        "ano": y,
        "mes": mo,
        "data_referencia": ini.isoformat(),
        "meta_mens": float(meta2_mens),
        "meta_venda": float(meta2_venda),
    }
    try:
        patch_result = _patch_json_meta(posto, ym, meta_obj_json)
    except Exception as e:
        patch_result = {"patched": False, "reason": f"falha ao atualizar JSON: {e}"}

    return jsonify({
        "ok": True,
        "idMetaFilial": int(new_id) if new_id is not None else None,
        "posto": posto,
        "ym": ym,
        "saved": {
            "Meta1Mensalidade": meta1_mens,
            "Meta2Mensalidade": meta2_mens,
            "Meta1Venda": meta1_venda,
            "Meta2Venda": meta2_venda,
        },
        "json_patch": patch_result
    })


# ===============================
# Execução manual
# ===============================

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8020)
