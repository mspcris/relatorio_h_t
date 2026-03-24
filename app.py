from flask import Flask, request, make_response, jsonify, render_template
import os, secrets, json, sqlite3
from datetime import date, datetime
import re
import time
from urllib.parse import quote_plus
from pathlib import Path

from sqlalchemy import create_engine, text

from dotenv import load_dotenv
load_dotenv("/opt/relatorio_h_t/.env")


# ===============================
# Configurações
# ===============================

ACL_PATH    = '/etc/nginx/postos_acl.json'   # mantido para /postos_acl.json

SESS_NAME   = 'appsess'
SECRET      = os.environ.get('SESSION_SECRET', secrets.token_hex(32))
TTL_SECONDS = 3600 * 8   # 8h

# Pasta única de templates
app = Flask(__name__, template_folder="/opt/camim-auth/templates")

from ia_router_openai import ia_bp
app.register_blueprint(ia_bp)

from auth_routes import auth_bp, init_auth, decode_user
app.register_blueprint(auth_bp)
init_auth(SESS_NAME, SECRET, TTL_SECONDS)

try:
    from wpp_cobranca_routes import wpp_bp
    app.register_blueprint(wpp_bp)
except Exception as _e:
    import logging
    logging.getLogger(__name__).error("wpp_bp não carregado: %s", _e)

PAGE_ACCESS_DB = os.getenv("PAGE_ACCESS_DB", "/opt/camim-auth/page_access.db")


def _page_db():
    conn = sqlite3.connect(PAGE_ACCESS_DB)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS page_access (
            path      TEXT PRIMARY KEY,
            hits      INTEGER NOT NULL DEFAULT 0,
            last_hit  TEXT    NOT NULL
        )
    """)
    return conn


def _route_paths() -> set[str]:
    paths = set()
    for rule in app.url_map.iter_rules():
        if "<" in rule.rule:
            continue
        paths.add(rule.rule)
    return paths


def _canonical_path(path: str) -> str:
    p = (path or "/").strip()
    if not p.startswith("/"):
        p = "/" + p
    p = re.sub(r"/{2,}", "/", p)
    if p in ("/", "/index", "/index.html"):
        return "/index.html"
    if p.endswith(".html"):
        return p[:-5]
    return p


def _is_trackable_path(path: str) -> bool:
    p = (path or "").lower()
    if not p or p.startswith("/api/"):
        return False
    if p.startswith(("/js/", "/css/", "/images/", "/fonts/", "/public/")):
        return False
    if p.startswith(("/session/", "/admin/api/", "/auth/")):
        return False
    if p.startswith("/json_") or p.startswith("/export_") or p.startswith("/sql_"):
        return False
    if p in ("/favicon.ico",):
        return False
    if "." in p and not p.endswith(".html"):
        return False
    return True


def _title_from_path(path: str) -> str:
    slug = path.strip("/").split("/")[-1]
    slug = slug.replace(".html", "")
    if not slug:
        return "Home page"
    if slug == "kpi_home":
        return "Indicadores"
    return slug.replace("_", " ").replace("-", " ").title()


def _icon_from_path(path: str) -> str:
    p = path.lower()
    if "kpi" in p or "indicador" in p:
        return "fas fa-chart-line"
    if "wpp" in p or "whatsapp" in p:
        return "fab fa-whatsapp"
    if "admin" in p:
        return "fas fa-users-cog"
    if "tef" in p:
        return "fas fa-credit-card"
    if "email" in p:
        return "fas fa-envelope"
    if "trello" in p:
        return "fab fa-trello"
    if "harvest" in p:
        return "fas fa-stopwatch"
    if "servico" in p:
        return "fas fa-th-large"
    return "fas fa-file-alt"


def _link_for_path(canonical: str, routes: set[str]) -> str:
    if canonical == "/index.html":
        return "/index.html"
    if canonical in routes:
        return canonical
    if canonical + ".html" in routes:
        return canonical + ".html"
    return canonical


@app.before_request
def track_page_hits():
    if request.method != "GET":
        return
    if not _is_trackable_path(request.path):
        return
    email, _ = decode_user()
    if not email:
        return
    canonical = _canonical_path(request.path)
    if canonical == "/index.html":
        return
    now = datetime.now().astimezone().isoformat(timespec="seconds")
    try:
        with _page_db() as conn:
            conn.execute(
                """
                INSERT INTO page_access(path, hits, last_hit)
                VALUES(?, 1, ?)
                ON CONFLICT(path) DO UPDATE SET
                  hits = hits + 1,
                  last_hit = excluded.last_hit
                """,
                (canonical, now),
            )
    except Exception:
        return

# ===============================
# Funções auxiliares
# ===============================

def load_acl():
    try:
        with open(ACL_PATH) as f:
            return json.load(f)
    except:
        return {}

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

# login/logout/auth/session/me → auth_routes.py (blueprint)


# ============================================
# RENDERIZAÇÃO PROTEGIDA PADRÃO
# ============================================

def render_protected_page(page_name):
    from auth_db import SessionLocal, get_user_by_email as _gue
    email, postos = decode_user()
    if not email:
        return ('', 401)

    db = SessionLocal()
    try:
        u = _gue(db, email)
        is_admin = u.is_admin if u else False
    finally:
        db.close()

    return render_template(
        page_name,
        USER_EMAIL=email,
        USER_POSTOS=json.dumps(postos),
        USER_IS_ADMIN=is_admin,
    )


# ===============================
# ROTAS SEM .html
# ===============================

@app.get('/mais_servicos')
def r_mais_servicos():
    return render_protected_page("mais_servicos.html")

@app.get('/email_clientes')
def r_email_clientes():
    return render_protected_page("email_clientes_dashboard.html")

@app.get('/email_clientes/logs')
def r_email_clientes_logs():
    return render_protected_page("email_clientes_logs.html")

@app.get('/tef')
def r_tef():
    return render_protected_page("tef_dashboard.html")

@app.get('/tef/logs')
def r_tef_logs():
    return render_protected_page("tef_logs.html")

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

@app.get('/mais_servicos.html')
def h_mais_servicos():
    return render_protected_page("mais_servicos.html")

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
# API Acessos de Páginas
# ===============================

@app.get("/api/pages/acessos")
def api_pages_acessos():
    email, postos = decode_user()
    if not email:
        return ("", 401)

    routes = _route_paths()
    tpl_dir = Path(app.template_folder or ".")
    pages = []
    try:
        for fp in sorted(tpl_dir.glob("*.html")):
            name = fp.name
            if name in ("index.html", "login.html", "login_admin.html", "nova_senha.html", "reset_senha.html"):
                continue
            canonical = _canonical_path("/" + name)
            pages.append(canonical)
    except Exception:
        pass

    # rotas sem .html relevantes do projeto
    for p in routes:
        if p in ("/", "/index.html"):
            continue
        if not _is_trackable_path(p):
            continue
        pages.append(_canonical_path(p))

    uniq = sorted(set(pages))
    counts = {}
    try:
        with _page_db() as conn:
            rows = conn.execute("SELECT path, hits, last_hit FROM page_access").fetchall()
            counts = {r["path"]: {"hits": int(r["hits"]), "last_hit": r["last_hit"]} for r in rows}
    except Exception:
        counts = {}

    items = []
    for canonical in uniq:
        info = counts.get(canonical, {"hits": 0, "last_hit": None})
        items.append({
            "path": canonical,
            "url": _link_for_path(canonical, routes),
            "title": _title_from_path(canonical),
            "icon": _icon_from_path(canonical),
            "hits": info["hits"],
            "last_hit": info["last_hit"],
        })

    items.sort(key=lambda x: (x["hits"], x["title"]), reverse=True)
    total_hits = sum(x["hits"] for x in items)
    return jsonify({"ok": True, "total_hits": total_hits, "total_pages": len(items), "items": items})


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
    # Retorna apenas o mapeamento do usuário atual (do DB, não do arquivo estático)
    return jsonify({email: postos})


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
