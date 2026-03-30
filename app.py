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

try:
    from alarmes_routes import alarmes_bp
    app.register_blueprint(alarmes_bp)
except Exception as _e:
    import logging
    logging.getLogger(__name__).error("alarmes_bp não carregado: %s", _e)

PAGE_ACCESS_DB = os.getenv("PAGE_ACCESS_DB", "/opt/camim-auth/page_access.db")

# Mapeamento page_key → template para controle de acesso por página
_TEMPLATE_TO_PAGINA = {
    "alimentacao.html":                 "alimentacao",
    "kpi_alimentacao.html":             "alimentacao",
    "medicos.html":                     "medicos",
    "kpi_medicos.html":                 "medicos",
    "ctrlq_relatorio.html":             "ctrlq_relatorio",
    "kpi_v2.html":                      "kpi_v2",
    "kpi_vendas.html":                  "kpi_vendas",
    "clientes.html":                    "clientes",
    "kpi_clientes.html":                "clientes",
    "KPI_prescricao.html":              "kpi_prescricao",
    "kpi_prescricao.html":              "kpi_prescricao",
    "kpi_fidelizacao_cliente.html":     "kpi_fidelizacao",
    "kpi_governo.html":                 "kpi_governo",
    "kpi_liberty.html":                 "kpi_liberty",
    "kpi_receita_despesa.html":         "kpi_receita_despesa",
    "kpi_receita_despesa_rateio.html":  "kpi_receita_despesa_rateio",
    "kpi_consultas_status.html":        "kpi_consultas",
    "kpi_notas_rps.html":               "kpi_notas_rps",
    "kpi_metas_vendas_mensalidades.html": "kpi_metas",
    "kpi_metas.html":                   "kpi_metas",
    "growth.html":                      "growth",
    "leads_analytics.html":             "leads_analytics",
    "mais_servicos.html":               "mais_servicos",
    "trello_harvest.html":              "trello_harvest",
    "tef_dashboard.html":               "tef",
    "tef_logs.html":                    "tef",
    "email_clientes_dashboard.html":    "email_clientes",
    "email_clientes_logs.html":         "email_clientes",
    "chat_avaliacoes.html":             "chat_avaliacoes",
    # rotas Flask sem .html (usadas como href no sidebar e em cards)
    "chat_avaliacoes":                  "chat_avaliacoes",
    "email_clientes":                   "email_clientes",
    "tef":                              "tef",
    "ctrlq_desbloqueio.html":           "ctrlq_desbloqueio",
    "ctrlq_desbloqueio":               "ctrlq_desbloqueio",
    "qualidade_agenda.html":            "qualidade_agenda",
    "qualidade_agenda":                 "qualidade_agenda",
    # Itens de mais_servicos.html (internos)
    "k_adicional_NBS-IBS-CBS.html":    "k_nbs_ibs_cbs",
    "k_adicional_relatorio_pcs.html":  "k_relatorio_pcs",
    "k_whatsapp_como_funciona.html":   "k_whatsapp_explicado",
    # Itens de mais_servicos.html (externos — usados no filtro do sidebar)
    "https://cobranca.camim.com.br/":                   "cobranca",
    "https://chat.camim.com.br/":                       "chat_externo",
    "https://broker.camim.com.br/":                     "broker",
    "https://corretores.camim.com.br/":                 "corretores",
    "https://tarefas.camim.com.br/":                    "tarefas",
    "https://camila5.ia.camim.com.br/login?next=/":     "push_cobranca",
    "https://camila1.ia.camim.com.br/":                 "wpp_campanhas",
    "https://atendimento.camilaia.camim.com.br/crm":    "camila_crm",
    "https://crm.camim.com.br/":                        "crm",
}

_MENU_RESOURCES_CACHE = None


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


def _extract_menu_links(html: str) -> list[dict]:
    links = []
    for m in re.finditer(r'<a\b([^>]*?)>', html, flags=re.I | re.S):
        attrs = m.group(1)
        attrs_l = attrs.lower()
        if "nav-link" not in attrs_l and "menu-link" not in attrs_l:
            continue
        href_m = re.search(r'href\s*=\s*["\']([^"\']+)["\']', attrs, flags=re.I)
        if not href_m:
            continue
        href = href_m.group(1).strip()
        after = html[m.end(): m.end() + 350]
        icon_m = re.search(r'<i\b[^>]*class\s*=\s*["\']([^"\']+)["\']', after, flags=re.I)
        title_m = re.search(r'<p[^>]*>(.*?)</p>', after, flags=re.I | re.S)
        title = re.sub(r'\s+', ' ', title_m.group(1)).strip() if title_m else ""
        icon = icon_m.group(1).strip() if icon_m else "fas fa-file-alt"
        links.append({"href": href, "title": title, "icon": icon})
    return links


def _is_internal_href(href: str) -> bool:
    h = (href or "").strip()
    if not h or h.startswith(("#", "javascript:", "mailto:", "tel:")):
        return False
    if h.startswith(("http://", "https://", "//")):
        return False
    return True


def _canonical_path(path: str) -> str:
    p = (path or "/").strip()
    if not p.startswith("/"):
        p = "/" + p
    p = re.sub(r"/{2,}", "/", p)
    if p in ("/", "/index", "/index.html"):
        return "/index.html"
    if p.endswith("/") and len(p) > 1:
        p = p[:-1]
    if p.endswith(".html"):
        return p[:-5]
    return p


def _allowed_resource(path: str) -> bool:
    p = (path or "").lower()
    if p == "/index":
        return False
    if p.startswith("/api/") or p.startswith("/session/") or p.startswith("/admin/api/"):
        return False
    if p in ("/logout", "/login", "/login.html", "/teste", "/teste.html", "/overlay", "/overlay.html", "/header", "/footer"):
        return False
    if p.startswith("/te/"):
        return False
    if p.startswith("/tef/") and p != "/tef":
        return False
    if p.startswith("/wpp/"):
        return False
    return True


def _menu_resources() -> dict:
    global _MENU_RESOURCES_CACHE
    if _MENU_RESOURCES_CACHE is not None:
        return _MENU_RESOURCES_CACHE

    resources = {}
    tpl_dir = Path(app.template_folder or ".")
    for fp in tpl_dir.glob("*.html"):
        try:
            html = fp.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        for item in _extract_menu_links(html):
            href = item["href"]
            if not _is_internal_href(href):
                continue
            canonical = _canonical_path(href)
            if not _allowed_resource(canonical):
                continue
            if canonical == "/wpp":
                # Mantém somente home do módulo WPP.
                item["title"] = item["title"] or "WhatsApp Cobranca"
            url = href if href.startswith("/") else "/" + href
            if canonical not in resources:
                resources[canonical] = {
                    "path": canonical,
                    "url": url,
                    "title": item["title"] or _title_from_path(canonical),
                    "icon": item["icon"] or _icon_from_path(canonical),
                }
    _MENU_RESOURCES_CACHE = resources
    return resources


def _is_trackable_path(path: str) -> bool:
    p = _canonical_path(path)
    return p in _menu_resources()


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
    if not _is_trackable_path(canonical):
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

def _sidebar_filter_script(paginas: list) -> str:
    """JS injetado no </body> para ocultar itens do menu que o usuário não tem acesso."""
    href_map = json.dumps(_TEMPLATE_TO_PAGINA)
    plist    = json.dumps(paginas)
    return (
        f'<script>(function(){{'
        f'var p={plist},m={href_map};'
        f'function hide(){{'
        # sidebar nav items
        f'document.querySelectorAll(".nav-sidebar .nav-item a.nav-link").forEach(function(a){{'
        f'var h=(a.getAttribute("href")||"").replace(/^\//,"");'
        f'var k=m[h];if(k&&p.indexOf(k)===-1){{var li=a.closest(".nav-item");if(li)li.style.display="none";}}'
        f'}});'
        # cards (btn-acessar) — hide the parent col-* container
        f'document.querySelectorAll("a.btn-acessar").forEach(function(a){{'
        f'var h=(a.getAttribute("href")||"").replace(/^\//,"");'
        f'var k=m[h];if(k&&p.indexOf(k)===-1){{var col=a.closest("[class*=\'col-\']");if(col)col.style.display="none";}}'
        f'}});'
        f'}}'
        f'if(document.readyState==="loading")document.addEventListener("DOMContentLoaded",hide);else hide();'
        f'}})();</script></body>'
    )


def render_protected_page(page_name, **extra_vars):
    from auth_db import SessionLocal, get_user_by_email as _gue
    email, postos = decode_user()
    if not email:
        return ('', 401)

    db = SessionLocal()
    try:
        u = _gue(db, email)
        is_admin   = u.is_admin if u else False
        all_pages  = bool(u.all_pages) if u and hasattr(u, 'all_pages') else True
        paginas    = u.lista_paginas() if u and not all_pages else []
        # Check page-level access
        page_key = _TEMPLATE_TO_PAGINA.get(page_name)
        if page_key and not all_pages and page_key not in paginas:
            return render_template(
                "acesso_negado.html",
                USER_EMAIL=email,
                USER_IS_ADMIN=is_admin,
                USER_POSTOS=json.dumps(postos),
                PAGINA_BLOQUEADA=page_name,
            ), 403
    finally:
        db.close()

    html = render_template(
        page_name,
        USER_EMAIL=email,
        USER_POSTOS=json.dumps(postos),
        USER_IS_ADMIN=is_admin,
        **extra_vars,
    )
    if not all_pages:
        html = html.replace('</body>', _sidebar_filter_script(paginas))
    return html


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

@app.get('/chat_avaliacoes')
def r_chat_avaliacoes():
    return render_protected_page("chat_avaliacoes.html")

@app.get('/ctrlq_desbloqueio')
def r_ctrlq_desbloqueio():
    from auth_db import SessionLocal, get_user_by_email as _gue
    email, _ = decode_user()
    pode = False
    if email:
        db = SessionLocal()
        try:
            u = _gue(db, email)
            pode = bool(getattr(u, 'pode_desbloquear', False)) if u else False
        finally:
            db.close()
    return render_protected_page("ctrlq_desbloqueio.html",
                                 USER_PODE_DESBLOQUEAR=pode)


# ── API: ações de desbloqueio de agenda ──────────────────────────────────────

def _require_pode_desbloquear():
    """Retorna (user, email, id_usuario_sqlserver) se o usuário autenticado pode desbloquear, senão None."""
    from auth_db import SessionLocal, get_user_by_email as _gue
    email, _ = decode_user()
    if not email:
        return None
    db = SessionLocal()
    try:
        u = _gue(db, email)
        if u and getattr(u, 'pode_desbloquear', False) and getattr(u, 'id_usuario_sqlserver', None):
            return u, email, u.id_usuario_sqlserver
    finally:
        db.close()
    return None


def _get_sqlserver_engine(posto):
    """Retorna SQLAlchemy engine para o SQL Server do posto."""
    from ctrlq_desbloqueio import build_conns_from_env, make_engine
    conns = build_conns_from_env([posto])
    conn_str = conns.get(posto)
    if not conn_str:
        return None
    return make_engine(conn_str)


# IDs fixos das tabelas de lookup do SQL Server
_ID_TABELA_CAD_ESPECIALIDADE = 53
_ID_COMANDO_EDICAO = 2



@app.post('/api/ctrlq/retirar_data_fim')
def api_retirar_data_fim():
    auth = _require_pode_desbloquear()
    if not auth:
        return jsonify({"erro": "Não autorizado"}), 403
    user, email, id_usuario_sql = auth

    data = request.get_json(silent=True) or {}
    id_esp = data.get("idEspecialidade")
    posto = data.get("posto", "").upper()
    if not id_esp or not posto:
        return jsonify({"erro": "Parâmetros obrigatórios: idEspecialidade, posto"}), 400

    engine = _get_sqlserver_engine(posto)
    if not engine:
        return jsonify({"erro": f"Conexão do posto {posto} não configurada"}), 500

    from sqlalchemy import text as sa_text
    try:
        with engine.begin() as conn:
            # Busca valor atual + especialidade para auditoria
            row = conn.execute(sa_text(
                "SELECT ce.DataFimExibicao, ce.Especialidade "
                "FROM cad_especialidade ce WHERE ce.idEspecialidade = :id"
            ), {"id": id_esp}).fetchone()
            if not row:
                return jsonify({"erro": "Registro não encontrado"}), 404

            valor_antigo = str(row[0]) if row[0] else "NULL"
            especialidade = row[1] or ""

            # UPDATE: remove data fim
            conn.execute(sa_text(
                "UPDATE cad_especialidade SET DataFimExibicao = NULL WHERE idEspecialidade = :id"
            ), {"id": id_esp})

            # INSERT auditoria em Sis_Historico (tabela base com IDs de FK)
            nome_usuario = user.nome or email
            detalhe = (
                f"Alteração da Especialidade {especialidade} - "
                f"Limpou data fim de exibição - Usuário {nome_usuario}"
            )
            conn.execute(sa_text("""
                INSERT INTO Sis_Historico
                    (id, idTabela, idComando, idUsuario, DataHora, Detalhe, Computador)
                VALUES
                    (:id_esp, :id_tabela, :id_comando, :id_usuario, GETDATE(), :detalhe, 'teste-ia.camim.com.br')
            """), {
                "id_esp": id_esp,
                "id_tabela": _ID_TABELA_CAD_ESPECIALIDADE,
                "id_comando": _ID_COMANDO_EDICAO,
                "id_usuario": id_usuario_sql,
                "detalhe": detalhe,
            })

        # Log local no SQLite
        try:
            from auth_db import SessionLocal as _SL, HistoricoDesbloqueio
            _db = _SL()
            _db.add(HistoricoDesbloqueio(
                user_id=user.id, user_email=email, user_nome=user.nome or "",
                posto=posto, id_especialidade=id_esp, especialidade=especialidade,
                acao="retirar_data_fim", valor_antigo=valor_antigo, valor_novo="NULL",
                snapshot=data.get("snapshot"),
            ))
            _db.commit()
            _db.close()
        except Exception:
            pass  # log local é best-effort

        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"erro": str(e)}), 500


@app.post('/api/ctrlq/prorrogar_agenda')
def api_prorrogar_agenda():
    auth = _require_pode_desbloquear()
    if not auth:
        return jsonify({"erro": "Não autorizado"}), 403
    user, email, id_usuario_sql = auth

    data = request.get_json(silent=True) or {}
    id_esp = data.get("idEspecialidade")
    posto = data.get("posto", "").upper()
    nova_data = data.get("nova_data", "")
    if not id_esp or not posto or not nova_data:
        return jsonify({"erro": "Parâmetros obrigatórios: idEspecialidade, posto, nova_data"}), 400

    from datetime import datetime, timedelta
    try:
        dt = datetime.strptime(nova_data, "%Y-%m-%d").date()
    except ValueError:
        return jsonify({"erro": "Formato de data inválido (esperado YYYY-MM-DD)"}), 400

    amanha = (datetime.now().date() + timedelta(days=1))
    if dt < amanha:
        return jsonify({"erro": f"A data deve ser igual ou posterior a {amanha.isoformat()}"}), 400

    engine = _get_sqlserver_engine(posto)
    if not engine:
        return jsonify({"erro": f"Conexão do posto {posto} não configurada"}), 500

    from sqlalchemy import text as sa_text
    try:
        with engine.begin() as conn:
            # Busca valor atual + especialidade para auditoria
            row = conn.execute(sa_text(
                "SELECT ce.DataFimExibicao, ce.Especialidade "
                "FROM cad_especialidade ce WHERE ce.idEspecialidade = :id"
            ), {"id": id_esp}).fetchone()
            if not row:
                return jsonify({"erro": "Registro não encontrado"}), 404

            valor_antigo = str(row[0]) if row[0] else "NULL"
            especialidade = row[1] or ""

            # UPDATE: prorrogar data fim
            conn.execute(sa_text(
                "UPDATE cad_especialidade SET DataFimExibicao = :nova WHERE idEspecialidade = :id"
            ), {"nova": nova_data, "id": id_esp})

            # INSERT auditoria em Sis_Historico (tabela base com IDs de FK)
            nome_usuario = user.nome or email
            detalhe = (
                f"Alteração da Especialidade {especialidade} - "
                f"Prorrogou data fim de exibição para {nova_data} - Usuário {nome_usuario}"
            )
            conn.execute(sa_text("""
                INSERT INTO Sis_Historico
                    (id, idTabela, idComando, idUsuario, DataHora, Detalhe, Computador)
                VALUES
                    (:id_esp, :id_tabela, :id_comando, :id_usuario, GETDATE(), :detalhe, 'teste-ia.camim.com.br')
            """), {
                "id_esp": id_esp,
                "id_tabela": _ID_TABELA_CAD_ESPECIALIDADE,
                "id_comando": _ID_COMANDO_EDICAO,
                "id_usuario": id_usuario_sql,
                "detalhe": detalhe,
            })

        # Log local no SQLite
        try:
            from auth_db import SessionLocal as _SL, HistoricoDesbloqueio
            _db = _SL()
            _db.add(HistoricoDesbloqueio(
                user_id=user.id, user_email=email, user_nome=user.nome or "",
                posto=posto, id_especialidade=id_esp, especialidade=especialidade,
                acao="prorrogar_agenda", valor_antigo=valor_antigo, valor_novo=nova_data,
                snapshot=data.get("snapshot"),
            ))
            _db.commit()
            _db.close()
        except Exception:
            pass

        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"erro": str(e)}), 500


@app.get('/api/ctrlq/historico_acoes')
def api_ctrlq_historico_acoes():
    """Retorna histórico de ações de desbloqueio feitas pelo app (SQLite local)."""
    email, _ = decode_user()
    if not email:
        return jsonify({"erro": "Não autorizado"}), 401

    from auth_db import SessionLocal as _SL, HistoricoDesbloqueio
    db = _SL()
    try:
        rows = (
            db.query(HistoricoDesbloqueio)
            .order_by(HistoricoDesbloqueio.created_at.desc())
            .limit(200)
            .all()
        )
        return jsonify([{
            "id": r.id,
            "posto": r.posto,
            "idEspecialidade": r.id_especialidade,
            "especialidade": r.especialidade,
            "acao": r.acao,
            "usuario": r.user_nome or r.user_email,
            "data": r.created_at.isoformat() if r.created_at else None,
            "valor_antigo": r.valor_antigo,
            "valor_novo": r.valor_novo,
            "snapshot": r.snapshot,
        } for r in rows])
    finally:
        db.close()


@app.get('/qualidade_agenda')
def r_qualidade_agenda():
    resp = make_response(render_protected_page("qualidade_agenda.html"))
    resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate'
    resp.headers['Pragma'] = 'no-cache'
    return resp

@app.get('/qualidade_agenda.html')
def h_qualidade_agenda():
    resp = make_response(render_protected_page("qualidade_agenda.html"))
    resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate'
    resp.headers['Pragma'] = 'no-cache'
    return resp

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

@app.get('/indicadores')
def r_indicadores():
    return render_protected_page("indicadores.html")

@app.get('/kpi_prescricao')
def r5(): return render_protected_page("kpi_prescricao.html")

@app.get('/kpi_receita_despesa')
def r6(): return render_protected_page("kpi_receita_despesa.html")

@app.get('/kpi_v2')
def r7(): return render_protected_page("kpi_v2.html")

@app.get('/kpi_vendas')
def r8(): return render_protected_page("kpi_vendas.html")

@app.get('/kpi_fidelizacao_cliente')
def r_fidelizacao(): return render_protected_page("kpi_fidelizacao_cliente.html")

@app.get('/medicos')
def r9(): return render_protected_page("medicos.html")

@app.get('/carregando')
def r10(): return render_protected_page("carregando.html")

@app.get('/teste')
def r11():
    return ('', 404)

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

@app.get('/kpi_fidelizacao_cliente.html')
def h_fidelizacao(): return render_protected_page("kpi_fidelizacao_cliente.html")

@app.get('/kpi_v2.html')
def h7(): return render_protected_page("kpi_v2.html")

@app.get('/kpi_vendas.html')
def h8(): return render_protected_page("kpi_vendas.html")

@app.get('/medicos.html')
def h9(): return render_protected_page("medicos.html")

@app.get('/carregando.html')
def h10(): return render_protected_page("carregando.html")

@app.get('/teste.html')
def h11():
    return ('', 404)

@app.get('/trello_harvest.html')
def h12(): return render_protected_page("trello_harvest.html")

@app.get('/index.html')
def h13(): return render_protected_page("index.html")

@app.get('/overlay.html')
def h14():
    return ('', 404)


# ===============================
# API Acessos de Páginas
# ===============================

@app.get("/api/pages/acessos")
def api_pages_acessos():
    email, postos = decode_user()
    if not email:
        return ("", 401)

    resources = _menu_resources()
    counts = {}
    try:
        with _page_db() as conn:
            rows = conn.execute("SELECT path, hits, last_hit FROM page_access").fetchall()
            counts = {r["path"]: {"hits": int(r["hits"]), "last_hit": r["last_hit"]} for r in rows}
    except Exception:
        counts = {}

    items = []
    for canonical, meta in resources.items():
        info = counts.get(canonical, {"hits": 0, "last_hit": None})
        items.append({
            "path": canonical,
            "url": meta["url"],
            "title": meta["title"],
            "icon": meta["icon"],
            "hits": info["hits"],
            "last_hit": info["last_hit"],
        })

    items.sort(key=lambda x: (x["hits"], x["title"]), reverse=True)
    total_hits = sum(x["hits"] for x in items)
    return jsonify({"ok": True, "total_hits": total_hits, "total_pages": len(items), "items": items})


# ===============================
# API KPI Registry (Manus Integration)
# ===============================

try:
    from kpi_registry import get_manifest, get_kpi_by_id, get_kpis_by_category, search_kpis
except ImportError:
    # Fallback se o módulo não estiver disponível
    def get_manifest():
        return {"version": "1.0.0", "kpis": []}
    def get_kpi_by_id(kpi_id):
        return None
    def get_kpis_by_category(category):
        return []
    def search_kpis(query):
        return []


@app.get("/api/kpis/manifest")
def api_kpis_manifest():
    """
    Retorna o catálogo completo de KPIs com metadata para integração com sistemas externos.
    Sem autenticação para facilitar descoberta automática.
    """
    return jsonify(get_manifest())


@app.get("/api/kpis/metadata/<kpi_id>")
def api_kpis_metadata(kpi_id):
    """
    Retorna metadata de um KPI específico.
    """
    kpi = get_kpi_by_id(kpi_id)
    if not kpi:
        return jsonify({"ok": False, "error": f"KPI '{kpi_id}' não encontrado"}), 404
    return jsonify({"ok": True, "kpi": kpi})


@app.get("/api/kpis/category/<category>")
def api_kpis_by_category(category):
    """
    Retorna todos os KPIs de uma categoria específica.
    """
    kpis = get_kpis_by_category(category)
    return jsonify({"ok": True, "category": category, "kpis": kpis})


@app.get("/api/kpis/search")
def api_kpis_search():
    """
    Busca KPIs por keywords ou título.
    Query params: q=search_term
    """
    query = (request.args.get("q") or "").strip()
    if not query:
        return jsonify({"ok": False, "error": "Parâmetro 'q' é obrigatório"}), 400

    kpis = search_kpis(query)
    return jsonify({"ok": True, "query": query, "results": kpis})


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

    from auth_db import SessionLocal, get_user_by_email as _gue
    db = SessionLocal()
    try:
        u = _gue(db, email)
        is_admin  = u.is_admin if u else False
        all_pages = bool(u.all_pages) if u and hasattr(u, 'all_pages') else True
        paginas   = u.lista_paginas() if u and not all_pages else []
    finally:
        db.close()

    # Check page-level access
    page_key = _TEMPLATE_TO_PAGINA.get(filename)
    if page_key and not all_pages and page_key not in paginas:
        try:
            return render_template(
                "acesso_negado.html",
                USER_EMAIL=email,
                USER_IS_ADMIN=is_admin,
                USER_POSTOS=json.dumps(postos),
                PAGINA_BLOQUEADA=filename,
            ), 403
        except Exception:
            return ('Acesso negado', 403)

    try:
        html = render_template(
            filename,
            USER_EMAIL=email,
            USER_POSTOS=json.dumps(postos),
            USER_IS_ADMIN=is_admin,
        )
        if not all_pages:
            html = html.replace('</body>', _sidebar_filter_script(paginas))
        return html
    except:
        return ('', 404)

# ===============================
# API Qualidade da Agenda
# ===============================

@app.post("/api/qualidade_agenda/update_cbos")
def api_update_cbos():
    email, _ = decode_user()
    if not email:
        return ("", 401)
    from auth_db import SessionLocal, get_user_by_email as _gue
    db = SessionLocal()
    try:
        u = _gue(db, email)
        if not u or not u.is_admin:
            return jsonify({"ok": False, "error": "Apenas administradores podem alterar thresholds"}), 403
    finally:
        db.close()

    data = request.get_json(force=True, silent=True) or {}
    especialidade = (data.get("especialidade") or "").strip()
    valor = data.get("valor")

    if not especialidade or valor is None:
        return jsonify({"ok": False, "error": "especialidade e valor são obrigatórios"}), 400

    try:
        valor_f = float(valor)
        if not (0 <= valor_f <= 100):
            return jsonify({"ok": False, "error": "valor deve ser entre 0 e 100"}), 400
    except (ValueError, TypeError):
        return jsonify({"ok": False, "error": "valor inválido"}), 400

    try:
        conn_str = _build_conn_str_for_posto("A")
        eng = _make_engine(conn_str)
        with eng.begin() as con:
            result = con.execute(
                text("UPDATE cad_cbos SET ValorPMinimoVagaDisponivel = :v WHERE Especialidade = :e AND Desativado = 0"),
                {"v": valor_f, "e": especialidade}
            )
        return jsonify({"ok": True, "especialidade": especialidade, "valor": valor_f, "rows_affected": result.rowcount})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


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
