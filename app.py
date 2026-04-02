from flask import Flask, request, make_response, jsonify, render_template, redirect
import os, secrets, json, sqlite3
from datetime import date, datetime, timedelta
import re
import time
import logging
import subprocess
from urllib.parse import quote_plus
from pathlib import Path
from collections import defaultdict

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
    "higienizacao.html":                "higienizacao",
    "higienizacao":                     "higienizacao",
    "agenda_dia.html":                  "agenda_dia",
    "agenda_dia":                       "agenda_dia",
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
_log = logging.getLogger(__name__)
HIGIENIZACAO_SNAPSHOT_PATH = os.getenv(
    "HIGIENIZACAO_SNAPSHOT_PATH",
    "/opt/relatorio_h_t/json_consolidado/higienizacao_snapshot.json",
)


def _run_higienizacao_sql(sql: str) -> list[str]:
    """Executa SQL no PostgreSQL remoto da higienização via SSH e retorna linhas."""
    ssh_host = os.getenv("HIGIENIZACAO_DB_SSH", "root@217.216.85.81")
    db_name = os.getenv("HIGIENIZACAO_DB_NAME", "gestao_higienizacao")
    sql_safe = sql.replace("\\", "\\\\").replace('"', '\\"')
    remote_cmd = f"sudo -u postgres psql -d {db_name} -AtF '|' -c \"{sql_safe}\""
    p = subprocess.run(
        [
            "ssh",
            "-o", "BatchMode=yes",
            "-o", "StrictHostKeyChecking=no",
            "-o", "UserKnownHostsFile=/dev/null",
            "-o", "ConnectTimeout=10",
            ssh_host,
            remote_cmd,
        ],
        capture_output=True,
        text=True,
        timeout=60,
    )
    if p.returncode != 0:
        raise RuntimeError((p.stderr or p.stdout or "falha ao consultar higienização").strip())
    return [ln for ln in p.stdout.splitlines() if ln.strip()]


def _parse_higienizacao_rows(lines: list[str]) -> list[dict]:
    rows = []
    for ln in lines:
        parts = ln.split("|")
        if len(parts) != 10:
            continue
        rows.append({
            "posto": parts[0].strip() or "(sem posto)",
            "setor": parts[1].strip() or "(sem setor)",
            "periodicidade": parts[2].strip() or "(sem periodicidade)",
            "funcionario": parts[3].strip() or "SEM_FUNCIONARIO",
            "funcionario_id": parts[4].strip(),
            "data_hora": parts[5].strip(),
            "dia": parts[6].strip(),
            "ambiente_id": parts[7].strip(),
            "posto_id": parts[8].strip(),
            "qr_code_id": parts[9].strip(),
        })
    return rows


def _parse_higienizacao_ambientes(lines: list[str]) -> list[dict]:
    ambientes = []
    for ln in lines:
        p = ln.split("|")
        if len(p) != 4:
            continue
        ambientes.append({
            "posto": p[0].strip() or "(sem posto)",
            "setor": p[1].strip() or "(sem setor)",
            "periodicidade": p[2].strip() or "(sem periodicidade)",
            "ultimo_dia_log": p[3].strip(),
        })
    return ambientes


def _load_higienizacao_snapshot() -> tuple[list[dict], list[dict], str]:
    path = HIGIENIZACAO_SNAPSHOT_PATH
    with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    registros = payload.get("registros") if isinstance(payload, dict) else None
    ambientes = payload.get("ambientes") if isinstance(payload, dict) else None
    updated_at = (payload.get("updated_at") if isinstance(payload, dict) else None) or ""

    if not isinstance(registros, list):
        raise RuntimeError("snapshot sem chave 'registros' válida")
    if not isinstance(ambientes, list):
        raise RuntimeError("snapshot sem chave 'ambientes' válida")
    return registros, ambientes, updated_at


def _aggregate_higienizacao(
    registros: list[dict],
    ambientes: list[dict],
    dt_ini: str | None,
    dt_fim: str | None,
    updated_at: str | None = None,
) -> dict:
    hoje = datetime.now().strftime("%Y-%m-%d")
    registros_filtrados = []
    for r in registros:
        dia = (r.get("dia") or "").strip()
        if dt_ini and dia and dia < dt_ini:
            continue
        if dt_fim and dia and dia > dt_fim:
            continue
        registros_filtrados.append(r)

    total_logs = len(registros_filtrados)
    logs_hoje = sum(1 for r in registros_filtrados if (r.get("dia") or "") == hoje)
    postos_ativos = len({(r.get("posto") or "(sem posto)") for r in registros_filtrados})
    setores_ativos = len({(r.get("setor") or "(sem setor)") for r in registros_filtrados})
    funcionarios_ativos = len({(r.get("funcionario") or "SEM_FUNCIONARIO") for r in registros_filtrados})

    by_posto = defaultdict(lambda: {"posto": "", "total_logs": 0, "logs_hoje": 0, "setores_distintos": set(), "funcionarios_distintos": set(), "ultima_higienizacao": ""})
    by_setor = defaultdict(lambda: {"posto": "", "setor": "", "periodicidade": "", "total_logs": 0, "logs_hoje": 0, "funcionarios_distintos": set(), "ultima_higienizacao": ""})
    by_func = defaultdict(lambda: {"funcionario": "", "funcionario_id": "", "total_logs": 0, "logs_hoje": 0, "postos_distintos": set(), "setores_distintos": set(), "ultimo_registro": ""})
    by_dia = defaultdict(int)

    for r in registros_filtrados:
        posto = r.get("posto") or "(sem posto)"
        setor = r.get("setor") or "(sem setor)"
        funcionario = r.get("funcionario") or "SEM_FUNCIONARIO"
        funcionario_id = r.get("funcionario_id") or ""
        data_hora = r.get("data_hora") or ""
        dia = r.get("dia") or ""
        periodicidade = r.get("periodicidade") or "(sem periodicidade)"

        if dia:
            by_dia[dia] += 1

        p = by_posto[posto]
        p["posto"] = posto
        p["total_logs"] += 1
        if dia == hoje:
            p["logs_hoje"] += 1
        p["setores_distintos"].add(setor)
        p["funcionarios_distintos"].add(funcionario)
        if not p["ultima_higienizacao"] or data_hora > p["ultima_higienizacao"]:
            p["ultima_higienizacao"] = data_hora

        s_key = f"{posto}__{setor}"
        s = by_setor[s_key]
        s["posto"] = posto
        s["setor"] = setor
        s["periodicidade"] = periodicidade
        s["total_logs"] += 1
        if dia == hoje:
            s["logs_hoje"] += 1
        s["funcionarios_distintos"].add(funcionario)
        if not s["ultima_higienizacao"] or data_hora > s["ultima_higienizacao"]:
            s["ultima_higienizacao"] = data_hora

        f = by_func[funcionario]
        f["funcionario"] = funcionario
        f["funcionario_id"] = funcionario_id
        f["total_logs"] += 1
        if dia == hoje:
            f["logs_hoje"] += 1
        f["postos_distintos"].add(posto)
        f["setores_distintos"].add(setor)
        if not f["ultimo_registro"] or data_hora > f["ultimo_registro"]:
            f["ultimo_registro"] = data_hora

    postos = sorted([
        {
            "posto": v["posto"],
            "total_logs": v["total_logs"],
            "logs_hoje": v["logs_hoje"],
            "setores_distintos": len(v["setores_distintos"]),
            "funcionarios_distintos": len(v["funcionarios_distintos"]),
            "ultima_higienizacao": v["ultima_higienizacao"],
        }
        for v in by_posto.values()
    ], key=lambda x: (-x["total_logs"], x["posto"]))

    setores = sorted([
        {
            "posto": v["posto"],
            "setor": v["setor"],
            "periodicidade": v["periodicidade"],
            "total_logs": v["total_logs"],
            "logs_hoje": v["logs_hoje"],
            "funcionarios_distintos": len(v["funcionarios_distintos"]),
            "ultima_higienizacao": v["ultima_higienizacao"],
        }
        for v in by_setor.values()
    ], key=lambda x: (-x["total_logs"], x["posto"], x["setor"]))

    funcionarios = sorted([
        {
            "funcionario": v["funcionario"],
            "funcionario_id": v["funcionario_id"],
            "total_logs": v["total_logs"],
            "logs_hoje": v["logs_hoje"],
            "postos_distintos": len(v["postos_distintos"]),
            "setores_distintos": len(v["setores_distintos"]),
            "ultimo_registro": v["ultimo_registro"],
        }
        for v in by_func.values()
    ], key=lambda x: (-x["total_logs"], x["funcionario"]))

    timeline = sorted(
        [{"dia": k, "total_logs": v} for k, v in by_dia.items()],
        key=lambda x: x["dia"],
        reverse=True,
    )[:31]

    pendencias = []
    hoje_date = datetime.now().date()
    monday = hoje_date.fromordinal(hoje_date.toordinal() - hoje_date.weekday())
    for a in ambientes:
        periodicidade = ((a.get("periodicidade") or "").lower()).strip()
        ultimo = (a.get("ultimo_dia_log") or "").strip()
        pendente = False
        if periodicidade == "diaria":
            pendente = (not ultimo) or (ultimo < hoje)
        elif periodicidade == "semanal":
            pendente = True
            if ultimo:
                dt_ult = datetime.strptime(ultimo, "%Y-%m-%d").date()
                pendente = dt_ult < monday
        if pendente:
            pendencias.append({
                "posto": a.get("posto") or "(sem posto)",
                "setor": a.get("setor") or "(sem setor)",
                "periodicidade": a.get("periodicidade") or "(sem periodicidade)",
                "ultimo_dia_log": ultimo,
            })

    pendencias = sorted(pendencias, key=lambda x: (x["posto"], x["setor"]))[:300]

    return {
        "updated_at": updated_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "filtros": {"dt_ini": dt_ini, "dt_fim": dt_fim},
        "kpis": {
            "total_logs": total_logs,
            "logs_hoje": logs_hoje,
            "postos_ativos": postos_ativos,
            "setores_ativos": setores_ativos,
            "funcionarios_ativos": funcionarios_ativos,
            "ambientes_total": len(ambientes),
            "pendencias_hoje": len(pendencias),
        },
        "agrupado_posto": postos,
        "agrupado_setor": setores,
        "agrupado_funcionario": funcionarios,
        "timeline_diaria": timeline,
        "pendencias": pendencias,
        "registros": registros_filtrados[:1000],
    }


def _build_higienizacao_report_live(dt_ini: str | None, dt_fim: str | None) -> dict:
    sql_logs = """
SELECT
  COALESCE(l.name, '(sem posto)') AS posto,
  COALESCE(e.name, '(sem setor)') AS setor,
  COALESCE(e.periodicity, '(sem periodicidade)') AS periodicidade,
  COALESCE(NULLIF(BTRIM(hl.employee_name), ''), hl.employee_id, 'SEM_FUNCIONARIO') AS funcionario,
  COALESCE(hl.employee_id, '') AS funcionario_id,
  TO_CHAR((hl.created_at AT TIME ZONE 'America/Sao_Paulo'), 'YYYY-MM-DD HH24:MI:SS') AS data_hora,
  TO_CHAR((hl.created_at AT TIME ZONE 'America/Sao_Paulo')::date, 'YYYY-MM-DD') AS dia,
  COALESCE(e.id::text, '') AS ambiente_id,
  COALESCE(l.id::text, '') AS posto_id,
  COALESCE(hl.qr_code_id::text, '') AS qr_code_id
FROM public.hygiene_logs hl
LEFT JOIN public.environments e ON e.id = hl.environment_id
LEFT JOIN public.locations l ON l.id = e.location_id
ORDER BY hl.created_at DESC
LIMIT 50000;
"""
    registros = _parse_higienizacao_rows(_run_higienizacao_sql(sql_logs))

    sql_env = """
SELECT
  COALESCE(l.name, '(sem posto)') AS posto,
  COALESCE(e.name, '(sem setor)') AS setor,
  COALESCE(e.periodicity, '(sem periodicidade)') AS periodicidade,
  TO_CHAR(MAX((hl.created_at AT TIME ZONE 'America/Sao_Paulo')::date), 'YYYY-MM-DD') AS ultimo_dia_log
FROM public.environments e
LEFT JOIN public.locations l ON l.id = e.location_id
LEFT JOIN public.hygiene_logs hl ON hl.environment_id = e.id
GROUP BY l.name, e.name, e.periodicity
ORDER BY posto, setor;
"""
    ambientes = _parse_higienizacao_ambientes(_run_higienizacao_sql(sql_env))
    return _aggregate_higienizacao(
        registros=registros,
        ambientes=ambientes,
        dt_ini=dt_ini,
        dt_fim=dt_fim,
        updated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )


def _build_higienizacao_report(dt_ini: str | None, dt_fim: str | None) -> dict:
    source = (os.getenv("HIGIENIZACAO_SOURCE", "etl") or "etl").lower().strip()
    if source in ("etl", "snapshot", "json"):
        try:
            registros, ambientes, updated_at = _load_higienizacao_snapshot()
            return _aggregate_higienizacao(
                registros=registros,
                ambientes=ambientes,
                dt_ini=dt_ini,
                dt_fim=dt_fim,
                updated_at=updated_at,
            )
        except Exception as e:
            _log.warning("Snapshot ETL indisponível (%s). Tentando consulta direta.", e)

    return _build_higienizacao_report_live(dt_ini=dt_ini, dt_fim=dt_fim)


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
        return redirect("/login")

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

@app.get('/higienizacao')
def r_higienizacao():
    return render_protected_page("higienizacao.html")

@app.get('/qualidade_agenda.html')
def h_qualidade_agenda():
    resp = make_response(render_protected_page("qualidade_agenda.html"))
    resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate'
    resp.headers['Pragma'] = 'no-cache'
    return resp

@app.get('/agenda_dia.html')
def h_agenda_dia():
    return render_protected_page("agenda_dia.html")

@app.get('/higienizacao.html')
def h_higienizacao():
    return render_protected_page("higienizacao.html")

@app.get('/api/higienizacao/relatorio')
def api_higienizacao_relatorio():
    email, _ = decode_user()
    if not email:
        return jsonify({"erro": "Não autorizado"}), 401

    dt_ini = (request.args.get("dt_ini") or "").strip() or None
    dt_fim = (request.args.get("dt_fim") or "").strip() or None

    hoje = date.today()
    primeiro_dia = hoje.replace(day=1)
    proximo_mes = (primeiro_dia + timedelta(days=32)).replace(day=1)
    ultimo_dia = proximo_mes - timedelta(days=1)

    # Sempre trabalha com período fechado; se não vier, usa mês atual completo.
    if not dt_ini:
        dt_ini = primeiro_dia.isoformat()
    if not dt_fim:
        dt_fim = ultimo_dia.isoformat()

    if not re.match(r"^\d{4}-\d{2}-\d{2}$", dt_ini or "") or not re.match(r"^\d{4}-\d{2}-\d{2}$", dt_fim or ""):
        return jsonify({"erro": "Formato de data inválido. Use YYYY-MM-DD."}), 400
    if dt_ini > dt_fim:
        return jsonify({"erro": "Data inicial não pode ser maior que a final."}), 400

    try:
        payload = _build_higienizacao_report(dt_ini=dt_ini, dt_fim=dt_fim)
        return jsonify(payload)
    except Exception as e:
        _log.exception("Falha ao montar relatório de higienização")
        return jsonify({"erro": f"Falha ao gerar relatório: {e}"}), 500

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
        return redirect("/login")

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

@app.get("/api/qualidade_agenda/datas")
def api_qa_datas():
    email, _ = decode_user()
    if not email:
        return ("", 401)
    snap_dir = "/opt/relatorio_h_t/json_consolidado/qualidade_agenda"
    datas = []
    if os.path.isdir(snap_dir):
        for f in sorted(os.listdir(snap_dir), reverse=True):
            if f.endswith(".json"):
                datas.append(f.replace(".json", ""))
    return jsonify({"datas": datas})


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
# API Leads Analytics
# ===============================

@app.get("/api/leads_analytics")
def api_leads_analytics():
    email, _ = decode_user()
    if not email:
        return ("", 401)

    ini = (request.args.get("ini") or "").strip()[:10]
    fim = (request.args.get("fim") or "").strip()[:10]

    if not ini or not fim:
        return jsonify({"erro": "parametros ini e fim obrigatorios"}), 400

    try:
        from export_leads_analytics import (
            get_conn, fetch_resumo_geral, fetch_funil_por_status,
            fetch_funil_conversao, fetch_conversao_por_posto,
            fetch_conversao_por_fonte, fetch_corretor_performance,
            fetch_tempo_primeiro_contato_impacto, fetch_contatos_vs_conversao,
            fetch_motivos_perda, fetch_evolucao_mensal, fetch_dia_semana,
            fetch_hora_dia, fetch_hora_fechamento,
            fetch_tempo_ciclo_conversao, fetch_idade_leads,
            fetch_gargalos_funil, fetch_piores_dias, generate_insights,
            _serialize,
        )
        conn = get_conn()
        data = {}
        queries = [
            ("resumo_geral",           fetch_resumo_geral),
            ("funil_por_status",       fetch_funil_por_status),
            ("funil_conversao",        fetch_funil_conversao),
            ("conversao_por_posto",    fetch_conversao_por_posto),
            ("conversao_por_fonte",    fetch_conversao_por_fonte),
            ("corretor_performance",   fetch_corretor_performance),
            ("tempo_primeiro_contato", fetch_tempo_primeiro_contato_impacto),
            ("contatos_vs_conversao",  fetch_contatos_vs_conversao),
            ("motivos_perda",          fetch_motivos_perda),
            ("evolucao_mensal",        fetch_evolucao_mensal),
            ("dia_semana",             fetch_dia_semana),
            ("hora_dia",               fetch_hora_dia),
            ("tempo_ciclo_conversao",  fetch_tempo_ciclo_conversao),
            ("idade_leads",            fetch_idade_leads),
            ("gargalos_funil",         fetch_gargalos_funil),
            ("piores_dias",            fetch_piores_dias),
            ("hora_fechamento",        fetch_hora_fechamento),
        ]
        for name, fn in queries:
            try:
                result = fn(conn, ini, fim)
                if isinstance(result, dict):
                    result = {k: _serialize(v) for k, v in result.items()}
                elif isinstance(result, list):
                    result = [{k: _serialize(v) for k, v in row.items()} if isinstance(row, dict) else row for row in result]
                data[name] = result
            except Exception:
                data[name] = []
        conn.close()
        data["insights"] = generate_insights(data)
        return jsonify(data)
    except Exception as exc:
        return jsonify({"erro": str(exc)}), 500


@app.get("/api/leads_analytics_postos")
def api_leads_analytics_postos():
    email, _ = decode_user()
    if not email:
        return ("", 401)

    ini = (request.args.get("ini") or "").strip()[:10]
    fim = (request.args.get("fim") or "").strip()[:10]
    if not ini or not fim:
        return jsonify({"erro": "parametros ini e fim obrigatorios"}), 400

    try:
        from export_leads_analytics import (
            get_conn, fetch_posto_mensal_agg, fetch_posto_corretor,
            fetch_posto_fonte, fetch_posto_ciclo, _serialize,
        )
        conn = get_conn()
        data = {}
        queries = [
            ("posto_mensal",    fetch_posto_mensal_agg),
            ("posto_corretor",  fetch_posto_corretor),
            ("posto_fonte",     fetch_posto_fonte),
            ("posto_ciclo",     fetch_posto_ciclo),
        ]
        for name, fn in queries:
            try:
                result = fn(conn, ini, fim)
                if isinstance(result, list):
                    result = [{k: _serialize(v) for k, v in row.items()} if isinstance(row, dict) else row for row in result]
                data[name] = result
            except Exception:
                data[name] = []
        conn.close()
        return jsonify(data)
    except Exception as exc:
        return jsonify({"erro": str(exc)}), 500


@app.get("/api/leads_analytics_corretores")
def api_leads_analytics_corretores():
    email, _ = decode_user()
    if not email:
        return ("", 401)

    ini = (request.args.get("ini") or "").strip()[:10]
    fim = (request.args.get("fim") or "").strip()[:10]
    if not ini or not fim:
        return jsonify({"erro": "parametros ini e fim obrigatorios"}), 400

    try:
        from export_leads_analytics import (
            get_conn, fetch_corretor_performance, fetch_corretor_mensal,
            fetch_corretor_hora, fetch_corretor_dia_semana,
            fetch_corretor_fonte, fetch_corretor_desperdicio,
            fetch_corretor_ciclo, fetch_corretor_hora_fechamento,
            _serialize,
        )
        conn = get_conn()
        data = {}
        queries = [
            ("corretor_ranking",     fetch_corretor_performance),
            ("corretor_mensal",      fetch_corretor_mensal),
            ("corretor_hora",        fetch_corretor_hora),
            ("corretor_dia_semana",  fetch_corretor_dia_semana),
            ("corretor_fonte",       fetch_corretor_fonte),
            ("corretor_desperdicio", fetch_corretor_desperdicio),
            ("corretor_ciclo",       fetch_corretor_ciclo),
            ("corretor_hora_fechamento", fetch_corretor_hora_fechamento),
        ]
        for name, fn in queries:
            try:
                result = fn(conn, ini, fim)
                if isinstance(result, list):
                    result = [{k: _serialize(v) for k, v in row.items()} if isinstance(row, dict) else row for row in result]
                data[name] = result
            except Exception:
                data[name] = []
        conn.close()
        return jsonify(data)
    except Exception as exc:
        return jsonify({"erro": str(exc)}), 500


@app.post("/api/leads_analytics_refresh")
def api_leads_analytics_refresh():
    email, _ = decode_user()
    if not email:
        return ("", 401)

    from export_leads_analytics_cache import (
        is_running, get_daily_run_count, run_cache,
    )

    # Verificar se ja esta rodando
    if is_running():
        return jsonify({
            "ok": False,
            "erro": "Ja existe uma atualizacao em andamento. Aguarde.",
        })

    # Limite de 3 execucoes por dia
    runs_today = get_daily_run_count()
    if runs_today >= 3:
        return jsonify({
            "ok": False,
            "erro": f"Limite de 3 atualizacoes por dia atingido ({runs_today}/3). Tente novamente amanha.",
        })

    # Executar em thread separada para nao bloquear
    import threading
    def _run():
        try:
            run_cache(force_full=False)
        except Exception:
            pass

    t = threading.Thread(target=_run, daemon=True)
    t.start()

    return jsonify({
        "ok": True,
        "msg": f"Atualizacao iniciada ({runs_today + 1}/3 hoje). Pode levar 1-2 minutos.",
    })


@app.get("/api/leads_analytics_cache_status")
def api_leads_analytics_cache_status():
    """Retorna status do cache: ultima atualizacao, se esta rodando, runs hoje."""
    email, _ = decode_user()
    if not email:
        return ("", 401)

    from export_leads_analytics_cache import is_running, get_daily_run_count, load_meta
    meta = load_meta()
    return jsonify({
        "rodando": is_running(),
        "runs_hoje": get_daily_run_count(),
        "gerado_em": meta.get("gerado_em"),
        "validacao_ok": meta.get("validacao_ok"),
        "mysql_count": meta.get("mysql_count"),
        "modo": meta.get("modo"),
        "periodo_ini": meta.get("periodo_ini"),
        "periodo_fim": meta.get("periodo_fim"),
    })


# ===============================
# API Agenda do Dia (F3)
# ===============================

# Cache do mapa idEndereco → letra (carregado de cad_endereco)
_id_endereco_cache = {}   # {idEndereco: letra}
_letra_to_id_cache = {}   # {letra: idEndereco}


def _load_endereco_map(eng):
    """Carrega mapa idEndereco↔letra de cad_endereco (com cache)."""
    global _id_endereco_cache, _letra_to_id_cache
    if _id_endereco_cache:
        return
    try:
        with eng.connect() as con:
            rows = con.execute(text(
                "SET NOCOUNT ON; SELECT idEndereco, Codigo FROM cad_endereco"
            )).mappings().all()
        for r in rows:
            id_end = int(r["idEndereco"])
            letra = (r["Codigo"] or "").strip().upper()
            if letra:
                _id_endereco_cache[id_end] = letra
                _letra_to_id_cache[letra] = id_end
    except Exception:
        pass


def _get_letra(id_endereco, fallback="?"):
    return _id_endereco_cache.get(id_endereco, fallback)


def _get_id_endereco(letra):
    return _letra_to_id_cache.get(letra)


def _agenda_buscar_status_por_posto(matriculas_por_posto, dt_dmy, dt_next):
    """
    Para cada posto, conecta no banco daquele posto e busca:
    - [Cliente Situação] por matricula + idendereco (evita retornar 'Filial')
    - Se pagou no dia (idcontatipo=5) por matricula + idendereco
    Retorna (status_map, pagou_map).
    matriculas_por_posto: {letra: {(matricula, idendereco), ...}}
    """
    status_map = {}   # matricula -> situação
    pagou_map = set()  # matrículas que pagaram no dia

    # Usar cache de cad_endereco

    for letra_posto, mat_id_pairs in matriculas_por_posto.items():
        if not mat_id_pairs:
            continue
        try:
            eng_posto = _try_build_engine_for_posto(letra_posto, retries=2)
        except Exception:
            continue  # posto offline, fica sem status

        # Extrair matrículas e o idendereco do posto
        mat_list = list({m for m, _ in mat_id_pairs})
        id_endereco = _get_id_endereco(letra_posto)

        for i in range(0, len(mat_list), 500):
            batch = mat_list[i:i+500]
            placeholders = ",".join(str(m) for m in batch)

            # Filtro por idendereco para não retornar 'Filial'
            id_filter = f"AND idendereco = {id_endereco}" if id_endereco else ""

            # Status financeiro
            sql_status = f"""
            SET NOCOUNT ON;
            SELECT DISTINCT matricula, [Cliente Situação] AS situacao
            FROM vw_fin_receita2
            WHERE matricula IN ({placeholders})
            {id_filter}
            """
            try:
                with eng_posto.connect() as con:
                    for r in con.execute(text(sql_status)).mappings().all():
                        mat = int(r["matricula"])
                        sit = (r["situacao"] or "").strip()
                        status_map[mat] = sit
            except Exception:
                pass

            # Pagamento no dia
            sql_pagou = f"""
            SET NOCOUNT ON;
            SELECT DISTINCT matricula
            FROM vw_fin_receita2
            WHERE matricula IN ({placeholders})
              {id_filter}
              AND idcontatipo = 5
              AND [data prestação] >= :dt_ini
              AND [data prestação] <  :dt_fim
            """
            try:
                with eng_posto.connect() as con:
                    for r in con.execute(text(sql_pagou), {"dt_ini": dt_dmy, "dt_fim": dt_next}).mappings().all():
                        pagou_map.add(int(r["matricula"]))
            except Exception:
                pass

    return status_map, pagou_map


@app.get("/api/agenda_dia")
def api_agenda_dia():
    """Consulta agenda do dia por posto: pacientes, status financeiro, pagamento no dia."""
    email, postos_acl = decode_user()
    if not email:
        return ("", 401)

    posto = (request.args.get("posto") or "").strip().upper()
    data_str = (request.args.get("data") or "").strip()

    if not posto or not data_str:
        return jsonify({"ok": False, "error": "posto e data são obrigatórios"}), 400

    if posto not in (postos_acl or []):
        return jsonify({"ok": False, "error": "posto não autorizado"}), 403

    # Validar formato de data (YYYY-MM-DD)
    try:
        dt = datetime.strptime(data_str, "%Y-%m-%d").date()
    except ValueError:
        return jsonify({"ok": False, "error": "data inválida (use YYYY-MM-DD)"}), 400

    # Formato DD/MM/YYYY para views SQL Server (REGRA CRÍTICA)
    dt_dmy = dt.strftime("%d/%m/%Y")
    dt_next = (dt + timedelta(days=1)).strftime("%d/%m/%Y")

    try:
        eng = _try_build_engine_for_posto(posto, retries=3)
    except Exception as e:
        return jsonify({"ok": False, "error": f"Posto {posto} offline: {e}"}), 503

    # Carregar mapa idEndereco↔letra de cad_endereco (uma vez, com cache)
    _load_endereco_map(eng)

    # 1) Agenda do dia
    sql_agenda = """
    SET NOCOUNT ON;
    SELECT
        idendereco,
        matricula,
        codigo       AS cfcliente,
        paciente,
        idadePaciente,
        especialidade,
        nomemedico   AS medico,
        HoraPrevistaConsulta,
        CONVERT(varchar(5), dataconfirmacaoconsulta, 108) AS hora_confirmacao,
        Dif_dias_agend_cons,
        Atendido
    FROM vw_Cad_LancamentoProntuarioComDesistencia
    WHERE dataconsulta >= :dt_ini
      AND dataconsulta <  :dt_fim
      AND desistencia = 0
      AND atendido <> 'MÉDICO faltou'
    ORDER BY nomemedico, HoraPrevistaConsulta ASC
    """

    try:
        with eng.connect() as con:
            rows_agenda = con.execute(
                text(sql_agenda),
                {"dt_ini": dt_dmy, "dt_fim": dt_next}
            ).mappings().all()
    except Exception as e:
        return jsonify({"ok": False, "error": f"Erro na consulta da agenda: {e}"}), 500

    if not rows_agenda:
        return jsonify({
            "ok": True,
            "posto": posto,
            "data": data_str,
            "total": 0,
            "pacientes": [],
        })

    # 2) Agrupar matrículas por posto de ORIGEM (idendereco → letra)
    #    para buscar status/pagamento no banco correto
    matriculas_por_posto = defaultdict(set)  # letra -> {(matricula, idendereco)}
    mat_to_letra = {}  # matricula -> letra do posto de origem

    for r in rows_agenda:
        mat = int(r["matricula"]) if r.get("matricula") else None
        if not mat:
            continue
        id_end = r.get("idendereco")
        letra = _get_letra(id_end, posto)  # fallback: posto atual
        mat_to_letra[mat] = letra
        matriculas_por_posto[letra].add((mat, id_end))

    # 3) Buscar status e pagamento em cada posto de origem
    status_map, pagou_map = _agenda_buscar_status_por_posto(
        matriculas_por_posto, dt_dmy, dt_next
    )

    # 4) Montar resultado
    pacientes = []
    for r in rows_agenda:
        mat = int(r["matricula"]) if r.get("matricula") else None
        hora_prev = r.get("HoraPrevistaConsulta")
        if hora_prev is not None:
            if isinstance(hora_prev, datetime):
                hora_prev = hora_prev.strftime("%H:%M")
            elif isinstance(hora_prev, timedelta):
                total_sec = int(hora_prev.total_seconds())
                hora_prev = f"{total_sec // 3600:02d}:{(total_sec % 3600) // 60:02d}"
            else:
                hora_prev = str(hora_prev)[:5]

        atendido_raw = (str(r.get("Atendido") or "")).strip()
        id_end = r.get("idendereco")
        letra_origem = _get_letra(id_end, "?")
        situacao = status_map.get(mat, "") if mat else ""
        pagou = mat in pagou_map if mat else False

        pacientes.append({
            "matricula":        mat,
            "cfcliente":        r.get("cfcliente"),
            "posto_cliente":    letra_origem,
            "paciente":         (r.get("paciente") or "").strip(),
            "idade":            r.get("idadePaciente"),
            "especialidade":    (r.get("especialidade") or "").strip(),
            "medico":           (r.get("medico") or "").strip(),
            "hora_prevista":    hora_prev,
            "hora_confirmacao": (r.get("hora_confirmacao") or "").strip(),
            "dias_agend_cons":  r.get("Dif_dias_agend_cons"),
            "atendido":         atendido_raw,
            "situacao":         situacao,
            "pagou_no_dia":     pagou,
            "idendereco":       id_end,
        })

    return jsonify({
        "ok": True,
        "posto": posto,
        "data": data_str,
        "total": len(pacientes),
        "pacientes": pacientes,
    })


# ===============================
# Execução manual
# ===============================

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8020)
