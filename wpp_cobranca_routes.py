"""
wpp_cobranca_routes.py
Flask Blueprint — Cobrança WhatsApp.

Rotas:
  GET  /wpp                       — lista campanhas
  GET  /wpp/nova                  — form nova campanha
  POST /wpp/nova                  — salvar nova campanha
  GET  /wpp/<id>/editar           — form editar campanha
  POST /wpp/<id>/editar           — salvar edição
  POST /wpp/<id>/toggle           — ativar/desativar
  POST /wpp/<id>/excluir          — excluir campanha
  GET  /wpp/<id>/envios           — listar envios da campanha
  GET  /wpp/envio/<id>            — detalhe de um envio
  GET  /wpp/api/postos            — postos disponíveis no .env
  GET  /wpp/api/templates         — templates da API WhatsApp
"""

import os
import sys
import json
import subprocess
import threading
import time
import requests
from datetime import datetime
from flask import Blueprint, request, jsonify, render_template, redirect, url_for, Response

# Importa db do diretório de ETL
sys.path.insert(0, '/opt/relatorio_h_t')
import wpp_cobranca_db as db
import wpp_cobranca_sql as sql_helper

from dotenv import load_dotenv
load_dotenv('/opt/relatorio_h_t/.env')

WPP_API_URL = os.getenv("WAPP_API_URL",  "https://whatsapp-api.camim.com.br")
WPP_TOKEN   = os.getenv("WAPP_TOKEN",    "")
POSTOS_ALL  = list("ANXYBRPCDGIMJ")

wpp_bp = Blueprint("wpp", __name__, url_prefix="/wpp", template_folder=".")

# ---------------------------------------------------------------------------
# Auth helper (reutiliza decode_user do app principal)
# ---------------------------------------------------------------------------

def _check_auth():
    """Retorna (email, is_admin) ou (None, None) se não autenticado."""
    try:
        from auth_routes import decode_user
        from auth_db import SessionLocal, get_user_by_email
        email, postos = decode_user()
        if not email:
            return None, None
        db_sess = SessionLocal()
        try:
            u = get_user_by_email(db_sess, email)
            is_admin = u.is_admin if u else False
        finally:
            db_sess.close()
        return email, is_admin
    except Exception:
        return None, None


def _render(template, **ctx):
    email, is_admin = _check_auth()
    if not email:
        return ('', 401)
    return render_template(template, USER_EMAIL=email, USER_IS_ADMIN=is_admin, **ctx)


def _postos_disponiveis() -> list[str]:
    """Postos que têm DB_HOST_X e DB_BASE_X configurados no .env."""
    return [p for p in POSTOS_ALL
            if os.getenv(f"DB_HOST_{p}") and os.getenv(f"DB_BASE_{p}")]


def _fetch_templates() -> list[dict]:
    """Busca templates da API WhatsApp. Retorna lista de dicts com name, components."""
    try:
        r = requests.get(
            f"{WPP_API_URL}/templates",
            headers={"Authorization": f"Bearer {WPP_TOKEN}"},
            timeout=8,
        )
        r.raise_for_status()
        return r.json().get("items", [])
    except Exception:
        return []


# ---------------------------------------------------------------------------
# Upload de imagens para header de templates
# ---------------------------------------------------------------------------
# Pasta pública servida pelo nginx em https://camila1.ia.camim.com.br/wpp-uploads/
# (location ^~ /wpp-uploads/ em nginx/camila1.conf — whitelist de extensões,
#  X-Content-Type-Options nosniff, limit_except GET HEAD).
#
# Defesa em profundidade:
#   1. Nginx só serve arquivos com extensão de imagem (jpg/jpeg/png/gif/webp).
#   2. Flask exige login (qualquer usuário logado pode subir).
#   3. Validação por Pillow (Image.verify()) — confirma que é imagem real.
#   4. Tamanho máximo 5MB (limite Meta).
#   5. Nome sanitizado via secure_filename + slug + timestamp (sem colisão).
#   6. Extensão final derivada do MIME detectado pelo Pillow, não da extensão
#      enviada pelo cliente.
#   7. Cada upload gera log para auditoria.
WPP_UPLOADS_DIR    = os.getenv("WPP_UPLOADS_DIR", "/var/www/wpp-uploads")
WPP_UPLOADS_URL    = os.getenv("WPP_UPLOADS_URL", "https://camila1.ia.camim.com.br/wpp-uploads")
WPP_UPLOAD_MAX_MB  = 5
_PIL_TO_EXT = {"JPEG": "jpg", "PNG": "png", "GIF": "gif", "WEBP": "webp"}


def _slugify(s: str, max_len: int = 60) -> str:
    """Slug seguro para nome de arquivo: a-z, 0-9 e hífen."""
    import re, unicodedata
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode("ascii")
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    s = re.sub(r"-{2,}", "-", s)
    return (s or "imagem")[:max_len]


def _validate_image_bytes(raw: bytes) -> tuple[str | None, str | None]:
    """Confirma que `raw` é uma imagem suportada via Pillow.
    Retorna (ext, erro). Em sucesso: ext em {jpg, png, gif, webp}, erro=None.
    Em falha: ext=None, erro=mensagem.
    """
    try:
        from PIL import Image
        import io
        # Image.verify() consome o stream, então abrimos duas vezes
        with Image.open(io.BytesIO(raw)) as im:
            im.verify()
        with Image.open(io.BytesIO(raw)) as im:
            fmt = (im.format or "").upper()
            w, h = im.size
        if fmt not in _PIL_TO_EXT:
            return None, f"formato não suportado: {fmt or 'desconhecido'}"
        if w < 32 or h < 32:
            return None, "imagem muito pequena (mínimo 32x32)"
        if w > 8000 or h > 8000:
            return None, "imagem muito grande (máximo 8000x8000)"
        return _PIL_TO_EXT[fmt], None
    except Exception as e:
        return None, f"arquivo inválido: {str(e)[:120]}"


# ---------------------------------------------------------------------------
# API helpers (JSON)
# ---------------------------------------------------------------------------

@wpp_bp.get("/api/opcoes")
def api_opcoes():
    """Retorna valores distintos de um campo da view para os postos informados."""
    email, _ = _check_auth()
    if not email:
        return jsonify({"error": "unauthorized"}), 401
    campo  = request.args.get("campo", "")
    modo_envio = request.args.get("modo_envio", "atraso")
    postos = [p.strip() for p in request.args.get("postos", "").split(",") if p.strip()]
    valid = sql_helper.CAMPO_SQL_CLIENTES if modo_envio == "clientes_admissao" else sql_helper.CAMPO_SQL
    if campo not in valid:
        return jsonify({"error": "campo inválido"}), 400
    if not postos:
        return jsonify({"opcoes": []})
    try:
        opcoes, erros = sql_helper.buscar_opcoes_debug(postos, campo, modo_envio)
        return jsonify({"opcoes": opcoes, "erros": erros})
    except Exception as e:
        return jsonify({"opcoes": [], "erros": [str(e)[:300]]})


@wpp_bp.post("/api/preview")
def api_preview():
    """Conta registros que se enquadram nos filtros da campanha (sem enviar)."""
    email, _ = _check_auth()
    if not email:
        return jsonify({"error": "unauthorized"}), 401
    campanha = _form_to_dict(request.form)
    if not campanha.get("postos"):
        return jsonify({"error": "nenhum posto selecionado"}), 400
    try:
        resultado = sql_helper.contar_preview(campanha)
        return jsonify(resultado)
    except Exception as e:
        return jsonify({"error": str(e)[:200]}), 500


@wpp_bp.post("/api/preview/registros")
def api_preview_registros():
    """Retorna registros paginados que se enquadram nos filtros da campanha."""
    email, _ = _check_auth()
    if not email:
        return jsonify({"error": "unauthorized"}), 401
    campanha = _form_to_dict(request.form)
    if not campanha.get("postos"):
        return jsonify({"error": "nenhum posto selecionado"}), 400
    page = int(request.form.get("page", 1))
    per_page = int(request.form.get("per_page", 10))
    try:
        resultado = sql_helper.listar_preview(campanha, page, per_page)
        return jsonify(resultado)
    except Exception as e:
        return jsonify({"error": str(e)[:200]}), 500


# ---------------------------------------------------------------------------
# Cache refresh (SSE com progresso)
# ---------------------------------------------------------------------------
_cache_refresh_lock = threading.Lock()
_cache_refresh_status = {
    "running": False,
    "pct": 0,
    "msg": "",
    "done": False,
    "error": None,
}


def _run_cache_refresh():
    """Executa wpp_cache_clientes.py --full em subprocess e parseia progresso."""
    global _cache_refresh_status
    _cache_refresh_status = {"running": True, "pct": 0, "msg": "Iniciando...", "done": False, "error": None}
    try:
        etl_dir = "/opt/relatorio_h_t"
        script = os.path.join(etl_dir, "wpp_cache_clientes.py")
        venv_python = os.path.join(etl_dir, ".venv", "bin", "python3")
        python_bin = venv_python if os.path.isfile(venv_python) else sys.executable
        proc = subprocess.Popen(
            [python_bin, script, "--full"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            cwd="/opt/relatorio_h_t",
        )
        import re
        pct_re = re.compile(r"Posto (\w+):.*\|\s*([\d.]+)%")
        posto_re = re.compile(r"Posto (\w+): iniciando carga")
        done_re = re.compile(r"Posto (\w+): CONCLUÍDO.*inseridos=(\d+)")
        total_re = re.compile(r"tempo total")
        postos_done = []

        for line in proc.stdout:
            line = line.strip()
            if not line:
                continue

            m = pct_re.search(line)
            if m:
                _cache_refresh_status["pct"] = min(int(float(m.group(2))), 99)
                _cache_refresh_status["msg"] = f"Posto {m.group(1)}: {m.group(2)}%"
                continue

            m = posto_re.search(line)
            if m:
                _cache_refresh_status["msg"] = f"Carregando posto {m.group(1)}..."
                continue

            m = done_re.search(line)
            if m:
                postos_done.append(m.group(1))
                _cache_refresh_status["msg"] = f"Posto {m.group(1)} concluído ({m.group(2)} registros)"
                continue

            if total_re.search(line):
                _cache_refresh_status["pct"] = 100
                _cache_refresh_status["msg"] = f"Concluído! Postos: {', '.join(postos_done)}"

        proc.wait()
        if proc.returncode != 0:
            _cache_refresh_status["error"] = f"Processo finalizou com código {proc.returncode}"
        _cache_refresh_status["pct"] = 100
        _cache_refresh_status["done"] = True
        if not _cache_refresh_status.get("error"):
            _cache_refresh_status["msg"] = _cache_refresh_status["msg"] or "Concluído!"
    except Exception as e:
        _cache_refresh_status["error"] = str(e)[:200]
        _cache_refresh_status["done"] = True
    finally:
        _cache_refresh_status["running"] = False


@wpp_bp.post("/api/cache-refresh")
def api_cache_refresh_start():
    """Inicia atualização do cache de clientes (SQL Server → SQLite)."""
    email, is_admin = _check_auth()
    if not email:
        return jsonify({"error": "unauthorized"}), 401

    if _cache_refresh_status["running"]:
        return jsonify({"error": "Atualização já em andamento"}), 409

    t = threading.Thread(target=_run_cache_refresh, daemon=True)
    t.start()
    return jsonify({"ok": True, "msg": "Atualização iniciada"})


@wpp_bp.get("/api/cache-refresh/status")
def api_cache_refresh_status():
    """SSE stream com progresso da atualização do cache."""
    email, _ = _check_auth()
    if not email:
        return jsonify({"error": "unauthorized"}), 401

    def generate():
        while True:
            data = json.dumps(_cache_refresh_status)
            yield f"data: {data}\n\n"
            if _cache_refresh_status.get("done") or not _cache_refresh_status.get("running"):
                break
            time.sleep(1)

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


_INDICADORES_PAINEL_JSON = os.environ.get(
    "INDICADORES_PAINEL_JSON",
    "/opt/relatorio_h_t/json_consolidado/indicadores_painel.json",
)


@wpp_bp.get("/api/indicadores")
def api_indicadores():
    """Lê WPP do JSON pré-agregado por export_indicadores_painel.py (cron */5 min).

    Antes agregava ao vivo varrendo `envios` por (campanha × posto), o que travava
    o worker do gunicorn junto com os outros endpoints de indicadores.
    """
    from datetime import date as _date, datetime as _datetime
    email, _ = _check_auth()
    if not email:
        return jsonify({"error": "unauthorized"}), 401

    try:
        with open(_INDICADORES_PAINEL_JSON, "r", encoding="utf-8") as fh:
            painel = json.load(fh)
    except FileNotFoundError:
        return jsonify({"erro": "indicadores_painel.json ainda não gerado"}), 503
    except Exception as e:
        return jsonify({"erro": str(e)[:200]}), 500

    campanhas = painel.get("indicadores", {}).get("wpp", []) or []
    hoje = _date.today()

    def _dias_desde(ts):
        if not ts:
            return 999
        try:
            return (hoje - _datetime.fromisoformat(str(ts)).date()).days
        except Exception:
            return 999

    out = []
    for camp in campanhas:
        postos_dados = {}
        postos_com_envio = []   # postos com envio nos últimos 7 dias
        postos_sem_envio = []   # postos da campanha que nunca enviaram OU >7d
        for posto, d in (camp.get("postos") or {}).items():
            ultimo = d.get("ultimo_envio")
            dias = _dias_desde(ultimo)
            postos_dados[posto] = {"dias": dias, "ultimo_envio": ultimo}
            if ultimo and dias <= 7:
                postos_com_envio.append(posto)
            else:
                postos_sem_envio.append(posto)
        # Agregado da campanha: o melhor (mais recente) entre todos os postos.
        # Campanhas têm vários postos agrupados desde 2026-05; medir cada posto
        # como 'robô independente' gera falsos negativos (posto sem cliente naquele
        # ciclo não significa robô parado).
        ultimo_agg = camp.get("ultimo_envio_agregado")
        dias_agg = _dias_desde(ultimo_agg)
        out.append({
            "id": camp.get("id"),
            "nome": camp.get("nome"),
            "postos": postos_dados,
            "agregado": {
                "ultimo_envio": ultimo_agg,
                "dias": dias_agg,
                "postos_com_envio": sorted(postos_com_envio),
                "postos_sem_envio": sorted(postos_sem_envio),
                "total_postos": len(postos_dados),
            },
        })

    return jsonify({"campanhas": out})


# ---------------------------------------------------------------------------
# MySQL chat helpers (Queue / User do camim_chat_production)
# ---------------------------------------------------------------------------

def _chat_mysql_conn():
    """Abre conexão com o MySQL camim_chat_production (mesmas env vars de auth_routes)."""
    import pymysql
    return pymysql.connect(
        host=os.environ.get("CHAT_MYSQL_HOST", ""),
        port=int(os.environ.get("CHAT_MYSQL_PORT", 3306)),
        user=os.environ.get("CHAT_MYSQL_USER", ""),
        password=os.environ.get("CHAT_MYSQL_PASSWORD", ""),
        database=os.environ.get("CHAT_MYSQL_DATABASE", "camim_chat_production"),
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=10,
    )


@wpp_bp.get("/api/chat-queues")
def api_chat_queues():
    """Retorna filas ativas do camim_chat_production.Queue."""
    email, _ = _check_auth()
    if not email:
        return jsonify({"error": "unauthorized"}), 401
    try:
        conn = _chat_mysql_conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT id, name, color, tag "
            "FROM `Queue` WHERE isActive = 1 AND deletedAt IS NULL "
            "ORDER BY name"
        )
        rows = cur.fetchall()
        conn.close()
        return jsonify({"queues": rows})
    except Exception as e:
        return jsonify({"error": str(e)[:200], "queues": []}), 500


@wpp_bp.get("/api/chat-users")
def api_chat_users():
    """Retorna usuários ativos do camim_chat_production.User."""
    email, _ = _check_auth()
    if not email:
        return jsonify({"error": "unauthorized"}), 401
    try:
        conn = _chat_mysql_conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT id, name, email "
            "FROM `User` WHERE isActive = 1 AND deletedAt IS NULL "
            "ORDER BY name"
        )
        rows = cur.fetchall()
        conn.close()
        return jsonify({"users": rows})
    except Exception as e:
        return jsonify({"error": str(e)[:200], "users": []}), 500


@wpp_bp.get("/api/postos")
def api_postos():
    email, _ = _check_auth()
    if not email:
        return jsonify({"error": "unauthorized"}), 401
    return jsonify({"postos": _postos_disponiveis()})


@wpp_bp.get("/api/templates")
def api_templates():
    email, _ = _check_auth()
    if not email:
        return jsonify({"error": "unauthorized"}), 401
    templates = _fetch_templates()
    # ?only_visible=1 filtra pelos marcados na tela /wpp/templates.
    # Se não houver NENHUMA config ainda (instalação nova), devolve TUDO —
    # fallback compatível, evita dropdown vazio na campanha por engano.
    only_visible = request.args.get("only_visible") in ("1", "true", "yes")
    if only_visible:
        visiveis = db.templates_visiveis()
        # Vazio = sem config ainda → ignora filtro
        if visiveis:
            templates = [t for t in templates if t.get("name") in visiveis]
    result = []
    for t in templates:
        body_text  = ""
        params     = []
        header_type = None     # None | "TEXT" | "IMAGE" | "VIDEO" | "DOCUMENT"
        header_handle_preview = None
        for comp in t.get("components", []):
            ctype = comp.get("type")
            if ctype == "BODY":
                body_text = comp.get("text", "")
                for p in comp.get("example", {}).get("body_text_named_params", []):
                    params.append(p.get("param_name"))
            elif ctype == "HEADER":
                fmt = (comp.get("format") or "").upper()
                if fmt:
                    header_type = fmt
                else:
                    ex = comp.get("example") or {}
                    if "header_handle" in ex:
                        header_type = "IMAGE"
                    elif comp.get("text"):
                        header_type = "TEXT"
                hh = (comp.get("example") or {}).get("header_handle") or []
                if hh: header_handle_preview = hh[0]
        result.append({
            "name":     t["name"],
            "status":   t.get("status", ""),
            "language": t.get("language", ""),
            "preview":  body_text,
            "params":   params,
            "header_type":            header_type,            # "IMAGE" pra exigir imageUrl
            "header_handle_preview":  header_handle_preview,  # URL Meta (só preview)
        })
    return jsonify({"templates": result})


# ---------------------------------------------------------------------------
# Imagens para header de campanha (galeria + upload)
# ---------------------------------------------------------------------------

@wpp_bp.get("/api/imagens")
def api_imagens_listar():
    """Lista imagens disponíveis na galeria (somente arquivos com extensão válida).
    Retorna: { imagens: [ { nome, url, tamanho, mtime }, ... ] }
    """
    email, _ = _check_auth()
    if not email:
        return jsonify({"error": "unauthorized"}), 401
    try:
        os.makedirs(WPP_UPLOADS_DIR, exist_ok=True)
    except Exception as e:
        return jsonify({"error": f"pasta de uploads indisponível: {e}"}), 500

    valid_exts = tuple(_PIL_TO_EXT.values()) + ("jpeg",)
    items = []
    try:
        for fname in os.listdir(WPP_UPLOADS_DIR):
            if not fname.lower().endswith(tuple("." + e for e in valid_exts)):
                continue
            fpath = os.path.join(WPP_UPLOADS_DIR, fname)
            if not os.path.isfile(fpath):
                continue
            st = os.stat(fpath)
            items.append({
                "nome":    fname,
                "url":     f"{WPP_UPLOADS_URL}/{fname}",
                "tamanho": st.st_size,
                "mtime":   int(st.st_mtime),
            })
    except Exception as e:
        return jsonify({"error": f"erro ao listar: {e}"}), 500

    items.sort(key=lambda x: x["mtime"], reverse=True)
    return jsonify({"imagens": items})


@wpp_bp.post("/api/imagens")
def api_imagens_upload():
    """Recebe upload de imagem para header de template. Login obrigatório.

    Validações:
      - Content-Length ≤ 5MB
      - Pillow consegue abrir e identificar o formato
      - Formato em {JPEG, PNG, GIF, WEBP}
      - Dimensões entre 32x32 e 8000x8000
      - Nome final: <slug>-<YYYYMMDD_HHMMSS>.<ext-real>
    """
    email, _ = _check_auth()
    if not email:
        return jsonify({"error": "unauthorized"}), 401

    f = request.files.get("arquivo")
    if not f or not f.filename:
        return jsonify({"error": "arquivo ausente (campo 'arquivo')"}), 400

    # Limite de tamanho (lê o stream uma vez)
    raw = f.read(WPP_UPLOAD_MAX_MB * 1024 * 1024 + 1)
    if len(raw) > WPP_UPLOAD_MAX_MB * 1024 * 1024:
        return jsonify({"error": f"arquivo maior que {WPP_UPLOAD_MAX_MB} MB"}), 413
    if len(raw) < 100:
        return jsonify({"error": "arquivo vazio ou muito pequeno"}), 400

    ext, erro = _validate_image_bytes(raw)
    if erro or not ext:
        return jsonify({"error": erro or "imagem inválida"}), 400

    # Nome do arquivo final: <slug>-<timestamp>.<ext> — extensão vem do MIME real
    from werkzeug.utils import secure_filename
    base_original = secure_filename(f.filename) or "imagem"
    base_sem_ext  = base_original.rsplit(".", 1)[0]
    slug = _slugify(base_sem_ext)
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    nome = f"{slug}-{ts}.{ext}"

    try:
        os.makedirs(WPP_UPLOADS_DIR, exist_ok=True)
        dest = os.path.join(WPP_UPLOADS_DIR, nome)
        # Evita race condition de overwrite (timestamp já garante, mas seguro)
        if os.path.exists(dest):
            ts2 = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            nome = f"{slug}-{ts2}.{ext}"
            dest = os.path.join(WPP_UPLOADS_DIR, nome)
        with open(dest, "wb") as fh:
            fh.write(raw)
        os.chmod(dest, 0o644)
    except Exception as e:
        return jsonify({"error": f"falha ao salvar: {e}"}), 500

    # Auditoria via stderr (capturada pelo journalctl do serviço)
    print(f"[wpp-imagens] upload OK: user={email} file={nome} "
          f"size={len(raw)} ext={ext}", file=sys.stderr, flush=True)

    return jsonify({
        "ok":      True,
        "nome":    nome,
        "url":     f"{WPP_UPLOADS_URL}/{nome}",
        "tamanho": len(raw),
    }), 201


# ---------------------------------------------------------------------------
# Lista de campanhas
# ---------------------------------------------------------------------------

_DIAS_NOMES = ["seg", "ter", "qua", "qui", "sex", "sáb", "dom"]


def _motivo_sem_envio_hoje(c: dict) -> str | None:
    """Frase padronizada explicando por que a campanha não enviou hoje.

    Retorna None quando já houve envio hoje (a linha não deve aparecer).
    A análise espelha exatamente os gates do cron (send_whatsapp_cobranca):
    dia da semana, janela de horário, postos e o intervalo global por
    telefone — usando datetime.now() (horário da VM, igual ao JanelaEnvio).
    """
    if (c.get("enviados_hoje") or 0) > 0:
        return None
    if not c.get("ativa"):
        return "Pausada — não dispara enquanto estiver suspensa."
    if not (c.get("postos") or []):
        return "Sem posto configurado — não há para quem enviar."

    # Dias da semana permitidos (0=seg … 6=dom, igual ao cron e ao template)
    dias = {int(d.strip()) for d in str(c.get("dias_semana") or "0,1,2,3,4").split(",")
            if d.strip().isdigit()}
    agora = datetime.now()
    if agora.weekday() not in dias:
        permitidos = ", ".join(_DIAS_NOMES[d] for d in sorted(dias)) or "—"
        return f"Hoje não é dia de envio (programada para {permitidos})."

    # Janela de horário
    def _hm(s, default):
        try:
            h, m = str(s or default).split(":")
            return int(h), int(m)
        except Exception:
            return default
    hi = _hm(c.get("hora_inicio"), (8, 0))
    hf = _hm(c.get("hora_fim"), (20, 0))
    hi_s = f"{hi[0]:02d}:{hi[1]:02d}"
    hf_s = f"{hf[0]:02d}:{hf[1]:02d}"
    agora_hm = (agora.hour, agora.minute)
    if agora_hm < hi:
        return f"Aguardando a janela abrir hoje ({hi_s}–{hf_s})."
    if agora_hm >= hf:
        return f"Janela de hoje já encerrou ({hi_s}–{hf_s}) sem envios."

    # Dentro da janela e ainda 0 envios: os dois motivos reais são o intervalo
    # global por telefone (contato já avisado nos últimos N dias por QUALQUER
    # campanha) e a ordem do cron (campanhas de id menor rodam antes, com
    # espera de 5 min por lote — esta pode não ter sido alcançada na rodada).
    intervalo = int(c.get("intervalo_dias") or 7)
    return (f"Na janela ({hi_s}–{hf_s}), mas 0 hoje: contatos elegíveis já "
            f"avisados nos últimos {intervalo}d (intervalo global) ou o cron "
            f"ainda não alcançou esta campanha na rodada.")


@wpp_bp.get("")
@wpp_bp.get("/")
def campanhas():
    email, is_admin = _check_auth()
    if not email:
        return ('', 401)
    lista = db.listar_campanhas()
    for c in lista:
        c["resumo"]        = db.resumo_campanha(c["id"])
        c["enviados_hoje"] = db.enviados_hoje(c["id"])
        c["motivo_sem_envio_hoje"] = _motivo_sem_envio_hoje(c)
    return render_template(
        "wpp_campanhas.html",
        USER_EMAIL=email, USER_IS_ADMIN=is_admin,
        campanhas=lista,
    )


# ---------------------------------------------------------------------------
# Nova campanha
# ---------------------------------------------------------------------------

@wpp_bp.get("/nova")
def nova_form():
    email, is_admin = _check_auth()
    if not email:
        return ('', 401)
    if not is_admin:
        return ('Acesso restrito a administradores.', 403)
    postos = _postos_disponiveis()
    templates = _fetch_templates()
    return render_template(
        "wpp_campanha_form.html",
        USER_EMAIL=email, USER_IS_ADMIN=is_admin,
        campanha=None,
        postos_disponiveis=postos,
        templates=templates,
        titulo="Nova Campanha",
    )


@wpp_bp.post("/nova")
def nova_salvar():
    email, is_admin = _check_auth()
    if not email:
        return ('', 401)
    if not is_admin:
        return ('Acesso restrito a administradores.', 403)
    dados = _form_to_dict(request.form)
    novo_id = db.criar_campanha(dados)
    db.registrar_auditoria(email, "CRIAR", novo_id, dados["nome"], dados)
    return redirect(url_for("wpp.campanhas"))


# ---------------------------------------------------------------------------
# Editar campanha
# ---------------------------------------------------------------------------

@wpp_bp.get("/<int:cid>/editar")
def editar_form(cid):
    email, is_admin = _check_auth()
    if not email:
        return ('', 401)
    if not is_admin:
        return ('Acesso restrito a administradores.', 403)
    campanha = db.get_campanha(cid)
    if not campanha:
        return ("Campanha não encontrada", 404)
    postos = _postos_disponiveis()
    templates = _fetch_templates()
    return render_template(
        "wpp_campanha_form.html",
        USER_EMAIL=email, USER_IS_ADMIN=is_admin,
        campanha=campanha,
        postos_disponiveis=postos,
        templates=templates,
        titulo=f"Editar — {campanha['nome']}",
    )


@wpp_bp.post("/<int:cid>/editar")
def editar_salvar(cid):
    email, is_admin = _check_auth()
    if not email:
        return ('', 401)
    if not is_admin:
        return ('Acesso restrito a administradores.', 403)
    antes = db.get_campanha(cid)
    dados = _form_to_dict(request.form)
    db.atualizar_campanha(cid, dados)
    db.registrar_auditoria(email, "EDITAR", cid, dados["nome"],
                            {"antes": antes, "depois": dados})
    return redirect(url_for("wpp.campanhas"))


# ---------------------------------------------------------------------------
# Toggle / Excluir
# ---------------------------------------------------------------------------

@wpp_bp.post("/<int:cid>/toggle")
def toggle(cid):
    email, is_admin = _check_auth()
    if not email:
        return ('', 401)
    if not is_admin:
        return jsonify({"error": "Acesso restrito a administradores."}), 403
    campanha = db.get_campanha(cid)
    novo_estado = db.toggle_campanha(cid)
    acao = "ATIVAR" if novo_estado else "DESATIVAR"
    db.registrar_auditoria(email, acao, cid,
                            campanha["nome"] if campanha else None, None)
    return jsonify({"ativa": novo_estado})


@wpp_bp.post("/<int:cid>/excluir")
def excluir(cid):
    email, is_admin = _check_auth()
    if not email:
        return ('', 401)
    if not is_admin:
        return ('Acesso restrito a administradores.', 403)
    campanha = db.get_campanha(cid)
    db.registrar_auditoria(email, "EXCLUIR", cid,
                            campanha["nome"] if campanha else None, campanha)
    db.excluir_campanha(cid)
    return redirect(url_for("wpp.campanhas"))


# ---------------------------------------------------------------------------
# Envios da campanha
# ---------------------------------------------------------------------------

@wpp_bp.get("/<int:cid>/envios")
def envios(cid):
    email, is_admin = _check_auth()
    if not email:
        return ('', 401)
    campanha = db.get_campanha(cid)
    if not campanha:
        return ("Campanha não encontrada", 404)

    import math
    page = max(1, int(request.args.get("page", 1)))
    limit = 100
    offset = (page - 1) * limit

    lista_envios = db.listar_envios(cid, limit=limit, offset=offset)
    lista_nao    = db.listar_nao_enviados(cid)
    resumo       = db.resumo_campanha(cid)

    total_pages = max(1, math.ceil(resumo["enviados"] / limit))

    # Agrupa não enviados por motivo
    por_motivo: dict[str, list] = {}
    for r in lista_nao:
        por_motivo.setdefault(r["motivo"], []).append(r)

    return render_template(
        "wpp_campanha_envios.html",
        USER_EMAIL=email, USER_IS_ADMIN=is_admin,
        campanha=campanha,
        envios=lista_envios,
        nao_enviados_por_motivo=por_motivo,
        resumo=resumo,
        page=page,
        total_pages=total_pages,
    )


# ---------------------------------------------------------------------------
# Detalhe de um envio
# ---------------------------------------------------------------------------

def _expandir_template_envio(template_name: str, envio: dict) -> str:
    """Reconstrói o texto da mensagem que foi enviada, expandindo os
    placeholders {{nome}}, {{matricula}}, {{ref}}, {{valor}}, {{venc}}, etc.
    com os valores gravados em envios. Aplica as mesmas regras de
    montar_params_template em send_whatsapp_cobranca: {{nome}} = primeiro
    nome; {{matricula}} = matricula + letra do posto.
    Retorna '' se não conseguir achar o body do template."""
    body = ""
    for t in _fetch_templates():
        if t.get("name") == template_name:
            for comp in t.get("components", []):
                if comp.get("type") == "BODY":
                    body = comp.get("text", "")
                    break
            break
    if not body:
        return ""

    nome_completo = str(envio.get("nome") or "").strip()
    primeiro_nome = nome_completo.split()[0] if nome_completo else ""

    matricula_raw = str(envio.get("matricula") or "").strip()
    posto = str(envio.get("posto") or "").strip().upper()
    matricula_letra = (
        f"{matricula_raw}{posto}"
        if matricula_raw and len(posto) == 1 and posto.isalpha()
        else matricula_raw
    )

    mapa = {
        "nome":      primeiro_nome,
        "matricula": matricula_letra,
        "ref":       str(envio.get("ref") or ""),
        "valor":     str(envio.get("valor") or ""),
        "venc":      str(envio.get("venc") or ""),
        "idreceita": str(envio.get("idreceita") or ""),
    }
    texto = body
    for k, v in mapa.items():
        texto = texto.replace("{{" + k + "}}", v)
    return texto


def _achar_ticket_no_chat(telefone: str) -> dict | None:
    """Tenta localizar o ticket mais recente no chat externo (camim-db2) a
    partir do telefone do envio. Cruza por CustomerCommunicator.phoneNumber
    (cobre clientes que usaram o app/web chat) e por Customer.hash (padrão
    da maioria das integrações). Devolve {ticket_number, ticket_id, customer_id}
    ou None."""
    if not telefone:
        return None
    try:
        conn = _chat_mysql_conn()
        cur = conn.cursor()
        # 1) Cruzamento principal: phoneNumber em CustomerCommunicator
        cur.execute(
            """SELECT t.id, t.ticketNumber, t.customerId
                 FROM Ticket t
                 JOIN CustomerCommunicator cc
                       ON cc.customerId = t.customerId
                      AND cc.deletedAt IS NULL
                WHERE cc.phoneNumber = %s AND t.deletedAt IS NULL
             ORDER BY t.createdAt DESC LIMIT 1""",
            (telefone,),
        )
        row = cur.fetchone()
        if not row:
            # 2) Fallback: Customer.hash = telefone (padrão de Customer criado
            # via webhook whatsapp simples)
            cur.execute(
                """SELECT t.id, t.ticketNumber, t.customerId
                     FROM Ticket t
                     JOIN Customer c ON c.id = t.customerId
                    WHERE c.hash = %s AND t.deletedAt IS NULL
                 ORDER BY t.createdAt DESC LIMIT 1""",
                (telefone,),
            )
            row = cur.fetchone()
        conn.close()
        if not row:
            return None
        return {
            "ticket_id":     row["id"],
            "ticket_number": row["ticketNumber"],
            "customer_id":   row["customerId"],
            "chat_url":      f"https://chat.camim.com.br/tickets/{row['ticketNumber']}",
        }
    except Exception as e:
        log.warning("achar_ticket_no_chat falhou: %s", e)
        return None


@wpp_bp.get("/envio/<int:eid>")
def detalhe_envio(eid):
    email, is_admin = _check_auth()
    if not email:
        return ('', 401)
    envio = db.get_envio(eid)
    if not envio:
        return ("Envio não encontrado", 404)
    campanha = db.get_campanha(envio["campanha_id"])
    mensagem_expandida = _expandir_template_envio(envio.get("template", ""), envio)
    ticket_info = _achar_ticket_no_chat(envio.get("telefone"))
    return render_template(
        "wpp_envio_detalhe.html",
        USER_EMAIL=email, USER_IS_ADMIN=is_admin,
        envio=envio,
        campanha=campanha,
        mensagem_expandida=mensagem_expandida,
        ticket_info=ticket_info,
    )


# ---------------------------------------------------------------------------
# Visualização da conversa (estilo WhatsApp Web — ler-só)
# ---------------------------------------------------------------------------

def _chat_mysql_conn_tuple():
    """Conexão com o MySQL do chat para leitura por índice (Cursor tuple).
    Usada pelo viewer de conversa (Ticket/Message/Customer) que faz row[0..N].
    Não usar para endpoints que serializam direto via jsonify — para esses,
    use _chat_mysql_conn() que aplica DictCursor.
    """
    import pymysql
    host = os.getenv("CHAT_MYSQL_HOST", "")
    user = os.getenv("CHAT_MYSQL_USER", "")
    pwd  = os.getenv("CHAT_MYSQL_PASSWORD", "")
    dbn  = os.getenv("CHAT_MYSQL_DATABASE", "")
    if not (host and user and pwd and dbn):
        raise RuntimeError("CHAT_MYSQL_* não configurado no .env")
    return pymysql.connect(
        host=host, user=user, password=pwd, database=dbn,
        charset="utf8mb4", connect_timeout=8, read_timeout=15, autocommit=True,
    )


def _achar_ticket_para_envio(envio: dict) -> dict | None:
    """Tenta localizar o Ticket no chat correspondente a esse envio.

    Estratégias em ordem de confiabilidade:
      1) chat_ticket_id (cuid devolvido pelo /webhooks/chat e gravado em
         envios.chat_ticket_id) — lookup direto por PK no Ticket. CONFIÁVEL.
      2) Message.externalId == envio.hash_id que enviamos (futuro)
      3) Message.externalId == envio.wamid (raro: chat só grava wamid pra
         mensagens INCOMING — outgoing ficam com nosso ext_id)
      4) (descontinuado) por telefone + janela:
         Customer.pushNotificationId vem NULL no banco do chat — telefone
         não é indexável daquele lado. Só dá pra recuperar por chat_ticket_id.
    Devolve dict {ticket_id, ticketNumber, customerId, createdAt} ou None.
    """
    chat_ticket_id = (envio.get("chat_ticket_id") or "").strip()
    wamid = (envio.get("wamid") or "").strip()
    try:
        conn = _chat_mysql_conn_tuple()
    except Exception as e:
        log.warning("chat MySQL indisponível: %s", e)
        return None
    try:
        with conn.cursor() as cur:
            # 1) Lookup direto por PK — caminho preferido pra envios novos
            if chat_ticket_id:
                cur.execute(
                    """SELECT id, ticketNumber, customerId, createdAt
                         FROM Ticket
                        WHERE id = %s AND deletedAt IS NULL""",
                    (chat_ticket_id,),
                )
                row = cur.fetchone()
                if row:
                    return {"ticket_id": row[0], "ticketNumber": row[1],
                            "customerId": row[2], "createdAt": row[3]}
            # 2) Fallback pra envios LEGADOS (sem chat_ticket_id) — wamid
            if wamid:
                cur.execute(
                    """SELECT t.id, t.ticketNumber, t.customerId, t.createdAt
                         FROM Ticket t
                         JOIN Message m ON m.ticketId = t.id
                        WHERE m.externalId = %s AND m.deletedAt IS NULL
                        ORDER BY m.createdAt DESC LIMIT 1""",
                    (wamid,),
                )
                row = cur.fetchone()
                if row:
                    return {"ticket_id": row[0], "ticketNumber": row[1],
                            "customerId": row[2], "createdAt": row[3]}
        return None
    finally:
        conn.close()


def _carregar_conversa_ticket(ticket_id: str) -> dict:
    """Carrega Customer + lista de Messages do ticket.
    Retorna dict {customer, messages, ticket_id, ticketNumber}.
    """
    conn = _chat_mysql_conn_tuple()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT t.id, t.ticketNumber, t.customerId, t.createdAt,
                          t.closedAt, t.isActive, t.queueId
                     FROM Ticket t WHERE t.id = %s""",
                (ticket_id,),
            )
            t = cur.fetchone()
            if not t:
                return {"ticket_id": ticket_id, "ticketNumber": None,
                        "customer": None, "messages": []}
            customer_id = t[2]
            cur.execute(
                """SELECT id, name, pushNotificationId, imageUrl,
                          matricula, idCliente, idDependente
                     FROM Customer WHERE id = %s""",
                (customer_id,),
            )
            cust_row = cur.fetchone()
            customer = None
            if cust_row:
                customer = {
                    "id": cust_row[0], "name": cust_row[1] or "",
                    "phone": cust_row[2] or "", "imageUrl": cust_row[3] or "",
                    "matricula": cust_row[4] or "",
                    "id_cliente": cust_row[5], "id_dependente": cust_row[6],
                }
            cur.execute(
                """SELECT m.id, m.body, m.mediaUrl, m.mimeType,
                          m.userProfileId, m.customerId, m.createdAt,
                          m.deliveredAt, m.customerReadAt, m.userProfileReadAt,
                          u.name AS user_name
                     FROM Message m
                     LEFT JOIN User u ON u.id = m.userProfileId
                    WHERE m.ticketId = %s AND m.deletedAt IS NULL
                    ORDER BY m.createdAt ASC""",
                (ticket_id,),
            )
            msgs = []
            for r in cur.fetchall():
                is_out = bool(r[4])  # tem userProfileId → operador → saída
                msgs.append({
                    "id": r[0],
                    "body": r[1] or "",
                    "mediaUrl": r[2] or "",
                    "mimeType": r[3] or "",
                    "is_out": is_out,
                    "createdAt": r[6],
                    "deliveredAt": r[7],
                    "customerReadAt": r[8],
                    "userProfileReadAt": r[9],
                    "user_name": r[10] or "",
                })
            return {
                "ticket_id": t[0], "ticketNumber": t[1],
                "createdAt": t[3], "closedAt": t[4], "isActive": bool(t[5]),
                "customer": customer, "messages": msgs,
            }
    finally:
        conn.close()


@wpp_bp.get("/envio/<int:eid>/conversa")
def conversa_envio(eid):
    """Página estilo WhatsApp Web mostrando a conversa do ticket associado a esse envio."""
    email, is_admin = _check_auth()
    if not email:
        return ('', 401)
    envio = db.get_envio(eid)
    if not envio:
        return ("Envio não encontrado", 404)
    campanha = db.get_campanha(envio["campanha_id"])
    erro = None
    conversa = {"messages": [], "customer": None, "ticket_id": None, "ticketNumber": None}
    try:
        ticket = _achar_ticket_para_envio(envio)
        if ticket:
            conversa = _carregar_conversa_ticket(ticket["ticket_id"])
        else:
            erro = ("Não consegui localizar o ticket no chat associado a esse envio. "
                    "Pode ser que a Meta ainda não tenha confirmado entrega ou que "
                    "o ticket foi apagado.")
    except Exception as e:
        log.exception("falha ao carregar conversa do envio %s", eid)
        erro = f"Erro consultando chat: {str(e)[:200]}"
    return render_template(
        "wpp_envio_conversa.html",
        USER_EMAIL=email, USER_IS_ADMIN=is_admin,
        envio=envio, campanha=campanha,
        conversa=conversa, erro=erro,
    )


# ---------------------------------------------------------------------------
# RESPONDENTES — tela pra atender quem respondeu a um envio errado
# ---------------------------------------------------------------------------
# Caso de uso: o operador disparou uma campanha com template errado. Quer
# enviar pedido de desculpas SÓ pra quem respondeu (única forma grátis da
# Meta = service msg dentro da janela de 24h). Como a whatsapp-api interna
# só expõe /templates/send (não tem endpoint pra texto livre), o envio é
# manual pelo chat externo. Esta tela:
#   1) Cruza envios.chat_ticket_id com Message do chat MySQL pra identificar
#      quem respondeu e quando.
#   2) Calcula prazo restante da janela de 24h por cliente.
#   3) Mostra mensagem pronta com {{nome}} substituído pra copiar.
#   4) Link direto pro chat (chat.camim.com.br/tickets/<num>) pra responder.
#   5) Botão "marcar como desculpa enviada" → persiste em desculpas_enviadas
#      + registra em auditoria.
# ---------------------------------------------------------------------------

# Mensagem padrão de pedido de desculpa. Editável pelo operador no front
# antes de copiar — esta é só a versão base com nome placeholder.
_MSG_DESCULPA_DEFAULT = (
    "Olá {{nome}}, aqui é da CAMIM. Hoje cedo enviamos por engano um WhatsApp "
    "avisando que sua fatura estava vencida — não está. O envio correto era "
    "apenas um lembrete de que o vencimento se aproxima. Pedimos desculpas "
    "pelo susto e pelo incômodo. Qualquer dúvida estamos aqui.\n"
    "At. Cristiano Souza - Atendimento Camim"
)


def _calcular_status_janela(ultima_resposta_iso: str | None) -> dict:
    """Recebe timestamp ISO da última resposta do cliente, devolve
    {respondeu, expira_em_iso, restante_min, status}.
    status ∈ {'sem_resposta','ativa','apertada','vencida'}.
    'apertada' = menos de 2h restantes."""
    from datetime import datetime, timedelta, timezone
    if not ultima_resposta_iso:
        return {"respondeu": False, "expira_em_iso": None,
                "restante_min": None, "status": "sem_resposta"}
    # Aceita ISO com timezone ou sem (MySQL devolve sem TZ → assume UTC)
    s = str(ultima_resposta_iso).replace(" ", "T")
    if "+" not in s and "Z" not in s and s.count("-") <= 2:
        s = s + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return {"respondeu": True, "expira_em_iso": None,
                "restante_min": None, "status": "ativa"}
    expira = dt + timedelta(hours=24)
    agora = datetime.now(timezone.utc)
    restante_seg = (expira - agora).total_seconds()
    restante_min = int(restante_seg // 60)
    if restante_seg <= 0:
        st = "vencida"
    elif restante_seg < 2 * 3600:
        st = "apertada"
    else:
        st = "ativa"
    return {"respondeu": True, "expira_em_iso": expira.isoformat(),
            "restante_min": restante_min, "status": st}


# ---------------------------------------------------------------------------
# TEMPLATES — configuração de visibilidade e padrão por modo_envio
# ---------------------------------------------------------------------------

# Modos de envio reconhecidos pelo sistema (mesmos do form de campanha).
# Mantém em sync com wpp_campanha_form.html (select#modo_envio).
_MODOS_ENVIO = [
    ("atraso",            "Cobrança por atraso"),
    ("pre_vencimento",    "Lembrete antes do vencimento"),
    ("clientes_admissao", "Campanha de clientes (admissão)"),
    ("cliente_novo",      "Cliente Novo — Seja Bem-Vindo!"),
    ("falta_medico",      "Falta de Médico (disparo via API)"),
]


@wpp_bp.get("/templates")
def templates_config_page():
    email, is_admin = _check_auth()
    if not email:
        return ('', 401)
    if not is_admin:
        return ('Acesso restrito a administradores.', 403)

    # Templates da Meta (todos, mesmo os escondidos)
    todos = _fetch_templates()
    todos_min = [{
        "name":     t["name"],
        "status":   t.get("status", ""),
        "language": t.get("language", ""),
    } for t in todos]

    config_atual = db.listar_templates_config()
    defaults     = db.modo_template_defaults()

    return render_template(
        "wpp_templates_config.html",
        USER_EMAIL=email, USER_IS_ADMIN=is_admin,
        templates=todos_min,
        config_atual=config_atual,
        defaults=defaults,
        modos=_MODOS_ENVIO,
    )


@wpp_bp.post("/templates/save")
def templates_config_save():
    email, is_admin = _check_auth()
    if not email:
        return jsonify({"error": "unauthorized"}), 401
    if not is_admin:
        return jsonify({"error": "forbidden"}), 403

    data = request.get_json(force=True) or {}
    visiveis = data.get("visiveis") or []
    defaults = data.get("defaults") or {}

    # `todos_conhecidos` = todos os nomes vindos da Meta — garante que ao
    # desmarcar um template, ele grava visivel=0 (em vez de só sumir).
    todos_meta = [t["name"] for t in _fetch_templates()]

    # Filtra defaults pra só modos válidos e templates conhecidos
    modos_validos = {m for m, _ in _MODOS_ENVIO}
    defaults_clean = {
        m: t for m, t in defaults.items()
        if m in modos_validos and t and t in todos_meta
    }

    db.salvar_templates_config(visiveis, todos_meta, defaults_clean, email)
    db.registrar_auditoria(
        email, "TEMPLATES_CONFIG", None, "config de templates",
        {"qtd_visiveis": len(visiveis), "defaults": defaults_clean},
    )
    return jsonify({"ok": True, "qtd_visiveis": len(visiveis),
                    "qtd_defaults": len(defaults_clean)})


@wpp_bp.get("/api/modo_defaults")
def api_modo_defaults():
    """Retorna {modo_envio: template_padrao} pro JS do form de campanha
    preencher automaticamente quando o operador escolhe um tipo de envio."""
    email, _ = _check_auth()
    if not email:
        return jsonify({"error": "unauthorized"}), 401
    return jsonify({"defaults": db.modo_template_defaults()})


@wpp_bp.get("/desculpas")
def desculpas_lista_page():
    """Listagem de campanhas pra escolher qual ver respondentes. Útil quando
    uma campanha foi disparada com template errado e o operador precisa
    atender quem respondeu (caso a wrapper ainda não tenha endpoint pra
    texto livre — então o operador responde manualmente pelo chat externo).
    Cada linha leva pra /wpp/<id>/respondentes."""
    email, is_admin = _check_auth()
    if not email:
        return ('', 401)
    campanhas = db.listar_campanhas()
    # Anexa contadores leves: total envios + desculpas marcadas
    contagem_desculpas = db.contar_desculpas_por_campanha()
    for c in campanhas:
        resumo = db.resumo_campanha(c["id"])
        c["total_envios"]     = resumo.get("enviados", 0)
        c["desculpas_marcadas"] = contagem_desculpas.get(c["id"], 0)
    return render_template(
        "wpp_desculpas_lista.html",
        USER_EMAIL=email, USER_IS_ADMIN=is_admin,
        campanhas=campanhas,
    )


@wpp_bp.get("/<int:cid>/respondentes")
def respondentes_page(cid):
    email, is_admin = _check_auth()
    if not email:
        return ('', 401)
    campanha = db.get_campanha(cid)
    if not campanha:
        return ("Campanha não encontrada", 404)
    return render_template(
        "wpp_respondentes.html",
        USER_EMAIL=email, USER_IS_ADMIN=is_admin,
        campanha=campanha,
        msg_desculpa_default=_MSG_DESCULPA_DEFAULT,
    )


@wpp_bp.get("/<int:cid>/respondentes/data")
def respondentes_data(cid):
    """JSON consumido pelo auto-refresh do front. Cruza envios da campanha
    com o chat MySQL pra ver quem respondeu e quando."""
    email, _ = _check_auth()
    if not email:
        return jsonify({"error": "unauthorized"}), 401

    envios = db.envios_da_campanha(cid)
    desculpas = db.desculpas_por_campanha(cid)

    # Coleta os chat_ticket_id (UUIDs do chat) pra fazer 1 query batch
    ticket_ids = [e["chat_ticket_id"] for e in envios if e.get("chat_ticket_id")]
    resp_por_ticket: dict[str, dict] = {}
    chat_erro = None
    if ticket_ids:
        try:
            conn = _chat_mysql_conn()
            cur = conn.cursor()
            placeholders = ",".join(["%s"] * len(ticket_ids))
            cur.execute(
                f"""SELECT t.id            AS ticket_id,
                           t.ticketNumber  AS ticket_number,
                           MAX(CASE WHEN m.userProfileId IS NULL
                                    THEN m.createdAt END) AS ultima_resp_cliente,
                           COUNT(CASE WHEN m.userProfileId IS NULL
                                       THEN 1 END)         AS qtd_msg_cliente
                      FROM Ticket t
                 LEFT JOIN Message m ON m.ticketId = t.id AND m.deletedAt IS NULL
                     WHERE t.id IN ({placeholders}) AND t.deletedAt IS NULL
                  GROUP BY t.id, t.ticketNumber""",
                ticket_ids,
            )
            for r in cur.fetchall():
                resp_por_ticket[r["ticket_id"]] = {
                    "ticket_number": r["ticket_number"],
                    "ultima_resp_cliente": (
                        r["ultima_resp_cliente"].isoformat()
                        if r["ultima_resp_cliente"] else None
                    ),
                    "qtd_msg_cliente": int(r["qtd_msg_cliente"] or 0),
                }
            conn.close()
        except Exception as e:
            log.warning("respondentes_data: erro chat MySQL: %s", e)
            chat_erro = f"chat MySQL indisponível: {str(e)[:120]}"

    # Estatísticas + lista enriquecida
    stats = {
        "total_envios":      len(envios),
        "responderam":       0,
        "janela_ativa":      0,
        "janela_apertada":   0,
        "janela_vencida":    0,
        "desculpa_enviada":  len(desculpas),
        "pendentes":         0,
    }
    out = []
    for e in envios:
        tid = e.get("chat_ticket_id")
        meta_chat = resp_por_ticket.get(tid, {}) if tid else {}
        ultima = meta_chat.get("ultima_resp_cliente")
        janela = _calcular_status_janela(ultima)
        atendido = e["id"] in desculpas
        # Stats
        if janela["respondeu"]:
            stats["responderam"] += 1
            if janela["status"] == "ativa":      stats["janela_ativa"]    += 1
            if janela["status"] == "apertada":   stats["janela_apertada"] += 1
            if janela["status"] == "vencida":    stats["janela_vencida"]  += 1
            if not atendido and janela["status"] != "vencida":
                stats["pendentes"] += 1
        out.append({
            "envio_id":         e["id"],
            "posto":            e["posto"],
            "nome":             e["nome"],
            "matricula":        e["matricula"],
            "telefone":         e["telefone"],
            "idreceita":        e["idreceita"],
            "ref":              e["ref"],
            "valor":            e["valor"],
            "venc":             e["venc"],
            "dias_atraso":      e["dias_atraso"],
            "wamid":            e["wamid"],
            "chat_ticket_id":   tid,
            "ticket_number":    meta_chat.get("ticket_number"),
            "qtd_msg_cliente":  meta_chat.get("qtd_msg_cliente", 0),
            "ultima_resp_cliente": ultima,
            "enviado_em":       e["enviado_em"],
            "respondeu":        janela["respondeu"],
            "janela_status":    janela["status"],
            "janela_expira_em": janela["expira_em_iso"],
            "janela_restante_min": janela["restante_min"],
            "desculpa_enviada": atendido,
            "desculpa_marcada_em":  desculpas.get(e["id"], {}).get("marcado_em"),
            "desculpa_marcada_por": desculpas.get(e["id"], {}).get("marcado_por"),
        })

    return jsonify({
        "campanha_id": cid,
        "stats": stats,
        "envios": out,
        "chat_erro": chat_erro,
        "msg_desculpa_default": _MSG_DESCULPA_DEFAULT,
    })


@wpp_bp.post("/<int:cid>/respondentes/marcar/<int:envio_id>")
def respondentes_marcar(cid, envio_id):
    email, _ = _check_auth()
    if not email:
        return jsonify({"error": "unauthorized"}), 401
    envio = db.get_envio(envio_id)
    if not envio or envio.get("campanha_id") != cid:
        return jsonify({"error": "envio não pertence à campanha"}), 404
    obs = (request.json or {}).get("obs") if request.is_json else None
    inserido = db.marcar_desculpa_enviada(
        cid, envio_id, envio.get("chat_ticket_id"), email, obs,
    )
    db.registrar_auditoria(
        email, "DESCULPA_ENVIADA", cid,
        f"campanha {cid}",
        {"envio_id": envio_id, "telefone": envio.get("telefone"),
         "nome": envio.get("nome"), "ticket_id": envio.get("chat_ticket_id"),
         "ja_existia": not inserido, "obs": obs},
    )
    return jsonify({"ok": True, "inserido": inserido})


@wpp_bp.post("/<int:cid>/respondentes/desmarcar/<int:envio_id>")
def respondentes_desmarcar(cid, envio_id):
    email, _ = _check_auth()
    if not email:
        return jsonify({"error": "unauthorized"}), 401
    removeu = db.desmarcar_desculpa_enviada(envio_id)
    db.registrar_auditoria(
        email, "DESCULPA_DESMARCADA", cid,
        f"campanha {cid}",
        {"envio_id": envio_id, "removeu": removeu},
    )
    return jsonify({"ok": True, "removeu": removeu})


# ---------------------------------------------------------------------------
# Busca global de envios por telefone / nome
# ---------------------------------------------------------------------------

@wpp_bp.get("/buscar")
def buscar_envios_page():
    email, is_admin = _check_auth()
    if not email:
        return ('', 401)
    q = request.args.get("q", "").strip()
    resultados = []
    if q:
        resultados = db.buscar_envios_global(q, limit=500)
    return render_template(
        "wpp_buscar_envios.html",
        USER_EMAIL=email, USER_IS_ADMIN=is_admin,
        q=q, resultados=resultados,
    )


# ---------------------------------------------------------------------------
# Auditoria
# ---------------------------------------------------------------------------

@wpp_bp.get("/auditoria")
def auditoria():
    email, is_admin = _check_auth()
    if not email:
        return ('', 401)
    registros = db.listar_auditoria(limit=500)
    return render_template(
        "wpp_auditoria.html",
        USER_EMAIL=email, USER_IS_ADMIN=is_admin,
        registros=registros,
    )


# ---------------------------------------------------------------------------
# Dashboard WhatsApp (Meta) — envios + conversão + custo
# ---------------------------------------------------------------------------

@wpp_bp.get("/dashboard")
def dashboard_page():
    email, is_admin = _check_auth()
    if not email:
        return ('', 401)
    return render_template(
        "wpp_dashboard.html",
        USER_EMAIL=email, USER_IS_ADMIN=is_admin,
    )


@wpp_bp.get("/dashboard/data")
def dashboard_data():
    """Serve o JSON gerado por export_wpp_dashboard.py.

    O arquivo é produzido pelo cron em /opt/relatorio_h_t/json_consolidado/
    (ou /var/www/json_consolidado/ após sincronização). Tentamos ambos.
    """
    email, _ = _check_auth()
    if not email:
        return ('', 401)

    candidatos = [
        "/opt/relatorio_h_t/json_consolidado/wpp_dashboard.json",
        "/var/www/json_consolidado/wpp_dashboard.json",
        os.path.join(os.path.dirname(os.path.abspath(__file__)),
                     "json_consolidado", "wpp_dashboard.json"),
    ]
    for path in candidatos:
        if os.path.isfile(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    return Response(f.read(), mimetype="application/json")
            except Exception as e:
                return jsonify({"error": f"falha ao ler {path}: {e}"}), 500

    return jsonify({"error": "wpp_dashboard.json não encontrado — rode export_wpp_dashboard.py"}), 404


# ---------------------------------------------------------------------------
# Teste de envio manual
# ---------------------------------------------------------------------------

@wpp_bp.get("/teste")
def teste_envio_page():
    email, is_admin = _check_auth()
    if not email:
        return ('', 401)
    templates = _fetch_templates()
    return render_template(
        "wpp_teste_envio.html",
        USER_EMAIL=email, USER_IS_ADMIN=is_admin,
        templates=templates,
    )


@wpp_bp.post("/api/envio_teste")
def api_envio_teste():
    """Envia mensagem de teste pra um número, idêntico ao envio de produção:
    1) Meta (/templates/send da whatsapp-api) — esse é o canal que ENTREGA
       pro WhatsApp do cliente. Suporta numero_saida 2455-9600 ou 3529-6666.
    2) api-chat (/webhooks/whatsapp) — registra a conversa no chat externo.
       Best-effort: se falhar, retorna sucesso mesmo assim (a Meta já entregou).

    Antes esta rota só chamava a api-chat, que é registradora — por isso
    o teste 'nunca funcionou' (nada chegava no WhatsApp do destinatário)."""
    import uuid as _uuid
    import re as _re
    from datetime import datetime as _dt

    email, is_admin = _check_auth()
    if not email:
        return jsonify({"error": "unauthorized"}), 401

    data          = request.get_json(force=True) or {}
    telefone_raw  = (data.get("telefone") or "").strip()
    template_name = (data.get("template") or "").strip()
    params        = data.get("params") or {}
    numero_saida  = (data.get("numero_saida") or "2455-9600").strip()
    must_close    = bool(data.get("must_close_ticket"))
    telefone      = _re.sub(r"\D+", "", telefone_raw)

    if not telefone or not template_name:
        return jsonify({"error": "telefone e template são obrigatórios"}), 400
    if len(telefone) < 12 or len(telefone) > 15:
        return jsonify({"error": "telefone inválido (use DDI+DDD+número, ex.: 5521999999999)"}), 400

    # Resolve identidade do número de saída (igual rotina de produção)
    from_phone      = db.from_phone_por_numero_saida(numero_saida)       # ex: '552135296666' p/ Couto, None p/ default
    phone_number_id = db.phone_number_id_por_numero_saida(numero_saida)  # ex: '1101062063090022' p/ Couto

    # Busca o body do template — usado pra montar o preview de retorno
    body = ""
    for t in _fetch_templates():
        if t.get("name") == template_name:
            for comp in t.get("components", []):
                if comp.get("type") == "BODY":
                    body = comp.get("text", "")
                    break
            break
    if not body:
        return jsonify({"error": f"Template '{template_name}' não encontrado ou sem BODY"}), 400

    texto_preview = body
    for key, val in params.items():
        texto_preview = texto_preview.replace(f"{{{{{key}}}}}", str(val))

    # ============================================================
    # 1) ENVIO REAL via Meta (whatsapp-api wrapper /templates/send)
    # ============================================================
    WPP_API_URL = os.getenv("WAPP_API_URL", "https://whatsapp-api.camim.com.br")
    WPP_TOKEN   = os.getenv("WAPP_TOKEN", "")
    if not WPP_TOKEN:
        return jsonify({"error": "WAPP_TOKEN não configurado no .env"}), 500

    meta_payload = {
        "template": template_name,
        "people": [{
            "phone": telefone,
            "data":  {"BODY": params},
        }],
    }
    if from_phone:
        meta_payload["from"] = from_phone

    wamid_meta = None
    erro_meta  = None
    try:
        r_meta = requests.post(
            f"{WPP_API_URL}/templates/send",
            headers={"Authorization": f"Bearer {WPP_TOKEN}"},
            json=meta_payload, timeout=20,
        )
        r_meta.raise_for_status()
        try:
            jm = r_meta.json()
            wamid_meta = jm.get("id") or jm.get("wamid")
        except Exception:
            pass
    except requests.HTTPError as e:
        body_text = ""
        try:    body_text = (e.response.text or "")[:300]
        except Exception: pass
        erro_meta = f"HTTP {e.response.status_code}: {body_text}"
    except Exception as e:
        erro_meta = f"{type(e).__name__}: {str(e)[:200]}"

    # ============================================================
    # 2) REGISTRO via api-chat (/webhooks/whatsapp) — best-effort
    # ============================================================
    CHAT_API_URL  = os.getenv("CHAT_API_URL",   "")
    CHAT_FROM     = os.getenv("WAPP_CHAT_FROM", "")
    CHAT_QUEUE_ID = os.getenv("WAPP_QUEUE_ID",  "")

    chat_status = "skipped"
    chat_erro   = None
    if CHAT_API_URL and CHAT_FROM and CHAT_QUEUE_ID:
        hash_id = _uuid.uuid4().hex[:24]
        ts      = _dt.now().astimezone().isoformat(timespec="seconds")
        remetente = CHAT_FROM if CHAT_FROM.startswith("chat:") else "chat:" + CHAT_FROM
        chat_msg = {
            "id":        hash_id,
            "from":      remetente,
            "queue_id":  CHAT_QUEUE_ID,
            "text":      {"body": texto_preview},
            "type":      "text",
            "timestamp": ts,
        }
        # Quando o checkbox 'Fechar ticket ao enviar?' está marcado, o chat
        # fecha o ticket automaticamente após registrar essa mensagem.
        if must_close:
            chat_msg["must_close_ticket"] = True
        chat_payload = {
            "entry": [{
                "id": hash_id,
                "changes": [{
                    "field": "messages",
                    "value": {
                        "contacts": [{"wa_id": telefone, "profile": {"name": "Teste"}}],
                        "messages": [chat_msg],
                        "metadata": {
                            "phone_number_id":      phone_number_id or "",
                            "display_phone_number": from_phone or "",
                        },
                        "messaging_product": "whatsapp",
                    },
                }],
            }],
            "object": "whatsapp_business_account",
        }
        try:
            r_chat = requests.post(f"{CHAT_API_URL}/webhooks/whatsapp",
                                   json=chat_payload, timeout=15)
            r_chat.raise_for_status()
            chat_status = "accepted"
        except Exception as e:
            chat_status = "erro"
            chat_erro   = f"{type(e).__name__}: {str(e)[:200]}"

    # ============================================================
    # Resultado consolidado
    # ============================================================
    # Sucesso = Meta aceitou. api-chat falhar é detalhe (a mensagem chegou
    # mesmo assim no WhatsApp do destinatário).
    if erro_meta:
        return jsonify({
            "status":         f"erro_meta:{erro_meta}",
            "texto":          texto_preview,
            "telefone":       telefone,
            "numero_saida":   numero_saida,
            "wamid":          None,
            "meta_erro":      erro_meta,
            "chat_status":    chat_status,
            "chat_erro":      chat_erro,
        })
    return jsonify({
        "status":         "accepted",
        "texto":          texto_preview,
        "telefone":       telefone,
        "numero_saida":   numero_saida,
        "wamid":          wamid_meta,
        "chat_status":    chat_status,
        "chat_erro":      chat_erro,
    })


# ---------------------------------------------------------------------------
# Helper: form → dict
# ---------------------------------------------------------------------------

def _form_to_dict(form) -> dict:
    """Converte o MultiDict do form HTML para o dict esperado pelos helpers do DB."""
    postos = form.getlist("postos")  # checkboxes múltiplos
    modo = (form.get("modo_envio", "atraso") or "atraso").strip()
    is_cli = (modo == "clientes_admissao")
    return {
        "nome":               form.get("nome", "").strip(),
        "template":           form.get("template", "notificacao_de_fatura"),
        "modo_envio":         modo,
        "postos":             postos,
        "queue_id":           form.get("queue_id") or None,
        "dias_atraso_min":    _int(form.get("dias_atraso_min"), 1),
        "dias_atraso_max":    _int(form.get("dias_atraso_max"), None),
        "dias_ref_min":       _int(form.get("dias_ref_min"), 4),
        "dias_ref_max":       _int(form.get("dias_ref_max"), None),
        "incluir_cancelados": form.get("incluir_cancelados") == "1",
        "sem_email":          form.get("sem_email") == "1",
        "sexo":               form.get("sexo") or None,
        # Para modo clientes, age mín/máx vêm de campos prefixados cli_
        "idade_min":          _int(form.get("cli_idade_min" if is_cli else "idade_min"), None),
        "idade_max":          _int(form.get("cli_idade_max" if is_cli else "idade_max"), None),
        "nao_recorrente":     form.get("nao_recorrente") == "1",
        "operadora":          form.get("operadora") or None,
        # cobrador/corretor/bairro: prefixo cli_ no modo clientes
        "cobrador":           form.get("cli_cobrador" if is_cli else "cobrador") or None,
        "corretor":           form.get("cli_corretor" if is_cli else "corretor") or None,
        "bairro":             form.get("cli_bairro"   if is_cli else "bairro")   or None,
        "rua":                form.get("rua") or None,
        "hora_inicio":        form.get("hora_inicio", "08:00"),
        "hora_fim":           form.get("hora_fim", "20:00"),
        "dias_semana":        form.get("dias_semana", "0,1,2,3,4"),
        "intervalo_dias":     _int(form.get("intervalo_dias"), 7),
        "ativa":              form.get("ativa", "1") == "1",
        # Campos exclusivos do modo clientes_admissao
        "adm_data_ini":       form.get("adm_data_ini") or None,
        "adm_data_fim":       form.get("adm_data_fim") or None,
        "tipo_cliente":       form.get("tipo_cliente") or None,
        "titular_dependente": form.get("titular_dependente") or None,
        "situacao_cliente":   ",".join(v for v in form.getlist("situacao_cliente") if v) or None,
        "tipo_fj":            form.get("tipo_fj") or None,
        "clube_beneficio":    form.get("clube_beneficio") == "1",
        "clube_beneficio_joy": form.get("clube_beneficio_joy") == "1",
        "plano_premium":      form.get("plano_premium") == "1",
        "origem":             form.get("origem") or None,
        "pagador_atrasado":   form.get("pagador_atrasado") == "1",
        "from_user_id":       form.get("from_user_id") or "cmg8cum8g0519jbbm6r9l93f7",
        "enviar_chat":        "1" in form.getlist("enviar_chat"),
        "enviar_meta":        "1" in form.getlist("enviar_meta"),
        "header_image_url":   (form.get("header_image_url") or "").strip() or None,
        "numero_saida":       (form.get("numero_saida") or "").strip() or None,
        "must_close_ticket":  "1" in form.getlist("must_close_ticket"),
    }


def _int(val, default):
    try:
        v = int(val)
        return v if v >= 0 else default
    except (TypeError, ValueError):
        return default
