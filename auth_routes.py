"""
auth_routes.py — Blueprint Flask para autenticação e administração de usuários.

Rotas:
  POST /session/login
  POST /session/logout
  GET  /auth                   ← nginx auth_request
  GET  /session/me
  GET  /auth/reset             ← formulário "esqueci minha senha"
  POST /auth/reset
  GET  /auth/reset/<token>     ← formulário nova senha (link do e-mail)
  POST /auth/reset/<token>
  GET  /admin                  ← página admin (requer is_admin)
  GET  /admin/api/usuarios
  POST /admin/api/usuarios
  POST /admin/api/usuarios/<id>
  POST /admin/api/usuarios/<id>/delete
  POST /admin/api/usuarios/<id>/reset   ← envia e-mail de reset
"""

import os
import secrets
import smtplib
import sqlite3
from datetime import date, datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import requests as http_requests
from flask import Blueprint, jsonify, make_response, redirect, render_template, request
from itsdangerous import BadSignature, TimestampSigner
from sqlalchemy import func

from auth_db import (
    SessionLocal, User, UserPosto, LoginHistory, IAConversa, KPIContexto, IAConfigGlobal,
    get_user_by_email, get_user_by_id, init_db,
)

# Clientes LLM (carregados sob demanda para não quebrar se a API key faltar)
_openai_client = None
_anthropic_client = None
_groq_client = None

def _get_openai():
    global _openai_client
    if _openai_client is None:
        from llm_client_openai import LLMClientOpenAI
        _openai_client = LLMClientOpenAI()
    return _openai_client

def _get_anthropic():
    global _anthropic_client
    if _anthropic_client is None:
        from llm_client_anthropic import LLMClientAnthropic
        _anthropic_client = LLMClientAnthropic()
    return _anthropic_client

def _get_groq():
    global _groq_client
    if _groq_client is None:
        from llm_client import LLMClient
        _groq_client = LLMClient()
    return _groq_client

_IA_SYSTEM_PROMPT = """Você é analista de dados da CAMIM — rede de clínicas médicas.

REGRA FUNDAMENTAL DE NEGÓCIO:
- Retirada e Campinho NUNCA entram nos totais de receita ou despesa operacional.
- Elas aparecem no contexto como item separado, apenas para referência.
- EXCEÇÃO: quando o contexto indicar explicitamente "Retirada: incluída nos totais".

SEU PAPEL:
- Os dados chegam pré-calculados pelo sistema (pandas). Use-os EXATAMENTE como fornecidos.
- NUNCA recalcule, reestime ou invente valores. Se um número não consta no contexto, diga isso.
- Seu trabalho é EXPLICAR o que aconteceu com base nos dados recebidos — não calcular.
- Se a pergunta mencionar posto específico, use EXCLUSIVAMENTE os dados daquele posto.

FORMATAÇÃO OBRIGATÓRIA:
- Use ## para seções (ex: ## Receita, ## Despesas, ## Resultado)
- Use ### para subseções ou postos
- Use "- item" para listas
- Inclua os valores monetários exatos do contexto e as variações percentuais fornecidas
- Quando o contexto contiver seções "Aumentos significativos" ou "Reduções significativas" com itens listados, reproduza OBRIGATORIAMENTE TODOS os itens daquela seção com os valores e variações exatos. Não omita nenhum item.
- Termine com parágrafo de conclusão/interpretação
- NÃO use tabelas markdown, NÃO use negrito (**), NÃO use itálico (_)
"""

IA_GROQ_URL = os.environ.get("IA_GROQ_URL", "http://127.0.0.1:8030/ia/analisar")

auth_bp = Blueprint("auth_bp", __name__)

PAGINAS_DISPONIVEIS = [
    # ── KPIs ──
    {"key": "alimentacao",               "label": "KPI Custo Alimentação",      "group": "kpi"},
    {"key": "medicos",                   "label": "KPI Custo Médico",            "group": "kpi"},
    {"key": "ctrlq_relatorio",           "label": "KPI Médicos (Qualidade)",     "group": "kpi"},
    {"key": "kpi_v2",                    "label": "KPI Mensalidades",            "group": "kpi"},
    {"key": "kpi_vendas",                "label": "KPI Vendas",                  "group": "kpi"},
    {"key": "clientes",                  "label": "KPI Clientes",                "group": "kpi"},
    {"key": "kpi_prescricao",            "label": "KPI Prescrições",             "group": "kpi"},
    {"key": "kpi_fidelizacao",           "label": "KPI Fidelização Churn",       "group": "kpi"},
    {"key": "kpi_consultas",             "label": "KPI Consultas (Status)",      "group": "kpi"},
    {"key": "kpi_notas_rps",             "label": "KPI Notas x RPS",             "group": "kpi"},
    {"key": "kpi_metas",                 "label": "KPI Metas (Mens/Vendas)",     "group": "kpi"},
    {"key": "kpi_governo",               "label": "KPI Índices Oficiais",        "group": "kpi"},
    {"key": "kpi_liberty",               "label": "KPI CAMIM Liberty",           "group": "kpi"},
    {"key": "kpi_receita_despesa",       "label": "KPI Receitas x Despesas",     "group": "kpi"},
    {"key": "kpi_receita_despesa_rateio","label": "KPI R x D com Rateio",        "group": "kpi"},
    {"key": "growth",                    "label": "Growth Dashboard",            "group": "kpi"},
    {"key": "mais_servicos",             "label": "Mais Serviços",               "group": "kpi"},
    {"key": "trello_harvest",            "label": "Trello Harvest",              "group": "kpi"},
    {"key": "tef",                       "label": "TEF Recorrente",              "group": "kpi"},
    {"key": "email_clientes",            "label": "Email de Cobrança",           "group": "kpi"},
    {"key": "chat_avaliacoes",           "label": "CHAT Avaliações",             "group": "kpi"},
    {"key": "ctrlq_desbloqueio",         "label": "Desbloqueio de Agenda CTRL-Q","group": "kpi"},
    # ── Mais Serviços ──
    {"key": "k_nbs_ibs_cbs",            "label": "Notas Fiscais NBS/IBS/CBS",   "group": "mais"},
    {"key": "k_relatorio_pcs",          "label": "Planejamento PC's",           "group": "mais"},
    {"key": "k_whatsapp_explicado",     "label": "Explicando Cobrança WPP",     "group": "mais"},
    {"key": "cobranca",                 "label": "Cobrança",                    "group": "mais"},
    {"key": "chat_externo",             "label": "Chat",                        "group": "mais"},
    {"key": "broker",                   "label": "Vendas Efetivar",             "group": "mais"},
    {"key": "corretores",               "label": "Vendas Leads Corretores",     "group": "mais"},
    {"key": "tarefas",                  "label": "Tarefas",                     "group": "mais"},
    {"key": "push_cobranca",            "label": "Push de Cobrança IA",         "group": "mais"},
    {"key": "wpp_campanhas",            "label": "WhatsApp Campanhas",          "group": "mais"},
    {"key": "camila_crm",               "label": "Camila.ai CRM",               "group": "mais"},
    {"key": "crm",                      "label": "CRM",                         "group": "mais"},
]

# ── Estado inicializado por init_auth() ───────────────────────────────────────
_SESS_NAME   = "appsess"
_TTL_SECONDS = 3600 * 8
_signer: TimestampSigner | None = None


def init_auth(sess_name: str, secret: str, ttl: int):
    global _SESS_NAME, _TTL_SECONDS, _signer
    _SESS_NAME   = sess_name
    _TTL_SECONDS = ttl
    _signer      = TimestampSigner(secret)
    init_db()


# ── Helpers internos ──────────────────────────────────────────────────────────

def _set_cookie(resp, value: str):
    resp.set_cookie(
        _SESS_NAME, value,
        max_age=_TTL_SECONDS,
        httponly=True, secure=True, samesite="Lax", path="/"
    )


def decode_user():
    """Retorna (email, lista_postos) ou (None, None)."""
    c = request.cookies.get(_SESS_NAME)
    if not c or _signer is None:
        return None, None
    try:
        raw   = _signer.unsign(c, max_age=_TTL_SECONDS + 3600).decode()
        email = raw.split(":", 1)[0]
        db    = SessionLocal()
        try:
            user = get_user_by_email(db, email)
            if not user or not user.ativo:
                return None, None
            return email, user.lista_postos()
        finally:
            db.close()
    except BadSignature:
        return None, None


def _get_current_user():
    """Retorna o objeto User da sessão atual, ou None."""
    c = request.cookies.get(_SESS_NAME)
    if not c or _signer is None:
        return None
    try:
        raw   = _signer.unsign(c, max_age=_TTL_SECONDS + 3600).decode()
        email = raw.split(":", 1)[0]
        db    = SessionLocal()
        try:
            return get_user_by_email(db, email)
        finally:
            db.close()
    except BadSignature:
        return None


def _require_admin():
    """Retorna o User se for admin ativo, senão None."""
    user = _get_current_user()
    if user and user.is_admin and user.ativo:
        return user
    return None


def _enviar_reset(email_destino: str, token: str) -> bool:
    base = os.environ.get("APP_BASE_URL", "https://teste-ia.camim.com.br")
    link = f"{base}/auth/reset/{token}"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = "KPI CAMIM — Redefinição de senha"
    msg["From"]    = os.environ.get("DEFAULT_FROM_EMAIL", "tarefas@camim.com.br")
    msg["To"]      = email_destino

    html_body = f"""
<html><body style="font-family:sans-serif;color:#222;max-width:520px;margin:0 auto">
  <p>Recebemos uma solicitação para redefinir a senha de <b>{email_destino}</b>.</p>
  <p style="margin:24px 0">
    <a href="{link}"
       style="background:#2f6bb6;color:#fff;padding:12px 24px;border-radius:6px;
              text-decoration:none;display:inline-block;font-weight:600">
      Criar nova senha
    </a>
  </p>
  <p style="color:#888;font-size:13px">
    Link válido por <b>1 hora</b>. Se não foi você, ignore este e-mail.
  </p>
</body></html>
"""
    msg.attach(MIMEText(html_body, "html"))

    try:
        host = os.environ.get("EMAIL_HOST", "smtp.gmail.com")
        port = int(os.environ.get("EMAIL_PORT", 587))
        user = os.environ.get("EMAIL_HOST_USER", "")
        pwd  = os.environ.get("EMAIL_HOST_PASSWORD", "")

        if port == 465:
            with smtplib.SMTP_SSL(host, port) as s:
                s.login(user, pwd)
                s.sendmail(msg["From"], [email_destino], msg.as_string())
        else:
            with smtplib.SMTP(host, port) as s:
                s.ehlo()
                s.starttls()
                s.login(user, pwd)
                s.sendmail(msg["From"], [email_destino], msg.as_string())
        return True
    except Exception as exc:
        print(f"[auth_routes] erro e-mail reset: {exc}")
        return False


# ── Login / Logout ─────────────────────────────────────────────────────────────

@auth_bp.post("/session/login")
def login():
    email = request.form.get("email", "").strip().lower()
    senha = request.form.get("senha", "")

    db = SessionLocal()
    try:
        user = get_user_by_email(db, email)
        if not user or not user.ativo or not user.check_senha(senha):
            r = make_response("", 302)
            r.headers["Location"] = "/login?e=1"
            return r

        ip = (
            request.headers.get("X-Real-IP")
            or request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
            or request.remote_addr
            or ""
        )
        db.add(LoginHistory(user_id=user.id, ip=ip))
        db.commit()

        token = _signer.sign(f"{email}:{secrets.token_hex(8)}").decode()
        r = make_response("", 302)
        _set_cookie(r, token)
        r.headers["Location"] = "/"
        return r
    finally:
        db.close()


@auth_bp.post("/session/logout")
def logout():
    r = make_response("", 302)
    r.delete_cookie(_SESS_NAME, path="/")
    r.headers["Location"] = "/login"
    return r


# ── Login via idCamim (OAuth2/OIDC) ───────────────────────────────────────────

_IDCAMIM_BASE        = "https://auth.camim.com.br"
_IDCAMIM_CLIENT_ID   = os.environ.get("IDCAMIM_CLIENT_ID", "")
_IDCAMIM_CLIENT_SECRET = os.environ.get("IDCAMIM_CLIENT_SECRET", "")
_IDCAMIM_REDIRECT_URI = os.environ.get(
    "IDCAMIM_REDIRECT_URI",
    "https://teste-ia.camim.com.br/auth/idcamim/callback",
)

# estado OAuth armazenado em memória (processo único; suficiente para esse caso)
_oauth_states: dict[str, str] = {}


@auth_bp.get("/auth/idcamim")
def idcamim_login():
    """Inicia o fluxo OAuth2 — redireciona para auth.camim.com.br/authorize."""
    import urllib.parse
    state = secrets.token_urlsafe(16)
    _oauth_states[state] = "pending"
    params = {
        "client_id":     _IDCAMIM_CLIENT_ID,
        "redirect_uri":  _IDCAMIM_REDIRECT_URI,
        "response_type": "code",
        "scope":         "openid profile email",
        "state":         state,
    }
    url = f"{_IDCAMIM_BASE}/auth?" + urllib.parse.urlencode(params)
    return redirect(url)


@auth_bp.get("/auth/idcamim/callback")
def idcamim_callback():
    """Recebe o código OAuth2, troca por token, valida usuário no banco local."""
    state = request.args.get("state", "")
    code  = request.args.get("code", "")
    error = request.args.get("error", "")

    if error or state not in _oauth_states:
        r = make_response("", 302)
        r.headers["Location"] = "/login?e=1"
        return r

    _oauth_states.pop(state, None)

    # Trocar código por tokens
    try:
        token_resp = http_requests.post(
            f"{_IDCAMIM_BASE}/token",
            auth=(_IDCAMIM_CLIENT_ID, _IDCAMIM_CLIENT_SECRET),
            data={
                "grant_type":   "authorization_code",
                "code":         code,
                "redirect_uri": _IDCAMIM_REDIRECT_URI,
            },
            timeout=10,
        )
        token_resp.raise_for_status()
        access_token = token_resp.json().get("access_token", "")
    except Exception as exc:
        print(f"[idcamim] erro ao trocar código: {exc}")
        r = make_response("", 302)
        r.headers["Location"] = "/login?e=1"
        return r

    # Buscar dados do usuário no idCamim
    try:
        me_resp = http_requests.get(
            f"{_IDCAMIM_BASE}/me",
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=10,
        )
        me_resp.raise_for_status()
        me = me_resp.json()
    except Exception as exc:
        print(f"[idcamim] erro ao buscar /me: {exc}")
        r = make_response("", 302)
        r.headers["Location"] = "/login?e=1"
        return r

    email = (me.get("email") or "").strip().lower()
    if not email:
        r = make_response("", 302)
        r.headers["Location"] = "/login?e=1"
        return r

    # Verificar se o usuário JÁ existe e está ativo no banco local
    db = SessionLocal()
    try:
        user = get_user_by_email(db, email)
        if not user or not user.ativo:
            # Usuário não cadastrado — acesso negado
            r = make_response("", 302)
            r.headers["Location"] = "/login?e=acesso"
            return r

        ip = (
            request.headers.get("X-Real-IP")
            or request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
            or request.remote_addr
            or ""
        )
        db.add(LoginHistory(user_id=user.id, ip=ip))
        db.commit()
    finally:
        db.close()

    token = _signer.sign(f"{email}:{secrets.token_hex(8)}").decode()
    r = make_response("", 302)
    _set_cookie(r, token)
    r.headers["Location"] = "/"
    return r


# ── nginx auth_request ─────────────────────────────────────────────────────────

@auth_bp.get("/auth")
def auth_check():
    c = request.cookies.get(_SESS_NAME)
    if not c or _signer is None:
        return ("", 401)
    try:
        _signer.unsign(c, max_age=_TTL_SECONDS + 3600)
        return ("", 200)
    except BadSignature:
        return ("", 401)


@auth_bp.get("/session/me")
def session_me():
    email, postos = decode_user()
    if not email:
        return ("", 401)
    db = SessionLocal()
    try:
        u = get_user_by_email(db, email)
        is_admin = bool(u.is_admin) if u else False
        nome = (u.nome or "") if u else ""
    finally:
        db.close()
    return jsonify({"email": email, "postos": postos, "is_admin": is_admin, "nome": nome})


# ── Reset de senha ─────────────────────────────────────────────────────────────

@auth_bp.get("/auth/reset")
def reset_form():
    return render_template("reset_senha.html")


@auth_bp.post("/auth/reset")
def reset_request():
    email = request.form.get("email", "").strip().lower()
    db = SessionLocal()
    try:
        user = get_user_by_email(db, email)
        if user and user.ativo:
            token = user.gerar_reset_token()
            db.commit()
            _enviar_reset(email, token)
        # Sempre redireciona igual — não revela se o e-mail existe
        r = make_response("", 302)
        r.headers["Location"] = "/login?reset=1"
        return r
    finally:
        db.close()


@auth_bp.get("/auth/reset/<token>")
def nova_senha_form(token: str):
    db = SessionLocal()
    try:
        user = db.query(User).filter_by(reset_token=token).first()
        if not user or not user.reset_valido(token):
            r = make_response("", 302)
            r.headers["Location"] = "/login?reset=expirado"
            return r
        return render_template("nova_senha.html", token=token, erro=None)
    finally:
        db.close()


@auth_bp.post("/auth/reset/<token>")
def nova_senha_post(token: str):
    nova     = request.form.get("senha", "")
    confirma = request.form.get("confirma", "")

    if len(nova) < 8 or nova != confirma:
        return render_template(
            "nova_senha.html", token=token,
            erro="Senha inválida ou senhas diferentes (mínimo 8 caracteres)."
        )

    db = SessionLocal()
    try:
        user = db.query(User).filter_by(reset_token=token).first()
        if not user or not user.reset_valido(token):
            r = make_response("", 302)
            r.headers["Location"] = "/login?reset=expirado"
            return r
        user.set_senha(nova)
        user.reset_token   = None
        user.reset_expires = None
        db.commit()
        r = make_response("", 302)
        r.headers["Location"] = "/login?reset=ok"
        return r
    finally:
        db.close()


# ── Admin — página ─────────────────────────────────────────────────────────────

@auth_bp.get("/admin")
def admin_page():
    if not _require_admin():
        r = make_response("", 302)
        r.headers["Location"] = "/"
        return r
    return render_template("admin_usuarios.html")


# ── Admin — API JSON ───────────────────────────────────────────────────────────

@auth_bp.get("/admin/api/paginas")
def admin_paginas():
    if not _require_admin():
        return jsonify({"erro": "Não autorizado"}), 403
    return jsonify(PAGINAS_DISPONIVEIS)


@auth_bp.get("/admin/api/usuarios")
def admin_lista():
    if not _require_admin():
        return jsonify({"erro": "Não autorizado"}), 403
    db = SessionLocal()
    try:
        users = db.query(User).order_by(User.email).all()
        result = []
        for u in users:
            last = (
                db.query(LoginHistory)
                .filter_by(user_id=u.id)
                .order_by(LoginHistory.created_at.desc())
                .first()
            )
            result.append({
                "id":            u.id,
                "email":         u.email,
                "nome":          u.nome,
                "is_admin":      u.is_admin,
                "ativo":         u.ativo,
                "postos":        u.lista_postos(),
                "all_pages":     u.all_pages if hasattr(u, 'all_pages') else True,
                "paginas":       u.lista_paginas() if hasattr(u, 'lista_paginas') else [],
                "ultimo_login":  last.created_at.isoformat() if last else None,
                "ultimo_login_ip": last.ip if last else None,
            })
        return jsonify(result)
    finally:
        db.close()


@auth_bp.post("/admin/api/usuarios")
def admin_criar():
    if not _require_admin():
        return jsonify({"erro": "Não autorizado"}), 403
    d  = request.get_json(silent=True) or {}
    db = SessionLocal()
    try:
        if get_user_by_email(db, d.get("email", "")):
            return jsonify({"erro": "E-mail já cadastrado"}), 409

        user = User(
            email    = d["email"].strip().lower(),
            nome     = d.get("nome", "").strip(),
            is_admin = bool(d.get("is_admin", False)),
            ativo    = True,
        )
        user.set_senha(d.get("senha", secrets.token_urlsafe(12)))
        db.add(user)
        db.flush()
        for posto in d.get("postos", []):
            db.add(UserPosto(user_id=user.id, posto=posto.upper()))
        user.all_pages = bool(d.get("all_pages", False))  # new users start restricted by default
        from auth_db import UserPagePermission
        for key in d.get("paginas", []):
            db.add(UserPagePermission(user_id=user.id, page_key=key))
        db.commit()
        return jsonify({"id": user.id}), 201
    finally:
        db.close()


@auth_bp.post("/admin/api/usuarios/<int:uid>")
def admin_editar(uid: int):
    if not _require_admin():
        return jsonify({"erro": "Não autorizado"}), 403
    d  = request.get_json(silent=True) or {}
    db = SessionLocal()
    try:
        user = get_user_by_id(db, uid)
        if not user:
            return jsonify({"erro": "Usuário não encontrado"}), 404
        if "nome" in d:
            user.nome = d["nome"].strip()
        if "is_admin" in d:
            user.is_admin = bool(d["is_admin"])
        if "ativo" in d:
            user.ativo = bool(d["ativo"])
        if d.get("senha"):
            user.set_senha(d["senha"])
        if "postos" in d:
            db.query(UserPosto).filter_by(user_id=user.id).delete(synchronize_session="fetch")
            db.flush()
            for posto in d["postos"]:
                db.add(UserPosto(user_id=user.id, posto=posto.upper()))
        if "all_pages" in d:
            user.all_pages = bool(d["all_pages"])
        if "paginas" in d:
            from auth_db import UserPagePermission
            db.query(UserPagePermission).filter_by(user_id=user.id).delete(synchronize_session="fetch")
            db.flush()
            for key in d["paginas"]:
                db.add(UserPagePermission(user_id=user.id, page_key=key))
        db.commit()
        return jsonify({"ok": True})
    finally:
        db.close()


@auth_bp.post("/admin/api/usuarios/<int:uid>/delete")
def admin_deletar(uid: int):
    adm = _require_admin()
    if not adm:
        return jsonify({"erro": "Não autorizado"}), 403
    db = SessionLocal()
    try:
        user = get_user_by_id(db, uid)
        if not user:
            return jsonify({"erro": "Usuário não encontrado"}), 404
        if user.email == adm.email:
            return jsonify({"erro": "Não é possível excluir o próprio usuário"}), 400
        db.delete(user)
        db.commit()
        return jsonify({"ok": True})
    finally:
        db.close()


@auth_bp.get("/admin/api/usuarios/<int:uid>/logins")
def admin_logins(uid: int):
    if not _require_admin():
        return jsonify({"erro": "Não autorizado"}), 403
    db = SessionLocal()
    try:
        logins = (
            db.query(LoginHistory)
            .filter_by(user_id=uid)
            .order_by(LoginHistory.created_at.desc())
            .limit(200)
            .all()
        )
        return jsonify([{
            "ip":         l.ip or "—",
            "created_at": l.created_at.isoformat(),
        } for l in logins])
    finally:
        db.close()


@auth_bp.post("/admin/api/usuarios/<int:uid>/reset")
def admin_reset_link(uid: int):
    if not _require_admin():
        return jsonify({"erro": "Não autorizado"}), 403
    db = SessionLocal()
    try:
        user = get_user_by_id(db, uid)
        if not user:
            return jsonify({"erro": "Usuário não encontrado"}), 404
        token = user.gerar_reset_token()
        db.commit()
        ok = _enviar_reset(user.email, token)
        return jsonify({"ok": ok})
    finally:
        db.close()


# ── IA — proxy com persistência ────────────────────────────────────────────────

@auth_bp.post("/api/ia/chat")
def ia_chat():
    """Proxy autenticado: roteia para Groq / OpenAI / Anthropic conforme campo 'provider'."""
    email, _ = decode_user()
    if not email:
        return ("", 401)

    d            = request.get_json(silent=True) or {}
    pergunta_txt = str(d.get("prompt", ""))[:2000]
    pagina_txt   = str(d.get("pagina", ""))[:200]
    provider     = str(d.get("provider", "groq")).lower().strip()

    # ── Monta contexto: builder pandas ou contexto legado ──────────────────────
    kpi              = str(d.get("kpi", "")).strip()
    incluir_retirada = bool(d.get("incluir_retirada", False))

    if kpi:
        try:
            from ia_context_builder import build_context
            postos      = d.get("postos") or []
            periodo_ini = str(d.get("periodo_ini", ""))[:7]
            periodo_fim = str(d.get("periodo_fim", ""))[:7]
            contexto = build_context(
                kpi, postos, periodo_ini, periodo_fim, pergunta_txt, incluir_retirada
            )
        except Exception as exc:
            contexto = f"[Erro no context builder: {exc}]"
    else:
        contexto = str(d.get("contexto", ""))[:100000]

    # ── Carrega contexto de negócio do KPI (editável pelo admin) ───────────────
    kpi_contexto_txt = ""
    if kpi:
        _db = SessionLocal()
        try:
            kpi_ctx = _db.query(KPIContexto).filter_by(kpi_slug=kpi).first()
            if kpi_ctx and kpi_ctx.contexto and kpi_ctx.contexto.strip():
                kpi_contexto_txt = kpi_ctx.contexto.strip()
        except Exception:
            pass
        finally:
            _db.close()

    # ── Carrega regras gerais (editáveis pelo admin) ────────────────────────────
    regras_gerais_txt = ""
    _db2 = SessionLocal()
    try:
        cfg = _db2.query(IAConfigGlobal).filter_by(chave="regras_gerais").first()
        if cfg and cfg.valor and cfg.valor.strip():
            regras_gerais_txt = cfg.valor.strip()
    except Exception:
        pass
    finally:
        _db2.close()

    # ── Monta system prompt dinâmico ────────────────────────────────────────────
    system_prompt = _IA_SYSTEM_PROMPT
    if regras_gerais_txt:
        system_prompt += f"\nREGRAS GERAIS (aplicam-se a todos os KPIs):\n{regras_gerais_txt}\n"
    if kpi_contexto_txt:
        system_prompt += f"\nCONTEXTO DE NEGÓCIO DESTE KPI (escrito pelo gestor):\n{kpi_contexto_txt}\n"

    resposta_json = None
    resposta_txt  = ""

    prompt_completo = (
        f"Dados do relatório (pré-calculados pelo sistema):\n\n{contexto}"
        f"\n\nPergunta do usuário:\n{pergunta_txt}"
    )

    # ── Groq ─────────────────────────────────────────────────────────────────
    if provider == "groq":
        try:
            resposta_txt  = _get_groq().gerar_texto(prompt=prompt_completo, system_prompt=system_prompt)
            resposta_json = {"resposta": resposta_txt, "provider": "groq"}
        except Exception as exc:
            return jsonify({"erro": f"Groq indisponível: {exc}"}), 502

    # ── OpenAI ────────────────────────────────────────────────────────────────
    elif provider == "openai":
        try:
            resposta_txt  = _get_openai().gerar_texto(prompt=prompt_completo, system_prompt=system_prompt)
            resposta_json = {"resposta": resposta_txt, "provider": "openai"}
        except Exception as exc:
            return jsonify({"erro": f"OpenAI indisponível: {exc}"}), 502

    # ── Anthropic ─────────────────────────────────────────────────────────────
    elif provider == "anthropic":
        try:
            resposta_txt  = _get_anthropic().gerar_texto(prompt=prompt_completo, system_prompt=system_prompt)
            resposta_json = {"resposta": resposta_txt, "provider": "anthropic"}
        except Exception as exc:
            return jsonify({"erro": f"Anthropic indisponível: {exc}"}), 502

    else:
        return jsonify({"erro": f"Provedor desconhecido: {provider}"}), 400

    # ── Persistir no SQLite ──────────────────────────────────────────────────
    db = SessionLocal()
    try:
        u = get_user_by_email(db, email)
        if u and pergunta_txt:
            db.add(IAConversa(
                user_id  = u.id,
                pagina   = pagina_txt or None,
                pergunta = pergunta_txt,
                resposta = resposta_txt[:5000],
            ))
            db.commit()
    except Exception as exc:
        print(f"[ia_chat] erro ao salvar conversa: {exc}")
    finally:
        db.close()

    return jsonify(resposta_json)


@auth_bp.get("/admin/api/kpi_contexto")
def admin_kpi_contexto_list():
    if not _require_admin():
        return jsonify({"erro": "Não autorizado"}), 403
    db = SessionLocal()
    try:
        items = db.query(KPIContexto).order_by(KPIContexto.kpi_slug).all()
        return jsonify([{
            "kpi_slug":  i.kpi_slug,
            "titulo":    i.titulo,
            "contexto":  i.contexto or "",
            "updated_at": i.updated_at.isoformat() if i.updated_at else None,
        } for i in items])
    finally:
        db.close()


@auth_bp.post("/admin/api/kpi_contexto/<slug>")
def admin_kpi_contexto_save(slug: str):
    if not _require_admin():
        return jsonify({"erro": "Não autorizado"}), 403
    d = request.get_json(silent=True) or {}
    db = SessionLocal()
    try:
        item = db.query(KPIContexto).filter_by(kpi_slug=slug).first()
        if not item:
            item = KPIContexto(kpi_slug=slug, titulo=d.get("titulo", slug))
            db.add(item)
        item.contexto   = str(d.get("contexto", "")).strip()
        item.titulo     = str(d.get("titulo", item.titulo or slug)).strip()
        from datetime import datetime
        item.updated_at = datetime.utcnow()
        db.commit()
        return jsonify({"ok": True})
    except Exception as exc:
        db.rollback()
        return jsonify({"erro": str(exc)}), 500
    finally:
        db.close()


@auth_bp.get("/admin/api/config_global")
def admin_config_global_list():
    if not _require_admin():
        return jsonify({"erro": "Não autorizado"}), 403
    db = SessionLocal()
    try:
        items = db.query(IAConfigGlobal).order_by(IAConfigGlobal.chave).all()
        return jsonify([{
            "chave":      i.chave,
            "valor":      i.valor or "",
            "updated_at": i.updated_at.isoformat() if i.updated_at else None,
        } for i in items])
    finally:
        db.close()


@auth_bp.post("/admin/api/config_global/<chave>")
def admin_config_global_save(chave: str):
    if not _require_admin():
        return jsonify({"erro": "Não autorizado"}), 403
    d = request.get_json(silent=True) or {}
    db = SessionLocal()
    try:
        item = db.query(IAConfigGlobal).filter_by(chave=chave).first()
        if not item:
            item = IAConfigGlobal(chave=chave)
            db.add(item)
        item.valor = str(d.get("valor", "")).strip()
        from datetime import datetime
        item.updated_at = datetime.utcnow()
        db.commit()
        return jsonify({"ok": True})
    except Exception as exc:
        db.rollback()
        return jsonify({"erro": str(exc)}), 500
    finally:
        db.close()


@auth_bp.get("/api/ia/saudacao")
def ia_saudacao():
    """Retorna saudação personalizada + perguntas mais frequentes do usuário."""
    email, _ = decode_user()
    if not email:
        return ("", 401)

    db = SessionLocal()
    try:
        u = get_user_by_email(db, email)
        if not u:
            return ("", 401)

        # Primeiro nome
        raw_nome = (u.nome or email.split("@")[0]).strip()
        nome = raw_nome.split()[0].capitalize() if raw_nome else "Você"

        # Top 5 perguntas mais frequentes do usuário
        top = (
            db.query(IAConversa.pergunta, func.count(IAConversa.id).label("cnt"))
            .filter(IAConversa.user_id == u.id)
            .group_by(IAConversa.pergunta)
            .order_by(func.count(IAConversa.id).desc())
            .limit(5)
            .all()
        )
        perguntas = [r.pergunta for r in top]

        return jsonify({"nome": nome, "perguntas_frequentes": perguntas})
    finally:
        db.close()


# ── Indicadores: Push de Cobrança ───────────────────────────────────────────

@auth_bp.get("/api/indicadores/push")
def indicadores_push():
    """Retorna último envio de push por posto, para o painel de indicadores."""
    email, user_postos = decode_user()
    if not email:
        return ("", 401)

    push_db = os.environ.get("PUSH_LOG_DB", "/opt/push_clientes/push_log.db")
    hoje = date.today()

    try:
        conn = sqlite3.connect(f"file:{push_db}?mode=ro", uri=True)

        # Descobre o nome da tabela dinamicamente
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()]
        tabela = next(
            (t for t in tables if "push" in t.lower() or "log" in t.lower()),
            tables[0] if tables else None,
        )
        if not tabela:
            conn.close()
            return jsonify({"erro": "Nenhuma tabela encontrada em push_log.db"}), 500

        # Descobre colunas dinamicamente
        cols = [r[1] for r in conn.execute(f"PRAGMA table_info({tabela})").fetchall()]
        col_data  = next((c for c in cols if any(k in c.lower() for k in ("data", "hora", "created", "time", "sent"))), None)
        col_posto = next((c for c in cols if "posto" in c.lower()), None)
        col_modo  = next((c for c in cols if "modo" in c.lower()), None)

        if not col_data or not col_posto:
            conn.close()
            return jsonify({"erro": f"Colunas não encontradas. Disponíveis: {cols}"}), 500

        where = f"WHERE {col_modo} = 'producao'" if col_modo else ""
        rows = conn.execute(f"""
            SELECT {col_posto} AS posto,
                   MAX({col_data}) AS ultimo_envio,
                   COUNT(*) AS total_dia
            FROM {tabela}
            {where}
            GROUP BY {col_posto}
        """).fetchall()
        conn.close()

        result = {}
        for posto_raw, ultimo_str, total in rows:
            posto = str(posto_raw).strip().upper() if posto_raw else None
            if not posto:
                continue
            if user_postos and posto not in user_postos:
                continue
            try:
                ultimo_dt = datetime.fromisoformat(str(ultimo_str)).date()
                dias = (hoje - ultimo_dt).days
            except Exception:
                dias = 999
            result[posto] = {
                "ultimo_envio": ultimo_str,
                "dias": dias,
            }

        return jsonify(result)

    except Exception as exc:
        return jsonify({"erro": str(exc)}), 500


# ── Indicadores: Email de Cobrança ───────────────────────────────────────────

@auth_bp.get("/api/indicadores/email")
def indicadores_email():
    """Retorna último envio de email por (posto, categoria) a partir do camim_kpi.db."""
    email_usr, user_postos = decode_user()
    if not email_usr:
        return ("", 401)

    kpi_db = os.environ.get("KPI_DB_PATH", "/opt/relatorio_h_t/camim_kpi.db")
    hoje   = date.today()

    try:
        conn = sqlite3.connect(f"file:{kpi_db}?mode=ro", uri=True)

        rows = conn.execute("""
            SELECT posto,
                   titulo_categoria,
                   MAX(datahora)  AS ultimo_envio,
                   COUNT(CASE WHEN DATE(datahora) = DATE('now','localtime') THEN 1 END) AS total
            FROM ind_email
            WHERE titulo_categoria = 'Boleto'
            GROUP BY posto, titulo_categoria
            ORDER BY posto, titulo_categoria
        """).fetchall()

        sync_row = conn.execute("""
            SELECT synced_at, total_records, status, mensagem
            FROM   ind_sync_log
            WHERE  indicador = 'email'
            ORDER BY id DESC LIMIT 1
        """).fetchone()

        conn.close()

        dados = {}
        for posto, categoria, ultimo_str, total in rows:
            posto = (posto or "").strip().upper()
            if user_postos and posto not in user_postos:
                continue
            try:
                ultimo_dt = datetime.fromisoformat(str(ultimo_str)).date()
                dias = (hoje - ultimo_dt).days
            except Exception:
                dias = 999
            chave = f"{posto}|Boleto"
            dados[chave] = {
                "posto":         posto,
                "categoria":     "Boleto",
                "ultimo_envio":  ultimo_str,
                "dias":          dias,
                "total":         total,
            }

        return jsonify({
            "dados": dados,
            "sync":  {
                "synced_at":     sync_row[0] if sync_row else None,
                "total_records": sync_row[1] if sync_row else 0,
                "status":        sync_row[2] if sync_row else None,
                "mensagem":      sync_row[3] if sync_row else None,
            },
        })

    except Exception as exc:
        return jsonify({"erro": str(exc)}), 500


# ── Email Clientes: helpers ───────────────────────────────────────────────────

import re as _re

_RE_EMAIL_CODIGO_PREF = _re.compile(r'^\d[\w\d]*\s*[-–]\s*')

_EMAIL_CAT_RULES = [
    (_re.compile(r'\bboleto\b',                             _re.I), "Boleto"),
    (_re.compile(r'\bfalta\b',                              _re.I), "Falta do Médico"),
    (_re.compile(r'\bcancelamento\b',                       _re.I), "Cancelamento"),
    (_re.compile(r'\bnota\s*fiscal\b',                      _re.I), "Nota Fiscal"),
    (_re.compile(r'\bsolicitação\s+de\s+nova\s+amostra\b',  _re.I), "Solicitação de Nova Amostra"),
]

def _clean_cat(cat: str) -> str:
    """Remove prefixo alfanumérico e agrupa em categoria canônica."""
    t = (cat or "").strip()
    t = _RE_EMAIL_CODIGO_PREF.sub("", t).strip(" .-–")
    for regex, nome in _EMAIL_CAT_RULES:
        if regex.search(t):
            return nome
    return t or (cat or "")


# ── Email Clientes: Dashboard ─────────────────────────────────────────────────

POSTOS_NAMES_EMAIL = {
    'A': 'Anchieta', 'N': 'Nilópolis', 'I': 'Nova Iguaçu', 'X': 'CGX',
    'G': 'Campo Grande', 'Y': 'CGY', 'B': 'Bangu', 'R': 'Realengo',
    'M': 'Madureira', 'C': 'Campinho', 'D': 'Del Castilho', 'J': 'JPA',
    'P': 'Rio das Pedras',
}

@auth_bp.get("/api/email_clientes/dashboard")
def email_clientes_dashboard():
    email_usr, user_postos = decode_user()
    if not email_usr:
        return ("", 401)

    kpi_db = os.environ.get("KPI_DB_PATH", "/opt/relatorio_h_t/camim_kpi.db")
    hoje_str = date.today().isoformat()

    try:
        conn = sqlite3.connect(f"file:{kpi_db}?mode=ro", uri=True)

        postos_filter = ""
        params_p = []
        if user_postos:
            ph = ",".join("?" * len(user_postos))
            postos_filter = f"AND posto IN ({ph})"
            params_p = list(user_postos)

        hoje_row = conn.execute(f"""
            SELECT COUNT(*) AS total,
                   COUNT(DISTINCT posto) AS postos_ativos,
                   COUNT(DISTINCT titulo_categoria) AS categorias
            FROM ind_email
            WHERE date(datahora) = ? {postos_filter}
        """, [hoje_str] + params_p).fetchone()

        por_cat_7dias = conn.execute(f"""
            SELECT date(datahora) AS dia, titulo_categoria, COUNT(*) AS total
            FROM ind_email
            WHERE date(datahora) >= date(?, '-6 days') {postos_filter}
            GROUP BY date(datahora), titulo_categoria
            ORDER BY dia, titulo_categoria
        """, [hoje_str] + params_p).fetchall()

        por_cat_hoje = conn.execute(f"""
            SELECT titulo_categoria, COUNT(*) AS total
            FROM ind_email
            WHERE date(datahora) = ? {postos_filter}
            GROUP BY titulo_categoria
            ORDER BY total DESC
        """, [hoje_str] + params_p).fetchall()

        _ERR_COND = "(titulo_original LIKE '%SMTP Error%' OR titulo_original LIKE 'Erro :%')"

        por_posto = conn.execute(f"""
            SELECT posto,
                   MAX(datahora) AS ultimo_envio,
                   SUM(CASE WHEN date(datahora) = ? THEN 1 ELSE 0 END) AS total_hoje,
                   COUNT(*) AS total_geral,
                   SUM(CASE WHEN {_ERR_COND} THEN 1 ELSE 0 END) AS total_erros
            FROM ind_email
            WHERE 1=1 {postos_filter}
            GROUP BY posto
            ORDER BY posto
        """, [hoje_str] + params_p).fetchall()

        erros_total = conn.execute(f"""
            SELECT COUNT(*) FROM ind_email
            WHERE {_ERR_COND} {postos_filter}
        """, params_p).fetchone()

        ultimo_batch = conn.execute(f"""
            SELECT MAX(datahora) FROM ind_email WHERE 1=1 {postos_filter}
        """, params_p).fetchone()

        sync_row = conn.execute("""
            SELECT synced_at, total_records, status, mensagem
            FROM ind_sync_log WHERE indicador = 'email'
            ORDER BY id DESC LIMIT 1
        """).fetchone()

        conn.close()

        hoje_date = date.today()
        postos_result = []
        postos_sem_hoje = []
        for row in por_posto:
            p, ultimo_str, total_hoje, total_geral, total_erros = row
            p = (p or "").strip().upper()
            try:
                ultimo_dt = datetime.fromisoformat(str(ultimo_str)).date()
                diff = (hoje_date - ultimo_dt).days
                if diff == 0:
                    status_envio = "hoje"
                elif diff == 1:
                    status_envio = "ontem"
                else:
                    status_envio = "desatualizado"
            except Exception:
                status_envio = "sem_dados"

            if status_envio != "hoje":
                postos_sem_hoje.append({"posto": p, "nome": POSTOS_NAMES_EMAIL.get(p, p)})

            postos_result.append({
                "posto": p,
                "nome": POSTOS_NAMES_EMAIL.get(p, p),
                "status_envio": status_envio,
                "ultimo_envio": ultimo_str,
                "total_hoje": total_hoje or 0,
                "total_geral": total_geral or 0,
                "total_erros": total_erros or 0,
            })

        # Re-agrega por categoria já limpa (corrige dados históricos no DB)
        from collections import defaultdict
        cat7_agg: dict = defaultdict(lambda: defaultdict(int))
        for r in por_cat_7dias:
            cat7_agg[r[0]][_clean_cat(r[1])] += r[2]
        por_cat_7dias_clean = [
            {"dia": dia, "categoria": cat, "total": total}
            for dia, cats in sorted(cat7_agg.items())
            for cat, total in sorted(cats.items(), key=lambda x: -x[1])
        ]

        cat_hoje_agg: dict = defaultdict(int)
        for r in por_cat_hoje:
            cat_hoje_agg[_clean_cat(r[0])] += r[1]
        por_cat_hoje_clean = [
            {"categoria": cat, "total": total}
            for cat, total in sorted(cat_hoje_agg.items(), key=lambda x: -x[1])
        ]

        return jsonify({
            "hoje": {
                "total": hoje_row[0] if hoje_row else 0,
                "postos_ativos": hoje_row[1] if hoje_row else 0,
                "categorias": len(cat_hoje_agg) if cat_hoje_agg else (hoje_row[2] if hoje_row else 0),
            },
            "por_categoria_7dias": por_cat_7dias_clean,
            "por_categoria_hoje": por_cat_hoje_clean,
            "por_posto": postos_result,
            "postos_sem_hoje": postos_sem_hoje,
            "erros_total": erros_total[0] if erros_total else 0,
            "ultimo_batch": ultimo_batch[0] if ultimo_batch else None,
            "sync": {
                "synced_at":     sync_row[0] if sync_row else None,
                "total_records": sync_row[1] if sync_row else 0,
                "status":        sync_row[2] if sync_row else None,
                "mensagem":      sync_row[3] if sync_row else None,
            },
            "total_postos": len(POSTOS_NAMES_EMAIL),
        })

    except Exception as exc:
        return jsonify({"erro": str(exc)}), 500


# ── Email Clientes: Filtros (categorias e status distintos) ──────────────────

@auth_bp.get("/api/email_clientes/filters")
def email_clientes_filters():
    email_usr, user_postos = decode_user()
    if not email_usr:
        return ("", 401)

    kpi_db = os.environ.get("KPI_DB_PATH", "/opt/relatorio_h_t/camim_kpi.db")

    postos_filter = ""
    params_p = []
    if user_postos:
        ph = ",".join("?" * len(user_postos))
        postos_filter = f"WHERE posto IN ({ph})"
        params_p = list(user_postos)

    try:
        conn = sqlite3.connect(f"file:{kpi_db}?mode=ro", uri=True)

        cats_raw = conn.execute(
            f"SELECT DISTINCT titulo_categoria FROM ind_email {postos_filter} ORDER BY titulo_categoria",
            params_p
        ).fetchall()

        statuses = conn.execute(
            f"SELECT DISTINCT status FROM ind_email {postos_filter} ORDER BY status",
            params_p
        ).fetchall()

        conn.close()

        # Normaliza e deduplica categorias
        cats_set = sorted({_clean_cat(r[0]) for r in cats_raw if r[0]})

        return jsonify({
            "categorias": cats_set,
            "statuses": [r[0] for r in statuses if r[0]],
        })

    except Exception as exc:
        return jsonify({"erro": str(exc)}), 500


# ── Email Clientes: Logs ──────────────────────────────────────────────────────

@auth_bp.get("/api/email_clientes/logs")
def email_clientes_logs():
    email_usr, user_postos = decode_user()
    if not email_usr:
        return ("", 401)

    kpi_db = os.environ.get("KPI_DB_PATH", "/opt/relatorio_h_t/camim_kpi.db")
    PER_PAGE = 50

    posto     = (request.args.get("posto")     or "").strip().upper()
    categoria = (request.args.get("categoria") or "").strip()
    status    = (request.args.get("status")    or "").strip()
    matricula = (request.args.get("matricula") or "").strip()
    date_from = (request.args.get("date_from") or "").strip()
    date_to   = (request.args.get("date_to")   or "").strip()
    try:
        page = max(1, int(request.args.get("page", 1)))
    except Exception:
        page = 1

    where  = ["1=1"]
    params = []

    if user_postos:
        ph = ",".join("?" * len(user_postos))
        where.append(f"posto IN ({ph})")
        params.extend(list(user_postos))

    if posto:
        where.append("posto = ?")
        params.append(posto)
    if categoria:
        # Mapeia categoria canônica → padrão LIKE no banco (que guarda texto antes do agrupamento)
        _cat_sql = {
            "Boleto":                      "%boleto%",
            "Falta do Médico":             "%falta%",
            "Cancelamento":                "%cancelamento%",
            "Nota Fiscal":                 "%nota%fiscal%",
            "Solicitação de Nova Amostra": "%solicitação%amostra%",
        }
        like_pat = _cat_sql.get(categoria, f"%{categoria}%")
        where.append("titulo_categoria LIKE ?")
        params.append(like_pat)
    if status:
        where.append("status LIKE ?")
        params.append(f"%{status}%")
    if matricula:
        where.append("matricula LIKE ?")
        params.append(f"%{matricula}%")
    if date_from:
        where.append("date(datahora) >= ?")
        params.append(date_from)
    if date_to:
        where.append("date(datahora) <= ?")
        params.append(date_to)

    where_str = " AND ".join(where)

    try:
        # Abre rw só para garantir migração da coluna mensagem (adicionada em versão posterior)
        conn_rw = sqlite3.connect(kpi_db)
        try:
            conn_rw.execute("ALTER TABLE ind_email ADD COLUMN mensagem TEXT")
            conn_rw.commit()
        except Exception:
            pass  # coluna já existe
        conn_rw.close()

        conn = sqlite3.connect(f"file:{kpi_db}?mode=ro", uri=True)

        total = conn.execute(
            f"SELECT COUNT(*) FROM ind_email WHERE {where_str}", params
        ).fetchone()[0]

        rows = conn.execute(f"""
            SELECT id, datahora, posto, matricula, titulo_categoria, status, mensagem
            FROM ind_email
            WHERE {where_str}
            ORDER BY datahora DESC, id DESC
            LIMIT ? OFFSET ?
        """, params + [PER_PAGE, (page - 1) * PER_PAGE]).fetchall()

        por_posto = conn.execute(f"""
            SELECT posto, COUNT(*) AS cnt
            FROM ind_email
            WHERE {where_str}
            GROUP BY posto ORDER BY posto
        """, params).fetchall()

        conn.close()

        return jsonify({
            "rows": [
                {"id": r[0], "datahora": r[1], "posto": r[2],
                 "matricula": r[3], "titulo_categoria": _clean_cat(r[4]),
                 "status": r[5], "mensagem": r[6] or ""}
                for r in rows
            ],
            "total":    total,
            "page":     page,
            "per_page": PER_PAGE,
            "por_posto": [{"posto": r[0], "cnt": r[1]} for r in por_posto],
        })

    except Exception as exc:
        return jsonify({"erro": str(exc)}), 500


# ═══════════════════════════════════════════════════════════════════════════════
# TEF Recorrente
# ═══════════════════════════════════════════════════════════════════════════════

def _tef_is_aprovado(erro: str) -> bool:
    if not erro:
        return True
    e = erro.lower()
    return any(k in e for k in ("autoriza", "sucesso", "aprovad"))


def _tef_ensure_table(kpi_db: str) -> None:
    """Cria ind_tef se ainda não existir (antes do primeiro sync)."""
    conn = sqlite3.connect(kpi_db)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS ind_tef (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            posto          TEXT NOT NULL,
            datahora       TEXT,
            matricula      TEXT,
            resposta_cielo TEXT,
            erro           TEXT,
            valor          REAL,
            aprovado       INTEGER DEFAULT 0,
            synced_at      TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_tef_posto    ON ind_tef(posto);
        CREATE INDEX IF NOT EXISTS idx_tef_datahora ON ind_tef(datahora);
        CREATE INDEX IF NOT EXISTS idx_tef_aprovado ON ind_tef(aprovado);
    """)
    conn.commit()
    conn.close()


@auth_bp.get("/api/indicadores/tef")
def indicadores_tef():
    """Último registro por posto para o painel de indicadores."""
    email_usr, user_postos = decode_user()
    if not email_usr:
        return ("", 401)

    kpi_db = os.environ.get("KPI_DB_PATH", "/opt/relatorio_h_t/camim_kpi.db")
    hoje   = date.today()

    try:
        _tef_ensure_table(kpi_db)
        conn = sqlite3.connect(f"file:{kpi_db}?mode=ro", uri=True)

        rows = conn.execute("""
            SELECT posto, MAX(datahora) AS ultimo, COUNT(*) AS total
            FROM ind_tef
            GROUP BY posto
            ORDER BY posto
        """).fetchall()

        sync_row = conn.execute("""
            SELECT synced_at, total_records, status, mensagem
            FROM   ind_sync_log
            WHERE  indicador = 'tef'
            ORDER BY id DESC LIMIT 1
        """).fetchone()

        conn.close()

        dados = {}
        for posto, ultimo_str, total in rows:
            posto = (posto or "").strip().upper()
            if user_postos and posto not in user_postos:
                continue
            try:
                ultimo_dt = datetime.fromisoformat(str(ultimo_str)).date()
                dias = (hoje - ultimo_dt).days
            except Exception:
                dias = 999
            dados[posto] = {
                "posto":       posto,
                "ultimo_tef":  ultimo_str,
                "dias":        dias,
                "total":       total,
            }

        return jsonify({
            "dados": dados,
            "sync": {
                "synced_at":     sync_row[0] if sync_row else None,
                "total_records": sync_row[1] if sync_row else 0,
                "status":        sync_row[2] if sync_row else None,
                "mensagem":      sync_row[3] if sync_row else None,
            },
        })

    except Exception as exc:
        return jsonify({"erro": str(exc)}), 500


@auth_bp.get("/api/tef/dashboard")
def tef_dashboard():
    email_usr, user_postos = decode_user()
    if not email_usr:
        return ("", 401)

    kpi_db = os.environ.get("KPI_DB_PATH", "/opt/relatorio_h_t/camim_kpi.db")
    hoje_str = date.today().isoformat()

    try:
        _tef_ensure_table(kpi_db)
        conn = sqlite3.connect(f"file:{kpi_db}?mode=ro", uri=True)

        posto_filter = ""
        posto_params = []
        if user_postos:
            ph = ",".join("?" * len(user_postos))
            posto_filter = f"AND posto IN ({ph})"
            posto_params = list(user_postos)

        # KPIs de hoje
        hoje_row = conn.execute(f"""
            SELECT COUNT(*),
                   SUM(CASE WHEN aprovado=1 THEN 1 ELSE 0 END),
                   SUM(CASE WHEN aprovado=0 THEN 1 ELSE 0 END),
                   SUM(CASE WHEN aprovado=1 THEN valor ELSE 0 END)
            FROM ind_tef
            WHERE date(datahora) = ? {posto_filter}
        """, [hoje_str] + posto_params).fetchone()

        total_hoje    = hoje_row[0] or 0
        aprov_hoje    = hoje_row[1] or 0
        negad_hoje    = hoje_row[2] or 0
        valor_hoje    = hoje_row[3] or 0.0

        # Últimos 7 dias por dia
        trend7 = conn.execute(f"""
            SELECT date(datahora) AS dia,
                   SUM(CASE WHEN aprovado=1 THEN 1 ELSE 0 END) AS aprovadas,
                   SUM(CASE WHEN aprovado=0 THEN 1 ELSE 0 END) AS negadas
            FROM ind_tef
            WHERE date(datahora) >= date(?, '-6 days') {posto_filter}
            GROUP BY dia ORDER BY dia
        """, [hoje_str] + posto_params).fetchall()

        # Top erros de negação (últimos 7 dias)
        top_erros = conn.execute(f"""
            SELECT COALESCE(NULLIF(TRIM(erro),''), '(sem descrição)') AS motivo,
                   COUNT(*) AS cnt
            FROM ind_tef
            WHERE aprovado = 0
              AND date(datahora) >= date(?, '-6 days') {posto_filter}
            GROUP BY motivo ORDER BY cnt DESC LIMIT 10
        """, [hoje_str] + posto_params).fetchall()

        # Por posto
        por_posto = conn.execute(f"""
            SELECT posto,
                   COUNT(*) AS total,
                   SUM(CASE WHEN aprovado=1 THEN 1 ELSE 0 END) AS aprovadas,
                   SUM(CASE WHEN aprovado=0 THEN 1 ELSE 0 END) AS negadas,
                   SUM(CASE WHEN aprovado=1 THEN valor ELSE 0 END) AS valor_aprov,
                   MAX(datahora) AS ultimo
            FROM ind_tef
            WHERE date(datahora) = ? {posto_filter}
            GROUP BY posto ORDER BY posto
        """, [hoje_str] + posto_params).fetchall()

        # Por posto — total geral (60 dias)
        por_posto_total = conn.execute(f"""
            SELECT posto, COUNT(*), SUM(CASE WHEN aprovado=1 THEN 1 ELSE 0 END)
            FROM ind_tef
            WHERE 1=1 {posto_filter}
            GROUP BY posto ORDER BY posto
        """, posto_params).fetchall()
        posto_total_map = {r[0]: {"total": r[1], "aprovadas": r[2]} for r in por_posto_total}

        # Sync
        sync_row = conn.execute("""
            SELECT synced_at, status, mensagem FROM ind_sync_log
            WHERE indicador='tef' ORDER BY id DESC LIMIT 1
        """).fetchone()

        conn.close()

        return jsonify({
            "hoje": {
                "total":      total_hoje,
                "aprovadas":  aprov_hoje,
                "negadas":    negad_hoje,
                "valor":      round(valor_hoje, 2),
            },
            "trend7": [
                {"dia": r[0], "aprovadas": r[1], "negadas": r[2]}
                for r in trend7
            ],
            "top_erros": [{"motivo": r[0], "cnt": r[1]} for r in top_erros],
            "por_posto_hoje": [
                {"posto": r[0], "total": r[1], "aprovadas": r[2],
                 "negadas": r[3], "valor": round(r[4] or 0, 2), "ultimo": r[5],
                 "total_geral": posto_total_map.get(r[0], {}).get("total", 0),
                 "aprov_geral": posto_total_map.get(r[0], {}).get("aprovadas", 0)}
                for r in por_posto
            ],
            "sync": {
                "synced_at": sync_row[0] if sync_row else None,
                "status":    sync_row[1] if sync_row else None,
                "mensagem":  sync_row[2] if sync_row else None,
            },
        })

    except Exception as exc:
        return jsonify({"erro": str(exc)}), 500


@auth_bp.get("/api/tef/filters")
def tef_filters():
    email_usr, user_postos = decode_user()
    if not email_usr:
        return ("", 401)

    kpi_db = os.environ.get("KPI_DB_PATH", "/opt/relatorio_h_t/camim_kpi.db")
    try:
        _tef_ensure_table(kpi_db)
        conn = sqlite3.connect(f"file:{kpi_db}?mode=ro", uri=True)
        erros = conn.execute("""
            SELECT DISTINCT TRIM(erro) FROM ind_tef
            WHERE erro IS NOT NULL AND TRIM(erro) != ''
            ORDER BY 1
        """).fetchall()
        respostas = conn.execute("""
            SELECT DISTINCT TRIM(resposta_cielo) FROM ind_tef
            WHERE resposta_cielo IS NOT NULL AND TRIM(resposta_cielo) != ''
            ORDER BY 1
        """).fetchall()
        conn.close()
        return jsonify({
            "erros":     [r[0] for r in erros],
            "respostas": [r[0] for r in respostas],
        })
    except Exception as exc:
        return jsonify({"erro": str(exc)}), 500


@auth_bp.get("/api/tef/logs")
def tef_logs():
    email_usr, user_postos = decode_user()
    if not email_usr:
        return ("", 401)

    kpi_db = os.environ.get("KPI_DB_PATH", "/opt/relatorio_h_t/camim_kpi.db")
    PER_PAGE = 50

    posto     = (request.args.get("posto")    or "").strip().upper()
    status    = (request.args.get("status")   or "").strip()   # "aprovada" | "negada"
    matricula = (request.args.get("matricula") or "").strip()
    erro_f    = (request.args.get("erro")     or "").strip()
    date_from = (request.args.get("date_from") or "").strip()
    date_to   = (request.args.get("date_to")   or "").strip()
    try:
        page = max(1, int(request.args.get("page", 1)))
    except Exception:
        page = 1

    where  = ["1=1"]
    params = []

    if user_postos:
        ph = ",".join("?" * len(user_postos))
        where.append(f"posto IN ({ph})")
        params.extend(list(user_postos))

    if posto:
        where.append("posto = ?")
        params.append(posto)
    if status == "aprovada":
        where.append("aprovado = 1")
    elif status == "negada":
        where.append("aprovado = 0")
    if matricula:
        where.append("matricula LIKE ?")
        params.append(f"%{matricula}%")
    if erro_f:
        where.append("erro LIKE ?")
        params.append(f"%{erro_f}%")
    if date_from:
        where.append("date(datahora) >= ?")
        params.append(date_from)
    if date_to:
        where.append("date(datahora) <= ?")
        params.append(date_to)

    where_str = " AND ".join(where)

    try:
        _tef_ensure_table(kpi_db)
        conn = sqlite3.connect(f"file:{kpi_db}?mode=ro", uri=True)

        total = conn.execute(
            f"SELECT COUNT(*) FROM ind_tef WHERE {where_str}", params
        ).fetchone()[0]

        rows = conn.execute(f"""
            SELECT id, datahora, posto, matricula, resposta_cielo, erro, valor, aprovado
            FROM ind_tef
            WHERE {where_str}
            ORDER BY datahora DESC, id DESC
            LIMIT ? OFFSET ?
        """, params + [PER_PAGE, (page - 1) * PER_PAGE]).fetchall()

        por_posto = conn.execute(f"""
            SELECT posto, COUNT(*) AS cnt,
                   SUM(CASE WHEN aprovado=1 THEN 1 ELSE 0 END) AS aprov
            FROM ind_tef WHERE {where_str}
            GROUP BY posto ORDER BY posto
        """, params).fetchall()

        conn.close()

        return jsonify({
            "rows": [
                {"id": r[0], "datahora": r[1], "posto": r[2],
                 "matricula": r[3], "resposta_cielo": r[4] or "",
                 "erro": r[5] or "", "valor": r[6], "aprovado": r[7]}
                for r in rows
            ],
            "total":    total,
            "page":     page,
            "per_page": PER_PAGE,
            "por_posto": [{"posto": r[0], "cnt": r[1], "aprov": r[2]} for r in por_posto],
        })

    except Exception as exc:
        return jsonify({"erro": str(exc)}), 500


# ============================================================
# API — Chat Avaliações (MySQL camim_chat_production)
# ============================================================

@auth_bp.get("/api/chat_avaliacoes")
def chat_avaliacoes_api():
    email_usr, _ = decode_user()
    if not email_usr:
        return ("", 401)

    ini      = request.args.get("ini", "")
    fim      = request.args.get("fim", "")
    com_obs  = request.args.get("com_obs", "0") == "1"
    page     = max(1, int(request.args.get("page", 1) or 1))
    PER_PAGE = 200

    try:
        import pymysql
        host = os.environ.get("CHAT_MYSQL_HOST", "")
        port = int(os.environ.get("CHAT_MYSQL_PORT", 3306))
        user = os.environ.get("CHAT_MYSQL_USER", "")
        pwd  = os.environ.get("CHAT_MYSQL_PASSWORD", "")
        db   = os.environ.get("CHAT_MYSQL_DATABASE", "camim_chat_production")

        conn = pymysql.connect(
            host=host, port=port, user=user, password=pwd,
            database=db, charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=10,
        )
        cur = conn.cursor()

        where_parts = ["te.deletedAt IS NULL"]
        params = []
        if ini:
            where_parts.append("DATE(t.closedAt) >= %s")
            params.append(ini)
        if fim:
            where_parts.append("DATE(t.closedAt) <= %s")
            params.append(fim)
        if com_obs:
            where_parts.append("te.obs IS NOT NULL AND te.obs != ''")
        where_str = " AND ".join(where_parts)

        # Total
        cur.execute(
            f"SELECT COUNT(*) AS n FROM TicketEvaluation te "
            f"LEFT JOIN Ticket t ON t.id = te.ticketId WHERE {where_str}",
            params,
        )
        total = (cur.fetchone() or {}).get("n", 0)

        # Distribuição por nota
        cur.execute(f"""
            SELECT te.score, COUNT(*) AS cnt
            FROM TicketEvaluation te
            LEFT JOIN Ticket t ON t.id = te.ticketId
            WHERE {where_str}
            GROUP BY te.score ORDER BY te.score
        """, params)
        dist = [{"score": r["score"], "cnt": r["cnt"]} for r in cur.fetchall()]

        # Média
        cur.execute(f"""
            SELECT ROUND(AVG(te.score), 2) AS media, COUNT(*) AS total_com_nota
            FROM TicketEvaluation te
            LEFT JOIN Ticket t ON t.id = te.ticketId
            WHERE {where_str} AND te.score IS NOT NULL
        """, params)
        stats_row = cur.fetchone() or {}
        media          = stats_row.get("media")
        total_com_nota = stats_row.get("total_com_nota", 0)

        # Tendência diária
        cur.execute(f"""
            SELECT DATE(t.closedAt) AS dia,
                   ROUND(AVG(te.score), 2) AS media,
                   COUNT(*) AS cnt
            FROM TicketEvaluation te
            LEFT JOIN Ticket t ON t.id = te.ticketId
            WHERE {where_str} AND te.score IS NOT NULL AND t.closedAt IS NOT NULL
            GROUP BY dia ORDER BY dia
        """, params)
        trend = [
            {"dia": str(r["dia"]), "media": float(r["media"] or 0), "cnt": r["cnt"]}
            for r in cur.fetchall()
        ]

        # Linhas paginadas
        offset = (page - 1) * PER_PAGE
        cur.execute(f"""
            SELECT te.id, te.ticketId, te.score, te.obs,
                   te.createdAt, t.closedAt
            FROM TicketEvaluation te
            LEFT JOIN Ticket t ON t.id = te.ticketId
            WHERE {where_str}
            ORDER BY t.closedAt DESC, te.id DESC
            LIMIT %s OFFSET %s
        """, params + [PER_PAGE, offset])
        rows = cur.fetchall()
        cur.close()
        conn.close()

        def _fmt(v):
            if v is None:
                return None
            if hasattr(v, "isoformat"):
                return v.isoformat()
            return str(v)

        return jsonify({
            "total":          total,
            "total_com_nota": total_com_nota,
            "media":          float(media) if media is not None else None,
            "dist":           dist,
            "trend":          trend,
            "page":           page,
            "per_page":       PER_PAGE,
            "rows": [
                {
                    "id":        r["id"],
                    "ticketId":  r["ticketId"],
                    "score":     r["score"],
                    "obs":       r["obs"] or "",
                    "createdAt": _fmt(r["createdAt"]),
                    "closedAt":  _fmt(r["closedAt"]),
                }
                for r in rows
            ],
        })

    except Exception as exc:
        return jsonify({"erro": str(exc)}), 500
