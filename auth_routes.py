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

import json
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
    PageUsagePing, RegraAnomalia, AnomaliaVerificacao,
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
- NÃO use tags HTML (como <div>, <h2>, <span>, <p>, <style>, etc). Responda APENAS em texto puro com formatação Markdown simples (##, ###, - item).
"""

_IA_PROMPTS_POR_KPI = {
    "qualidade_agenda": """Você é um ESPECIALISTA em gestão de agenda médica da CAMIM (rede de clínicas médicas).
Você tem 20 anos de experiência operacional em clínicas e entende profundamente de:
- Regulação ANS (prazos máximos por especialidade para primeira consulta)
- Gestão de capacidade de agenda (vagas, capacidade, ocupação)
- Remanejamento de profissionais entre postos
- Impacto da agenda no negócio (churn, vendas, fidelização)
- Detecção de problemas sistêmicos vs. pontuais

COMO USAR OS DADOS:
O contexto contém TODOS os dados da página de Qualidade da Agenda, organizados assim:
1. RESUMO GERAL: score de saúde, contagem por status
2. DETALHAMENTO POR POSTO: cada especialidade de cada posto com dias até vaga, vagas disponíveis, capacidade, prazos ANS/CAMIM
3. VISÃO POR ESPECIALIDADE: cada especialidade em todos os postos (para responder "Ortopedia está ruim ONDE?")
4. PROBLEMAS SISTÊMICOS: especialidades com 3+ postos em estado crítico
5. INDICADORES CRUZADOS: churn, contratos ativos, consultas/faltas
6. ZONAS GEOGRÁFICAS E VIZINHOS: para sugestões de remanejamento

REGRAS:
- SEMPRE use os dados EXATOS do contexto. NUNCA invente números.
- Quando perguntarem sobre uma ESPECIALIDADE (ex: "Ortopedia"), busque na seção VISÃO POR ESPECIALIDADE para ver todos os postos.
- Quando perguntarem sobre um POSTO (ex: "como está Anchieta?"), busque na seção DETALHAMENTO POR POSTO.
- Quando perguntarem "qual o pior posto?", compare os scores de saúde.
- Para sugestões de remanejamento, use a info de vizinhos geográficos.
- RESPEITE os postos selecionados — só fale dos postos que aparecem no contexto.
- Se o dado não está no contexto, diga claramente que não tem essa informação.
- Quando listar postos com problemas, inclua: o nome do posto, o status, quantos dias até vaga, e vagas disponíveis.
- Se detectar problema sistêmico (3+ postos com mesma especialidade crítica), destaque e recomende contratação.
- Se for problema pontual (1-2 postos), recomende remanejamento usando vizinhos.

FORMATAÇÃO:
- Use ## para seções e ### para subseções
- Use "- item" para listas
- Inclua números exatos do contexto (dias, vagas, percentuais)
- Termine com recomendação prática e objetiva
- NÃO use tabelas markdown, NÃO use negrito (**), NÃO use itálico (_)
- NÃO use tags HTML. Responda APENAS em texto puro com Markdown simples.
- Seja direto e técnico. Você fala para gestores de saúde com experiência.
""",
}

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
    {"key": "kpi_egide",                 "label": "KPI Égide Saúde",             "group": "kpi"},
    {"key": "kpi_notas_rps",             "label": "KPI Notas x RPS",             "group": "kpi"},
    {"key": "kpi_metas",                 "label": "KPI Metas (Mens/Vendas)",     "group": "kpi"},
    {"key": "kpi_governo",               "label": "KPI Índices Oficiais",        "group": "kpi"},
    {"key": "kpi_liberty",               "label": "KPI CAMIM Liberty",           "group": "kpi"},
    {"key": "kpi_receita_despesa",       "label": "KPI Receitas x Despesas",     "group": "kpi"},
    {"key": "kpi_receita_despesa_rateio","label": "KPI R x D com Rateio",        "group": "kpi"},
    {"key": "growth",                    "label": "Growth Dashboard",            "group": "kpi"},
    {"key": "mais_servicos",             "label": "Mais Serviços",               "group": "kpi"},
    {"key": "email_clientes",            "label": "Email de Cobrança",           "group": "kpi"},
    {"key": "chat_avaliacoes",           "label": "CHAT Avaliações",             "group": "kpi"},
    {"key": "leiame",                    "label": "Leia-me (Painel Antigo)",     "group": "kpi"},
    # ── Mais Serviços ──
    {"key": "k_nbs_ibs_cbs",            "label": "Notas Fiscais NBS/IBS/CBS",   "group": "mais"},
    {"key": "k_relatorio_pcs",          "label": "Planejamento PC's",           "group": "mais"},
    {"key": "k_whatsapp_explicado",     "label": "WhatsApp - Explicando a Cobrança", "group": "mais"},
    {"key": "cobranca",                 "label": "Cobrança",                    "group": "mais"},
    {"key": "chat_externo",             "label": "Chat",                        "group": "mais"},
    {"key": "broker",                   "label": "Vendas Efetivar",             "group": "mais"},
    {"key": "corretores",               "label": "Vendas Leads Corretores",     "group": "mais"},
    {"key": "leads_analytics",           "label": "Vendas - Leads Analytics",    "group": "mais"},
    {"key": "tarefas",                  "label": "Tarefas",                     "group": "mais"},
    {"key": "push_cobranca",            "label": "Push de Cobrança IA",         "group": "mais"},
    {"key": "wpp_campanhas",            "label": "WhatsApp Campanhas",          "group": "mais"},
    {"key": "camila_crm",               "label": "Camila.ai CRM",               "group": "mais"},
    {"key": "crm",                      "label": "CRM",                         "group": "mais"},
    {"key": "central",                  "label": "Central",                     "group": "mais"},
    {"key": "agenda_dia",                "label": "Agenda do Dia (F3)",          "group": "mais"},
    {"key": "preagendamento",            "label": "Dashboard Pré-Agendamento",   "group": "mais"},
    {"key": "iot_monitor",               "label": "Monitor IoT (Ar Condicionado)","group": "mais"},
    {"key": "camila_funcionarios",       "label": "Camila dos Funcionários",     "group": "mais"},
    {"key": "medico_novo",               "label": "Médico - Inclusão Agenda Temporária", "group": "mais"},
    {"key": "medico_falta",              "label": "Médico - Cadastrar Falta + WhatsApp", "group": "mais"},
    {"key": "tef",                       "label": "TEF Recorrente",              "group": "mais"},
    {"key": "chat_dashboard",            "label": "Dashboard Chat (Camila.ai)",  "group": "mais"},
    {"key": "wpp_dashboard",             "label": "Dashboard WhatsApp (Meta)",   "group": "mais"},
    {"key": "ctrlq_desbloqueio",         "label": "Médico - Desbloqueio de Agenda — CTRL-Q","group": "mais"},
    {"key": "qualidade_agenda",          "label": "Qualidade da Agenda Médica",  "group": "mais"},
    {"key": "higienizacao",              "label": "Higienização",                "group": "mais"},
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


def _manus_key_ok() -> bool:
    """Valida header X-Manus-Key contra env MANUS_SERVICE_KEY (ambos presentes e iguais)."""
    expected = os.getenv("MANUS_SERVICE_KEY", "").strip()
    if not expected:
        return False
    provided = (request.headers.get("X-Manus-Key") or "").strip()
    if not provided:
        return False
    return secrets.compare_digest(provided, expected)


def require_auth_or_key():
    """
    Valida cookie de sessão OU header X-Manus-Key.
    Retorna (principal, postos):
      - cookie válido  → (email, lista_postos)
      - key válida      → ("manus@service", [])
      - nenhum          → (None, None)
    """
    email, postos = decode_user()
    if email:
        return email, postos
    if _manus_key_ok():
        return "manus@service", []
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
    base = os.environ.get("APP_BASE_URL", "https://kpi.camim.com.br")
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
    "https://kpi.camim.com.br/auth/idcamim/callback",
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
    """
    Usado pelo nginx via auth_request. Valida cookie de sessão OU X-Manus-Key.
    Retorna 204 (OK) ou 401.
    """
    c = request.cookies.get(_SESS_NAME)
    if c and _signer is not None:
        try:
            _signer.unsign(c, max_age=_TTL_SECONDS + 3600)
            return ("", 204)
        except BadSignature:
            pass
    if _manus_key_ok():
        return ("", 204)
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
                "pode_desbloquear": getattr(u, 'pode_desbloquear', False),
                "id_usuario_sqlserver": getattr(u, 'id_usuario_sqlserver', None),
                "login_campinho": getattr(u, 'login_campinho', None) or "",
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
        user.pode_desbloquear = bool(d.get("pode_desbloquear", False))
        if d.get("id_usuario_sqlserver"):
            user.id_usuario_sqlserver = int(d["id_usuario_sqlserver"])
        if d.get("login_campinho"):
            user.login_campinho = d["login_campinho"].strip()
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
        if "pode_desbloquear" in d:
            # Só permite ativar se o usuário estiver mapeado ao SQL Server
            if d["pode_desbloquear"] and not (d.get("id_usuario_sqlserver") or getattr(user, 'id_usuario_sqlserver', None)):
                return jsonify({"erro": "É necessário vincular ao usuário SQL Server antes de ativar Desbloquear Agenda."}), 400
            user.pode_desbloquear = bool(d["pode_desbloquear"])
        if "id_usuario_sqlserver" in d:
            user.id_usuario_sqlserver = int(d["id_usuario_sqlserver"]) if d["id_usuario_sqlserver"] else None
        if "login_campinho" in d:
            user.login_campinho = (d["login_campinho"] or "").strip() or None
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


@auth_bp.get("/admin/api/validar_usuario_sqlserver")
def admin_validar_usuario_sqlserver():
    """Valida login Campinho contra Sis_Usuario no SQL Server."""
    if not _require_admin():
        return jsonify({"erro": "Não autorizado"}), 403
    login = (request.args.get("login") or "").strip()
    if not login:
        return jsonify({"erro": "Login obrigatório"}), 400
    try:
        from ctrlq_desbloqueio import build_conns_from_env, make_engine
        from sqlalchemy import text as sa_text
        conns = build_conns_from_env()
        if not conns:
            return jsonify({"erro": "Nenhuma conexão SQL Server configurada"}), 500
        # Usa a primeira conexão disponível (Sis_Usuario é compartilhada)
        conn_str = next(iter(conns.values()))
        engine = make_engine(conn_str)
        with engine.connect() as conn:
            row = conn.execute(sa_text(
                "SELECT TOP 1 idUsuario, Nome, Usuario FROM Sis_Usuario WHERE Usuario = :login"
            ), {"login": login}).fetchone()
        if not row:
            return jsonify({"erro": f"Usuário '{login}' não encontrado no Campinho"}), 404
        return jsonify({"ok": True, "idusuario": row[0], "nome": row[1], "usuario": row[2]})
    except Exception as e:
        import logging
        logging.getLogger(__name__).error("Erro ao validar usuario SQL Server: %s", e)
        return jsonify({"erro": str(e)}), 500


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


# ── Tracking de tempo por página ───────────────────────────────────────────────

def _normalize_page(raw: str) -> str:
    """Normaliza path pra agrupar pings. Tira query string/hash, limita tamanho."""
    if not raw:
        return "/"
    s = str(raw).split("?", 1)[0].split("#", 1)[0].strip()
    if not s:
        return "/"
    if not s.startswith("/"):
        s = "/" + s
    return s[:200]


@auth_bp.post("/api/usage/ping")
def api_usage_ping():
    """Registra 1 minuto de uso do usuário logado na página atual.

    Body JSON: {"page": "/kpi_home.html"}. Silenciosamente ignora se o bucket do
    minuto já foi inserido (constraint unique). Arredonda pra baixo ao minuto UTC.
    """
    from auth_db import get_user_by_email as _gue
    email, _ = decode_user()
    if not email:
        return jsonify({"erro": "Não autorizado"}), 401

    d = request.get_json(silent=True) or {}
    page = _normalize_page(d.get("page") or request.referrer or "/")
    bucket = datetime.utcnow().strftime("%Y-%m-%d %H:%M:00")

    db = SessionLocal()
    try:
        u = _gue(db, email)
        if not u:
            return jsonify({"erro": "Usuário não encontrado"}), 401
        try:
            db.add(PageUsagePing(user_id=u.id, page=page, minute_bucket=bucket))
            db.commit()
            inserted = True
        except Exception:
            # Conflito no unique (user_id, page, minute_bucket) — ping repetido no mesmo minuto
            db.rollback()
            inserted = False
        return jsonify({"ok": True, "inserted": inserted, "bucket": bucket, "page": page})
    finally:
        db.close()


@auth_bp.get("/admin/api/usuarios/<int:uid>/usage")
def admin_usage(uid: int):
    """Tempo por página para um usuário. Query: ?days=30 (default 30, max 365)."""
    if not _require_admin():
        return jsonify({"erro": "Não autorizado"}), 403
    try:
        days = max(1, min(int(request.args.get("days", 30)), 365))
    except Exception:
        days = 30
    corte = (datetime.utcnow() - __import__("datetime").timedelta(days=days)) \
        .strftime("%Y-%m-%d %H:%M:00")

    db = SessionLocal()
    try:
        # Minutos por página (cada bucket distinto = 1 minuto)
        rows = (
            db.query(
                PageUsagePing.page,
                func.count(PageUsagePing.minute_bucket).label("minutos"),
                func.min(PageUsagePing.minute_bucket).label("primeiro"),
                func.max(PageUsagePing.minute_bucket).label("ultimo"),
            )
            .filter(PageUsagePing.user_id == uid)
            .filter(PageUsagePing.minute_bucket >= corte)
            .group_by(PageUsagePing.page)
            .all()
        )
        paginas = [{
            "page": r.page,
            "minutos": int(r.minutos or 0),
            "primeiro": r.primeiro,
            "ultimo": r.ultimo,
        } for r in rows]
        paginas.sort(key=lambda x: x["minutos"], reverse=True)

        total_minutos = sum(p["minutos"] for p in paginas)
        return jsonify({
            "dias": days,
            "corte_utc": corte,
            "total_minutos": total_minutos,
            "total_paginas": len(paginas),
            "paginas": paginas,
        })
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
    system_prompt = _IA_PROMPTS_POR_KPI.get(kpi, _IA_SYSTEM_PROMPT)
    if regras_gerais_txt:
        system_prompt += f"\nREGRAS GERAIS (aplicam-se a todos os KPIs):\n{regras_gerais_txt}\n"
    if kpi_contexto_txt:
        system_prompt += f"\nCONTEXTO DE NEGÓCIO DESTE KPI (escrito pelo gestor):\n{kpi_contexto_txt}\n"

    resposta_json = None
    resposta_txt  = ""

    # ── Monta histórico de conversa (se houver) ──────────────────────────────
    historico_txt = ""
    historico = d.get("historico") or []
    if historico and isinstance(historico, list):
        partes = []
        for msg in historico[-6:]:  # últimas 6 mensagens (3 trocas)
            role = msg.get("role", "")
            text = str(msg.get("text", ""))[:800]
            if role == "user":
                partes.append(f"Usuário: {text}")
            elif role == "ia":
                partes.append(f"IA: {text}")
        if partes:
            historico_txt = (
                "\n\nHISTÓRICO DA CONVERSA (mensagens anteriores — "
                "use para entender o contexto da pergunta atual, "
                "a pergunta de agora provavelmente é continuação da anterior):\n"
                + "\n".join(partes)
            )

    prompt_completo = (
        f"Dados do relatório (pré-calculados pelo sistema):\n\n{contexto}"
        f"{historico_txt}"
        f"\n\nPergunta atual do usuário:\n{pergunta_txt}"
    )

    # ── Seleciona o cliente LLM ──────────────────────────────────────────────
    llm_client = None
    if provider == "groq":
        try:
            llm_client = _get_groq()
        except Exception as exc:
            return jsonify({"erro": f"Groq indisponível: {exc}"}), 502
    elif provider == "openai":
        try:
            llm_client = _get_openai()
        except Exception as exc:
            return jsonify({"erro": f"OpenAI indisponível: {exc}"}), 502
    elif provider == "anthropic":
        try:
            llm_client = _get_anthropic()
        except Exception as exc:
            return jsonify({"erro": f"Anthropic indisponível: {exc}"}), 502
    else:
        return jsonify({"erro": f"Provedor desconhecido: {provider}"}), 400

    # ── Chamada com validação de resposta completa ───────────────────────────
    try:
        resposta_txt = llm_client.gerar_texto(
            prompt=prompt_completo, system_prompt=system_prompt,
        )
        truncada = getattr(llm_client, "last_finish_reason", None) == "length"

        # Se truncada, tenta de novo pedindo resposta concisa
        if truncada:
            prompt_retry = (
                f"{prompt_completo}\n\n"
                "ATENÇÃO: sua resposta anterior foi cortada por limite de tokens. "
                "Responda a mesma pergunta de forma mais CONCISA e COMPLETA. "
                "Priorize os dados mais relevantes. Não repita cabeçalhos longos."
            )
            resposta_txt = llm_client.gerar_texto(
                prompt=prompt_retry, system_prompt=system_prompt,
            )
            ainda_truncada = getattr(llm_client, "last_finish_reason", None) == "length"

            if ainda_truncada:
                resposta_txt = (
                    "Desculpe, não consegui gerar uma resposta completa para esta pergunta. "
                    "Tente reformular de forma mais específica (ex: pergunte sobre um posto "
                    "ou especialidade por vez).\n\n"
                    "Resposta parcial:\n\n" + resposta_txt
                )

        resposta_json = {"resposta": resposta_txt, "provider": provider}
    except Exception as exc:
        return jsonify({"erro": f"{provider} indisponível: {exc}"}), 502

    # ── Captura uso / custo da última chamada ────────────────────────────────
    usage = getattr(llm_client, "last_usage", {}) or {}
    model_used = getattr(llm_client, "last_model", None)
    ptoks = usage.get("prompt_tokens")
    ctoks = usage.get("completion_tokens")
    ttoks = usage.get("total_tokens")
    try:
        from ia_pricing import estimar_custo_usd
        cost = estimar_custo_usd(model_used, ptoks, ctoks)
    except Exception:
        cost = None

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
                provider = provider,
                model    = model_used,
                prompt_tokens     = ptoks,
                completion_tokens = ctoks,
                total_tokens      = ttoks,
                cost_usd = cost,
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


# ── Admin: Monitoramento de uso e custo da IA ────────────────────────────────

def _parse_iso_dt(s: str):
    from datetime import datetime as _dt
    if not s:
        return None
    try:
        s = s.strip().replace("Z", "")
        # aceita "YYYY-MM-DD" e "YYYY-MM-DDTHH:MM:SS"
        if "T" in s:
            return _dt.fromisoformat(s)
        return _dt.fromisoformat(s + "T00:00:00")
    except Exception:
        return None


@auth_bp.get("/admin/api/ia/resumo")
def admin_ia_resumo():
    """Resumo de uso da IA nos últimos N dias:
       - total por usuário (tokens, custo, conversas)
       - série temporal diária (tokens + custo)
       - top N usuários com maior custo em um único dia
    """
    if not _require_admin():
        return jsonify({"erro": "Não autorizado"}), 403

    from datetime import timedelta as _td

    try:
        dias = max(1, min(90, int(request.args.get("dias", 7))))
    except Exception:
        dias = 7

    desde = datetime.utcnow() - _td(days=dias)

    db = SessionLocal()
    try:
        # Por usuário
        q_user = (
            db.query(
                User.id, User.email, User.nome,
                func.count(IAConversa.id).label("conversas"),
                func.coalesce(func.sum(IAConversa.total_tokens), 0).label("tokens"),
                func.coalesce(func.sum(IAConversa.prompt_tokens), 0).label("prompt_tokens"),
                func.coalesce(func.sum(IAConversa.completion_tokens), 0).label("completion_tokens"),
                func.coalesce(func.sum(IAConversa.cost_usd), 0.0).label("custo_usd"),
                func.max(IAConversa.created_at).label("ultimo"),
            )
            .join(IAConversa, IAConversa.user_id == User.id)
            .filter(IAConversa.created_at >= desde)
            .group_by(User.id)
            .order_by(func.coalesce(func.sum(IAConversa.cost_usd), 0.0).desc())
            .all()
        )
        por_usuario = [{
            "user_id":           r.id,
            "email":             r.email,
            "nome":              r.nome or "",
            "conversas":         int(r.conversas or 0),
            "tokens":            int(r.tokens or 0),
            "prompt_tokens":     int(r.prompt_tokens or 0),
            "completion_tokens": int(r.completion_tokens or 0),
            "custo_usd":         round(float(r.custo_usd or 0.0), 6),
            "ultimo":            r.ultimo.isoformat() if r.ultimo else None,
        } for r in q_user]

        # Série diária
        dia_expr = func.date(IAConversa.created_at)
        q_dia = (
            db.query(
                dia_expr.label("dia"),
                func.count(IAConversa.id).label("conversas"),
                func.coalesce(func.sum(IAConversa.total_tokens), 0).label("tokens"),
                func.coalesce(func.sum(IAConversa.cost_usd), 0.0).label("custo_usd"),
            )
            .filter(IAConversa.created_at >= desde)
            .group_by(dia_expr)
            .order_by(dia_expr)
            .all()
        )
        por_dia = [{
            "dia":       r.dia,
            "conversas": int(r.conversas or 0),
            "tokens":    int(r.tokens or 0),
            "custo_usd": round(float(r.custo_usd or 0.0), 6),
        } for r in q_dia]

        # Picos: usuário+dia com maior custo (para detectar quem "estourou" em um único dia)
        q_pico = (
            db.query(
                User.email,
                dia_expr.label("dia"),
                func.count(IAConversa.id).label("conversas"),
                func.coalesce(func.sum(IAConversa.total_tokens), 0).label("tokens"),
                func.coalesce(func.sum(IAConversa.cost_usd), 0.0).label("custo_usd"),
            )
            .join(IAConversa, IAConversa.user_id == User.id)
            .filter(IAConversa.created_at >= desde)
            .group_by(User.id, dia_expr)
            .order_by(func.coalesce(func.sum(IAConversa.cost_usd), 0.0).desc())
            .limit(20)
            .all()
        )
        picos = [{
            "email":     r.email,
            "dia":       r.dia,
            "conversas": int(r.conversas or 0),
            "tokens":    int(r.tokens or 0),
            "custo_usd": round(float(r.custo_usd or 0.0), 6),
        } for r in q_pico]

        # Por provedor
        q_prov = (
            db.query(
                IAConversa.provider,
                func.count(IAConversa.id).label("conversas"),
                func.coalesce(func.sum(IAConversa.total_tokens), 0).label("tokens"),
                func.coalesce(func.sum(IAConversa.cost_usd), 0.0).label("custo_usd"),
            )
            .filter(IAConversa.created_at >= desde)
            .group_by(IAConversa.provider)
            .order_by(func.coalesce(func.sum(IAConversa.cost_usd), 0.0).desc())
            .all()
        )
        por_provider = [{
            "provider":  r.provider or "—",
            "conversas": int(r.conversas or 0),
            "tokens":    int(r.tokens or 0),
            "custo_usd": round(float(r.custo_usd or 0.0), 6),
        } for r in q_prov]

        # Totais do período
        totais = {
            "conversas": sum(u["conversas"] for u in por_usuario),
            "tokens":    sum(u["tokens"]    for u in por_usuario),
            "custo_usd": round(sum(u["custo_usd"] for u in por_usuario), 6),
        }

        return jsonify({
            "dias":          dias,
            "desde":         desde.isoformat(),
            "totais":        totais,
            "por_usuario":   por_usuario,
            "por_dia":       por_dia,
            "por_provider":  por_provider,
            "picos_usuario_dia": picos,
        })
    finally:
        db.close()


@auth_bp.get("/admin/api/ia/conversas")
def admin_ia_conversas():
    """Lista detalhada de conversas IA com filtros (email, provider, dia, desde/ate).

    Parâmetros:
      - email     (contém, case-insensitive)
      - provider  (groq/openai/anthropic)
      - since     (ISO date/datetime — UTC)
      - until     (ISO date/datetime — UTC)
      - dia       (YYYY-MM-DD — atalho que força janela do dia inteiro)
      - limit     (padrão 200, máx 1000)
    """
    if not _require_admin():
        return jsonify({"erro": "Não autorizado"}), 403

    email_q  = (request.args.get("email")    or "").strip().lower()
    provider = (request.args.get("provider") or "").strip().lower()
    dia      = (request.args.get("dia")      or "").strip()
    since    = _parse_iso_dt(request.args.get("since", ""))
    until    = _parse_iso_dt(request.args.get("until", ""))

    if dia:
        d = _parse_iso_dt(dia)
        if d is not None:
            from datetime import timedelta as _td
            since = d
            until = d + _td(days=1)

    try:
        limit = max(1, min(1000, int(request.args.get("limit", 200))))
    except Exception:
        limit = 200

    db = SessionLocal()
    try:
        q = (
            db.query(IAConversa, User.email, User.nome)
            .join(User, User.id == IAConversa.user_id)
        )
        if email_q:
            q = q.filter(func.lower(User.email).like(f"%{email_q}%"))
        if provider:
            q = q.filter(IAConversa.provider == provider)
        if since is not None:
            q = q.filter(IAConversa.created_at >= since)
        if until is not None:
            q = q.filter(IAConversa.created_at < until)

        q = q.order_by(IAConversa.created_at.desc()).limit(limit)
        items = []
        for conv, uemail, unome in q.all():
            items.append({
                "id":        conv.id,
                "email":     uemail,
                "nome":      unome or "",
                "pagina":    conv.pagina or "",
                "pergunta":  conv.pergunta or "",
                "resposta":  conv.resposta or "",
                "provider":  conv.provider or "",
                "model":     conv.model or "",
                "prompt_tokens":     conv.prompt_tokens,
                "completion_tokens": conv.completion_tokens,
                "total_tokens":      conv.total_tokens,
                "cost_usd":          conv.cost_usd,
                "created_at":        conv.created_at.isoformat() if conv.created_at else None,
            })
        return jsonify({"items": items, "count": len(items)})
    finally:
        db.close()


@auth_bp.get("/admin/api/ia/conversas/<int:cid>")
def admin_ia_conversa_detalhe(cid: int):
    """Retorna uma conversa completa (pergunta/resposta sem corte)."""
    if not _require_admin():
        return jsonify({"erro": "Não autorizado"}), 403

    db = SessionLocal()
    try:
        row = (
            db.query(IAConversa, User.email, User.nome)
            .join(User, User.id == IAConversa.user_id)
            .filter(IAConversa.id == cid)
            .first()
        )
        if not row:
            return jsonify({"erro": "Não encontrado"}), 404
        conv, uemail, unome = row
        return jsonify({
            "id":        conv.id,
            "email":     uemail,
            "nome":      unome or "",
            "pagina":    conv.pagina or "",
            "pergunta":  conv.pergunta or "",
            "resposta":  conv.resposta or "",
            "provider":  conv.provider or "",
            "model":     conv.model or "",
            "prompt_tokens":     conv.prompt_tokens,
            "completion_tokens": conv.completion_tokens,
            "total_tokens":      conv.total_tokens,
            "cost_usd":          conv.cost_usd,
            "created_at":        conv.created_at.isoformat() if conv.created_at else None,
        })
    finally:
        db.close()


# ── Indicadores: Push de Cobrança ───────────────────────────────────────────

INDICADORES_PAINEL_JSON = os.environ.get(
    "INDICADORES_PAINEL_JSON",
    "/opt/relatorio_h_t/json_consolidado/indicadores_painel.json",
)


def _load_indicadores_painel() -> dict:
    """Lê o JSON pré-agregado por export_indicadores_painel.py (cron */5 min)."""
    with open(INDICADORES_PAINEL_JSON, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _calc_dias(ultimo_str: str | None, hoje: date) -> int:
    if not ultimo_str:
        return 999
    try:
        return (hoje - datetime.fromisoformat(str(ultimo_str)).date()).days
    except Exception:
        return 999


@auth_bp.get("/api/indicadores/push")
def indicadores_push():
    """Retorna último envio de push por posto, para o painel de indicadores."""
    email, user_postos = decode_user()
    if not email:
        return ("", 401)

    try:
        painel = _load_indicadores_painel()
    except FileNotFoundError:
        return jsonify({"erro": "indicadores_painel.json ainda não gerado"}), 503
    except Exception as exc:
        return jsonify({"erro": str(exc)}), 500

    push = painel.get("indicadores", {}).get("push", {}) or {}
    hoje = date.today()
    result = {}
    for posto, item in push.items():
        if user_postos and posto not in user_postos:
            continue
        ultimo = item.get("ultimo_envio")
        result[posto] = {
            "ultimo_envio": ultimo,
            "dias":         _calc_dias(ultimo, hoje),
        }
    return jsonify(result)


# ── Indicadores: Email de Cobrança ───────────────────────────────────────────

@auth_bp.get("/api/indicadores/email")
def indicadores_email():
    """Retorna último envio de email por (posto, categoria) a partir do JSON pré-agregado."""
    email_usr, user_postos = decode_user()
    if not email_usr:
        return ("", 401)

    try:
        painel = _load_indicadores_painel()
    except FileNotFoundError:
        return jsonify({"erro": "indicadores_painel.json ainda não gerado"}), 503
    except Exception as exc:
        return jsonify({"erro": str(exc)}), 500

    bloco = painel.get("indicadores", {}).get("email", {}) or {}
    raw   = bloco.get("data", {}) or {}
    sync  = bloco.get("sync", {}) or {}
    hoje  = date.today()

    dados = {}
    for chave, item in raw.items():
        posto = (item.get("posto") or "").strip().upper()
        if user_postos and posto not in user_postos:
            continue
        ultimo = item.get("ultimo_envio")
        dados[chave] = {
            "posto":        posto,
            "categoria":    item.get("categoria") or "Boleto",
            "ultimo_envio": ultimo,
            "dias":         _calc_dias(ultimo, hoje),
            "total":        item.get("total", 0),
        }
    return jsonify({"dados": dados, "sync": sync})


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
    """Último registro por posto para o painel de indicadores (lê JSON pré-agregado)."""
    email_usr, user_postos = decode_user()
    if not email_usr:
        return ("", 401)

    try:
        painel = _load_indicadores_painel()
    except FileNotFoundError:
        return jsonify({"erro": "indicadores_painel.json ainda não gerado"}), 503
    except Exception as exc:
        return jsonify({"erro": str(exc)}), 500

    bloco = painel.get("indicadores", {}).get("tef", {}) or {}
    raw   = bloco.get("data", {}) or {}
    sync  = bloco.get("sync", {}) or {}
    hoje  = date.today()

    dados = {}
    for posto, item in raw.items():
        if user_postos and posto not in user_postos:
            continue
        ultimo = item.get("ultimo_tef")
        dados[posto] = {
            "posto":      posto,
            "ultimo_tef": ultimo,
            "dias":       _calc_dias(ultimo, hoje),
            "total":      item.get("total", 0),
        }
    return jsonify({"dados": dados, "sync": sync})


@auth_bp.get("/api/tef/dashboard")
def tef_dashboard():
    email_usr, user_postos = decode_user()
    if not email_usr:
        return ("", 401)

    kpi_db = os.environ.get("KPI_DB_PATH", "/opt/relatorio_h_t/camim_kpi.db")

    # ── Parâmetros de filtro (query string) ───────────────────
    data_ini = request.args.get("data_ini", "").strip()
    data_fim = request.args.get("data_fim", "").strip()
    postos_param = request.args.get("postos", "").strip()

    hoje_str = date.today().isoformat()
    if not data_fim:
        data_fim = hoje_str
    if not data_ini:
        data_ini = data_fim          # padrão = só o dia final

    # postos selecionados no frontend (intersecção com ACL do usuário)
    if postos_param:
        sel = [p.strip().upper() for p in postos_param.split(",") if p.strip()]
        if user_postos:
            sel = [p for p in sel if p in user_postos]
        effective_postos = sel or (list(user_postos) if user_postos else [])
    else:
        effective_postos = list(user_postos) if user_postos else []

    try:
        _tef_ensure_table(kpi_db)
        conn = sqlite3.connect(f"file:{kpi_db}?mode=ro", uri=True)

        posto_filter = ""
        posto_params = []
        if effective_postos:
            ph = ",".join("?" * len(effective_postos))
            posto_filter = f"AND posto IN ({ph})"
            posto_params = list(effective_postos)

        # KPIs do período
        hoje_row = conn.execute(f"""
            SELECT COUNT(*),
                   SUM(CASE WHEN aprovado=1 THEN 1 ELSE 0 END),
                   SUM(CASE WHEN aprovado=0 THEN 1 ELSE 0 END),
                   SUM(CASE WHEN aprovado=1 THEN valor ELSE 0 END)
            FROM ind_tef
            WHERE date(datahora) BETWEEN ? AND ? {posto_filter}
        """, [data_ini, data_fim] + posto_params).fetchone()

        total_hoje    = hoje_row[0] or 0
        aprov_hoje    = hoje_row[1] or 0
        negad_hoje    = hoje_row[2] or 0
        valor_hoje    = hoje_row[3] or 0.0

        # Tendência por dia no período
        trend7 = conn.execute(f"""
            SELECT date(datahora) AS dia,
                   SUM(CASE WHEN aprovado=1 THEN 1 ELSE 0 END) AS aprovadas,
                   SUM(CASE WHEN aprovado=0 THEN 1 ELSE 0 END) AS negadas
            FROM ind_tef
            WHERE date(datahora) BETWEEN ? AND ? {posto_filter}
            GROUP BY dia ORDER BY dia
        """, [data_ini, data_fim] + posto_params).fetchall()

        # Top erros de negação no período
        top_erros = conn.execute(f"""
            SELECT COALESCE(NULLIF(TRIM(erro),''), '(sem descrição)') AS motivo,
                   COUNT(*) AS cnt
            FROM ind_tef
            WHERE aprovado = 0
              AND date(datahora) BETWEEN ? AND ? {posto_filter}
            GROUP BY motivo ORDER BY cnt DESC LIMIT 10
        """, [data_ini, data_fim] + posto_params).fetchall()

        # Por posto no período
        por_posto = conn.execute(f"""
            SELECT posto,
                   COUNT(*) AS total,
                   SUM(CASE WHEN aprovado=1 THEN 1 ELSE 0 END) AS aprovadas,
                   SUM(CASE WHEN aprovado=0 THEN 1 ELSE 0 END) AS negadas,
                   SUM(CASE WHEN aprovado=1 THEN valor ELSE 0 END) AS valor_aprov,
                   MAX(datahora) AS ultimo
            FROM ind_tef
            WHERE date(datahora) BETWEEN ? AND ? {posto_filter}
            GROUP BY posto ORDER BY posto
        """, [data_ini, data_fim] + posto_params).fetchall()

        # Por posto — total geral (todos os dados)
        por_posto_total = conn.execute(f"""
            SELECT posto, COUNT(*), SUM(CASE WHEN aprovado=1 THEN 1 ELSE 0 END)
            FROM ind_tef
            WHERE 1=1 {posto_filter}
            GROUP BY posto ORDER BY posto
        """, posto_params).fetchall()
        posto_total_map = {r[0]: {"total": r[1], "aprovadas": r[2]} for r in por_posto_total}

        # Registros negados (detalhados) no período
        negados = conn.execute(f"""
            SELECT posto, datahora, matricula, erro, valor, resposta_cielo
            FROM ind_tef
            WHERE aprovado = 0
              AND date(datahora) BETWEEN ? AND ? {posto_filter}
            ORDER BY erro, posto, datahora
        """, [data_ini, data_fim] + posto_params).fetchall()

        # Sync
        sync_row = conn.execute("""
            SELECT synced_at, status, mensagem FROM ind_sync_log
            WHERE indicador='tef' ORDER BY id DESC LIMIT 1
        """).fetchone()

        conn.close()

        return jsonify({
            "data_ini": data_ini,
            "data_fim": data_fim,
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
            "negados": [
                {"posto": r[0], "datahora": r[1], "matricula": r[2],
                 "erro": r[3] or "(sem descrição)", "valor": round(r[4] or 0, 2),
                 "resposta": r[5] or ""}
                for r in negados
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


# ── Auditoria Financeira ────────────────────────────────────────────────────

AUDITORIA_JSON = os.environ.get(
    "AUDITORIA_FINANCEIRA_JSON",
    "/opt/relatorio_h_t/json_consolidado/auditoria_financeira.json",
)


def _load_auditoria() -> dict:
    with open(AUDITORIA_JSON, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _filtra_por_postos(items, user_postos, key="posto"):
    if not user_postos:
        return items
    return [i for i in items if i.get(key) in user_postos]


@auth_bp.get("/api/auditoria/painel")
def auditoria_painel():
    """JSON pré-agregado por export_auditoria_financeira.py (cron 03:00).

    Filtra anomalias e scores pelos postos do usuário; Benford é entregue
    inteiro (a UI decide qual aba/posto exibir).
    """
    email_usr, user_postos = decode_user()
    if not email_usr:
        return ("", 401)

    try:
        data = _load_auditoria()
    except FileNotFoundError:
        return jsonify({"erro": "auditoria_financeira.json ainda não gerado"}), 503
    except Exception as exc:
        return jsonify({"erro": str(exc)}), 500

    anomalias = _filtra_por_postos(data.get("anomalias", []), user_postos)
    scores = data.get("scores_postos", {}) or {}
    if user_postos:
        scores = {p: v for p, v in scores.items() if p in user_postos}

    benford = data.get("benford", {}) or {}
    if user_postos:
        per = benford.get("por_posto") or {}
        benford = {
            **benford,
            "por_posto": {p: v for p, v in per.items() if p in user_postos},
        }

    return jsonify({
        "generated_at":  data.get("generated_at"),
        "janela_meses":  data.get("janela_meses"),
        "benford":       benford,
        "anomalias":     anomalias,
        "scores_postos": scores,
        "tipos_label":   data.get("tipos_label", {}),
        "totais": {
            "anomalias":   len(anomalias),
            "abertas":     sum(1 for a in anomalias if not a.get("verificado")),
            "verificadas": sum(1 for a in anomalias if a.get("verificado")),
        },
    })


@auth_bp.post("/api/auditoria/verificar")
def auditoria_verificar():
    """Marca uma anomalia como verificada. Body JSON:
       {"chave": "<sha1>", "observacao": "..."}
    """
    email_usr, user_postos = decode_user()
    if not email_usr:
        return ("", 401)

    payload = request.get_json(silent=True) or {}
    chave = (payload.get("chave") or "").strip()
    if not chave:
        return jsonify({"erro": "chave obrigatória"}), 400

    # busca o item no JSON pra extrair posto/id_conta_tipo/mes_ref/regra_id
    try:
        data = _load_auditoria()
    except Exception as exc:
        return jsonify({"erro": str(exc)}), 503

    item = next((a for a in data.get("anomalias", []) if a.get("chave") == chave), None)
    if not item:
        return jsonify({"erro": "anomalia não encontrada"}), 404
    if user_postos and item["posto"] not in user_postos:
        return jsonify({"erro": "fora do escopo do usuário"}), 403

    db = SessionLocal()
    try:
        existing = db.query(AnomaliaVerificacao).filter_by(chave_anomalia=chave).first()
        if existing:
            existing.verificado_por = email_usr
            existing.verificado_em  = datetime.now()
            existing.observacao     = (payload.get("observacao") or "")[:1000]
        else:
            db.add(AnomaliaVerificacao(
                chave_anomalia=chave,
                posto=item["posto"],
                id_conta_tipo=item.get("id_conta_tipo"),
                mes_ref=item.get("mes_ref"),
                regra_id=item.get("regra_id"),
                verificado_por=email_usr,
                verificado_em=datetime.now(),
                observacao=(payload.get("observacao") or "")[:1000],
            ))
        db.commit()
        return jsonify({"ok": True})
    except Exception as exc:
        db.rollback()
        return jsonify({"erro": str(exc)}), 500
    finally:
        db.close()


@auth_bp.get("/api/auditoria/regras")
def auditoria_regras_list():
    if not _require_admin():
        return ("", 403)
    db = SessionLocal()
    try:
        rows = db.query(RegraAnomalia).order_by(RegraAnomalia.id).all()
        return jsonify({"regras": [{
            "id": r.id, "nome": r.nome, "tipo": r.tipo,
            "parametros": json.loads(r.parametros_json or "{}"),
            "escopo_postos": r.escopo_postos, "escopo_tipos": r.escopo_tipos,
            "ativa": bool(r.ativa),
            "criado_por": r.criado_por,
            "criado_em":  r.criado_em.isoformat() if r.criado_em else None,
            "observacao": r.observacao or "",
        } for r in rows]})
    finally:
        db.close()


@auth_bp.post("/api/auditoria/regras")
def auditoria_regras_save():
    """Cria nova regra ou edita uma existente (body com id opcional)."""
    adm = _require_admin()
    if not adm:
        return ("", 403)
    payload = request.get_json(silent=True) or {}

    db = SessionLocal()
    try:
        rid = payload.get("id")
        if rid:
            r = db.query(RegraAnomalia).filter_by(id=rid).first()
            if not r:
                return jsonify({"erro": "regra não encontrada"}), 404
        else:
            r = RegraAnomalia(criado_por=adm.email)
            db.add(r)

        r.nome           = (payload.get("nome") or "").strip()[:120]
        r.tipo           = (payload.get("tipo") or "").strip()[:40]
        r.parametros_json = json.dumps(payload.get("parametros") or {})
        r.escopo_postos  = (payload.get("escopo_postos") or "*").strip()[:120]
        r.escopo_tipos   = (payload.get("escopo_tipos")  or "*").strip()[:255]
        r.ativa          = bool(payload.get("ativa", True))
        r.observacao     = (payload.get("observacao") or "")[:1000]
        db.commit()
        return jsonify({"ok": True, "id": r.id})
    except Exception as exc:
        db.rollback()
        return jsonify({"erro": str(exc)}), 500
    finally:
        db.close()


@auth_bp.post("/api/auditoria/regras/<int:rid>/toggle")
def auditoria_regras_toggle(rid: int):
    if not _require_admin():
        return ("", 403)
    db = SessionLocal()
    try:
        r = db.query(RegraAnomalia).filter_by(id=rid).first()
        if not r:
            return jsonify({"erro": "regra não encontrada"}), 404
        r.ativa = not r.ativa
        db.commit()
        return jsonify({"ok": True, "ativa": bool(r.ativa)})
    finally:
        db.close()


@auth_bp.post("/api/auditoria/regras/<int:rid>/delete")
def auditoria_regras_delete(rid: int):
    if not _require_admin():
        return ("", 403)
    db = SessionLocal()
    try:
        r = db.query(RegraAnomalia).filter_by(id=rid).first()
        if not r:
            return jsonify({"erro": "regra não encontrada"}), 404
        db.delete(r)
        db.commit()
        return jsonify({"ok": True})
    finally:
        db.close()
