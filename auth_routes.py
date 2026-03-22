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
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import requests as http_requests
from flask import Blueprint, jsonify, make_response, render_template, request
from itsdangerous import BadSignature, TimestampSigner
from sqlalchemy import func

from auth_db import (
    SessionLocal, User, UserPosto, LoginHistory, IAConversa,
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

REGRAS OBRIGATÓRIAS:
1. Use APENAS os dados fornecidos no contexto. NUNCA invente, estime ou use conhecimento externo para gerar valores monetários.
2. Se a pergunta mencionar um posto específico (ex: "em X", "no posto A"), use EXCLUSIVAMENTE os dados daquele posto no contexto.
3. Se os dados de um posto não estiverem disponíveis no contexto, informe isso claramente.
4. Responda em português brasileiro com os valores exatos do contexto.

FORMATAÇÃO DA RESPOSTA:
- Use ## para seções principais (ex: ## Receita, ## Despesas, ## Resultado)
- Use ### para subseções
- Use "- item" para listas de itens
- Sempre inclua os valores monetários exatos (R$ X.XXX,XX) e variações percentuais
- Termine com um parágrafo de resumo/conclusão
- NÃO use tabelas markdown, NÃO use negrito (**), NÃO use itálico
"""

IA_GROQ_URL = os.environ.get("IA_GROQ_URL", "http://127.0.0.1:8030/ia/analisar")

auth_bp = Blueprint("auth_bp", __name__)

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

    # ── Monta contexto: builder pandas (novo) ou contexto enviado pelo browser (legado) ──
    kpi = d.get("kpi", "")
    if kpi:
        try:
            from ia_context_builder import build_context
            postos      = d.get("postos") or []
            periodo_ini = str(d.get("periodo_ini", ""))[:7]
            periodo_fim = str(d.get("periodo_fim", ""))[:7]
            contexto = build_context(kpi, postos, periodo_ini, periodo_fim, pergunta_txt)
        except Exception as exc:
            contexto = f"[Erro no context builder: {exc}]"
    else:
        contexto = str(d.get("contexto", ""))[:100000]

    resposta_json = None
    resposta_txt  = ""

    # ── Groq (SDK direto, igual OpenAI/Anthropic) ────────────────────────────
    if provider == "groq":
        try:
            llm = _get_groq()
            prompt_completo = f"Contexto do relatório:\n\n{contexto}\n\nPergunta do usuário:\n\n{pergunta_txt}"
            resposta_txt  = llm.gerar_texto(prompt=prompt_completo, system_prompt=_IA_SYSTEM_PROMPT)
            resposta_json = {"resposta": resposta_txt, "provider": "groq"}
        except Exception as exc:
            return jsonify({"erro": f"Groq indisponível: {exc}"}), 502

    # ── OpenAI ───────────────────────────────────────────────────────────────
    elif provider == "openai":
        try:
            llm = _get_openai()
            prompt_completo = f"Contexto do relatório:\n\n{contexto}\n\nPergunta do usuário:\n\n{pergunta_txt}"
            resposta_txt  = llm.gerar_texto(prompt=prompt_completo, system_prompt=_IA_SYSTEM_PROMPT)
            resposta_json = {"resposta": resposta_txt, "provider": "openai"}
        except Exception as exc:
            return jsonify({"erro": f"OpenAI indisponível: {exc}"}), 502

    # ── Anthropic ────────────────────────────────────────────────────────────
    elif provider == "anthropic":
        try:
            llm = _get_anthropic()
            prompt_completo = f"Contexto do relatório:\n\n{contexto}\n\nPergunta do usuário:\n\n{pergunta_txt}"
            resposta_txt  = llm.gerar_texto(prompt=prompt_completo, system_prompt=_IA_SYSTEM_PROMPT)
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
