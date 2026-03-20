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

from flask import Blueprint, jsonify, make_response, render_template, request
from itsdangerous import BadSignature, TimestampSigner

from auth_db import (
    SessionLocal, User, UserPosto,
    get_user_by_email, get_user_by_id, init_db,
)

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
    msg["Subject"] = "CAMIM — Redefinição de senha"
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
        port = int(os.environ.get("EMAIL_PORT", 465))
        user = os.environ.get("EMAIL_HOST_USER", "")
        pwd  = os.environ.get("EMAIL_HOST_PASSWORD", "")

        with smtplib.SMTP_SSL(host, port) as s:
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
    return jsonify({"email": email, "postos": postos})


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
        return ("", 403)
    db = SessionLocal()
    try:
        users = db.query(User).order_by(User.email).all()
        return jsonify([{
            "id":       u.id,
            "email":    u.email,
            "nome":     u.nome,
            "is_admin": u.is_admin,
            "ativo":    u.ativo,
            "postos":   u.lista_postos(),
        } for u in users])
    finally:
        db.close()


@auth_bp.post("/admin/api/usuarios")
def admin_criar():
    if not _require_admin():
        return ("", 403)
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
        return ("", 403)
    d  = request.get_json(silent=True) or {}
    db = SessionLocal()
    try:
        user = get_user_by_id(db, uid)
        if not user:
            return ("", 404)
        if "nome" in d:
            user.nome = d["nome"].strip()
        if "is_admin" in d:
            user.is_admin = bool(d["is_admin"])
        if "ativo" in d:
            user.ativo = bool(d["ativo"])
        if d.get("senha"):
            user.set_senha(d["senha"])
        if "postos" in d:
            for p in list(user.postos):
                db.delete(p)
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
        return ("", 403)
    db = SessionLocal()
    try:
        user = get_user_by_id(db, uid)
        if not user:
            return ("", 404)
        if user.email == adm.email:
            return jsonify({"erro": "Não é possível excluir o próprio usuário"}), 400
        db.delete(user)
        db.commit()
        return jsonify({"ok": True})
    finally:
        db.close()


@auth_bp.post("/admin/api/usuarios/<int:uid>/reset")
def admin_reset_link(uid: int):
    if not _require_admin():
        return ("", 403)
    db = SessionLocal()
    try:
        user = get_user_by_id(db, uid)
        if not user:
            return ("", 404)
        token = user.gerar_reset_token()
        db.commit()
        ok = _enviar_reset(user.email, token)
        return jsonify({"ok": ok})
    finally:
        db.close()
