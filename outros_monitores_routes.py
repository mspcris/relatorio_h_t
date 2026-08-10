"""
outros_monitores_routes.py — Blueprint dos monitores de serviços específicos
(página outros_monitores.html). Primeiro monitor: Leads criados.

Rotas:
  GET  /api/monitores/leads      — JSON do ETL export_monitor_leads.py
  GET  /api/monitores/inscricao  — ?monitor=leads → o usuário logado recebe e-mail?
  POST /api/monitores/inscricao  — {monitor, ativo} liga/desliga a notificação

A inscrição fica em monitor_notificacoes no camim_auth.db (chave
email+monitor_key). Quem CONSOME é o ETL horário, que manda o e-mail.
Conexões sqlite fechadas explicitamente — Py3.14 não fecha no GC
(incidente 2026-08-10, ver CLAUDE.md).
"""

import os
import sqlite3
from datetime import datetime, timezone

from flask import Blueprint, request, jsonify, Response

AUTH_DB = os.getenv("AUTH_DB_PATH", "/opt/relatorio_h_t/camim_auth.db")
MONITORES_VALIDOS = {"leads"}

monitores_bp = Blueprint("monitores", __name__, url_prefix="/api/monitores")


def _email_logado() -> str | None:
    try:
        from auth_routes import decode_user
        email, _postos = decode_user()
        return email or None
    except Exception:
        return None


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(AUTH_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS monitor_notificacoes (
            email          TEXT NOT NULL,
            monitor_key    TEXT NOT NULL,
            ativo          INTEGER NOT NULL DEFAULT 1,
            atualizado_em  TEXT NOT NULL,
            PRIMARY KEY (email, monitor_key)
        )""")
    return conn


@monitores_bp.get("/leads")
def dados_leads():
    if not _email_logado():
        return jsonify({"error": "unauthorized"}), 401
    candidatos = [
        "/opt/relatorio_h_t/json_consolidado/monitor_leads.json",
        os.path.join(os.path.dirname(os.path.abspath(__file__)),
                     "json_consolidado", "monitor_leads.json"),
    ]
    for path in candidatos:
        if os.path.isfile(path):
            with open(path, encoding="utf-8") as f:
                return Response(f.read(), mimetype="application/json")
    return jsonify({"error": "monitor_leads.json ainda não gerado — "
                             "aguarde a primeira passada do cron (a cada hora, aos 10 min)"}), 404


@monitores_bp.get("/inscricao")
def inscricao_get():
    email = _email_logado()
    if not email:
        return jsonify({"error": "unauthorized"}), 401
    monitor = (request.args.get("monitor") or "").strip()
    if monitor not in MONITORES_VALIDOS:
        return jsonify({"error": "monitor inválido"}), 400
    conn = _conn()
    try:
        row = conn.execute(
            "SELECT ativo FROM monitor_notificacoes WHERE email=? AND monitor_key=?",
            (email, monitor)).fetchone()
        return jsonify({"monitor": monitor, "inscrito": bool(row and row[0])})
    finally:
        conn.close()


@monitores_bp.post("/inscricao")
def inscricao_post():
    email = _email_logado()
    if not email:
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json(silent=True) or {}
    monitor = str(data.get("monitor") or "").strip()
    ativo = 1 if data.get("ativo") else 0
    if monitor not in MONITORES_VALIDOS:
        return jsonify({"error": "monitor inválido"}), 400
    now = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")
    conn = _conn()
    try:
        conn.execute(
            """INSERT INTO monitor_notificacoes (email, monitor_key, ativo, atualizado_em)
               VALUES (?,?,?,?)
               ON CONFLICT(email, monitor_key) DO UPDATE SET
                 ativo=excluded.ativo, atualizado_em=excluded.atualizado_em""",
            (email, monitor, ativo, now))
        conn.commit()
        return jsonify({"ok": True, "monitor": monitor, "inscrito": bool(ativo)})
    finally:
        conn.close()
