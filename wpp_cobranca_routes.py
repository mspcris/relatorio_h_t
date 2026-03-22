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
import requests
from flask import Blueprint, request, jsonify, render_template, redirect, url_for

# Importa db do diretório de ETL
sys.path.insert(0, '/opt/relatorio_h_t')
import wpp_cobranca_db as db
import wpp_cobranca_sql as sql_helper

from dotenv import load_dotenv
load_dotenv('/opt/relatorio_h_t/.env')

WPP_API_URL = os.getenv("WAPP_API_URL",  "https://whatsapp-api.camim.com.br")
WPP_TOKEN   = os.getenv("WAPP_TOKEN",    "")
POSTOS_ALL  = list("ANXYBRPCDGIMJ")

wpp_bp = Blueprint("wpp", __name__, url_prefix="/wpp")

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
# API helpers (JSON)
# ---------------------------------------------------------------------------

@wpp_bp.get("/api/opcoes")
def api_opcoes():
    """Retorna valores distintos de um campo da view para os postos informados."""
    email, _ = _check_auth()
    if not email:
        return jsonify({"error": "unauthorized"}), 401
    campo  = request.args.get("campo", "")
    postos = [p.strip() for p in request.args.get("postos", "").split(",") if p.strip()]
    if campo not in sql_helper.CAMPO_SQL:
        return jsonify({"error": "campo inválido"}), 400
    if not postos:
        return jsonify({"opcoes": []})
    try:
        opcoes = sql_helper.buscar_opcoes(postos, campo)
        return jsonify({"opcoes": opcoes})
    except Exception as e:
        return jsonify({"opcoes": [], "erro": str(e)[:200]})


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
    # Retorna apenas name + preview text (primeiro BODY)
    result = []
    for t in templates:
        body_text = ""
        for comp in t.get("components", []):
            if comp.get("type") == "BODY":
                body_text = comp.get("text", "")
                break
        params = []
        for comp in t.get("components", []):
            if comp.get("type") == "BODY":
                for p in comp.get("example", {}).get("body_text_named_params", []):
                    params.append(p.get("param_name"))
        result.append({
            "name": t["name"],
            "status": t.get("status", ""),
            "language": t.get("language", ""),
            "preview": body_text[:120],
            "params": params,
        })
    return jsonify({"templates": result})


# ---------------------------------------------------------------------------
# Lista de campanhas
# ---------------------------------------------------------------------------

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
    email, _ = _check_auth()
    if not email:
        return ('', 401)
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
    email, _ = _check_auth()
    if not email:
        return ('', 401)
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
    email, _ = _check_auth()
    if not email:
        return ('', 401)
    campanha = db.get_campanha(cid)
    novo_estado = db.toggle_campanha(cid)
    acao = "ATIVAR" if novo_estado else "DESATIVAR"
    db.registrar_auditoria(email, acao, cid,
                            campanha["nome"] if campanha else None, None)
    return jsonify({"ativa": novo_estado})


@wpp_bp.post("/<int:cid>/excluir")
def excluir(cid):
    email, _ = _check_auth()
    if not email:
        return ('', 401)
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

    page = int(request.args.get("page", 1))
    limit = 100
    offset = (page - 1) * limit

    lista_envios = db.listar_envios(cid, limit=limit, offset=offset)
    lista_nao    = db.listar_nao_enviados(cid, limit=200)
    resumo       = db.resumo_campanha(cid)

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
    )


# ---------------------------------------------------------------------------
# Detalhe de um envio
# ---------------------------------------------------------------------------

@wpp_bp.get("/envio/<int:eid>")
def detalhe_envio(eid):
    email, is_admin = _check_auth()
    if not email:
        return ('', 401)
    envio = db.get_envio(eid)
    if not envio:
        return ("Envio não encontrado", 404)
    campanha = db.get_campanha(envio["campanha_id"])
    return render_template(
        "wpp_envio_detalhe.html",
        USER_EMAIL=email, USER_IS_ADMIN=is_admin,
        envio=envio,
        campanha=campanha,
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
# Helper: form → dict
# ---------------------------------------------------------------------------

def _form_to_dict(form) -> dict:
    """Converte o MultiDict do form HTML para o dict esperado pelos helpers do DB."""
    postos = form.getlist("postos")  # checkboxes múltiplos
    return {
        "nome":               form.get("nome", "").strip(),
        "template":           form.get("template", "notificacao_de_fatura"),
        "postos":             postos,
        "dias_atraso_min":    _int(form.get("dias_atraso_min"), 1),
        "dias_atraso_max":    _int(form.get("dias_atraso_max"), None),
        "incluir_cancelados": form.get("incluir_cancelados") == "1",
        "sem_email":          form.get("sem_email") == "1",
        "sexo":               form.get("sexo") or None,
        "idade_min":          _int(form.get("idade_min"), None),
        "idade_max":          _int(form.get("idade_max"), None),
        "nao_recorrente":     form.get("nao_recorrente") == "1",
        "operadora":          form.get("operadora") or None,
        "cobrador":           form.get("cobrador") or None,
        "corretor":           form.get("corretor") or None,
        "bairro":             form.get("bairro") or None,
        "rua":                form.get("rua") or None,
        "hora_inicio":        form.get("hora_inicio", "08:00"),
        "hora_fim":           form.get("hora_fim", "20:00"),
        "dias_semana":        form.get("dias_semana", "0,1,2,3,4"),
        "intervalo_dias":     _int(form.get("intervalo_dias"), 7),
        "ativa":              form.get("ativa", "1") == "1",
    }


def _int(val, default):
    try:
        v = int(val)
        return v if v >= 0 else default
    except (TypeError, ValueError):
        return default
