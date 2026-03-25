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


@wpp_bp.get("/api/indicadores")
def api_indicadores():
    """Retorna último envio accepted por campanha ativa e posto (para painel de indicadores)."""
    email, _ = _check_auth()
    if not email:
        return jsonify({"error": "unauthorized"}), 401
    try:
        dados = db.indicadores_wpp()
        return jsonify({"campanhas": dados})
    except Exception as e:
        return jsonify({"erro": str(e)[:200]}), 500


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
    """Envia mensagem de teste para um número, com variáveis preenchidas manualmente."""
    import uuid as _uuid
    import re as _re
    from datetime import datetime as _dt

    email, is_admin = _check_auth()
    if not email:
        return jsonify({"error": "unauthorized"}), 401

    data         = request.get_json(force=True) or {}
    telefone_raw = (data.get("telefone") or "").strip()
    template_name = (data.get("template") or "").strip()
    params       = data.get("params") or {}
    telefone     = _re.sub(r"\D+", "", telefone_raw)

    if not telefone or not template_name:
        return jsonify({"error": "telefone e template são obrigatórios"}), 400
    if len(telefone) < 12 or len(telefone) > 15:
        return jsonify({"error": "telefone inválido (use DDI+DDD+número, ex.: 5521999999999)"}), 400

    # Busca o body do template na API
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

    # Substitui variáveis
    texto = body
    for key, val in params.items():
        texto = texto.replace(f"{{{{{key}}}}}", str(val))

    CHAT_API_URL  = os.getenv("CHAT_API_URL",   "")
    CHAT_FROM     = os.getenv("WAPP_CHAT_FROM", "")
    CHAT_QUEUE_ID = os.getenv("WAPP_QUEUE_ID",  "")

    if not CHAT_API_URL:
        return jsonify({"error": "CHAT_API_URL não configurado no .env"}), 500
    if not CHAT_FROM:
        return jsonify({"error": "WAPP_CHAT_FROM não configurado no .env"}), 500
    if not CHAT_QUEUE_ID:
        return jsonify({"error": "WAPP_QUEUE_ID não configurado no .env"}), 500

    hash_id = _uuid.uuid4().hex[:24]
    ts      = _dt.now().astimezone().isoformat(timespec="seconds")

    payload = {
        "entry": [{
            "id": hash_id,
            "changes": [{
                "field": "messages",
                "value": {
                    "contacts": [{"wa_id": telefone, "profile": {"name": "Teste"}}],
                    "messages": [{
                        "id":        hash_id,
                        "from":      CHAT_FROM,
                        "queue_id":  CHAT_QUEUE_ID,
                        "text":      {"body": texto},
                        "type":      "text",
                        "timestamp": ts,
                    }],
                    "metadata":          {"phone_number_id": "", "display_phone_number": ""},
                    "messaging_product": "whatsapp",
                },
            }],
        }],
        "object": "whatsapp_business_account",
    }

    try:
        r = requests.post(f"{CHAT_API_URL}/webhooks/whatsapp", json=payload, timeout=15)
        r.raise_for_status()
        body_json = None
        body_text = ""
        try:
            body_json = r.json()
        except Exception:
            body_text = (r.text or "")[:800]
        return jsonify({
            "status": "accepted",
            "texto": texto,
            "telefone": telefone,
            "hash_id": hash_id,
            "gateway_http": r.status_code,
            "gateway_body": body_json if body_json is not None else body_text,
        })
    except requests.HTTPError as e:
        body_text = ""
        try:
            body_text = (e.response.text or "")[:800]
        except Exception:
            body_text = ""
        return jsonify({
            "status": f"erro:HTTP {e.response.status_code}",
            "texto": texto,
            "telefone": telefone,
            "hash_id": hash_id,
            "gateway_body": body_text,
        }), 502
    except Exception as e:
        return jsonify({"status": f"erro:{str(e)[:120]}", "texto": texto}), 500


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
        "situacao_cliente":   form.get("situacao_cliente") or None,
        "tipo_fj":            form.get("tipo_fj") or None,
        "clube_beneficio":    form.get("clube_beneficio") == "1",
        "clube_beneficio_joy": form.get("clube_beneficio_joy") == "1",
        "plano_premium":      form.get("plano_premium") == "1",
        "origem":             form.get("origem") or None,
        "pagador_atrasado":   form.get("pagador_atrasado") == "1",
    }


def _int(val, default):
    try:
        v = int(val)
        return v if v >= 0 else default
    except (TypeError, ValueError):
        return default
