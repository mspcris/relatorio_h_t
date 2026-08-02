"""
custos_ti_routes.py — Blueprint Flask do módulo "Custos de TI" (admin-only).

As páginas (/custos_ti, /custos_ti/<key>, /custos_ti_cadastros) são servidas por
app.py via render_protected_page com page_key "custos_ti", PROPOSITALMENTE fora
do catálogo public.servicos — mesmo truque do acesso_avancado e do custos_ia:
só quem tem all_pages=True entra, e nenhum admin consegue liberar avulso.
Estes endpoints repetem a checagem por baixo (defesa em profundidade).

Endpoints:
  GET    /api/custos-ti/home?de=YYYY-MM&ate=YYYY-MM      → consolidado do período
  GET    /api/custos-ti/centro/<key>?de=&ate=            → página de um centro
  GET    /api/custos-ti/cadastros                        → centros + formas + contas
  POST   /api/custos-ti/centros        DELETE .../<id>   → CRUD de centro de custo
  POST   /api/custos-ti/formas         DELETE .../<id>   → CRUD de forma de pagamento
  POST   /api/custos-ti/contas         DELETE .../<id>   → CRUD de conta
  POST   /api/custos-ti/lancamentos    DELETE .../<id>   → CRUD de lançamento
  GET/POST /api/custos-ti/cotacao                        → USD→BRL do mês
  POST   /api/custos-ti/meta/importar                    → colar extrato da Meta
  GET    /api/custos-ti/meta/status                      → a API da Meta está ligada?
  POST   /api/custos-ti/meta/sync                        → puxa conversation_analytics

Nenhum endpoint aqui envia mensagem, cobra ou escreve no SQL Server da CAMIM.
Os únicos writes são no Postgres RDS (tabelas ti_*).
"""
from __future__ import annotations

import logging

from flask import Blueprint, g, jsonify, request

import custos_ti
import custos_ti_db as tidb

log = logging.getLogger(__name__)

custos_ti_bp = Blueprint("custos_ti_api", __name__)

_MAX_TEXTO = 400_000   # ~400 KB de texto colado; extrato real tem uns 3 KB


def _require_admin():
    """Email do usuário logado se ele tem all_pages e está ativo; senão None."""
    from auth_routes import decode_user
    from auth_db import SessionLocal, get_user_by_email as _gue
    email, _ = decode_user()
    if not email:
        return None
    db = SessionLocal()
    try:
        u = _gue(db, email)
        if not u or not getattr(u, "ativo", True):
            return None
        if not bool(getattr(u, "all_pages", False)):
            return None
        return email
    finally:
        db.close()


def _deny():
    return jsonify({"ok": False, "error": "acesso restrito (somente administradores)"}), 403


def _sess():
    """Sessão por request, fechada no teardown do blueprint."""
    if "ti_sess" not in g:
        g.ti_sess = tidb.TiSession()
    return g.ti_sess


@custos_ti_bp.teardown_app_request
def _close_sess(exc):
    sess = g.pop("ti_sess", None)
    if sess is not None:
        if exc is not None:
            sess.rollback()
        sess.close()


def _erro(e: Exception, contexto: str, code: int = 400):
    """ValueError = erro de validação (mostrável); o resto vira 500 com log."""
    if isinstance(e, ValueError):
        return jsonify({"ok": False, "error": str(e)}), code
    log.exception("custos-ti %s", contexto)
    return jsonify({"ok": False, "error": f"falha interna: {e}"}), 500


def _body() -> dict:
    return request.get_json(silent=True) or {}


# ─────────────────────────────────────────────────────────────────────────────
# Leitura
# ─────────────────────────────────────────────────────────────────────────────
@custos_ti_bp.get("/api/custos-ti/home")
def api_home():
    if not _require_admin():
        return _deny()
    try:
        dados = custos_ti.home_payload(_sess(), request.args.get("de"),
                                       request.args.get("ate"))
        return jsonify({"ok": True, "data": dados})
    except Exception as e:  # noqa: BLE001
        return _erro(e, "home", 500)


@custos_ti_bp.get("/api/custos-ti/centro/<key>")
def api_centro(key):
    if not _require_admin():
        return _deny()
    try:
        dados = custos_ti.centro_payload(_sess(), key, request.args.get("de"),
                                         request.args.get("ate"))
        return jsonify({"ok": True, "data": dados})
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 404
    except Exception as e:  # noqa: BLE001
        return _erro(e, f"centro {key}", 500)


@custos_ti_bp.get("/api/custos-ti/cadastros")
def api_cadastros():
    if not _require_admin():
        return _deny()
    sess = _sess()
    try:
        return jsonify({"ok": True, "data": {
            "centros": [c.to_dict() for c in custos_ti.listar_centros(sess, True)],
            "formas": [f.to_dict() for f in custos_ti.listar_formas(sess)],
            "contas": [c.to_dict() for c in custos_ti.listar_contas(sess)],
            "tipos_pagamento": list(tidb.TIPOS_PAGAMENTO),
            "recorrencias": list(tidb.RECORRENCIAS),
            "moedas": list(tidb.MOEDAS),
            "cotacao_mes": custos_ti.get_cotacao(sess, custos_ti.mes_atual()),
        }})
    except Exception as e:  # noqa: BLE001
        return _erro(e, "cadastros", 500)


# ─────────────────────────────────────────────────────────────────────────────
# Centros de custo — criar um centro cria a página e o item de menu
# ─────────────────────────────────────────────────────────────────────────────
@custos_ti_bp.post("/api/custos-ti/centros")
def api_centro_salvar():
    if not _require_admin():
        return _deny()
    try:
        return jsonify({"ok": True, "centro": custos_ti.salvar_centro(_sess(), _body())})
    except Exception as e:  # noqa: BLE001
        _sess().rollback()
        return _erro(e, "salvar centro")


@custos_ti_bp.delete("/api/custos-ti/centros/<int:centro_id>")
def api_centro_excluir(centro_id):
    if not _require_admin():
        return _deny()
    try:
        return jsonify({"ok": True, **custos_ti.excluir_centro(_sess(), centro_id)})
    except Exception as e:  # noqa: BLE001
        _sess().rollback()
        return _erro(e, "excluir centro")


# ─────────────────────────────────────────────────────────────────────────────
# Formas de pagamento
# ─────────────────────────────────────────────────────────────────────────────
@custos_ti_bp.post("/api/custos-ti/formas")
def api_forma_salvar():
    if not _require_admin():
        return _deny()
    try:
        return jsonify({"ok": True, "forma": custos_ti.salvar_forma(_sess(), _body())})
    except Exception as e:  # noqa: BLE001
        _sess().rollback()
        return _erro(e, "salvar forma")


@custos_ti_bp.delete("/api/custos-ti/formas/<int:forma_id>")
def api_forma_excluir(forma_id):
    if not _require_admin():
        return _deny()
    try:
        return jsonify({"ok": True, **custos_ti.excluir_forma(_sess(), forma_id)})
    except Exception as e:  # noqa: BLE001
        _sess().rollback()
        return _erro(e, "excluir forma")


# ─────────────────────────────────────────────────────────────────────────────
# Contas
# ─────────────────────────────────────────────────────────────────────────────
@custos_ti_bp.post("/api/custos-ti/contas")
def api_conta_salvar():
    if not _require_admin():
        return _deny()
    try:
        return jsonify({"ok": True, "conta": custos_ti.salvar_conta(_sess(), _body())})
    except Exception as e:  # noqa: BLE001
        _sess().rollback()
        return _erro(e, "salvar conta")


@custos_ti_bp.delete("/api/custos-ti/contas/<int:conta_id>")
def api_conta_excluir(conta_id):
    if not _require_admin():
        return _deny()
    try:
        return jsonify({"ok": True, **custos_ti.excluir_conta(_sess(), conta_id)})
    except Exception as e:  # noqa: BLE001
        _sess().rollback()
        return _erro(e, "excluir conta")


# ─────────────────────────────────────────────────────────────────────────────
# Lançamentos
# ─────────────────────────────────────────────────────────────────────────────
@custos_ti_bp.post("/api/custos-ti/lancamentos")
def api_lanc_salvar():
    email = _require_admin()
    if not email:
        return _deny()
    try:
        lanc = custos_ti.salvar_lancamento(_sess(), _body(), email=email)
        return jsonify({"ok": True, "lancamento": lanc})
    except Exception as e:  # noqa: BLE001
        _sess().rollback()
        return _erro(e, "salvar lançamento")


@custos_ti_bp.delete("/api/custos-ti/lancamentos/<int:lanc_id>")
def api_lanc_excluir(lanc_id):
    if not _require_admin():
        return _deny()
    try:
        return jsonify({"ok": True, **custos_ti.excluir_lancamento(_sess(), lanc_id)})
    except Exception as e:  # noqa: BLE001
        _sess().rollback()
        return _erro(e, "excluir lançamento")


# ─────────────────────────────────────────────────────────────────────────────
# Cotação USD→BRL
# ─────────────────────────────────────────────────────────────────────────────
@custos_ti_bp.get("/api/custos-ti/cotacao")
def api_cotacao_get():
    if not _require_admin():
        return _deny()
    mes = custos_ti.valid_month(request.args.get("mes"))
    resp = {"ok": True, "mes": mes, "usd_brl": custos_ti.get_cotacao(_sess(), mes)}
    if request.args.get("buscar") in ("1", "true"):
        resp["hoje"] = custos_ti.fetch_cotacao_usd_brl()
    return jsonify(resp)


@custos_ti_bp.post("/api/custos-ti/cotacao")
def api_cotacao_set():
    if not _require_admin():
        return _deny()
    body = _body()
    mes = custos_ti.valid_month(body.get("mes"))
    try:
        valor = body.get("usd_brl")
        if body.get("buscar"):
            achado = custos_ti.fetch_cotacao_usd_brl()
            if not achado.get("ok"):
                return jsonify({"ok": False,
                                "error": f"não consegui buscar a cotação: {achado.get('error')}"}), 502
            valor = achado["usd_brl"]
        cot = custos_ti.set_cotacao(_sess(), mes, valor,
                                    fonte=body.get("fonte") or "manual")
        return jsonify({"ok": True, "cotacao": cot})
    except Exception as e:  # noqa: BLE001
        _sess().rollback()
        return _erro(e, "cotação")


# ─────────────────────────────────────────────────────────────────────────────
# Meta / WhatsApp
# ─────────────────────────────────────────────────────────────────────────────
@custos_ti_bp.post("/api/custos-ti/cotacao/preencher")
def api_cotacao_preencher():
    """Preenche as cotações do período com a PTAX do Banco Central e, se pedido,
    recalcula os valores convertidos dos lançamentos daqueles meses."""
    if not _require_admin():
        return _deny()
    body = _body()
    try:
        res = custos_ti.preencher_cotacoes(
            _sess(), body.get("de"), body.get("ate"),
            sobrescrever=bool(body.get("sobrescrever")))
        if body.get("recalcular"):
            res["recalculo"] = custos_ti.recalcular_conversoes(
                _sess(), body.get("de"), body.get("ate"))
        return jsonify({"ok": True, **res})
    except Exception as e:  # noqa: BLE001
        _sess().rollback()
        return _erro(e, "preencher cotações")


@custos_ti_bp.get("/api/custos-ti/meta/status")
def api_meta_status():
    """Diz se a integração está configurada. Com ?testar=1, bate na Graph API
    de verdade para validar o token — é o botão 'Testar conexão' da tela."""
    if not _require_admin():
        return _deny()
    import custos_ti_meta as meta
    cfg = meta.meta_config()
    data = {
        "api_disponivel": meta.meta_api_disponivel(),
        "waba_id": cfg["waba_id"] or None,
        "business_id": cfg["business_id"] or None,
        "tem_token": bool(cfg["token"]),
        "graph_version": meta.GRAPH_VERSION,
        "nota": (
            "A cobrança do cartão (Atividade de pagamento) NÃO tem API pública "
            "na Meta — ela entra por texto colado. A Graph API devolve o custo "
            "estimado das mensagens (pricing_analytics)."
        ),
    }
    if request.args.get("testar") in ("1", "true") and data["api_disponivel"]:
        data["teste"] = meta.testar_credencial()
    return jsonify({"ok": True, "data": data})


@custos_ti_bp.get("/api/custos-ti/meta/detalhe")
def api_meta_detalhe():
    """Custo da Meta no período quebrado por telefone e por categoria de preço.

    Uma chamada GET à Graph API por mês do intervalo — só leitura, sem custo.
    """
    if not _require_admin():
        return _deny()
    try:
        dados = custos_ti.meta_detalhe(_sess(), request.args.get("de"),
                                       request.args.get("ate"))
        return jsonify({"ok": True, "data": dados})
    except Exception as e:  # noqa: BLE001
        return _erro(e, "detalhe meta", 500)


@custos_ti_bp.post("/api/custos-ti/meta/importar")
def api_meta_importar():
    email = _require_admin()
    if not email:
        return _deny()
    body = _body()
    texto = body.get("texto") or ""
    if not texto.strip():
        return jsonify({"ok": False,
                        "error": "cole o texto da tela Atividade de pagamento da Meta"}), 400
    if len(texto) > _MAX_TEXTO:
        return jsonify({"ok": False, "error": "texto grande demais"}), 413
    try:
        res = custos_ti.importar_meta_texto(
            _sess(), texto,
            centro_key=(body.get("centro_key") or "comunicacao"),
            conta_id=body.get("conta_id"),
            criar_forma=body.get("criar_forma", True) is not False,
            salvar=bool(body.get("salvar")),
            email=email,
        )
        return jsonify(res), (200 if res.get("ok") else 422)
    except Exception as e:  # noqa: BLE001
        _sess().rollback()
        return _erro(e, "importar meta")


@custos_ti_bp.post("/api/custos-ti/meta/sync")
def api_meta_sync():
    email = _require_admin()
    if not email:
        return _deny()
    body = _body()
    try:
        res = custos_ti.importar_meta_api(
            _sess(), custos_ti.valid_month(body.get("mes")),
            centro_key=(body.get("centro_key") or "comunicacao"),
            salvar=bool(body.get("salvar")),
            email=email,
        )
        return jsonify(res), (200 if res.get("ok") else 502)
    except Exception as e:  # noqa: BLE001
        _sess().rollback()
        return _erro(e, "sync meta")
