"""
custos_ia_routes.py — Blueprint Flask do dashboard "Custos com IA" (admin-only).

A página (/custos_ia) é servida por app.py via render_protected_page, cujo
page_key "custos_ia" fica PROPOSITALMENTE fora do catálogo de serviços (mesmo
truque do acesso_avancado): só quem tem all_pages=True a acessa, e nenhum admin
consegue liberá-la avulsa. Estes endpoints repetem a checagem de all_pages por
baixo (defesa em profundidade — nunca confiar só no gate da página).

Endpoints (todos exigem all_pages):
  GET  /api/custos-ia/dashboard?month=YYYY-MM   → payload (OpenAI + Groq + histórico)
  POST /api/custos-ia/openai/refresh            → re-busca a Costs API agora
  POST /api/custos-ia/groq/print                → print → visão OpenAI → grava/preview
  POST /api/custos-ia/groq/manual               → grava custos da Groq digitados
  POST /api/custos-ia/month/close               → fecha o mês (congela)
  POST /api/custos-ia/month/reopen              → reabre o mês

Custo: a Costs API é só-leitura (grátis). A leitura do print gasta uma chamada
de visão da OpenAI (centavos), disparada à mão pelo admin — nunca em lote.
"""
from __future__ import annotations

import logging
import os

from flask import Blueprint, jsonify, request, send_file

import custos_ia

log = logging.getLogger(__name__)

custos_ia_bp = Blueprint("custos_ia_api", __name__)

_MAX_IMG_BYTES = 8 * 1024 * 1024   # 8 MB
_MAX_PDF_BYTES = 15 * 1024 * 1024  # 15 MB
_ALLOWED_MIME = {"image/png", "image/jpeg", "image/webp", "image/gif"}


def _require_admin():
    """Retorna o email se o usuário logado tem all_pages e está ativo; senão None."""
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


def _month_arg(default_source) -> str:
    return custos_ia.valid_month(default_source.get("month") or default_source.get("ref_month"))


@custos_ia_bp.get("/api/custos-ia/dashboard")
def api_dashboard():
    if not _require_admin():
        return _deny()
    month = custos_ia.valid_month(request.args.get("month"))
    try:
        return jsonify({"ok": True, "data": custos_ia.load_dashboard(month)})
    except Exception as e:  # noqa: BLE001
        log.exception("custos-ia dashboard")
        return jsonify({"ok": False, "error": str(e)}), 500


@custos_ia_bp.post("/api/custos-ia/openai/refresh")
def api_openai_refresh():
    if not _require_admin():
        return _deny()
    body = request.get_json(silent=True) or {}
    month = custos_ia.valid_month(body.get("month"))
    force = bool(body.get("force"))
    snap = custos_ia.save_openai_snapshot(month, force=force)
    code = 200 if snap.get("ok") else 502
    return jsonify({"ok": snap.get("ok"), "error": snap.get("error"), "openai": snap}), code


@custos_ia_bp.post("/api/custos-ia/groq/print")
def api_groq_print():
    if not _require_admin():
        return _deny()
    f = request.files.get("image")
    if not f:
        return jsonify({"ok": False, "error": "envie um arquivo no campo 'image'"}), 400
    data = f.read()
    if not data:
        return jsonify({"ok": False, "error": "arquivo vazio"}), 400
    if len(data) > _MAX_IMG_BYTES:
        return jsonify({"ok": False, "error": "imagem maior que 8 MB"}), 413
    mime = (f.mimetype or "image/png").lower()
    if mime not in _ALLOWED_MIME:
        return jsonify({"ok": False, "error": f"formato não suportado ({mime})"}), 415

    month = _month_arg(request.form)
    salvar = (request.form.get("salvar") or "1").strip() not in ("0", "false", "")
    try:
        snap = custos_ia.extract_groq_from_image(data, mime=mime, month=month)
    except Exception as e:  # noqa: BLE001
        log.exception("custos-ia groq print")
        return jsonify({"ok": False, "error": f"falha ao ler o print: {e}"}), 502

    # salvar=0 → só pré-visualiza o que a visão extraiu, antes de gravar.
    if salvar:
        custos_ia.save_groq_snapshot(snap)
    return jsonify({"ok": True, "saved": bool(salvar), "groq": snap})


@custos_ia_bp.post("/api/custos-ia/groq/manual")
def api_groq_manual():
    if not _require_admin():
        return _deny()
    body = request.get_json(silent=True) or {}
    projects = body.get("projects")
    if not isinstance(projects, list) or not projects:
        return jsonify({"ok": False, "error": "envie 'projects': [{name, amount_usd}]"}), 400
    month = _month_arg(body)
    try:
        snap = custos_ia.save_groq_manual(projects, month=month)
    except Exception as e:  # noqa: BLE001
        log.exception("custos-ia groq manual")
        return jsonify({"ok": False, "error": str(e)}), 500
    return jsonify({"ok": True, "saved": True, "groq": snap})


@custos_ia_bp.post("/api/custos-ia/groq/text")
def api_groq_text():
    if not _require_admin():
        return _deny()
    body = request.get_json(silent=True) or {}
    text = body.get("text") or ""
    if not text.strip():
        return jsonify({"ok": False, "error": "cole o texto da tela Projects da Groq"}), 400
    month = _month_arg(body)
    try:
        snap = custos_ia.save_groq_text(text, month=month)
    except Exception as e:  # noqa: BLE001
        log.exception("custos-ia groq text")
        return jsonify({"ok": False, "error": str(e)}), 500
    if not snap.get("projects"):
        return jsonify({"ok": False, "error": "nenhum projeto reconhecido no texto colado"}), 422
    return jsonify({"ok": True, "saved": True, "groq": snap})


@custos_ia_bp.post("/api/custos-ia/openai/backfill")
def api_openai_backfill():
    if not _require_admin():
        return _deny()
    body = request.get_json(silent=True) or {}
    try:
        n = int(body.get("months") or custos_ia.DEFAULT_HISTORY_MONTHS)
    except (TypeError, ValueError):
        n = custos_ia.DEFAULT_HISTORY_MONTHS
    n = max(1, min(n, 12))
    res = custos_ia.backfill_openai(n, force=bool(body.get("force")))
    ok = any(r.get("ok") for r in res)
    return jsonify({"ok": ok, "result": res})


@custos_ia_bp.get("/api/custos-ia/mapping")
def api_mapping_get():
    if not _require_admin():
        return _deny()
    return jsonify({"ok": True,
                    "projects": custos_ia.distinct_projects(),
                    "labels": custos_ia.load_mapping().get("labels", {})})


@custos_ia_bp.post("/api/custos-ia/mapping")
def api_mapping_set():
    if not _require_admin():
        return _deny()
    body = request.get_json(silent=True) or {}
    labels = body.get("labels")
    if not isinstance(labels, dict):
        return jsonify({"ok": False, "error": "envie 'labels': {\"provider::nome\": \"Rótulo\"}"}), 400
    custos_ia.save_mapping(labels)
    return jsonify({"ok": True})


@custos_ia_bp.get("/api/custos-ia/subscriptions")
def api_subs_get():
    if not _require_admin():
        return _deny()
    return jsonify({"ok": True, "items": custos_ia.load_subscriptions()})


@custos_ia_bp.post("/api/custos-ia/subscriptions")
def api_subs_set():
    if not _require_admin():
        return _deny()
    body = request.get_json(silent=True) or {}
    items = body.get("items")
    if not isinstance(items, list):
        return jsonify({"ok": False, "error": "envie 'items': [{name, ..., month_amount?, obs?}]"}), 400
    month = custos_ia.valid_month(body.get("month")) if body.get("month") else None
    saved = custos_ia.save_subscriptions(items, month=month)
    return jsonify({"ok": True, "items": saved})


@custos_ia_bp.post("/api/custos-ia/subscriptions/<sub_id>/invoice")
def api_sub_invoice_upload(sub_id):
    if not _require_admin():
        return _deny()
    f = request.files.get("pdf")
    if not f:
        return jsonify({"ok": False, "error": "envie o arquivo no campo 'pdf'"}), 400
    data = f.read()
    if not data:
        return jsonify({"ok": False, "error": "arquivo vazio"}), 400
    if len(data) > _MAX_PDF_BYTES:
        return jsonify({"ok": False, "error": "PDF maior que 15 MB"}), 413
    mime = (f.mimetype or "").lower()
    if "pdf" not in mime and not f.filename.lower().endswith(".pdf"):
        return jsonify({"ok": False, "error": "envie um PDF"}), 415
    month = custos_ia.valid_month(request.form.get("month"))
    path = custos_ia.invoice_path(sub_id, month)
    with open(path, "wb") as out:
        out.write(data)
    custos_ia.set_sub_invoice(sub_id, month, f"{month}.pdf")
    return jsonify({"ok": True, "month": month})


@custos_ia_bp.get("/api/custos-ia/subscriptions/<sub_id>/invoice")
def api_sub_invoice_get(sub_id):
    if not _require_admin():
        return _deny()
    month = custos_ia.valid_month(request.args.get("month"))
    path = custos_ia.invoice_path(sub_id, month)
    if not os.path.exists(path):
        return jsonify({"ok": False, "error": "sem fatura para este mês"}), 404
    return send_file(path, mimetype="application/pdf",
                     download_name=f"fatura_{sub_id}_{month}.pdf")


@custos_ia_bp.delete("/api/custos-ia/subscriptions/<sub_id>/invoice")
def api_sub_invoice_del(sub_id):
    if not _require_admin():
        return _deny()
    month = custos_ia.valid_month(request.args.get("month"))
    path = custos_ia.invoice_path(sub_id, month)
    try:
        if os.path.exists(path):
            os.remove(path)
    except OSError:
        pass
    custos_ia.set_sub_invoice(sub_id, month, None)
    return jsonify({"ok": True, "month": month})


@custos_ia_bp.get("/api/custos-ia/limits")
def api_limits_get():
    if not _require_admin():
        return _deny()
    limits = custos_ia.load_limits()
    # rótulos de projeto (lógicos) dos últimos meses, para o editor
    labels = custos_ia.project_matrix(custos_ia.DEFAULT_HISTORY_MONTHS)["labels"]
    # inclui projetos que já têm limite mesmo que não apareçam na janela
    for k in limits["projects"]:
        if k not in labels:
            labels.append(k)
    return jsonify({"ok": True, "providers": limits["providers"],
                    "projects": limits["projects"], "project_labels": labels})


@custos_ia_bp.post("/api/custos-ia/limits")
def api_limits_set():
    if not _require_admin():
        return _deny()
    body = request.get_json(silent=True) or {}
    providers = body.get("providers") or {}
    projects = body.get("projects") or {}
    if not isinstance(providers, dict) or not isinstance(projects, dict):
        return jsonify({"ok": False, "error": "envie 'providers' e 'projects' como objetos"}), 400
    saved = custos_ia.save_limits(providers, projects)
    return jsonify({"ok": True, "limits": saved})


@custos_ia_bp.post("/api/custos-ia/overage-note")
def api_overage_note():
    if not _require_admin():
        return _deny()
    body = request.get_json(silent=True) or {}
    key = (body.get("key") or "").strip()
    if not key:
        return jsonify({"ok": False, "error": "'key' do estouro é obrigatória"}), 400
    month = custos_ia.valid_month(body.get("month"))
    note = custos_ia.set_overage_note(month, key, body.get("note", ""))
    return jsonify({"ok": True, "note": note})


@custos_ia_bp.post("/api/custos-ia/month/close")
def api_month_close():
    email = _require_admin()
    if not email:
        return _deny()
    body = request.get_json(silent=True) or {}
    month = custos_ia.valid_month(body.get("month"))
    info = custos_ia.close_month(month, by=email)
    return jsonify({"ok": True, "month": month, "closed": True, "info": info})


@custos_ia_bp.post("/api/custos-ia/month/reopen")
def api_month_reopen():
    if not _require_admin():
        return _deny()
    body = request.get_json(silent=True) or {}
    month = custos_ia.valid_month(body.get("month"))
    custos_ia.reopen_month(month)
    return jsonify({"ok": True, "month": month, "closed": False})
