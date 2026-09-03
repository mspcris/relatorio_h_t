"""
painel_financeiro_routes.py — API do KPI Painel Financeiro (impostos).

  GET  /api/painel_financeiro/impostos?postos=A,B      painel avaliado (linhas por posto × imposto)
  GET  /api/painel_financeiro/impostos/registros?posto=G&imposto=INSS&mes=2026-08
  POST /api/painel_financeiro/impostos/faixa           {imposto, amarelo_pct, vermelho_pct, base_meses, email, observacao}
  DELETE /api/painel_financeiro/impostos/faixa/<imposto>

Leitura: qualquer usuário logado com a página liberada (o gate da página é o
render_protected_page; a API só exige sessão). Faixa: só admin — é regra de
alarme que dispara e-mail, não é filtro de tela. Quem gravou fica registrado.
"""
from __future__ import annotations

import logging

from flask import Blueprint, jsonify, request

log = logging.getLogger(__name__)
painel_financeiro_bp = Blueprint("painel_financeiro", __name__)

POSTOS_VALIDOS = set("ABCDGIJMNPRXY")


def _usuario():
    """→ (email, is_admin) ou (None, False)."""
    try:
        from auth_routes import decode_user
        from auth_db import SessionLocal, get_user_by_email
    except Exception as e:  # pragma: no cover
        log.error("painel_financeiro: auth indisponível (%s)", e)
        return None, False
    email, _ = decode_user()
    if not email:
        return None, False
    db = SessionLocal()
    try:
        u = get_user_by_email(db, email)
        return email, bool(u and getattr(u, "is_admin", False))
    finally:
        db.close()


@painel_financeiro_bp.before_request
def _exigir_sessao():
    email, _ = _usuario()
    if not email:
        return jsonify({"ok": False, "error": "não autenticado"}), 401
    return None


@painel_financeiro_bp.get("/api/painel_financeiro/impostos")
def api_impostos():
    import painel_financeiro as pf
    raw = (request.args.get("postos") or "").upper()
    postos = [p for p in raw.split(",") if p in POSTOS_VALIDOS] or None
    try:
        dados = pf.painel(postos)
    except Exception as e:
        log.exception("painel_financeiro: erro ao montar painel")
        return jsonify({"ok": False, "error": str(e)[:300]}), 500
    _, admin = _usuario()
    dados["pode_editar_faixa"] = admin
    dados["ok"] = True
    return jsonify(dados)


@painel_financeiro_bp.get("/api/painel_financeiro/impostos/registros")
def api_registros():
    import painel_financeiro as pf
    posto = (request.args.get("posto") or "").upper()
    imposto = (request.args.get("imposto") or "").upper()
    mes = (request.args.get("mes") or "").strip() or None
    if posto not in POSTOS_VALIDOS or imposto not in pf.ROTULOS:
        return jsonify({"ok": False, "error": "posto/imposto inválido"}), 400
    try:
        rows = pf.registros(posto, imposto, mes)
    except Exception as e:
        log.exception("painel_financeiro: erro registros")
        return jsonify({"ok": False, "error": str(e)[:300]}), 500
    return jsonify({"ok": True, "rows": rows, "count": len(rows),
                    "soma": round(sum(float(r.get("valor_pago") or 0) for r in rows), 2)})


@painel_financeiro_bp.post("/api/painel_financeiro/impostos/faixa")
def api_faixa_salvar():
    import painel_financeiro as pf
    email, admin = _usuario()
    if not admin:
        return jsonify({"ok": False, "error": "só administrador cadastra faixa de alarme"}), 403
    d = request.get_json(silent=True) or {}
    try:
        pg = pf.pg_conn()
        try:
            pf.ensure_schema(pg)
            faixa = pf.salvar_faixa(
                pg, str(d.get("imposto") or "").upper(),
                float(str(d.get("amarelo_pct", "")).replace(",", ".")),
                float(str(d.get("vermelho_pct", "")).replace(",", ".")),
                int(d.get("base_meses") or 12),
                d.get("email"), d.get("observacao"), email,
            )
        finally:
            pg.close()
    except (ValueError, TypeError) as e:
        return jsonify({"ok": False, "error": str(e)}), 400
    except Exception as e:
        log.exception("painel_financeiro: erro ao salvar faixa")
        return jsonify({"ok": False, "error": str(e)[:300]}), 500
    return jsonify({"ok": True, "faixa": faixa})


@painel_financeiro_bp.delete("/api/painel_financeiro/impostos/faixa/<imposto>")
def api_faixa_apagar(imposto):
    import painel_financeiro as pf
    _, admin = _usuario()
    if not admin:
        return jsonify({"ok": False, "error": "só administrador"}), 403
    pg = pf.pg_conn()
    try:
        pf.ensure_schema(pg)
        pf.apagar_faixa(pg, imposto.upper())
    finally:
        pg.close()
    return jsonify({"ok": True})
