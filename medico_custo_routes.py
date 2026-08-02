"""
medico_custo_routes.py — histórico da página Custo Efetivo Nominal.

  GET /api/medico_custo/datas              → dias disponíveis (o seletor da tela)
  GET /api/medico_custo/snapshot?data=...  → o cadastro como estava naquele dia

A página abre SEMPRE no dia de hoje, lendo o JSON estático do nginx (rápido,
com cache). Estes endpoints só entram em cena quando o usuário escolhe uma data
no seletor — aí o Flask remonta aquele dia a partir das tabelas mc_*.

O snapshot devolve o MESMO formato do json_consolidado/medico_custo.json, pelo
mesmo `montar_payload()` do ETL: a página não sabe (nem precisa saber) se está
lendo o arquivo de hoje ou um dia reconstruído.

Acesso: mesma page_key `medico_custo` da página. Quem não pode ver a página não
pode ver o histórico dela.
"""
from __future__ import annotations

import logging
from datetime import date, datetime

from flask import Blueprint, jsonify, request

log = logging.getLogger(__name__)

medico_custo_bp = Blueprint("medico_custo_hist", __name__)

PAGE_KEY = "medico_custo"


def _autorizado() -> bool:
    """True se o usuário logado pode ver a página medico_custo.

    Defesa em profundidade: o nginx já protege a página, mas a API é uma porta
    própria e não pode depender disso.
    """
    try:
        from auth_routes import decode_user
        from auth_db import SessionLocal, get_user_by_email as _gue
        email, _ = decode_user()
        if not email:
            return False
        db = SessionLocal()
        try:
            u = _gue(db, email)
            if not u or not getattr(u, "ativo", True):
                return False
            if bool(getattr(u, "all_pages", False)):
                return True
            return PAGE_KEY in (u.lista_paginas() or [])
        finally:
            db.close()
    except Exception:  # noqa: BLE001
        log.exception("falha ao checar acesso ao histórico de medico_custo")
        return False


@medico_custo_bp.get("/api/medico_custo/datas")
def datas():
    if not _autorizado():
        return jsonify({"erro": "sem acesso"}), 403
    try:
        import medico_custo_hist as hist
        ds = hist.datas_disponiveis()
    except Exception as e:  # noqa: BLE001
        # Sem histórico ainda (tabelas não criadas, RDS fora): a página some com
        # o seletor e continua funcionando no dia de hoje. Não é erro de tela.
        log.warning("histórico indisponível: %s", e)
        return jsonify({"datas": [], "indisponivel": True})
    return jsonify({
        "datas": ds,
        "primeira": ds[0]["data"] if ds else None,
        "ultima": ds[-1]["data"] if ds else None,
    })


@medico_custo_bp.get("/api/medico_custo/snapshot")
def snapshot():
    if not _autorizado():
        return jsonify({"erro": "sem acesso"}), 403

    bruto = (request.args.get("data") or "").strip()
    try:
        alvo = datetime.strptime(bruto, "%Y-%m-%d").date()
    except ValueError:
        return jsonify({"erro": "data inválida (use AAAA-MM-DD)"}), 400
    if alvo > date.today():
        return jsonify({"erro": "data no futuro"}), 400

    try:
        import medico_custo_hist as hist
        from export_medico_custo import montar_payload, nome_posto
        linhas, meta = hist.linhas_em(alvo)
    except Exception as e:  # noqa: BLE001
        log.exception("falha ao reconstruir snapshot")
        return jsonify({"erro": f"{type(e).__name__}: {str(e)[:120]}"}), 500

    if meta is None:
        return jsonify({"erro": "não há registro nessa data nem antes dela"}), 404

    # `status` reconstruído: a página usa para contar postos ok/erro. Os postos
    # que falharam NAQUELE dia entram com erro para a tela poder avisar que a
    # foto está incompleta — número menor por falha de ETL parece economia.
    falhos = {p for p in (meta["postos_falhos"] or "").split(",") if p}
    postos = sorted({l["posto"] for l in linhas} | falhos)
    status = {p: {"posto": p, "nome": nome_posto(p),
                  "linhas": sum(1 for l in linhas if l["posto"] == p),
                  "erro": "posto sem coleta nesta data" if p in falhos else None}
              for p in postos}

    payload = montar_payload(linhas, status, gerado_em=meta["gerado_em"])
    payload["historico"] = {
        "data": meta["data"],
        "pedida": alvo.isoformat(),
        # A data pedida pode cair num dia sem execução; nesse caso vale a
        # anterior mais próxima e a TELA precisa dizer isso, senão o usuário
        # acha que está vendo 14/09 quando está vendo 12/09.
        "aproximada": meta["data"] != alvo.isoformat(),
        "completa": meta["completa"],
        "postos_falhos": meta["postos_falhos"],
        "parametros_do_dia": meta["parametros"],
        "novas": meta["novas"], "alteradas": meta["alteradas"],
        "removidas": meta["removidas"],
    }
    return jsonify(payload)
