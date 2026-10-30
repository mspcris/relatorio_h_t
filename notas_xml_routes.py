"""
notas_xml_routes.py — XML das notas para a aba "Ver Registros" do KPI Notas x RPS.

  GET  /api/notas_xml/<posto>/<ym>/<id_retorno>     um XML (download)
  POST /api/notas_xml/zip  {posto, ym, ids:[...]}    dispara o ZIP → {job}
  GET  /api/notas_xml/zip/<job>                      progresso
  GET  /api/notas_xml/zip/<job>/arquivo              o ZIP pronto

Acesso: sessão + posto no ACL do usuário (mesma régua do resto do KPI). O id
pedido tem que estar no JSON do mês (json_notas_rps/<P>_notas_ind_<ym>.json,
campo id_retorno) — a URL não serve de leitura livre da Fin_NotaFiscalRetorno.

A contabilidade (/contabilidade_notas) tem as rotas dela em
contabilidade_notas_routes.py, sob /api/contabilidade_notas/, porque o usuário
de escopo fechado só alcança aquele prefixo no nginx.
"""
from __future__ import annotations

import json
import logging
import os
import re

from flask import Blueprint, jsonify, request, send_file

import notas_xml

log = logging.getLogger(__name__)
notas_xml_bp = Blueprint("notas_xml", __name__)

_BASE = os.path.dirname(os.path.abspath(__file__))
_DIRS = [os.getenv("NOTAS_RPS_DIR", ""), "/opt/relatorio_h_t/json_notas_rps", os.path.join(_BASE, "json_notas_rps")]
_RE_YM = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


def _quem():
    from medico_novo_routes import _check_admin
    email, postos, _ = _check_admin()
    return email, {p.upper() for p in (postos or [])}


def _json_mes(posto: str, ym: str) -> dict | None:
    for d in _DIRS:
        if not d:
            continue
        p = os.path.join(d, f"{posto}_notas_ind_{ym}.json")
        if os.path.isfile(p):
            with open(p, encoding="utf-8") as f:
                return json.load(f)
    return None


def _indice(posto: str, ym: str) -> dict[int, dict]:
    d = _json_mes(posto, ym)
    out = {}
    for n in (d or {}).get("notas") or []:
        i = int(n.get("id_retorno") or 0)
        if i > 0:
            n["posto"] = posto
            out[i] = n
    return out


def _valida(posto: str, ym: str):
    email, acl = _quem()
    if not email:
        return None, None, (jsonify({"ok": False, "error": "não autenticado"}), 401)
    posto = (posto or "").strip().upper()
    if len(posto) != 1 or posto not in acl:
        return None, None, (jsonify({"ok": False, "error": "posto fora do seu acesso"}), 403)
    if not _RE_YM.match(ym or ""):
        return None, None, (jsonify({"ok": False, "error": "mês inválido"}), 400)
    return email, posto, None


@notas_xml_bp.get("/api/notas_xml/<posto>/<ym>/<int:id_retorno>")
def api_um(posto, ym, id_retorno):
    email, posto, erro = _valida(posto, ym)
    if erro:
        return erro
    nota = _indice(posto, ym).get(int(id_retorno))
    if not nota:
        return jsonify({"ok": False, "error": "nota não está no mês carregado"}), 404
    try:
        xml = notas_xml.ler_xml(posto, id_retorno)
    except Exception as e:
        log.warning("notas_xml: posto %s não respondeu: %s", posto, e)
        return jsonify({"ok": False, "error": f"o posto {posto} não respondeu; tente de novo"}), 502
    if not xml:
        return jsonify({"ok": False, "error": "esta nota não tem XML no sistema"}), 404
    return send_file(_io(xml), mimetype="application/xml", as_attachment=True, download_name=notas_xml.nome_xml(nota))


def _io(b: bytes):
    import io
    return io.BytesIO(b)


@notas_xml_bp.post("/api/notas_xml/zip")
def api_zip():
    d = request.get_json(silent=True) or {}
    email, posto, erro = _valida(d.get("posto", ""), d.get("ym", ""))
    if erro:
        return erro
    ym = d["ym"]
    idx = _indice(posto, ym)
    pedidos = d.get("ids") or []
    if not isinstance(pedidos, list):
        return jsonify({"ok": False, "error": "ids deve ser uma lista"}), 400
    itens, ignorados = [], 0
    for i in pedidos:
        try:
            n = idx.get(int(i))
        except (TypeError, ValueError):
            n = None
        if not n:
            ignorados += 1
            continue
        itens.append({"posto": posto, "id": int(i), "nome": notas_xml.nome_xml(n),
                      "rotulo": f"NF {n.get('NotaFiscal')} de {n.get('data_emissao')} — {n.get('Cliente') or ''}"})
    if not itens:
        return jsonify({"ok": False, "error": "nenhuma nota com XML na seleção"}), 400
    nome_zip = f"xml_notas_{posto}_{ym}.zip"
    cab = f"XML das notas fiscais — posto {posto} — {ym}\r\nPedido por {email}."
    try:
        job = notas_xml.iniciar_zip(email, nome_zip, itens, cab)
    except RuntimeError as e:
        return jsonify({"ok": False, "error": str(e)}), 429
    out = job.dict()
    out.update({"ok": True, "ignorados": ignorados})
    return jsonify(out)


@notas_xml_bp.get("/api/notas_xml/zip/<job_id>")
def api_zip_status(job_id):
    email, _ = _quem()
    if not email:
        return jsonify({"ok": False, "error": "não autenticado"}), 401
    job = notas_xml.obter(job_id, email)
    if not job:
        return jsonify({"ok": False, "error": "geração não encontrada (o serviço pode ter reiniciado); peça de novo"}), 404
    out = job.dict()
    out["ok"] = True
    return jsonify(out)


@notas_xml_bp.get("/api/notas_xml/zip/<job_id>/arquivo")
def api_zip_arquivo(job_id):
    email, _ = _quem()
    if not email:
        return jsonify({"ok": False, "error": "não autenticado"}), 401
    job = notas_xml.obter(job_id, email)
    if not job or job.estado != "pronto" or not os.path.isfile(job.arquivo):
        return jsonify({"ok": False, "error": "ZIP ainda não está pronto"}), 404
    return send_file(job.arquivo, mimetype="application/zip", as_attachment=True, download_name=job.nome_zip)
