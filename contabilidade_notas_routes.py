"""
contabilidade_notas_routes.py — API do app "Contabilidade - Notas".

  GET /api/contabilidade_notas/meses                      meses com dado
  GET /api/contabilidade_notas/resumo?ym=2026-09          totais por EMPRESA + emissão por dia
  GET /api/contabilidade_notas/notas?ym=2026-09&cnpj=...  as notas de uma empresa, linha a linha
                                      (cnpj=todos = todas as empresas do mês)
  GET /api/contabilidade_notas/xml?ym=&posto=&id=         o XML de UMA nota (download)
  POST /api/contabilidade_notas/xml/zip {ym, itens:[[posto,id],...]}  ZIP em segundo plano → {job}
  GET /api/contabilidade_notas/xml/zip/<job>              progresso
  GET /api/contabilidade_notas/xml/zip/<job>/arquivo      o ZIP pronto

XML (30/10/2026): cada nota traz `id_retorno` (Fin_NotaFiscalRetorno) e
`tem_xml`; o XML é lido AO VIVO no posto por notas_xml.py, nunca fica em
arquivo. Estas rotas ficam sob /api/contabilidade_notas/ de propósito: é o
único prefixo que o usuário de escopo fechado (a contabilidade) alcança no
nginx. O resumo diz quantas notas estão SEM XML e lista quais — JSON gravado
antes de 30/10 não tem o campo e aparece como "xml_desc" (desconhecido: rodar
o ETL), nunca como "sem XML".

Aqui a unidade é a EMPRESA (CNPJ que emitiu), não o posto. Cada clínica é uma
empresa e emite só no posto dela; as duas operadoras (CAMIM e SD-M) emitem em
vários postos — a contabilidade quer "quantas notas a operadora emitiu", não
"quantas a operadora emitiu em Anchieta". Por isso NÃO existe filtro por posto
nem ACL de posto nesta API: quem tem a página vê todas as empresas.

Fonte: json_contab_notas/<POSTO>_<AAAA-MM>.json, gravado pelo
export_contab_notas.py (cron de hora em hora). A pasta não é publicada em
/var/www — o dado só sai por aqui.

Acesso: sessão + página 'contabilidade_notas' liberada (ou all_pages). A API
confere sozinha, sem depender do gate do HTML: o usuário da contabilidade é
de fora da empresa.

Regras que valem para a tela inteira:
  * emitidas − canceladas = contabilizadas (mesma conta do KPI Notas x RPS);
  * nota cancelada conta no mês em que foi EMITIDA;
  * a mesma nota (mesma chave da NFS-e, mesma situação) gravada em dois
    postos conta uma vez;
  * a mesma chave que o ERP devolve duas vezes, uma emitida e outra cancelada
    (medido: 4 notas da SD-M em Jacarepaguá, ago/26), fica com AS DUAS linhas —
    é assim que o KPI Notas x RPS conta, o líquido fecha (2 emitidas − 1
    cancelada) e escolher uma delas seria decidir sozinho se a nota vale. A API
    devolve quantas são em `duplas_emitida_cancelada` e a tela avisa;
  * posto sem arquivo no mês aparece em `postos_sem_dado` — falha de coleta
    não pode parecer "zero nota".
"""
from __future__ import annotations

import glob
import json
import logging
import os
import re
import threading

from flask import Blueprint, jsonify, request, send_file

import notas_xml

log = logging.getLogger(__name__)
contabilidade_notas_bp = Blueprint("contabilidade_notas", __name__)

PAGE_KEY = "contabilidade_notas"
_BASE = os.path.dirname(os.path.abspath(__file__))
_DIRS = [
    os.getenv("CONTAB_NOTAS_DIR", ""),
    "/opt/relatorio_h_t/json_contab_notas",
    os.path.join(_BASE, "json_contab_notas"),
]
_RE_ARQ = re.compile(r"^([A-Z])_(\d{4}-\d{2})\.json$")
_RE_YM = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
TIPO = {"Clinica": "Clínica", "OperadoraCamim": "Operadora", "OperadoraSDM": "Operadora"}

_cache: dict = {}
_cache_lock = threading.Lock()


def _dir() -> str | None:
    for d in _DIRS:
        if d and os.path.isdir(d):
            return d
    return None


# ── acesso ────────────────────────────────────────────────────────────────────
def _pode() -> tuple[bool, int]:
    """→ (liberado, status http quando não)."""
    try:
        from auth_routes import decode_user
        from auth_db import SessionLocal, get_user_by_email
    except Exception as e:  # pragma: no cover
        log.error("contabilidade_notas: auth indisponível (%s)", e)
        return False, 503
    email, _ = decode_user()
    if not email:
        return False, 401
    db = SessionLocal()
    try:
        u = get_user_by_email(db, email)
        if not u:
            return False, 401
        if bool(getattr(u, "all_pages", False)) or PAGE_KEY in u.lista_paginas():
            return True, 200
        return False, 403
    finally:
        db.close()


@contabilidade_notas_bp.before_request
def _gate():
    ok, status = _pode()
    if not ok:
        msg = "não autenticado" if status == 401 else "sem acesso a esta página"
        return jsonify({"ok": False, "error": msg}), status
    return None


# ── leitura ───────────────────────────────────────────────────────────────────
def _arquivos(ym: str) -> dict[str, str]:
    d = _dir()
    if not d:
        return {}
    out = {}
    for f in glob.glob(os.path.join(d, f"?_{ym}.json")):
        m = _RE_ARQ.match(os.path.basename(f))
        if m:
            out[m.group(1)] = f
    return out


def _postos_conhecidos() -> list[str]:
    d = _dir()
    if not d:
        return []
    return sorted({m.group(1) for f in os.listdir(d) if (m := _RE_ARQ.match(f))})


def _canc(n: dict) -> bool:
    return str(n.get("Cancelada") or "").strip().lower() == "sim"


def _xml(n: dict) -> str:
    """ok = tem XML · sem = o sistema não tem XML desta nota · desc = JSON antigo, sem o campo."""
    if "id_retorno" not in n:
        return "desc"
    try:
        return "ok" if int(n.get("id_retorno") or 0) > 0 and int(n.get("tem_xml") or 0) == 1 else "sem"
    except (TypeError, ValueError):
        return "sem"


def _motivo_sem_xml(n: dict) -> str:
    try:
        tem_ret = int(n.get("id_retorno") or 0) > 0
    except (TypeError, ValueError):
        tem_ret = False
    return "retorno da prefeitura gravado sem o XML" if tem_ret else "sem retorno nacional da prefeitura no sistema"


def _carregar(ym: str) -> dict:
    """Junta os postos do mês. Cacheado pela assinatura (arquivo, mtime)."""
    arqs = _arquivos(ym)
    assinatura = tuple(sorted((p, os.path.getmtime(f)) for p, f in arqs.items()))
    with _cache_lock:
        c = _cache.get(ym)
        if c and c["assinatura"] == assinatura:
            return c

    notas, coleta, erros, vistos, repetidas = [], {}, {}, set(), 0
    idx: dict = {}            # (posto, id_retorno) -> nota, para validar pedidos de XML
    situacoes: dict = {}      # (cnpj, chave) -> {True, False}: cancelada / não
    for posto, f in sorted(arqs.items()):
        try:
            with open(f, encoding="utf-8") as fh:
                d = json.load(fh)
        except Exception as e:
            log.error("contabilidade_notas: não li %s (%s)", f, e)
            erros[posto] = "arquivo ilegível"
            continue
        coleta[posto] = d.get("gerado_em")
        for n in d.get("notas") or []:
            cnpj = str(n.get("cnpj") or "").strip()
            # mesma chave da NFS-e e mesma situação = mesma nota, mesmo que
            # esteja em dois postos
            chave = str(n.get("chave") or "").strip()
            ident = ((cnpj, chave, _canc(n)) if chave
                     else (cnpj, n.get("NotaFiscal"), n.get("data_emissao"), n.get("valor"), _canc(n), posto))
            if chave:
                situacoes.setdefault((cnpj, chave), set()).add(_canc(n))
            if ident in vistos:
                repetidas += 1
                continue
            vistos.add(ident)
            n["posto"] = posto
            n["cnpj"] = cnpj
            n["xml"] = _xml(n)
            notas.append(n)
            if n["xml"] == "ok":
                idx[(posto, int(n["id_retorno"]))] = n

    c = {
        "assinatura": assinatura,
        "notas": notas,
        "idx": idx,
        "coleta": coleta,
        "erros": erros,
        "repetidas": repetidas,
        "duplas": sum(1 for s in situacoes.values() if len(s) > 1),
        "postos_sem_dado": [p for p in _postos_conhecidos() if p not in arqs or p in erros],
    }
    with _cache_lock:
        _cache[ym] = c
        for velho in sorted(_cache)[:-6]:     # no máximo 6 meses na memória
            _cache.pop(velho, None)
    return c


def _zeros() -> dict:
    return {"emit_qtd": 0, "emit_valor": 0.0, "canc_qtd": 0, "canc_valor": 0.0,
            "xml_ok": 0, "sem_xml": 0, "xml_desc": 0}


def _soma(acc: dict, n: dict) -> None:
    v = float(n.get("valor") or 0)
    acc["emit_qtd"] += 1
    acc["emit_valor"] += v
    if _canc(n):
        acc["canc_qtd"] += 1
        acc["canc_valor"] += v
    x = n.get("xml") or _xml(n)
    acc["xml_ok" if x == "ok" else "sem_xml" if x == "sem" else "xml_desc"] += 1


def _fecha(acc: dict) -> dict:
    acc["emit_valor"] = round(acc["emit_valor"], 2)
    acc["canc_valor"] = round(acc["canc_valor"], 2)
    acc["liq_qtd"] = acc["emit_qtd"] - acc["canc_qtd"]
    acc["liq_valor"] = round(acc["emit_valor"] - acc["canc_valor"], 2)
    return acc


def _ym_arg():
    ym = (request.args.get("ym") or "").strip()
    return ym if _RE_YM.match(ym) else None


# ── rotas ─────────────────────────────────────────────────────────────────────
@contabilidade_notas_bp.get("/api/contabilidade_notas/meses")
def api_meses():
    d = _dir()
    meses = sorted({m.group(2) for f in (os.listdir(d) if d else []) if (m := _RE_ARQ.match(f))}, reverse=True)
    return jsonify({"ok": True, "meses": meses})


@contabilidade_notas_bp.get("/api/contabilidade_notas/resumo")
def api_resumo():
    ym = _ym_arg()
    if not ym:
        return jsonify({"ok": False, "error": "informe ym=AAAA-MM"}), 400
    c = _carregar(ym)
    empresas: dict[str, dict] = {}
    total = _zeros()
    por_dia: dict[str, dict] = {}
    for n in c["notas"]:
        e = empresas.get(n["cnpj"])
        if e is None:
            e = empresas[n["cnpj"]] = {
                "cnpj": n["cnpj"], "nome": str(n.get("Empresa") or "").strip(),
                "tipo": TIPO.get(n.get("origem"), n.get("origem") or ""),
                "postos": set(), **_zeros(),
            }
        e["postos"].add(n["posto"])
        _soma(e, n)
        _soma(total, n)
        dia = por_dia.setdefault(str(n.get("data_emissao") or "")[:10], {})
        _soma(dia.setdefault(n["cnpj"], _zeros()), n)
    lista = []
    for e in empresas.values():
        e["postos"] = sorted(e["postos"])
        lista.append(_fecha(e))
    lista.sort(key=lambda e: -e["liq_valor"])
    dias = {d: {cnpj: _fecha(a) for cnpj, a in emp.items()} for d, emp in sorted(por_dia.items())}
    # Quais notas estão sem XML — o resumo "claro e fácil" pedido pela
    # contabilidade: zero é a resposta esperada; quando não for, a lista diz
    # nota a nota, com o motivo. Teto de 1.000 (a tela avisa quando cortou).
    sem = [n for n in c["notas"] if n.get("xml") == "sem"]
    sem_lista = [{
        "posto": n["posto"], "cnpj": n["cnpj"], "Empresa": n.get("Empresa"), "NotaFiscal": n.get("NotaFiscal"),
        "data_emissao": n.get("data_emissao"), "Cliente": n.get("Cliente"), "valor": n.get("valor"),
        "Cancelada": n.get("Cancelada"), "motivo": _motivo_sem_xml(n),
    } for n in sem[:1000]]
    return jsonify({
        "ok": True, "ym": ym, "empresas": lista, "total": _fecha(total), "por_dia": dias,
        "coleta": c["coleta"], "postos_sem_dado": c["postos_sem_dado"], "repetidas": c["repetidas"],
        "duplas_emitida_cancelada": c["duplas"],
        "sem_xml_lista": sem_lista, "sem_xml_cortada": len(sem) > len(sem_lista),
    })


@contabilidade_notas_bp.get("/api/contabilidade_notas/notas")
def api_notas():
    ym = _ym_arg()
    bruto = (request.args.get("cnpj") or "").strip().lower()
    todos = bruto == "todos"
    cnpj = "todos" if todos else re.sub(r"\D", "", bruto)
    if not ym or not cnpj:
        return jsonify({"ok": False, "error": "informe ym=AAAA-MM e cnpj (ou cnpj=todos)"}), 400
    c = _carregar(ym)
    notas = c["notas"] if todos else [n for n in c["notas"] if n["cnpj"] == cnpj]
    notas.sort(key=lambda n: (str(n.get("data_emissao") or ""), str(n.get("hora") or ""), n.get("NotaFiscal") or 0))
    total = _zeros()
    for n in notas:
        _soma(total, n)
    return jsonify({
        "ok": True, "ym": ym, "cnpj": cnpj,
        "nome": "Todas as empresas" if todos else (str(notas[0].get("Empresa") or "").strip() if notas else ""),
        "total": _fecha(total), "notas": notas,
        "coleta": c["coleta"], "postos_sem_dado": c["postos_sem_dado"],
    })


# ── XML ───────────────────────────────────────────────────────────────────────
def _quem() -> str:
    from auth_routes import decode_user
    email, _ = decode_user()
    return email or ""


def _rotulo(n: dict) -> str:
    return f"NF {n.get('NotaFiscal')} de {n.get('data_emissao')} — {n.get('Empresa') or ''} — {n.get('Cliente') or ''}"


@contabilidade_notas_bp.get("/api/contabilidade_notas/xml")
def api_xml():
    ym = _ym_arg()
    posto = (request.args.get("posto") or "").strip().upper()
    try:
        id_ret = int(request.args.get("id") or 0)
    except ValueError:
        id_ret = 0
    if not ym or len(posto) != 1 or id_ret <= 0:
        return jsonify({"ok": False, "error": "informe ym, posto e id"}), 400
    n = _carregar(ym)["idx"].get((posto, id_ret))
    if not n:
        return jsonify({"ok": False, "error": "nota não está no mês carregado ou não tem XML"}), 404
    try:
        xml = notas_xml.ler_xml(posto, id_ret)
    except Exception as e:
        log.warning("contabilidade_notas: posto %s não respondeu ao XML: %s", posto, e)
        return jsonify({"ok": False, "error": f"o posto {posto} não respondeu; tente de novo em instantes"}), 502
    if not xml:
        return jsonify({"ok": False, "error": "esta nota não tem XML no sistema"}), 404
    import io
    return send_file(io.BytesIO(xml), mimetype="application/xml", as_attachment=True, download_name=notas_xml.nome_xml(n))


@contabilidade_notas_bp.post("/api/contabilidade_notas/xml/zip")
def api_xml_zip():
    d = request.get_json(silent=True) or {}
    ym = str(d.get("ym") or "")
    if not _RE_YM.match(ym):
        return jsonify({"ok": False, "error": "informe ym=AAAA-MM"}), 400
    c = _carregar(ym)
    pedidos = d.get("itens")
    if not isinstance(pedidos, list):
        return jsonify({"ok": False, "error": "itens deve ser uma lista de [posto, id]"}), 400
    itens, ignorados, empresas = [], 0, set()
    for par in pedidos:
        try:
            posto, i = str(par[0]).upper(), int(par[1])
        except (TypeError, ValueError, IndexError):
            ignorados += 1
            continue
        n = c["idx"].get((posto, i))
        if not n:
            ignorados += 1
            continue
        empresas.add(n["cnpj"])
        # uma pasta por empresa: é assim que a contabilidade separa
        pasta = f"{n['cnpj']}_{notas_xml.slug(n.get('Empresa'))}"
        itens.append({"posto": posto, "id": i, "nome": f"{pasta}/{notas_xml.nome_xml(n)}", "rotulo": _rotulo(n)})
    if not itens:
        return jsonify({"ok": False, "error": "nenhuma nota com XML na seleção"}), 400
    email = _quem()
    nome_zip = f"xml_notas_{ym}_" + (next(iter(empresas)) if len(empresas) == 1 else "todas_as_empresas") + ".zip"
    cab = (f"XML das notas fiscais — {ym} — " + (f"CNPJ {next(iter(empresas))}" if len(empresas) == 1 else f"{len(empresas)} empresas")
           + f"\r\nPedido por {email}.")
    try:
        job = notas_xml.iniciar_zip(email, nome_zip, itens, cab)
    except RuntimeError as e:
        return jsonify({"ok": False, "error": str(e)}), 429
    out = job.dict()
    out.update({"ok": True, "ignorados": ignorados})
    return jsonify(out)


@contabilidade_notas_bp.get("/api/contabilidade_notas/xml/zip/<job_id>")
def api_xml_zip_status(job_id):
    job = notas_xml.obter(job_id, _quem())
    if not job:
        return jsonify({"ok": False, "error": "geração não encontrada (o serviço pode ter reiniciado); peça de novo"}), 404
    out = job.dict()
    out["ok"] = True
    return jsonify(out)


@contabilidade_notas_bp.get("/api/contabilidade_notas/xml/zip/<job_id>/arquivo")
def api_xml_zip_arquivo(job_id):
    job = notas_xml.obter(job_id, _quem())
    if not job or job.estado != "pronto" or not os.path.isfile(job.arquivo):
        return jsonify({"ok": False, "error": "ZIP ainda não está pronto"}), 404
    return send_file(job.arquivo, mimetype="application/zip", as_attachment=True, download_name=job.nome_zip)
