"""
contabilidade_notas_routes.py — API do app "Contabilidade - Notas".

  GET /api/contabilidade_notas/meses                      meses com dado
  GET /api/contabilidade_notas/resumo?ym=2026-09          totais por EMPRESA + emissão por dia
  GET /api/contabilidade_notas/notas?ym=2026-09&cnpj=...  as notas de uma empresa, linha a linha

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

from flask import Blueprint, jsonify, request

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


def _carregar(ym: str) -> dict:
    """Junta os postos do mês. Cacheado pela assinatura (arquivo, mtime)."""
    arqs = _arquivos(ym)
    assinatura = tuple(sorted((p, os.path.getmtime(f)) for p, f in arqs.items()))
    with _cache_lock:
        c = _cache.get(ym)
        if c and c["assinatura"] == assinatura:
            return c

    notas, coleta, erros, vistos, repetidas = [], {}, {}, set(), 0
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
            notas.append(n)

    c = {
        "assinatura": assinatura,
        "notas": notas,
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
    return {"emit_qtd": 0, "emit_valor": 0.0, "canc_qtd": 0, "canc_valor": 0.0}


def _soma(acc: dict, n: dict) -> None:
    v = float(n.get("valor") or 0)
    acc["emit_qtd"] += 1
    acc["emit_valor"] += v
    if _canc(n):
        acc["canc_qtd"] += 1
        acc["canc_valor"] += v


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
    return jsonify({
        "ok": True, "ym": ym, "empresas": lista, "total": _fecha(total), "por_dia": dias,
        "coleta": c["coleta"], "postos_sem_dado": c["postos_sem_dado"], "repetidas": c["repetidas"],
        "duplas_emitida_cancelada": c["duplas"],
    })


@contabilidade_notas_bp.get("/api/contabilidade_notas/notas")
def api_notas():
    ym = _ym_arg()
    cnpj = re.sub(r"\D", "", request.args.get("cnpj") or "")
    if not ym or not cnpj:
        return jsonify({"ok": False, "error": "informe ym=AAAA-MM e cnpj"}), 400
    c = _carregar(ym)
    notas = [n for n in c["notas"] if n["cnpj"] == cnpj]
    notas.sort(key=lambda n: (str(n.get("data_emissao") or ""), str(n.get("hora") or ""), n.get("NotaFiscal") or 0))
    total = _zeros()
    for n in notas:
        _soma(total, n)
    return jsonify({
        "ok": True, "ym": ym, "cnpj": cnpj,
        "nome": (str(notas[0].get("Empresa") or "").strip() if notas else ""),
        "total": _fecha(total), "notas": notas,
        "coleta": c["coleta"], "postos_sem_dado": c["postos_sem_dado"],
    })
