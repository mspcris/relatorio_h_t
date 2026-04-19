"""
API analítica completa do KPI Receita x Despesa para integração com Manus.

Exposição de TODOS os dados da página kpi_receita_despesa.html em endpoints
JSON prontos para consumo por LLMs / agentes externos.

Replica a lógica de agregação do frontend (JS embutido em kpi_receita_despesa.html):
  - Regras de retirada (RETIRADA/CAMPINHO)
  - Filtros por posto, grupo (Altamiro/Couto/Todos) ou lista arbitrária
  - Dimensões de receita (tipo / forma / servico) e despesa (plano_principal / plano / tipo)
  - Agregação por mês, intervalo, MoM, YoY
  - Ranking de postos por variação
  - Drilldown de variação por plano/tipo
  - Análise completa consolidada (pacote executivo)

Endpoints:
  GET /api/receita_despesa/contexto
  GET /api/receita_despesa/resumo
  GET /api/receita_despesa/serie
  GET /api/receita_despesa/crescimento
  GET /api/receita_despesa/ranking_postos
  GET /api/receita_despesa/composicao
  GET /api/receita_despesa/drilldown_variacao
  GET /api/receita_despesa/posto_detalhe
  GET /api/receita_despesa/alertas
  GET /api/receita_despesa/analise_completa
  GET /api/receita_despesa/pergunta_assistida

Configuração por env:
  FIN_JSON_DIR  — pasta com fin_*.json (default /opt/relatorio_h_t/json_consolidado)
"""
from __future__ import annotations

import json
import logging
import os
from collections import defaultdict
from pathlib import Path
from typing import Any

from flask import Blueprint, jsonify, request

log = logging.getLogger(__name__)

receita_despesa_bp = Blueprint("receita_despesa_api", __name__)


# ============================================================
# DEFENSE-IN-DEPTH AUTH
# ----------------------------------------------------------------
# O nginx já faz auth_request em /api/receita_despesa/*. Este hook
# revalida dentro do Flask — se alguém bypassar o nginx (acesso direto
# à porta 8020), ainda assim recebe 401 sem cookie nem X-Manus-Key.
# ============================================================

@receita_despesa_bp.before_request
def _exigir_auth():
    try:
        from auth_routes import require_auth_or_key
    except Exception as _e:  # pragma: no cover
        log.error("receita_despesa: auth_routes indisponível (%s) — bloqueando", _e)
        return jsonify({"ok": False, "error": "auth indisponível"}), 503
    principal, _ = require_auth_or_key()
    if not principal:
        return jsonify({
            "ok": False,
            "error": "não autenticado",
            "hint": "envie cookie de sessão válido OU header X-Manus-Key",
        }), 401
    return None

# ============================================================
# CONFIG
# ============================================================

JSON_DIR = os.getenv(
    "FIN_JSON_DIR",
    "/opt/relatorio_h_t/json_consolidado",
)

GRUPOS_POSTO: dict[str, list[str]] = {
    "todos": ["A", "B", "C", "D", "G", "I", "J", "M", "N", "P", "R", "X", "Y"],
    "altamiro": ["A", "B", "G", "I", "N", "R", "X", "Y"],
    "couto": ["C", "D", "J", "M", "P"],
}

INDICADORES = (
    "fin_receita_tipo",
    "fin_receita_forma",
    "fin_receita_lancamento",
    "fin_despesa_planodeprincipal",
    "fin_despesa_plano",
    "fin_despesa_tipo",
)

# ============================================================
# LOADER COM CACHE POR mtime
# ============================================================

_cache: dict[str, tuple[float, dict]] = {}


def _load(ind: str) -> dict | None:
    path = Path(JSON_DIR) / f"{ind}.json"
    try:
        mtime = path.stat().st_mtime
    except FileNotFoundError:
        return None
    cached = _cache.get(ind)
    if cached and cached[0] == mtime:
        return cached[1]
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    _cache[ind] = (mtime, data)
    return data


# ============================================================
# HELPERS
# ============================================================


def _truthy(v: str | None) -> bool:
    if v is None:
        return False
    return v.strip().lower() in ("1", "true", "sim", "yes", "on", "y")


def _resolve_postos(raw: str | None) -> list[str]:
    """Aceita: None/'' (=todos), nome de grupo (altamiro/couto/todos) ou lista 'A,B,C'."""
    if not raw:
        return list(GRUPOS_POSTO["todos"])
    low = raw.strip().lower()
    if low in GRUPOS_POSTO:
        return list(GRUPOS_POSTO[low])
    return [p.strip().upper() for p in raw.split(",") if p.strip()]


def _load_postos_info() -> dict:
    """Nomes oficiais dos postos, vindos de cad_endereco (via qualidade_agenda.json).

    Fonte única de verdade: tabela cad_endereco (Codigo + Descricao).
    export_qualidade_agenda.py gera qualidade_agenda.json com postos_info:
    {letra: {letra, idEndereco, nome}}.
    """
    path = Path(JSON_DIR) / "qualidade_agenda.json"
    try:
        mtime = path.stat().st_mtime
    except FileNotFoundError:
        return {}
    cached = _cache.get("__postos_info__")
    if cached and cached[0] == mtime:
        return cached[1]
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        info = data.get("postos_info") or {}
    except (OSError, ValueError):
        info = {}
    _cache["__postos_info__"] = (mtime, info)
    return info


def _nome_grupo(postos: list[str]) -> str | None:
    """Retorna 'altamiro'/'couto'/'todos' se a lista corresponder a um grupo, senão None."""
    s = sorted(postos)
    for nome, lst in GRUPOS_POSTO.items():
        if sorted(lst) == s:
            return nome
    return None


def _num(v: Any) -> float:
    try:
        n = float(v)
        if n != n:  # NaN
            return 0.0
        return n
    except (TypeError, ValueError):
        return 0.0


def _is_ret_tipo(r: dict) -> bool:
    v = (r.get("tipo") or r.get("Tipo") or "").strip().upper()
    return v == "RETIRADA"


def _is_ret_plano(r: dict) -> bool:
    v = (r.get("plano") or r.get("Plano") or "").strip().upper()
    return v == "RETIRADA"


def _is_ret_pp(r: dict) -> bool:
    v = (r.get("PlanoPrincipal") or r.get("planoprincipal") or "").strip().upper()
    return v == "CAMPINHO"


def _linhas(ds: dict | None, mes: str, postos: list[str]) -> list[dict]:
    if not ds:
        return []
    dm = (ds.get("dados") or {}).get(mes) or {}
    out: list[dict] = []
    for p in postos:
        bloco = dm.get(p)
        if bloco and isinstance(bloco.get("linhas"), list):
            out.extend(bloco["linhas"])
    return out


def _meses_disponiveis() -> list[str]:
    ds = _load("fin_receita_tipo")
    return list(ds.get("meses") or []) if ds else []


def _meses_em_intervalo(de: str | None, ate: str | None) -> list[str]:
    meses = _meses_disponiveis()
    return [m for m in meses if (not de or m >= de) and (not ate or m <= ate)]


def _ym_prev(ym: str) -> str | None:
    """Mês anterior a YYYY-MM, sem consultar disponibilidade."""
    try:
        y, m = ym.split("-")
        y_i, m_i = int(y), int(m)
    except Exception:
        return None
    if m_i == 1:
        return f"{y_i - 1}-12"
    return f"{y_i}-{m_i - 1:02d}"


def _ym_yoy(ym: str) -> str | None:
    try:
        y, m = ym.split("-")
        return f"{int(y) - 1}-{m}"
    except Exception:
        return None


def _round2(v: float | None) -> float | None:
    if v is None:
        return None
    return round(v, 2)


def _pct(a: float | None, b: float | None) -> float | None:
    """Variação percentual: ((a-b)/b)*100. None se b=0/None."""
    if a is None or b is None or b == 0:
        return None
    return round(((a - b) / b) * 100.0, 2)


# ============================================================
# AGREGAÇÕES
# ============================================================


def agregar_receita(mes: str, postos: list[str]) -> dict[str, float]:
    ds_tipo = _load("fin_receita_tipo")
    ds_lanc = _load("fin_receita_lancamento")

    total = 0.0
    for r in _linhas(ds_tipo, mes, postos):
        total += _num(r.get("valorpago"))

    servicos = 0.0
    if ds_lanc:
        for r in _linhas(ds_lanc, mes, postos):
            servicos += _num(r.get("valorpago"))

    return {
        "receita_total": total,
        "receita_servicos": servicos,
        "receita_mensalidades": max(0.0, total - servicos),
    }


def agregar_despesa(mes: str, postos: list[str]) -> dict[str, float]:
    ds_tipo = _load("fin_despesa_tipo")

    base = 0.0
    retir = 0.0
    for r in _linhas(ds_tipo, mes, postos):
        v = _num(r.get("valorpago"))
        if not v:
            continue
        if _is_ret_tipo(r):
            retir += v
        else:
            base += v

    return {
        "despesa_sem_retirada": base,
        "despesa_com_retirada": base + retir,
        "retirada": retir,
    }


def agregar_periodo(meses: list[str], postos: list[str], retirada: bool) -> dict:
    rec_tot = rec_serv = rec_mens = 0.0
    desp_sem = desp_com = retir = 0.0
    por_mes: list[dict] = []

    for ym in meses:
        r = agregar_receita(ym, postos)
        d = agregar_despesa(ym, postos)
        rec_tot += r["receita_total"]
        rec_serv += r["receita_servicos"]
        rec_mens += r["receita_mensalidades"]
        desp_sem += d["despesa_sem_retirada"]
        desp_com += d["despesa_com_retirada"]
        retir += d["retirada"]

        desp_mes = d["despesa_com_retirada"] if retirada else d["despesa_sem_retirada"]
        res_mes = r["receita_total"] - desp_mes
        marg_mes = (res_mes / r["receita_total"] * 100.0) if r["receita_total"] > 0 else None

        por_mes.append({
            "mes": ym,
            "receita_total": _round2(r["receita_total"]),
            "receita_servicos": _round2(r["receita_servicos"]),
            "receita_mensalidades": _round2(r["receita_mensalidades"]),
            "despesa": _round2(desp_mes),
            "despesa_sem_retirada": _round2(d["despesa_sem_retirada"]),
            "despesa_com_retirada": _round2(d["despesa_com_retirada"]),
            "retirada": _round2(d["retirada"]),
            "resultado": _round2(res_mes),
            "margem_pct": _round2(marg_mes) if marg_mes is not None else None,
        })

    desp_usada_tot = desp_com if retirada else desp_sem
    res_tot = rec_tot - desp_usada_tot
    marg_tot = (res_tot / rec_tot * 100.0) if rec_tot > 0 else None

    return {
        "receita_total": _round2(rec_tot),
        "receita_servicos": _round2(rec_serv),
        "receita_mensalidades": _round2(rec_mens),
        "despesa_total": _round2(desp_usada_tot),
        "despesa_sem_retirada": _round2(desp_sem),
        "despesa_com_retirada": _round2(desp_com),
        "retirada_total": _round2(retir),
        "resultado": _round2(res_tot),
        "margem_pct": _round2(marg_tot) if marg_tot is not None else None,
        "n_meses": len(meses),
        "por_mes": por_mes,
    }


# ============================================================
# COMPOSIÇÕES (drilldown)
# ============================================================


def composicao_receita(meses: list[str], postos: list[str], dimensao: str) -> dict:
    """dimensao: tipo | forma | servico"""
    if dimensao == "forma":
        ds = _load("fin_receita_forma")
        key = "forma"
    elif dimensao == "servico":
        ds = _load("fin_receita_lancamento")
        key = "classe"
    else:
        ds = _load("fin_receita_tipo")
        key = "tipo"

    if not ds:
        return {"itens": [], "total": 0.0}

    acc: dict[str, float] = defaultdict(float)
    for ym in meses:
        for r in _linhas(ds, ym, postos):
            nome = (r.get(key) or "Sem classificação")
            nome = (nome.strip() if isinstance(nome, str) else str(nome)) or "Sem classificação"
            acc[nome] += _num(r.get("valorpago"))

    itens = [{"grupo": k, "valor": _round2(v)} for k, v in acc.items() if v]
    itens.sort(key=lambda x: x["valor"], reverse=True)
    total = sum(x["valor"] for x in itens)
    for it in itens:
        it["pct"] = round((it["valor"] / total * 100.0) if total > 0 else 0.0, 2)

    return {"itens": itens, "total": _round2(total)}


def composicao_despesa(
    meses: list[str], postos: list[str], dimensao: str, retirada: bool
) -> dict:
    """dimensao: plano_principal | plano | tipo"""
    if dimensao == "plano_principal":
        ds = _load("fin_despesa_planodeprincipal")
        is_ret = _is_ret_pp
    elif dimensao == "plano":
        ds = _load("fin_despesa_plano")
        is_ret = _is_ret_plano
    else:
        ds = _load("fin_despesa_tipo")
        is_ret = _is_ret_tipo

    if not ds:
        return {"itens": [], "total": 0.0}

    def _norm(v: Any, fallback: str) -> str:
        if v is None:
            return fallback
        s = str(v).strip()
        return s if s else fallback

    agregados: dict[tuple, dict] = {}
    for ym in meses:
        for r in _linhas(ds, ym, postos):
            v = _num(r.get("valorpago"))
            if not v:
                continue
            if (not retirada) and is_ret(r):
                continue

            pp = _norm(r.get("PlanoPrincipal") or r.get("planoprincipal"), "Sem classificação")
            plano = _norm(r.get("plano") or r.get("Plano"), "Sem plano")
            tipo = _norm(r.get("tipo") or r.get("Tipo"), "Sem classificação")

            if dimensao == "plano_principal":
                k = (pp,)
                info = {"plano_principal": pp}
            elif dimensao == "plano":
                k = (pp, plano)
                info = {"plano_principal": pp, "plano": plano}
            else:
                k = (pp, plano, tipo)
                info = {"plano_principal": pp, "plano": plano, "tipo": tipo}

            if k in agregados:
                agregados[k]["valor"] += v
            else:
                info["valor"] = v
                agregados[k] = info

    itens = list(agregados.values())
    itens.sort(key=lambda x: x["valor"], reverse=True)
    total = sum(x["valor"] for x in itens)
    for it in itens:
        it["valor"] = _round2(it["valor"])
        it["pct"] = round((it["valor"] / total * 100.0) if total > 0 else 0.0, 2)

    return {"itens": itens, "total": _round2(total)}


def _label_composicao(item: dict, tipo: str, dim: str) -> str:
    if tipo == "receita":
        return item.get("grupo") or "?"
    if dim == "plano_principal":
        return item.get("plano_principal") or "?"
    if dim == "plano":
        return f"{item.get('plano_principal','?')} / {item.get('plano','?')}"
    return f"{item.get('plano_principal','?')} / {item.get('plano','?')} / {item.get('tipo','?')}"


def calcular_variacao(
    mes_ref: str,
    mes_comp: str,
    postos: list[str],
    tipo: str,
    dimensao: str,
    retirada: bool,
) -> list[dict]:
    """Retorna diferenças (ref - comp) por grupo, ordenadas por |delta|."""
    if tipo == "receita":
        a = composicao_receita([mes_ref], postos, dimensao)
        b = composicao_receita([mes_comp], postos, dimensao)
    else:
        a = composicao_despesa([mes_ref], postos, dimensao, retirada)
        b = composicao_despesa([mes_comp], postos, dimensao, retirada)

    ma = {_label_composicao(i, tipo, dimensao): i for i in a["itens"]}
    mb = {_label_composicao(i, tipo, dimensao): i for i in b["itens"]}

    out: list[dict] = []
    for k in set(ma) | set(mb):
        va = _num(ma.get(k, {}).get("valor", 0))
        vb = _num(mb.get(k, {}).get("valor", 0))
        delta = va - vb
        out.append({
            "grupo": k,
            "valor_mes_ref": _round2(va),
            "valor_mes_comp": _round2(vb),
            "delta_abs": _round2(delta),
            "delta_pct": _pct(va, vb),
        })
    out.sort(key=lambda x: abs(x["delta_abs"] or 0.0), reverse=True)
    return out


# ============================================================
# RANKING DE POSTOS
# ============================================================


def ranking_postos(
    mes: str,
    mes_comp: str | None,
    postos_universo: list[str],
    metrica: str,
    retirada: bool,
) -> list[dict]:
    def _valor(r: dict, d: dict, desp: float, res: float, metrica: str) -> float:
        if metrica == "receita":
            return r["receita_total"]
        if metrica == "despesa":
            return desp
        if metrica == "margem":
            return (res / r["receita_total"] * 100.0) if r["receita_total"] > 0 else 0.0
        return res  # resultado

    linhas: list[dict] = []
    for p in postos_universo:
        r1 = agregar_receita(mes, [p])
        d1 = agregar_despesa(mes, [p])
        desp1 = d1["despesa_com_retirada"] if retirada else d1["despesa_sem_retirada"]
        res1 = r1["receita_total"] - desp1

        v_ref = _valor(r1, d1, desp1, res1, metrica)

        r0 = d0 = None
        desp0 = res0 = None
        v_comp = None
        if mes_comp:
            r0 = agregar_receita(mes_comp, [p])
            d0 = agregar_despesa(mes_comp, [p])
            desp0 = d0["despesa_com_retirada"] if retirada else d0["despesa_sem_retirada"]
            res0 = r0["receita_total"] - desp0
            v_comp = _valor(r0, d0, desp0, res0, metrica)

        delta = (v_ref - v_comp) if v_comp is not None else None
        delta_pct = _pct(v_ref, v_comp)

        linhas.append({
            "posto": p,
            "receita_mes_ref": _round2(r1["receita_total"]),
            "despesa_mes_ref": _round2(desp1),
            "resultado_mes_ref": _round2(res1),
            "margem_mes_ref_pct": (_round2(res1 / r1["receita_total"] * 100.0) if r1["receita_total"] > 0 else None),
            "receita_mes_comp": _round2(r0["receita_total"]) if r0 else None,
            "despesa_mes_comp": _round2(desp0) if desp0 is not None else None,
            "resultado_mes_comp": _round2(res0) if res0 is not None else None,
            "valor_metrica_ref": _round2(v_ref),
            "valor_metrica_comp": _round2(v_comp) if v_comp is not None else None,
            "delta_abs": _round2(delta) if delta is not None else None,
            "delta_pct": delta_pct,
        })

    # ordena: quem mais variou primeiro (em valor absoluto), com sinal correto
    linhas.sort(
        key=lambda x: (x["delta_abs"] if x["delta_abs"] is not None else x["valor_metrica_ref"] or 0.0),
        reverse=True,
    )
    return linhas


# ============================================================
# ALERTAS (anomalias simples)
# ============================================================


def detectar_alertas(postos: list[str], retirada: bool) -> list[dict]:
    """
    Alertas automáticos baseados em comparação com média dos 6 meses anteriores:
    - Despesa > média+1σ
    - Receita < média-1σ
    - Resultado negativo
    - Queda de receita YoY > 10%
    - Alta de despesa YoY > 20%
    """
    meses = _meses_disponiveis()
    if len(meses) < 2:
        return []

    alertas: list[dict] = []
    # pega últimos 6 meses para análise
    ultimos = meses[-6:] if len(meses) >= 6 else meses[:]
    ag = agregar_periodo(ultimos, postos, retirada)
    por_mes = ag["por_mes"]
    if len(por_mes) < 2:
        return []

    mes_atual = por_mes[-1]
    anteriores = por_mes[:-1]

    def _media_desv(valores: list[float]) -> tuple[float, float]:
        if not valores:
            return 0.0, 0.0
        m = sum(valores) / len(valores)
        if len(valores) < 2:
            return m, 0.0
        var = sum((v - m) ** 2 for v in valores) / (len(valores) - 1)
        return m, var ** 0.5

    # Despesa anômala
    desp_hist = [m["despesa"] for m in anteriores if m["despesa"]]
    md, sdd = _media_desv(desp_hist)
    if md > 0 and mes_atual["despesa"] > md + sdd and sdd > 0:
        alertas.append({
            "severidade": "alta",
            "tipo": "despesa_acima_da_media",
            "mes": mes_atual["mes"],
            "valor": mes_atual["despesa"],
            "media_6m": _round2(md),
            "desvio_padrao_6m": _round2(sdd),
            "excesso_pct": _pct(mes_atual["despesa"], md),
            "mensagem": f"Despesa em {mes_atual['mes']} está acima da média+1σ dos últimos 6 meses.",
        })

    # Receita abaixo do esperado
    rec_hist = [m["receita_total"] for m in anteriores if m["receita_total"]]
    mr, sdr = _media_desv(rec_hist)
    if mr > 0 and mes_atual["receita_total"] < mr - sdr and sdr > 0:
        alertas.append({
            "severidade": "alta",
            "tipo": "receita_abaixo_da_media",
            "mes": mes_atual["mes"],
            "valor": mes_atual["receita_total"],
            "media_6m": _round2(mr),
            "desvio_padrao_6m": _round2(sdr),
            "deficit_pct": _pct(mes_atual["receita_total"], mr),
            "mensagem": f"Receita em {mes_atual['mes']} está abaixo da média-1σ dos últimos 6 meses.",
        })

    # Resultado negativo
    if mes_atual["resultado"] is not None and mes_atual["resultado"] < 0:
        alertas.append({
            "severidade": "alta" if mes_atual["margem_pct"] and mes_atual["margem_pct"] < -10 else "media",
            "tipo": "resultado_negativo",
            "mes": mes_atual["mes"],
            "valor": mes_atual["resultado"],
            "margem_pct": mes_atual["margem_pct"],
            "mensagem": f"Resultado negativo em {mes_atual['mes']}: R$ {mes_atual['resultado']:.2f}.",
        })

    # YoY
    ym_yoy = _ym_yoy(mes_atual["mes"])
    if ym_yoy and ym_yoy in meses:
        ag_yoy = agregar_periodo([ym_yoy], postos, retirada)
        if ag_yoy["por_mes"]:
            pm_yoy = ag_yoy["por_mes"][0]
            d_rec = _pct(mes_atual["receita_total"], pm_yoy["receita_total"])
            d_desp = _pct(mes_atual["despesa"], pm_yoy["despesa"])
            if d_rec is not None and d_rec < -10.0:
                alertas.append({
                    "severidade": "alta",
                    "tipo": "queda_receita_yoy",
                    "mes": mes_atual["mes"],
                    "delta_pct": d_rec,
                    "mensagem": f"Receita caiu {abs(d_rec):.1f}% vs {ym_yoy} (ano anterior).",
                })
            if d_desp is not None and d_desp > 20.0:
                alertas.append({
                    "severidade": "alta",
                    "tipo": "alta_despesa_yoy",
                    "mes": mes_atual["mes"],
                    "delta_pct": d_desp,
                    "mensagem": f"Despesa subiu {d_desp:.1f}% vs {ym_yoy} (ano anterior).",
                })

    return alertas


# ============================================================
# ENDPOINTS
# ============================================================


def _filtros_padrao_req():
    """Extrai filtros padrão de request.args."""
    de = request.args.get("de") or None
    ate = request.args.get("ate") or None
    postos_raw = request.args.get("postos")
    postos = _resolve_postos(postos_raw)
    retirada = _truthy(request.args.get("retirada"))
    grupo_nome = _nome_grupo(postos)
    return {
        "de": de,
        "ate": ate,
        "postos": postos,
        "postos_raw": postos_raw,
        "grupo_nome": grupo_nome,
        "retirada": retirada,
    }


@receita_despesa_bp.get("/api/receita_despesa/contexto")
def ep_contexto():
    """Pacote de descoberta — LLM/agente lê UMA vez e passa a entender o KPI."""
    meses = _meses_disponiveis()
    return jsonify({
        "ok": True,
        "kpi": "receita_despesa",
        "titulo": "KPI Receitas x Despesas",
        "descricao": (
            "Dashboard financeiro agregando receitas e despesas de 13 servidores SQL "
            "(um por posto), com decomposição por plano de contas, tipo de conta, forma "
            "de recebimento e classe de serviço. Permite análise mensal, série temporal, "
            "MoM, YoY e drilldown por qualquer dimensão."
        ),
        "pagina_html": "/kpi_receita_despesa.html",
        "grupos_postos": GRUPOS_POSTO,
        "postos_individuais": list(GRUPOS_POSTO["todos"]),
        "dimensoes_receita": {
            "tipo": "Tipo de conta de receita (mensalidade, lançamento, etc.)",
            "forma": "Forma de recebimento (dinheiro, cartão, PIX, égide, etc.)",
            "servico": "Classe do serviço (somente receita de lançamentos)",
        },
        "dimensoes_despesa": {
            "plano_principal": "Plano principal (macro-categoria)",
            "plano": "Plano de contas (nível intermediário)",
            "tipo": "Tipo de conta (granularidade fina)",
        },
        "modos_retirada": {
            "0 (padrão)": "Exclui RETIRADA/CAMPINHO da despesa — visão operacional",
            "1": "Inclui retirada na despesa — visão executiva/sócio",
        },
        "periodo_disponivel": {
            "mes_inicial": meses[0] if meses else None,
            "mes_final": meses[-1] if meses else None,
            "n_meses": len(meses),
            "meses": meses,
        },
        "endpoints": {
            "resumo": "/api/receita_despesa/resumo?de=YYYY-MM&ate=YYYY-MM&postos=A,B|altamiro|couto|todos&retirada=0|1",
            "serie": "/api/receita_despesa/serie?postos=...&retirada=0|1",
            "crescimento": "/api/receita_despesa/crescimento?postos=...&retirada=0|1",
            "ranking_postos": "/api/receita_despesa/ranking_postos?mes=YYYY-MM&metrica=despesa|receita|resultado|margem&vs=mes_anterior|ano_anterior&grupo=todos|altamiro|couto&retirada=0|1",
            "composicao": "/api/receita_despesa/composicao?de=YYYY-MM&ate=YYYY-MM&postos=...&tipo=receita|despesa&dimensao=tipo|forma|servico|plano_principal|plano&retirada=0|1",
            "composicao_multi": "/api/receita_despesa/composicao_multi?de=YYYY-MM&ate=YYYY-MM&postos=altamiro|couto|todos|A,B,C&tipo=receita|despesa&dimensao=...&top=10 — USE ISTO para composição segregada por posto em UMA chamada",
            "drilldown_variacao": "/api/receita_despesa/drilldown_variacao?mes_ref=YYYY-MM&mes_comp=YYYY-MM&postos=...&tipo=despesa|receita&dimensao=...&retirada=0|1",
            "drilldown_variacao_multi": "/api/receita_despesa/drilldown_variacao_multi?mes_ref=YYYY-MM&mes_comp=YYYY-MM&postos=altamiro|couto|todos|A,B,C&tipo=...&dimensao=...&top=5 — USE ISTO para variação segregada por posto em UMA chamada",
            "posto_detalhe": "/api/receita_despesa/posto_detalhe?posto=A&mes=YYYY-MM&retirada=0|1",
            "alertas": "/api/receita_despesa/alertas?postos=...&retirada=0|1",
            "analise_completa": "/api/receita_despesa/analise_completa?mes=YYYY-MM&postos=...&retirada=0|1",
            "pergunta_assistida": "/api/receita_despesa/pergunta_assistida?q=texto+livre",
        },
        "instrucoes_para_agente": {
            "perguntas_obrigatorias_antes_de_responder": [
                "Todos os postos, grupo Altamiro (A B G I N R X Y), grupo Couto (C D J M P) ou posto específico?",
                "Considera RETIRADA/CAMPINHO? Sem retirada é operacional; com retirada é executivo.",
                "Período: mês específico, intervalo ou últimos N meses?",
                "Para comparações, comparar com mês anterior (MoM) ou mesmo mês do ano anterior (YoY)?",
            ],
            "exemplos_perguntas_suportadas": [
                "Qual posto aumentou o custo em março?",
                "Qual tipo de conta subiu mais no posto A em março?",
                "Estou crescendo ou encolhendo nos últimos 12 meses?",
                "O Altamiro está encolhendo ou crescendo em despesa?",
                "Quem gastou mais este mês?",
                "Qual plano de contas subiu mais no ano contra o ano passado?",
                "Como está a margem do Altamiro vs Couto?",
                "Tem algum posto com resultado negativo?",
                "O que mais pesou na despesa deste mês?",
                "Quanto cresceu a receita de serviços vs mensalidades?",
            ],
            "fluxo_recomendado": [
                "1) GET /api/receita_despesa/contexto (descoberta)",
                "2) Pergunte ao usuário os filtros (postos/grupo/retirada/período)",
                "3) GET /api/receita_despesa/analise_completa com os filtros → responde 80% das perguntas executivas numa só chamada",
                "4) Drilldown via /composicao ou /drilldown_variacao conforme o seguimento",
                "5) /alertas para sinalizar anomalias sem perguntar",
            ],
        },
        "glossario": {
            "receita_total": "Soma de Fin_Receita.Valorpago no período (todos os tipos)",
            "receita_servicos": "Subset de Receita em que ct.tipo='lancamento' (serviços avulsos)",
            "receita_mensalidades": "Receita total menos receita de serviços (proxy para receita recorrente)",
            "despesa_sem_retirada": "Despesa operacional, exclui tipo=RETIRADA do Fin_ContaTipo",
            "despesa_com_retirada": "Inclui RETIRADA/CAMPINHO (visão de saída total de caixa)",
            "retirada": "Saída financeira para sócios (plano/tipo=RETIRADA ou PlanoPrincipal=CAMPINHO)",
            "resultado": "receita_total - despesa_usada (sinal positivo = superávit)",
            "margem_pct": "resultado / receita_total * 100",
            "delta_abs": "Diferença absoluta entre ref e comp",
            "delta_pct": "((ref - comp) / comp) * 100",
        },
    })


@receita_despesa_bp.get("/api/receita_despesa/resumo")
def ep_resumo():
    f = _filtros_padrao_req()
    meses = _meses_em_intervalo(f["de"], f["ate"])
    if not meses:
        return jsonify({"ok": False, "error": "Nenhum mês disponível no intervalo informado"}), 400
    ag = agregar_periodo(meses, f["postos"], f["retirada"])
    return jsonify({
        "ok": True,
        "filtros": f,
        "periodo": {"mes_inicial": meses[0], "mes_final": meses[-1], "n_meses": len(meses)},
        "resumo": ag,
    })


@receita_despesa_bp.get("/api/receita_despesa/serie")
def ep_serie():
    postos = _resolve_postos(request.args.get("postos"))
    retirada = _truthy(request.args.get("retirada"))
    meses = _meses_disponiveis()
    if not meses:
        return jsonify({"ok": False, "error": "Sem dados disponíveis"}), 404
    ag = agregar_periodo(meses, postos, retirada)
    return jsonify({
        "ok": True,
        "filtros": {"postos": postos, "grupo_nome": _nome_grupo(postos), "retirada": retirada},
        "periodo": {"mes_inicial": meses[0], "mes_final": meses[-1], "n_meses": len(meses)},
        "serie": ag["por_mes"],
        "totais": {k: v for k, v in ag.items() if k != "por_mes"},
    })


@receita_despesa_bp.get("/api/receita_despesa/crescimento")
def ep_crescimento():
    """Série com MoM e YoY em receita, despesa e resultado."""
    postos = _resolve_postos(request.args.get("postos"))
    retirada = _truthy(request.args.get("retirada"))
    meses = _meses_disponiveis()
    if not meses:
        return jsonify({"ok": False, "error": "Sem dados disponíveis"}), 404
    ag = agregar_periodo(meses, postos, retirada)
    por_mes = ag["por_mes"]
    idx_mes = {m["mes"]: i for i, m in enumerate(por_mes)}

    result: list[dict] = []
    for i, m in enumerate(por_mes):
        ym = m["mes"]
        prev_mom = por_mes[i - 1] if i > 0 else None
        yoy_key = _ym_yoy(ym)
        prev_yoy = por_mes[idx_mes[yoy_key]] if yoy_key in idx_mes else None

        result.append({
            "mes": ym,
            "receita": m["receita_total"],
            "despesa": m["despesa"],
            "resultado": m["resultado"],
            "margem_pct": m["margem_pct"],
            "mom": {
                "mes_anterior": prev_mom["mes"] if prev_mom else None,
                "receita_pct": _pct(m["receita_total"], prev_mom["receita_total"]) if prev_mom else None,
                "despesa_pct": _pct(m["despesa"], prev_mom["despesa"]) if prev_mom else None,
                "resultado_pct": _pct(m["resultado"], prev_mom["resultado"]) if prev_mom else None,
            },
            "yoy": {
                "mes_ano_anterior": prev_yoy["mes"] if prev_yoy else None,
                "receita_pct": _pct(m["receita_total"], prev_yoy["receita_total"]) if prev_yoy else None,
                "despesa_pct": _pct(m["despesa"], prev_yoy["despesa"]) if prev_yoy else None,
                "resultado_pct": _pct(m["resultado"], prev_yoy["resultado"]) if prev_yoy else None,
            },
        })

    # Tendência simples
    if len(por_mes) >= 3:
        ult = por_mes[-3:]
        rec_trend = ult[-1]["receita_total"] - ult[0]["receita_total"]
        desp_trend = ult[-1]["despesa"] - ult[0]["despesa"]
    else:
        rec_trend = desp_trend = 0

    return jsonify({
        "ok": True,
        "filtros": {"postos": postos, "grupo_nome": _nome_grupo(postos), "retirada": retirada},
        "crescimento": result,
        "resumo_tendencia": {
            "receita_3m_abs": _round2(rec_trend),
            "despesa_3m_abs": _round2(desp_trend),
            "receita_3m_sentido": "subindo" if rec_trend > 0 else "caindo" if rec_trend < 0 else "estavel",
            "despesa_3m_sentido": "subindo" if desp_trend > 0 else "caindo" if desp_trend < 0 else "estavel",
        },
    })


@receita_despesa_bp.get("/api/receita_despesa/ranking_postos")
def ep_ranking_postos():
    mes = request.args.get("mes")
    if not mes:
        return jsonify({"ok": False, "error": "Parâmetro 'mes' obrigatório (YYYY-MM)"}), 400

    metrica = (request.args.get("metrica") or "despesa").lower()
    if metrica not in ("receita", "despesa", "resultado", "margem"):
        return jsonify({"ok": False, "error": "metrica deve ser receita|despesa|resultado|margem"}), 400

    vs = (request.args.get("vs") or "mes_anterior").lower()
    grupo = (request.args.get("grupo") or "todos").lower()
    retirada = _truthy(request.args.get("retirada"))

    postos_univ = GRUPOS_POSTO.get(grupo, GRUPOS_POSTO["todos"])

    meses = _meses_disponiveis()
    mes_comp = None
    if vs == "ano_anterior":
        yoy = _ym_yoy(mes)
        if yoy and yoy in meses:
            mes_comp = yoy
    else:
        if mes in meses:
            idx = meses.index(mes)
            mes_comp = meses[idx - 1] if idx > 0 else None

    ranking = ranking_postos(mes, mes_comp, postos_univ, metrica, retirada)

    return jsonify({
        "ok": True,
        "filtros": {
            "mes": mes,
            "mes_comp": mes_comp,
            "metrica": metrica,
            "vs": vs,
            "grupo": grupo,
            "postos_universo": postos_univ,
            "retirada": retirada,
        },
        "ranking": ranking,
    })


@receita_despesa_bp.get("/api/receita_despesa/composicao")
def ep_composicao():
    f = _filtros_padrao_req()
    if not f["ate"]:
        f["ate"] = f["de"]
    meses = _meses_em_intervalo(f["de"], f["ate"])
    if not meses:
        return jsonify({"ok": False, "error": "Nenhum mês disponível no intervalo informado"}), 400

    tipo = (request.args.get("tipo") or "despesa").lower()
    if tipo not in ("receita", "despesa"):
        return jsonify({"ok": False, "error": "tipo deve ser receita|despesa"}), 400

    if tipo == "receita":
        dim = (request.args.get("dimensao") or "tipo").lower()
        if dim not in ("tipo", "forma", "servico"):
            return jsonify({"ok": False, "error": "dimensao receita: tipo|forma|servico"}), 400
        comp = composicao_receita(meses, f["postos"], dim)
    else:
        dim = (request.args.get("dimensao") or "plano_principal").lower()
        if dim not in ("plano_principal", "plano", "tipo"):
            return jsonify({"ok": False, "error": "dimensao despesa: plano_principal|plano|tipo"}), 400
        comp = composicao_despesa(meses, f["postos"], dim, f["retirada"])

    return jsonify({
        "ok": True,
        "filtros": {**f, "tipo": tipo, "dimensao": dim},
        "periodo": {"mes_inicial": meses[0], "mes_final": meses[-1], "n_meses": len(meses)},
        "composicao": comp,
    })


@receita_despesa_bp.get("/api/receita_despesa/drilldown_variacao")
def ep_drilldown():
    mes_ref = request.args.get("mes_ref")
    mes_comp = request.args.get("mes_comp")
    if not mes_ref or not mes_comp:
        return jsonify({"ok": False, "error": "mes_ref e mes_comp obrigatórios"}), 400

    tipo = (request.args.get("tipo") or "despesa").lower()
    if tipo not in ("receita", "despesa"):
        return jsonify({"ok": False, "error": "tipo deve ser receita|despesa"}), 400

    if tipo == "receita":
        dim = (request.args.get("dimensao") or "tipo").lower()
        if dim not in ("tipo", "forma", "servico"):
            return jsonify({"ok": False, "error": "dimensao receita: tipo|forma|servico"}), 400
    else:
        dim = (request.args.get("dimensao") or "tipo").lower()
        if dim not in ("plano_principal", "plano", "tipo"):
            return jsonify({"ok": False, "error": "dimensao despesa: plano_principal|plano|tipo"}), 400

    postos = _resolve_postos(request.args.get("postos"))
    retirada = _truthy(request.args.get("retirada"))

    variacoes = calcular_variacao(mes_ref, mes_comp, postos, tipo, dim, retirada)

    return jsonify({
        "ok": True,
        "filtros": {
            "mes_ref": mes_ref,
            "mes_comp": mes_comp,
            "postos": postos,
            "grupo_nome": _nome_grupo(postos),
            "tipo": tipo,
            "dimensao": dim,
            "retirada": retirada,
        },
        "variacoes": variacoes,
        "top5_aumentou": [v for v in variacoes if v["delta_abs"] and v["delta_abs"] > 0][:5],
        "top5_diminuiu": sorted(
            [v for v in variacoes if v["delta_abs"] and v["delta_abs"] < 0],
            key=lambda x: x["delta_abs"],
        )[:5],
    })


@receita_despesa_bp.get("/api/receita_despesa/drilldown_variacao_multi")
def ep_drilldown_multi():
    """Variação por posto, segregada no servidor.
    1 request HTTP → N postos. Resolve o problema de agentes externos que
    falham por SSL/rate-limit ao iterar por posto do lado cliente, e evita
    o erro de achar que postos=A,B,C segrega (não segrega: agrega).
    """
    mes_ref = request.args.get("mes_ref")
    mes_comp = request.args.get("mes_comp")
    if not mes_ref or not mes_comp:
        return jsonify({"ok": False, "error": "mes_ref e mes_comp obrigatórios"}), 400

    tipo = (request.args.get("tipo") or "despesa").lower()
    if tipo not in ("receita", "despesa"):
        return jsonify({"ok": False, "error": "tipo deve ser receita|despesa"}), 400

    if tipo == "receita":
        dim = (request.args.get("dimensao") or "tipo").lower()
        if dim not in ("tipo", "forma", "servico"):
            return jsonify({"ok": False, "error": "dimensao receita: tipo|forma|servico"}), 400
    else:
        dim = (request.args.get("dimensao") or "tipo").lower()
        if dim not in ("plano_principal", "plano", "tipo"):
            return jsonify({"ok": False, "error": "dimensao despesa: plano_principal|plano|tipo"}), 400

    postos = _resolve_postos(request.args.get("postos"))
    retirada = _truthy(request.args.get("retirada"))
    try:
        top = max(1, min(50, int(request.args.get("top") or 5)))
    except (TypeError, ValueError):
        top = 5

    por_posto: dict[str, dict] = {}
    for p in postos:
        variacoes = calcular_variacao(mes_ref, mes_comp, [p], tipo, dim, retirada)
        if not variacoes:
            por_posto[p] = {"ok": False, "motivo": "sem dados no período"}
            continue
        # Top N por magnitude do delta (maior variação em R$ primeiro)
        aumentou = sorted(
            [v for v in variacoes if (v.get("delta_abs") or 0) > 0],
            key=lambda x: x["delta_abs"],
            reverse=True,
        )[:top]
        diminuiu = sorted(
            [v for v in variacoes if (v.get("delta_abs") or 0) < 0],
            key=lambda x: x["delta_abs"],
        )[:top]
        por_posto[p] = {
            "ok": True,
            "top_aumentou": aumentou,
            "top_diminuiu": diminuiu,
            "total_itens": len(variacoes),
        }

    return jsonify({
        "ok": True,
        "filtros": {
            "mes_ref": mes_ref,
            "mes_comp": mes_comp,
            "postos": postos,
            "grupo_nome": _nome_grupo(postos),
            "tipo": tipo,
            "dimensao": dim,
            "retirada": retirada,
            "top": top,
        },
        "por_posto": por_posto,
        "postos_info": _load_postos_info(),
    })


@receita_despesa_bp.get("/api/receita_despesa/postos_info")
def ep_postos_info():
    """Nomes oficiais dos postos (cad_endereco.Descricao).

    Saída: {ok, postos_info: {letra: {letra, idEndereco, nome}}}.
    """
    info = _load_postos_info()
    return jsonify({"ok": True, "postos_info": info, "n": len(info)})


@receita_despesa_bp.get("/api/receita_despesa/composicao_multi")
def ep_composicao_multi():
    """Composição (top-N itens) por posto, segregada no servidor.
    1 request HTTP → N postos. Mesma intenção do drilldown_multi.
    """
    f = _filtros_padrao_req()
    if not f["ate"]:
        f["ate"] = f["de"]
    meses = _meses_em_intervalo(f["de"], f["ate"])
    if not meses:
        return jsonify({"ok": False, "error": "Nenhum mês disponível no intervalo informado"}), 400

    tipo = (request.args.get("tipo") or "despesa").lower()
    if tipo not in ("receita", "despesa"):
        return jsonify({"ok": False, "error": "tipo deve ser receita|despesa"}), 400

    if tipo == "receita":
        dim = (request.args.get("dimensao") or "tipo").lower()
        if dim not in ("tipo", "forma", "servico"):
            return jsonify({"ok": False, "error": "dimensao receita: tipo|forma|servico"}), 400
    else:
        dim = (request.args.get("dimensao") or "plano_principal").lower()
        if dim not in ("plano_principal", "plano", "tipo"):
            return jsonify({"ok": False, "error": "dimensao despesa: plano_principal|plano|tipo"}), 400

    try:
        top = max(1, min(100, int(request.args.get("top") or 10)))
    except (TypeError, ValueError):
        top = 10

    por_posto: dict[str, dict] = {}
    for p in f["postos"]:
        if tipo == "receita":
            comp = composicao_receita(meses, [p], dim)
        else:
            comp = composicao_despesa(meses, [p], dim, f["retirada"])
        itens = comp.get("itens") or []
        if not itens:
            por_posto[p] = {"ok": False, "motivo": "sem dados no período"}
            continue
        por_posto[p] = {
            "ok": True,
            "total": comp.get("total", 0.0),
            "top": itens[:top],
            "n_itens": len(itens),
        }

    return jsonify({
        "ok": True,
        "filtros": {**f, "tipo": tipo, "dimensao": dim, "top": top},
        "periodo": {"mes_inicial": meses[0], "mes_final": meses[-1], "n_meses": len(meses)},
        "por_posto": por_posto,
    })


@receita_despesa_bp.get("/api/receita_despesa/posto_detalhe")
def ep_posto_detalhe():
    posto = (request.args.get("posto") or "").strip().upper()
    mes = request.args.get("mes")
    if not posto or not mes:
        return jsonify({"ok": False, "error": "posto e mes obrigatórios"}), 400
    if posto not in GRUPOS_POSTO["todos"]:
        return jsonify({"ok": False, "error": f"posto inválido: {posto}"}), 400

    retirada = _truthy(request.args.get("retirada"))
    meses = _meses_disponiveis()
    if mes not in meses:
        return jsonify({"ok": False, "error": f"mes {mes} indisponível"}), 400

    mes_anterior = None
    idx = meses.index(mes)
    if idx > 0:
        mes_anterior = meses[idx - 1]
    mes_yoy = _ym_yoy(mes) if _ym_yoy(mes) in meses else None

    postos = [posto]
    resumo_mes = agregar_periodo([mes], postos, retirada)
    resumo_ant = agregar_periodo([mes_anterior], postos, retirada) if mes_anterior else None
    resumo_yoy = agregar_periodo([mes_yoy], postos, retirada) if mes_yoy else None

    comp_desp = {
        "plano_principal": composicao_despesa([mes], postos, "plano_principal", retirada),
        "plano": composicao_despesa([mes], postos, "plano", retirada),
        "tipo": composicao_despesa([mes], postos, "tipo", retirada),
    }
    comp_rec = {
        "tipo": composicao_receita([mes], postos, "tipo"),
        "forma": composicao_receita([mes], postos, "forma"),
        "servico": composicao_receita([mes], postos, "servico"),
    }

    # últimos 12 meses
    start = max(0, idx - 11)
    ultimos = meses[start:idx + 1]
    serie = agregar_periodo(ultimos, postos, retirada)["por_mes"]

    var_desp_mom = {}
    if mes_anterior:
        for d in ("plano_principal", "plano", "tipo"):
            var_desp_mom[d] = calcular_variacao(mes, mes_anterior, postos, "despesa", d, retirada)[:10]

    var_desp_yoy = {}
    if mes_yoy:
        for d in ("plano_principal", "plano", "tipo"):
            var_desp_yoy[d] = calcular_variacao(mes, mes_yoy, postos, "despesa", d, retirada)[:10]

    return jsonify({
        "ok": True,
        "filtros": {"posto": posto, "mes": mes, "mes_anterior": mes_anterior, "mes_yoy": mes_yoy, "retirada": retirada},
        "resumo_mes": resumo_mes,
        "resumo_mes_anterior": resumo_ant,
        "resumo_ano_anterior": resumo_yoy,
        "serie_ultimos_12m": serie,
        "composicao_despesa": comp_desp,
        "composicao_receita": comp_rec,
        "variacoes_despesa_vs_mes_anterior": var_desp_mom,
        "variacoes_despesa_vs_ano_anterior": var_desp_yoy,
    })


@receita_despesa_bp.get("/api/receita_despesa/alertas")
def ep_alertas():
    postos = _resolve_postos(request.args.get("postos"))
    retirada = _truthy(request.args.get("retirada"))
    return jsonify({
        "ok": True,
        "filtros": {"postos": postos, "grupo_nome": _nome_grupo(postos), "retirada": retirada},
        "alertas": detectar_alertas(postos, retirada),
    })


@receita_despesa_bp.get("/api/receita_despesa/analise_completa")
def ep_analise_completa():
    """
    Pacote executivo: resumo + ranking + composição + variações + alertas.
    Pensado para responder a maioria das perguntas diretoriais em uma única chamada.
    """
    mes = request.args.get("mes")
    if not mes:
        return jsonify({"ok": False, "error": "mes obrigatório (YYYY-MM)"}), 400

    postos = _resolve_postos(request.args.get("postos"))
    grupo_solicitado = (request.args.get("grupo") or "").strip().lower() or None
    if grupo_solicitado and grupo_solicitado in GRUPOS_POSTO:
        postos = list(GRUPOS_POSTO[grupo_solicitado])

    retirada = _truthy(request.args.get("retirada"))

    meses = _meses_disponiveis()
    if mes not in meses:
        return jsonify({
            "ok": False,
            "error": f"mes {mes} indisponível",
            "periodo_disponivel": {"mes_inicial": meses[0] if meses else None, "mes_final": meses[-1] if meses else None},
        }), 400

    idx = meses.index(mes)
    mes_anterior = meses[idx - 1] if idx > 0 else None
    mes_yoy = _ym_yoy(mes) if _ym_yoy(mes) in meses else None

    resumo_mes = agregar_periodo([mes], postos, retirada)
    resumo_ant = agregar_periodo([mes_anterior], postos, retirada) if mes_anterior else None
    resumo_yoy = agregar_periodo([mes_yoy], postos, retirada) if mes_yoy else None

    start = max(0, idx - 11)
    ultimos = meses[start:idx + 1]
    serie_12m = agregar_periodo(ultimos, postos, retirada)["por_mes"]

    # Ranking por variação de despesa, receita e resultado (no universo filtrado)
    universo_postos = [p for p in GRUPOS_POSTO["todos"] if p in postos]
    ranking_var_despesa = ranking_postos(mes, mes_anterior, universo_postos, "despesa", retirada) if mes_anterior else []
    ranking_var_receita = ranking_postos(mes, mes_anterior, universo_postos, "receita", retirada) if mes_anterior else []
    ranking_var_resultado = ranking_postos(mes, mes_anterior, universo_postos, "resultado", retirada) if mes_anterior else []

    # Composições no mês
    comp_desp = {
        "plano_principal": composicao_despesa([mes], postos, "plano_principal", retirada),
        "plano": composicao_despesa([mes], postos, "plano", retirada),
        "tipo": composicao_despesa([mes], postos, "tipo", retirada),
    }
    comp_rec = {
        "tipo": composicao_receita([mes], postos, "tipo"),
        "forma": composicao_receita([mes], postos, "forma"),
        "servico": composicao_receita([mes], postos, "servico"),
    }

    # Variações MoM (drilldown)
    var_desp_mom = {}
    var_rec_mom = {}
    if mes_anterior:
        for d in ("plano_principal", "plano", "tipo"):
            var_desp_mom[d] = calcular_variacao(mes, mes_anterior, postos, "despesa", d, retirada)
        for d in ("tipo", "forma", "servico"):
            var_rec_mom[d] = calcular_variacao(mes, mes_anterior, postos, "receita", d, retirada)

    # Variações YoY
    var_desp_yoy = {}
    var_rec_yoy = {}
    if mes_yoy:
        for d in ("plano_principal", "plano", "tipo"):
            var_desp_yoy[d] = calcular_variacao(mes, mes_yoy, postos, "despesa", d, retirada)
        for d in ("tipo", "forma", "servico"):
            var_rec_yoy[d] = calcular_variacao(mes, mes_yoy, postos, "receita", d, retirada)

    # Tendência últimos 3 meses
    tendencia = {}
    if len(serie_12m) >= 3:
        rec_trend = serie_12m[-1]["receita_total"] - serie_12m[-3]["receita_total"]
        desp_trend = serie_12m[-1]["despesa"] - serie_12m[-3]["despesa"]
        tendencia = {
            "receita_3m_abs": _round2(rec_trend),
            "despesa_3m_abs": _round2(desp_trend),
            "receita_3m_sentido": "subindo" if rec_trend > 0 else "caindo" if rec_trend < 0 else "estavel",
            "despesa_3m_sentido": "subindo" if desp_trend > 0 else "caindo" if desp_trend < 0 else "estavel",
        }

    alertas = detectar_alertas(postos, retirada)

    return jsonify({
        "ok": True,
        "filtros": {
            "mes": mes,
            "mes_anterior": mes_anterior,
            "mes_ano_anterior": mes_yoy,
            "postos": postos,
            "grupo_nome": _nome_grupo(postos),
            "retirada": retirada,
        },
        "resumo_mes": resumo_mes,
        "resumo_mes_anterior": resumo_ant,
        "resumo_ano_anterior": resumo_yoy,
        "deltas_periodo": {
            "mom": {
                "receita_pct": _pct(resumo_mes["receita_total"], resumo_ant["receita_total"]) if resumo_ant else None,
                "despesa_pct": _pct(resumo_mes["despesa_total"], resumo_ant["despesa_total"]) if resumo_ant else None,
                "resultado_pct": _pct(resumo_mes["resultado"], resumo_ant["resultado"]) if resumo_ant else None,
            },
            "yoy": {
                "receita_pct": _pct(resumo_mes["receita_total"], resumo_yoy["receita_total"]) if resumo_yoy else None,
                "despesa_pct": _pct(resumo_mes["despesa_total"], resumo_yoy["despesa_total"]) if resumo_yoy else None,
                "resultado_pct": _pct(resumo_mes["resultado"], resumo_yoy["resultado"]) if resumo_yoy else None,
            },
        },
        "serie_ultimos_12m": serie_12m,
        "tendencia": tendencia,
        "ranking_postos": {
            "por_variacao_despesa_mom": ranking_var_despesa,
            "por_variacao_receita_mom": ranking_var_receita,
            "por_variacao_resultado_mom": ranking_var_resultado,
        },
        "composicao_mes": {"despesa": comp_desp, "receita": comp_rec},
        "variacoes_mom": {"despesa": var_desp_mom, "receita": var_rec_mom},
        "variacoes_yoy": {"despesa": var_desp_yoy, "receita": var_rec_yoy},
        "alertas": alertas,
    })


# ============================================================
# PERGUNTA ASSISTIDA — roteamento leve por palavras-chave
# ============================================================

_KEYWORDS = {
    "ranking_despesa": ["gastou mais", "quem gastou", "posto que mais", "custo maior", "maior custo", "aumentou o custo", "subiu o custo"],
    "ranking_receita": ["vendeu mais", "faturou mais", "maior receita", "melhor receita"],
    "crescimento": ["crescendo", "encolhendo", "crescimento", "evolução", "tendencia", "tendência", "cresceu", "caiu no ano"],
    "composicao_despesa": ["plano de contas", "tipo de conta", "onde gastou", "onde foi gasto", "em que gasta", "em que gastou"],
    "composicao_receita": ["origem da receita", "de onde vem", "por forma", "por servico", "por serviço"],
    "yoy": ["ano passado", "ano anterior", "ano contra ano", "vs ano", "yoy"],
    "mom": ["mês passado", "mes passado", "mês anterior", "mes anterior", "mom"],
    "alertas": ["anomalia", "alerta", "problema", "sinal"],
    "posto_especifico": [],  # detecta via regex
}


@receita_despesa_bp.get("/api/receita_despesa/pergunta_assistida")
def ep_pergunta_assistida():
    """
    Roteador heurístico: recebe texto livre e sugere quais endpoints chamar.
    Não substitui o LLM — serve como dica rápida se o agente não souber decidir.
    """
    q = (request.args.get("q") or "").strip().lower()
    if not q:
        return jsonify({"ok": False, "error": "q obrigatório"}), 400

    import re
    sugestoes: list[dict] = []

    # detecta grupo
    grupo_detectado = None
    if "altamiro" in q:
        grupo_detectado = "altamiro"
    elif "couto" in q:
        grupo_detectado = "couto"
    elif "todos" in q or "geral" in q or "consolidado" in q:
        grupo_detectado = "todos"

    # detecta posto (letra isolada A-Y)
    m = re.search(r"\bposto\s+([A-Z])\b", q.upper())
    posto_detectado = m.group(1) if m else None

    # detecta mês (MM/YYYY ou mes/mês nominal)
    meses_nominais = {"jan": "01", "fev": "02", "mar": "03", "abr": "04", "mai": "05", "jun": "06",
                      "jul": "07", "ago": "08", "set": "09", "out": "10", "nov": "11", "dez": "12"}
    mes_detectado = None
    for k, v in meses_nominais.items():
        if k in q:
            # pega próximo ano se houver
            ym = re.search(rf"{k}\w*\s*(?:de\s+)?(\d{{4}})", q)
            ano = ym.group(1) if ym else None
            if ano:
                mes_detectado = f"{ano}-{v}"
            break
    ym_match = re.search(r"(\d{4})-(\d{2})", q)
    if ym_match:
        mes_detectado = ym_match.group(0)

    # score por intenção
    def _has_any(key: str) -> bool:
        return any(kw in q for kw in _KEYWORDS.get(key, []))

    if _has_any("ranking_despesa"):
        sugestoes.append({
            "intencao": "ranking de postos por variação de despesa",
            "endpoint": "/api/receita_despesa/ranking_postos",
            "params_sugeridos": {
                "mes": mes_detectado or "PERGUNTAR_USUARIO",
                "metrica": "despesa",
                "vs": "ano_anterior" if _has_any("yoy") else "mes_anterior",
                "grupo": grupo_detectado or "PERGUNTAR_USUARIO",
                "retirada": 0,
            },
            "perguntar_ao_usuario": [] if mes_detectado and grupo_detectado else ["Qual mês?", "Todos/Altamiro/Couto/posto específico?"],
        })

    if _has_any("ranking_receita"):
        sugestoes.append({
            "intencao": "ranking de postos por receita",
            "endpoint": "/api/receita_despesa/ranking_postos",
            "params_sugeridos": {
                "mes": mes_detectado or "PERGUNTAR_USUARIO",
                "metrica": "receita",
                "vs": "ano_anterior" if _has_any("yoy") else "mes_anterior",
                "grupo": grupo_detectado or "PERGUNTAR_USUARIO",
            },
        })

    if _has_any("crescimento"):
        sugestoes.append({
            "intencao": "análise de crescimento / tendência",
            "endpoint": "/api/receita_despesa/crescimento",
            "params_sugeridos": {
                "postos": ",".join(GRUPOS_POSTO[grupo_detectado]) if grupo_detectado else (posto_detectado or "todos"),
                "retirada": 0,
            },
        })

    if _has_any("composicao_despesa"):
        sugestoes.append({
            "intencao": "composição de despesa por plano/tipo",
            "endpoint": "/api/receita_despesa/composicao",
            "params_sugeridos": {
                "de": mes_detectado or "PERGUNTAR_USUARIO",
                "ate": mes_detectado or "PERGUNTAR_USUARIO",
                "tipo": "despesa",
                "dimensao": "plano_principal",
                "postos": posto_detectado or grupo_detectado or "todos",
            },
        })

    if _has_any("composicao_receita"):
        sugestoes.append({
            "intencao": "composição de receita",
            "endpoint": "/api/receita_despesa/composicao",
            "params_sugeridos": {
                "de": mes_detectado or "PERGUNTAR_USUARIO",
                "ate": mes_detectado or "PERGUNTAR_USUARIO",
                "tipo": "receita",
                "dimensao": "tipo",
                "postos": posto_detectado or grupo_detectado or "todos",
            },
        })

    if _has_any("alertas"):
        sugestoes.append({
            "intencao": "alertas e anomalias",
            "endpoint": "/api/receita_despesa/alertas",
            "params_sugeridos": {
                "postos": posto_detectado or grupo_detectado or "todos",
                "retirada": 0,
            },
        })

    if posto_detectado and mes_detectado:
        sugestoes.append({
            "intencao": "detalhe completo de um posto em um mês",
            "endpoint": "/api/receita_despesa/posto_detalhe",
            "params_sugeridos": {"posto": posto_detectado, "mes": mes_detectado, "retirada": 0},
        })

    if not sugestoes:
        # fallback recomendando a análise completa
        sugestoes.append({
            "intencao": "fallback — pacote executivo completo",
            "endpoint": "/api/receita_despesa/analise_completa",
            "params_sugeridos": {
                "mes": mes_detectado or "PERGUNTAR_USUARIO",
                "postos": posto_detectado or grupo_detectado or "todos",
                "retirada": 0,
            },
            "perguntar_ao_usuario": [
                "Qual mês você quer analisar?",
                "Todos os postos, grupo Altamiro, Couto ou posto específico?",
                "Considerar RETIRADA/CAMPINHO na despesa? (operacional=não, executivo=sim)",
            ],
        })

    return jsonify({
        "ok": True,
        "query": q,
        "detectado": {"grupo": grupo_detectado, "posto": posto_detectado, "mes": mes_detectado},
        "sugestoes": sugestoes,
    })
