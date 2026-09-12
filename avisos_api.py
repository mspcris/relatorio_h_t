"""avisos_api.py — o que o avisos_gerenciais lê do KPI para a home dos gestores.

Porta de máquina: `Authorization: Bearer $AVISOS_API_TOKEN`. Só LEITURA, e
sempre por posto — cada gestor enxerga o posto dele, e é o avisos quem decide
quais postos pedir (o ACL é de lá). O nginx tem um `location ^~ /api/avisos/`
sem `auth_request` justamente porque máquina não tem cookie; quem autentica
aqui é o token.

  GET /api/avisos/notas?postos=A,C[&ym=2026-09]   NF emitidas × meta do mês
  GET /api/avisos/metas?postos=A,C[&ym=2026-09]   mensalidades e vendas × meta

Nenhum dos dois recalcula regra de negócio: as notas saem do
`relatorio_nf.carregar_dados` (o mesmo agregado do relatório "NF emitidas ×
meta" que vai por zap, modo Clínicas, % sobre as CONTABILIZADAS) e as metas de
mensalidade/venda saem do JSON do `export_metas.py`, somando os dias. Se a
régua mudar lá, muda aqui junto — não existe segunda verdade.
"""

from __future__ import annotations

import hmac
import json
import logging
import os
from datetime import date

from flask import Blueprint, jsonify, request

logger = logging.getLogger(__name__)

avisos_bp = Blueprint("avisos_bp", __name__)

_METAS_DIR_PADRAO = "/opt/relatorio_h_t/json_metas"
METAS_DIR = os.getenv("METAS_JSON_DIR") or (
    _METAS_DIR_PADRAO if os.path.isdir(_METAS_DIR_PADRAO)
    else os.path.join(os.path.dirname(os.path.abspath(__file__)), "json_metas")
)


# ── Porta de máquina ────────────────────────────────────────────────────────

def token_de_maquina() -> bool:
    """`Authorization: Bearer $AVISOS_API_TOKEN`. Sem token no .env, ninguém
    entra por essa porta — é o único guarda destas rotas."""
    esperado = (os.getenv("AVISOS_API_TOKEN") or "").strip()
    if not esperado:
        return False
    enviado = (request.headers.get("Authorization") or "").strip()
    if enviado.lower().startswith("bearer "):
        enviado = enviado[7:].strip()
    return bool(enviado) and hmac.compare_digest(enviado, esperado)


def _postos_pedidos() -> list:
    return sorted({p.strip().upper() for p in (request.args.get("postos") or "").split(",")
                   if len(p.strip()) == 1})


def _ym() -> str:
    ym = (request.args.get("ym") or "").strip()
    if len(ym) == 7 and ym[4] == "-" and ym[:4].isdigit() and ym[5:].isdigit():
        return ym
    hoje = date.today()
    return f"{hoje.year:04d}-{hoje.month:02d}"


def _num(v) -> float:
    try:
        return float(v or 0)
    except (TypeError, ValueError):
        return 0.0


# ── NF emitidas × meta ──────────────────────────────────────────────────────

@avisos_bp.get("/api/avisos/notas")
def api_notas():
    """Por posto: quanto foi contabilizado no mês, a meta e o % — mais a linha
    de cada empresa do posto, que é como o gerente cobra (um posto pode ter
    mais de um CNPJ, e a meta é por CNPJ)."""
    if not token_de_maquina():
        return jsonify({"error": "unauthorized"}), 401
    from relatorio_nf import carregar_dados  # import tardio: puxa PIL

    postos, ym = _postos_pedidos(), _ym()
    try:
        dados = carregar_dados(ym)
    except Exception as e:  # JSON do mês ainda não gerado, disco fora, etc.
        logger.warning("notas do avisos falharam (%s): %s", ym, e)
        return jsonify({"error": f"não consegui ler as notas de {ym}: {str(e)[:120]}"}), 503

    saida = {}
    for linha in dados["linhas"]:
        if postos and linha["posto"] not in postos:
            continue
        posto = saida.setdefault(linha["posto"], dict(
            contabilizado=0.0, emitidas=0.0, meta=0.0, qtd=0, canceladas=0, empresas=[],
        ))
        posto["contabilizado"] += _num(linha["cont_val"])
        posto["emitidas"] += _num(linha["emitidas"])
        posto["meta"] += _num(linha["objetivo"])
        posto["qtd"] += int(_num(linha["qtd"]))
        posto["canceladas"] += int(_num(linha["canc_qtd"]))
        posto["empresas"].append(dict(
            empresa=linha["empresa"], origem=linha["origem"], qtd=int(_num(linha["qtd"])),
            emitidas=_num(linha["emitidas"]), canceladas=int(_num(linha["canc_qtd"])),
            contabilizado=_num(linha["cont_val"]), meta=_num(linha["objetivo"]),
            pct=linha["pct"], faixa=linha["faixa"],
        ))
    for posto in saida.values():
        posto["pct"] = (posto["contabilizado"] / posto["meta"] * 100.0) if posto["meta"] else None
        posto["empresas"].sort(key=lambda e: -e["emitidas"])

    return jsonify({
        "ym": ym, "mes_nome": dados["mes_nome"], "postos": saida,
        "gerado_em": dados["gerado_em"].isoformat() if dados["gerado_em"] else None,
    })


# ── Mensalidades e vendas × meta ────────────────────────────────────────────

def _ler_metas(posto: str, ym: str) -> dict | None:
    caminho = os.path.join(METAS_DIR, f"{posto}_metas_{ym}.json")
    try:
        with open(caminho, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def _somar(dias: list, campo: str) -> float:
    return sum(_num(d.get(campo)) for d in dias or [])


@avisos_bp.get("/api/avisos/metas")
def api_metas():
    """Mensalidades e vendas do mês contra a meta cadastrada, por posto.
    Devolve também o dia de hoje dentro do mês: meta mensal olhada no dia 11
    sem saber que é dia 11 engana — metade do mês é metade da meta."""
    if not token_de_maquina():
        return jsonify({"error": "unauthorized"}), 401

    postos, ym = _postos_pedidos(), _ym()
    hoje = date.today()
    ano, mes = int(ym[:4]), int(ym[5:])
    dias_no_mes = (date(ano + (mes == 12), (mes % 12) + 1, 1) - date(ano, mes, 1)).days
    # Mês passado conta o mês inteiro; o corrente conta até hoje
    dia_corrente = hoje.day if (hoje.year, hoje.month) == (ano, mes) else dias_no_mes

    saida, erros = {}, {}
    for posto in postos:
        d = _ler_metas(posto, ym)
        if not d:
            erros[posto] = f"sem dados de metas para {ym}"
            continue
        meta = d.get("meta") or {}
        mens = _somar(d.get("mensalidades_por_dia"), "mens_dia")
        vendas = _somar(d.get("vendas_por_dia"), "vendas_dia")
        meta_mens, meta_venda = _num(meta.get("meta_mens")), _num(meta.get("meta_venda"))
        saida[posto] = dict(
            mensalidades=mens, meta_mensalidades=meta_mens,
            pct_mensalidades=(mens / meta_mens * 100.0) if meta_mens else None,
            vendas=vendas, meta_vendas=meta_venda,
            pct_vendas=(vendas / meta_venda * 100.0) if meta_venda else None,
            gerado_em=d.get("gerado_em"),
        )

    return jsonify({
        "ym": ym, "dia": dia_corrente, "dias_no_mes": dias_no_mes,
        "postos": saida, "erros": erros,
    })
