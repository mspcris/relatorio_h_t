#!/usr/bin/env python3
"""
export_auditoria_financeira.py

Motor de auditoria financeira (Benford + detector de anomalias).

Lê últimos 24 meses de fin_despesa e fin_receita no RDS Postgres, agrupa por
(posto, id_conta_tipo, mês) e aplica as regras ativas (lidas do auth_db) para
gerar uma lista de alertas e um JSON estático consumido por kpi_receita_despesa.

Saída:
    /opt/relatorio_h_t/json_consolidado/auditoria_financeira.json
    /opt/relatorio_h_t/json_consolidado/_etl_meta_auditoria_financeira.json

Cron: 0 3 * * *  (definido em cron/relatorio_ht)

Estrutura do JSON:
    {
      "generated_at": "...",
      "janela_meses": 24,
      "benford": {
        "rede": { "saidas": {...}, "entradas": {...} },
        "por_posto": { "A": { "saidas": {...}, "entradas": {...} }, ... },
        "cor_botao": "amarelo"   # pior MAD entre os 13 postos
      },
      "anomalias": [
        {
          "chave": "<sha1>",
          "posto": "A",
          "id_conta_tipo": 123,
          "tipo_label": "ENERGIA ELÉTRICA",
          "mes_ref": "2026-04",
          "valor_atual": 11600.00,
          "regra_id": 4,
          "regra_nome": "MM12 — variação > 10%",
          "regra_tipo": "mm_pct",
          "evidencia": { "mm12": 8400, "delta_pct": 38.1, "n_meses_hist": 12 },
          "verificado": false,
          "verificado_por": null,
          "verificado_em": null
        }, ...
      ],
      "scores_postos": {"A": 87, "B": 92, ...},  # 0-100, saúde financeira
      "tipos_label": {"123": "ENERGIA ELÉTRICA", ...},
      "regras_aplicadas": [...]
    }
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import statistics
import sys
import traceback
from collections import defaultdict
from datetime import date, datetime
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

import psycopg2
from psycopg2.extras import RealDictCursor

OUT_DIR   = "/opt/relatorio_h_t/json_consolidado"
OUT_FILE  = os.path.join(OUT_DIR, "auditoria_financeira.json")
META_FILE = os.path.join(OUT_DIR, "_etl_meta_auditoria_financeira.json")

JANELA_MESES = 24
POSTOS_ORDER = ["N", "X", "Y", "M", "P", "D", "B", "I", "G", "R", "J", "C", "A"]

AUTH_DB_PATH = os.environ.get("AUTH_DB_PATH", "/var/lib/camim-auth/camim_auth.db")


# ── conexões ────────────────────────────────────────────────────────────────

def pg_conn():
    return psycopg2.connect(
        host=os.environ["PG_RDS_HOST"],
        port=int(os.environ.get("PG_RDS_PORT", "9432")),
        dbname=os.environ.get("PG_RDS_DB", "relatorio_h_t"),
        user=os.environ["PG_RDS_USER"],
        password=os.environ["PG_RDS_PASSWORD"],
        sslmode=os.environ.get("PG_RDS_SSLMODE", "require"),
        connect_timeout=15,
    )


def auth_conn():
    import sqlite3
    return sqlite3.connect(AUTH_DB_PATH, timeout=30)


# ── leitura de dados ────────────────────────────────────────────────────────

def carregar_lancamentos(pg, tabela: str, valor_col: str, id_col: str) -> List[dict]:
    """Lê últimos JANELA_MESES de fin_despesa/fin_receita.

    Retorna lista de dicts com posto, id, id_conta_tipo, valor_pago, data_pagamento,
    fornecedor (só despesa), tipo_label, plano_principal, plano.
    """
    data_inicial = (date.today().replace(day=1)
                    .replace(year=date.today().year - 2))
    fornecedor_col = "fornecedor," if tabela == "fin_despesa" else "NULL AS fornecedor,"
    plano_principal_col = "plano_principal," if tabela == "fin_despesa" else "NULL AS plano_principal,"

    sql = f"""
        SELECT posto,
               {id_col}        AS id_lan,
               id_conta_tipo,
               tipo            AS tipo_label,
               {plano_principal_col}
               plano,
               {fornecedor_col}
               {valor_col}     AS valor,
               data_pagamento
        FROM   {tabela}
        WHERE  data_pagamento >= %s
          AND  {valor_col} IS NOT NULL
          AND  {valor_col} > 0
    """
    with pg.cursor(cursor_factory=RealDictCursor) as c:
        c.execute(sql, (data_inicial,))
        return c.fetchall()


def carregar_regras_ativas(auth) -> List[dict]:
    cur = auth.cursor()
    cur.execute("""
        SELECT id, nome, tipo, parametros_json, escopo_postos, escopo_tipos, observacao
        FROM   regras_anomalia
        WHERE  ativa = 1
        ORDER  BY id
    """)
    out = []
    for r in cur.fetchall():
        out.append({
            "id": r[0], "nome": r[1], "tipo": r[2],
            "parametros": json.loads(r[3] or "{}"),
            "escopo_postos": r[4] or "*",
            "escopo_tipos":  r[5] or "*",
            "observacao":    r[6] or "",
        })
    return out


def carregar_verificacoes(auth) -> Dict[str, dict]:
    cur = auth.cursor()
    cur.execute("""
        SELECT chave_anomalia, verificado_por, verificado_em, observacao
        FROM   anomalia_verificacao
    """)
    out = {}
    for r in cur.fetchall():
        out[r[0]] = {
            "verificado_por": r[1],
            "verificado_em":  r[2],
            "observacao":     r[3] or "",
        }
    return out


# ── helpers de escopo ───────────────────────────────────────────────────────

def _match_escopo(valor: str, escopo: str) -> bool:
    """Suporta '*', 'A,B,C' (whitelist), '!A,B' (blacklist)."""
    escopo = (escopo or "*").strip()
    if escopo in ("", "*"):
        return True
    if escopo.startswith("!"):
        bl = {x.strip() for x in escopo[1:].split(",") if x.strip()}
        return str(valor) not in bl
    wl = {x.strip() for x in escopo.split(",") if x.strip()}
    return str(valor) in wl


# ── Benford ─────────────────────────────────────────────────────────────────

BENFORD_ESPERADO = [math.log10(1 + 1/d) for d in range(1, 10)]  # d=1..9


def _primeiro_digito(v: float) -> Optional[int]:
    if v is None or v <= 0:
        return None
    s = f"{v:.10f}".lstrip("0.")
    for ch in s:
        if ch.isdigit() and ch != "0":
            return int(ch)
    return None


def benford_analise(valores: List[float]) -> dict:
    """Distribuição observada do 1º dígito + MAD vs Benford.

    Retorna {n, observado: [9 floats], esperado: [9 floats], mad, cor}.
    Se n < 80, devolve cor='cinza' (amostra insuficiente).
    """
    contagem = [0] * 9
    n = 0
    for v in valores:
        d = _primeiro_digito(float(v))
        if d:
            contagem[d - 1] += 1
            n += 1

    if n == 0:
        return {"n": 0, "observado": [0.0]*9, "esperado": BENFORD_ESPERADO,
                "mad": None, "cor": "cinza"}

    observado = [c / n for c in contagem]
    mad = sum(abs(o - e) for o, e in zip(observado, BENFORD_ESPERADO)) / 9

    if n < 80:
        cor = "cinza"
    elif mad < 0.012:
        cor = "verde"
    elif mad < 0.022:
        cor = "amarelo"
    else:
        cor = "vermelho"

    return {"n": n, "observado": observado, "esperado": BENFORD_ESPERADO,
            "mad": round(mad, 5), "cor": cor}


# ── motor de regras ─────────────────────────────────────────────────────────

def _hash_chave(*parts) -> str:
    return hashlib.sha1("|".join(str(p) for p in parts).encode("utf-8")).hexdigest()


def _series_mensal(lancamentos: List[dict]) -> Dict[Tuple[str, int], Dict[str, float]]:
    """Agrupa lançamentos em série mensal por (posto, id_conta_tipo).

    Retorna { (posto, id_conta_tipo): { "2026-04": soma_valor, ... } }
    """
    serie: Dict[Tuple[str, int], Dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for lan in lancamentos:
        if not lan.get("id_conta_tipo") or not lan.get("data_pagamento"):
            continue
        chave = (lan["posto"], int(lan["id_conta_tipo"]))
        mes = lan["data_pagamento"].strftime("%Y-%m")
        serie[chave][mes] += float(lan["valor"] or 0)
    return serie


def _conta_e_regular(serie_mes: Dict[str, float], n_meses_min: int = 6) -> bool:
    """≥ 6 dos últimos 12 meses tem lançamento."""
    hoje = date.today().replace(day=1)
    ultimos_12 = []
    for i in range(12):
        m = (hoje.replace(day=1).year, hoje.replace(day=1).month - i)
        # corrige overflow de mês
        ano = m[0] + (m[1] - 1) // 12
        mes = ((m[1] - 1) % 12) + 1
        ultimos_12.append(f"{ano:04d}-{mes:02d}")
    presentes = sum(1 for k in ultimos_12 if k in serie_mes)
    return presentes >= n_meses_min


def _mm(serie_mes: Dict[str, float], mes_ref: str, janela: int) -> Optional[float]:
    """Média dos últimos `janela` meses ANTERIORES a `mes_ref` que tenham valor."""
    ano, m = map(int, mes_ref.split("-"))
    valores = []
    for i in range(1, janela + 1):
        ar, mr = ano, m - i
        while mr <= 0:
            mr += 12
            ar -= 1
        k = f"{ar:04d}-{mr:02d}"
        if k in serie_mes:
            valores.append(serie_mes[k])
    if not valores:
        return None
    return sum(valores) / len(valores)


def _zscore_robusto(serie_mes: Dict[str, float], mes_ref: str) -> Optional[float]:
    """Score robusto contra mediana e MAD da própria série (excluindo mes_ref)."""
    if mes_ref not in serie_mes:
        return None
    valor = serie_mes[mes_ref]
    historicos = [v for k, v in serie_mes.items() if k != mes_ref]
    if len(historicos) < 3:
        return None
    mediana = statistics.median(historicos)
    desvios = [abs(v - mediana) for v in historicos]
    mad = statistics.median(desvios)
    if mad == 0:
        return None  # série constante; usa mm_pct
    return abs(valor - mediana) / (1.4826 * mad)


def _aplica_regra_mm_pct(serie_mes, posto, ict, regra) -> List[dict]:
    out = []
    janela = int(regra["parametros"].get("janela", 12))
    pct_lim = float(regra["parametros"].get("pct", 10))
    for mes_ref, valor in serie_mes.items():
        mm = _mm(serie_mes, mes_ref, janela)
        if mm is None or mm <= 0:
            continue
        delta_pct = (valor - mm) / mm * 100.0
        if delta_pct >= pct_lim:
            out.append({
                "posto": posto, "id_conta_tipo": ict,
                "mes_ref": mes_ref, "valor_atual": round(valor, 2),
                "regra_id": regra["id"], "regra_nome": regra["nome"],
                "regra_tipo": regra["tipo"],
                "evidencia": {
                    f"mm{janela}": round(mm, 2),
                    "delta_pct": round(delta_pct, 1),
                    "limite_pct": pct_lim,
                },
            })
    return out


def _aplica_regra_zscore(serie_mes, posto, ict, regra) -> List[dict]:
    out = []
    threshold = float(regra["parametros"].get("threshold", 3.0))
    for mes_ref in list(serie_mes.keys()):
        z = _zscore_robusto(serie_mes, mes_ref)
        if z is not None and z >= threshold:
            mediana = statistics.median([v for k, v in serie_mes.items() if k != mes_ref])
            out.append({
                "posto": posto, "id_conta_tipo": ict,
                "mes_ref": mes_ref, "valor_atual": round(serie_mes[mes_ref], 2),
                "regra_id": regra["id"], "regra_nome": regra["nome"],
                "regra_tipo": regra["tipo"],
                "evidencia": {
                    "zscore": round(z, 2),
                    "mediana": round(mediana, 2),
                    "threshold": threshold,
                },
            })
    return out


def _aplica_regra_gap(serie_mes, posto, ict, regra) -> List[dict]:
    """Conta regular sem lançamento neste mês."""
    if not _conta_e_regular(serie_mes):
        return []
    hoje = date.today()
    mes_atual = hoje.strftime("%Y-%m")
    if mes_atual in serie_mes:
        return []
    # quantos meses sem lançamento (consecutivos, contando para trás)
    meses_vazios = 0
    ano, m = hoje.year, hoje.month
    for _ in range(12):
        k = f"{ano:04d}-{m:02d}"
        if k in serie_mes:
            break
        meses_vazios += 1
        m -= 1
        if m == 0:
            m = 12
            ano -= 1
    n_min = int(regra["parametros"].get("meses_vazios", 1))
    if meses_vazios < n_min:
        return []
    return [{
        "posto": posto, "id_conta_tipo": ict,
        "mes_ref": mes_atual, "valor_atual": 0.0,
        "regra_id": regra["id"], "regra_nome": regra["nome"],
        "regra_tipo": regra["tipo"],
        "evidencia": {
            "meses_vazios": meses_vazios,
            "ultimo_lancamento": max(serie_mes.keys()) if serie_mes else None,
        },
    }]


def _aplica_regra_fornecedor_novo(lancamentos: List[dict], regra) -> List[dict]:
    """Para cada (posto, id_conta_tipo), detecta fornecedor inédito vs histórico anterior."""
    historico: Dict[Tuple[str, int], set] = defaultdict(set)
    out = []
    # ordena por data de pagamento ascendente — tudo que vier depois do "primeiro
    # contato" daquele fornecedor naquela conta deixa de ser "novo".
    ordenados = sorted(
        [l for l in lancamentos if l.get("fornecedor") and l.get("id_conta_tipo")],
        key=lambda l: l["data_pagamento"],
    )
    for lan in ordenados:
        chave = (lan["posto"], int(lan["id_conta_tipo"]))
        forn = (lan["fornecedor"] or "").strip().upper()
        if not forn:
            continue
        if forn not in historico[chave]:
            # primeira vez que esse fornecedor aparece nessa (posto, id_conta_tipo)
            if historico[chave]:
                # já havia outros fornecedores antes — é troca/novo de verdade
                out.append({
                    "posto": lan["posto"],
                    "id_conta_tipo": int(lan["id_conta_tipo"]),
                    "mes_ref": lan["data_pagamento"].strftime("%Y-%m"),
                    "valor_atual": round(float(lan["valor"] or 0), 2),
                    "regra_id": regra["id"], "regra_nome": regra["nome"],
                    "regra_tipo": regra["tipo"],
                    "evidencia": {
                        "fornecedor_novo": forn,
                        "fornecedores_anteriores": sorted(historico[chave]),
                    },
                })
            historico[chave].add(forn)
    return out


def _aplica_regra_nao_recorrente(lancamentos: List[dict], regra) -> List[dict]:
    """Lançamentos isolados (não-regulares) acima de X% do total do posto no mês."""
    pct = float(regra["parametros"].get("pct_posto", 1.0))
    serie = _series_mensal(lancamentos)
    contas_regulares = {chave for chave, ser in serie.items() if _conta_e_regular(ser)}

    # total por (posto, mês) — base p/ % comparativo
    total_posto_mes: Dict[Tuple[str, str], float] = defaultdict(float)
    for lan in lancamentos:
        if not lan.get("data_pagamento"):
            continue
        mes = lan["data_pagamento"].strftime("%Y-%m")
        total_posto_mes[(lan["posto"], mes)] += float(lan["valor"] or 0)

    out = []
    for lan in lancamentos:
        if not lan.get("id_conta_tipo") or not lan.get("data_pagamento"):
            continue
        chave = (lan["posto"], int(lan["id_conta_tipo"]))
        if chave in contas_regulares:
            continue  # essa conta tem outras regras
        valor = float(lan["valor"] or 0)
        mes = lan["data_pagamento"].strftime("%Y-%m")
        total = total_posto_mes.get((lan["posto"], mes), 0)
        if total <= 0:
            continue
        share = (valor / total) * 100.0
        if share >= pct:
            out.append({
                "posto": lan["posto"],
                "id_conta_tipo": int(lan["id_conta_tipo"]),
                "mes_ref": mes,
                "valor_atual": round(valor, 2),
                "regra_id": regra["id"], "regra_nome": regra["nome"],
                "regra_tipo": regra["tipo"],
                "evidencia": {
                    "pct_total_posto": round(share, 2),
                    "limite_pct": pct,
                    "total_posto_mes": round(total, 2),
                },
            })
    return out


# ── orquestração ────────────────────────────────────────────────────────────

def gerar_anomalias(despesas, regras) -> List[dict]:
    """Aplica todas as regras ativas de saída sobre as despesas."""
    serie = _series_mensal(despesas)
    contas_regulares = {chave for chave, ser in serie.items() if _conta_e_regular(ser)}

    out = []
    for regra in regras:
        if regra["tipo"] in ("benford_mad",):
            continue  # tratada à parte
        if regra["tipo"] == "fornecedor_novo":
            for it in _aplica_regra_fornecedor_novo(despesas, regra):
                if not (_match_escopo(it["posto"], regra["escopo_postos"]) and
                        _match_escopo(it["id_conta_tipo"], regra["escopo_tipos"])):
                    continue
                out.append(it)
            continue
        if regra["tipo"] == "nao_recorrente_pct":
            for it in _aplica_regra_nao_recorrente(despesas, regra):
                if not (_match_escopo(it["posto"], regra["escopo_postos"]) and
                        _match_escopo(it["id_conta_tipo"], regra["escopo_tipos"])):
                    continue
                out.append(it)
            continue

        for (posto, ict), serie_mes in serie.items():
            if not (_match_escopo(posto, regra["escopo_postos"]) and
                    _match_escopo(ict,   regra["escopo_tipos"])):
                continue
            # Para regras de série temporal, restringir a contas regulares
            if regra["tipo"] in ("mm_pct", "zscore_robusto", "gap_temporal"):
                if (posto, ict) not in contas_regulares:
                    continue
            if regra["tipo"] == "mm_pct":
                out.extend(_aplica_regra_mm_pct(serie_mes, posto, ict, regra))
            elif regra["tipo"] == "zscore_robusto":
                out.extend(_aplica_regra_zscore(serie_mes, posto, ict, regra))
            elif regra["tipo"] == "gap_temporal":
                out.extend(_aplica_regra_gap(serie_mes, posto, ict, regra))
    return out


def calcular_score_postos(anomalias: List[dict], serie) -> Dict[str, int]:
    """Score 0-100 por posto: começa em 100, penaliza por anomalia aberta."""
    anom_por_posto = defaultdict(int)
    for a in anomalias:
        if not a.get("verificado"):
            anom_por_posto[a["posto"]] += 1
    total_contas_por_posto = defaultdict(int)
    for (p, _), _ in serie.items():
        total_contas_por_posto[p] += 1

    scores = {}
    for p in POSTOS_ORDER:
        n_anom = anom_por_posto.get(p, 0)
        n_contas = total_contas_por_posto.get(p, 1)
        # 1 anomalia / conta = ~30 pontos perdidos
        penalidade = min(100, int((n_anom / n_contas) * 100))
        scores[p] = max(0, 100 - penalidade)
    return scores


def _atomic_write(path: str, payload) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2, default=str)
    os.replace(tmp, path)


def main() -> int:
    started_at = datetime.now()
    os.makedirs(OUT_DIR, exist_ok=True)

    pg = pg_conn()
    auth = auth_conn()
    erros = {}

    try:
        despesas = carregar_lancamentos(pg, "fin_despesa", "valor_pago", "id_despesa")
        try:
            receitas = carregar_lancamentos(pg, "fin_receita", "valor_pago", "id_receita")
        except Exception as exc:
            # Fase 0 ainda pode não ter rodado; segue só com despesas no Benford.
            receitas = []
            erros["fin_receita"] = f"{type(exc).__name__}: {exc}"

        regras = carregar_regras_ativas(auth)
        verificacoes = carregar_verificacoes(auth)

        # ── Benford ────────────────────────────────────────────────────
        valores_saida_rede = [float(l["valor"]) for l in despesas]
        valores_entrada_rede = [float(l["valor"]) for l in receitas]
        benford = {
            "rede": {
                "saidas":   benford_analise(valores_saida_rede),
                "entradas": benford_analise(valores_entrada_rede),
            },
            "por_posto": {},
        }
        cores_postos = []
        for p in POSTOS_ORDER:
            saidas_p   = [float(l["valor"]) for l in despesas if l["posto"] == p]
            entradas_p = [float(l["valor"]) for l in receitas if l["posto"] == p]
            bp = {
                "saidas":   benford_analise(saidas_p),
                "entradas": benford_analise(entradas_p),
            }
            benford["por_posto"][p] = bp
            cores_postos.append(bp["saidas"]["cor"])

        # cor do botão = pior cor entre os 13 postos (ignora cinza)
        ordem_cor = {"verde": 0, "amarelo": 1, "vermelho": 2, "cinza": -1}
        nao_cinzas = [c for c in cores_postos if c != "cinza"]
        cor_botao = "cinza"
        if nao_cinzas:
            cor_botao = max(nao_cinzas, key=lambda c: ordem_cor[c])
        benford["cor_botao"] = cor_botao

        # ── Anomalias (saídas) ─────────────────────────────────────────
        anomalias_raw = gerar_anomalias(despesas, regras)

        # carrega labels mais recentes de id_conta_tipo → texto
        tipos_label: Dict[int, str] = {}
        for lan in despesas:
            ict = lan.get("id_conta_tipo")
            if ict:
                tipos_label[int(ict)] = lan.get("tipo_label") or f"#{ict}"

        # enriquece com chave + status de verificação
        anomalias = []
        for a in anomalias_raw:
            chave = _hash_chave(
                a["posto"], a["id_conta_tipo"], a["mes_ref"],
                a["regra_id"], json.dumps(a.get("evidencia", {}), sort_keys=True),
            )
            v = verificacoes.get(chave)
            a["chave"] = chave
            a["tipo_label"] = tipos_label.get(int(a["id_conta_tipo"]),
                                              f"#{a['id_conta_tipo']}")
            a["verificado"]      = v is not None
            a["verificado_por"]  = v["verificado_por"]   if v else None
            a["verificado_em"]   = str(v["verificado_em"]) if v else None
            a["observacao"]      = v["observacao"]       if v else ""
            anomalias.append(a)

        # ordena por severidade (regra_tipo) + valor desc
        prio = {"zscore_robusto": 0, "mm_pct": 1, "fornecedor_novo": 2,
                "gap_temporal": 3, "nao_recorrente_pct": 4}
        anomalias.sort(key=lambda a: (prio.get(a["regra_tipo"], 99),
                                      -float(a.get("valor_atual") or 0)))

        # ── Score por posto ────────────────────────────────────────────
        serie = _series_mensal(despesas)
        scores_postos = calcular_score_postos(anomalias, serie)

        # ── Persistência ───────────────────────────────────────────────
        finished_at = datetime.now()
        payload = {
            "generated_at":     started_at.isoformat(timespec="seconds"),
            "janela_meses":     JANELA_MESES,
            "benford":          benford,
            "anomalias":        anomalias,
            "scores_postos":    scores_postos,
            "tipos_label":      {str(k): v for k, v in tipos_label.items()},
            "regras_aplicadas": [{"id": r["id"], "nome": r["nome"], "tipo": r["tipo"],
                                  "parametros": r["parametros"]} for r in regras],
            "totais": {
                "despesas":         len(despesas),
                "receitas":         len(receitas),
                "anomalias":        len(anomalias),
                "abertas":          sum(1 for a in anomalias if not a["verificado"]),
                "verificadas":      sum(1 for a in anomalias if a["verificado"]),
            },
            "erros": erros,
        }
        _atomic_write(OUT_FILE, payload)
        _atomic_write(META_FILE, {
            "script": "export_auditoria_financeira",
            "started_at":  started_at.isoformat(timespec="seconds"),
            "finished_at": finished_at.isoformat(timespec="seconds"),
            "duracao_segundos": round((finished_at - started_at).total_seconds(), 2),
            "totais": payload["totais"],
            "erros":  erros,
        })
        print(f"[OK] {OUT_FILE}  "
              f"{(finished_at - started_at).total_seconds():.2f}s  "
              f"anomalias={len(anomalias)}  abertas={payload['totais']['abertas']}  "
              f"erros={list(erros)}")
        return 0
    except Exception:
        traceback.print_exc()
        return 1
    finally:
        try: pg.close()
        except Exception: pass
        try: auth.close()
        except Exception: pass


if __name__ == "__main__":
    sys.exit(main())
