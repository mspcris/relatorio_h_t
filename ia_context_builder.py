"""
ia_context_builder.py v2 — Analytics engine para IA CAMIM.

Arquitetura:
  1. Detecta intenção da pergunta (comparação / ranking / tendência / resumo)
  2. Pandas pré-computa o resultado (delta R$, %, tops, séries mensais)
  3. LLM recebe contexto compacto e pré-calculado (~20-40 linhas)
  4. LLM só narra — nunca calcula

Regra de negócio hardcoded:
  - Retirada/Campinho NUNCA entra nos totais operacionais
  - Exceto quando incluir_retirada=True (toggle explícito do usuário)
"""

import json
import os
import re
from typing import List, Optional, Tuple

import pandas as pd

JSON_ROOT = os.getenv("JSON_ROOT", "/var/www")

_RETIRADA_TIPOS = {"retirada", "campinho", "campinho/retirada", "retirada/campinho"}

_MES_NOMES = {
    "jan": "01", "fev": "02", "mar": "03", "abr": "04",
    "mai": "05", "jun": "06", "jul": "07", "ago": "08",
    "set": "09", "out": "10", "nov": "11", "dez": "12",
    "janeiro": "01", "fevereiro": "02", "marco": "03", "março": "03",
    "abril": "04", "maio": "05", "junho": "06", "julho": "07",
    "agosto": "08", "setembro": "09", "outubro": "10",
    "novembro": "11", "dezembro": "12",
}

_MES_LABELS = {
    "01": "Jan", "02": "Fev", "03": "Mar", "04": "Abr",
    "05": "Mai", "06": "Jun", "07": "Jul", "08": "Ago",
    "09": "Set", "10": "Out", "11": "Nov", "12": "Dez",
}


# ─────────────────────────────────────────────────────────────
# I/O
# ─────────────────────────────────────────────────────────────

def _path(*parts: str) -> str:
    return os.path.join(JSON_ROOT, *parts)


def _load_linhas_df(rel_path: str) -> pd.DataFrame:
    full = _path(rel_path)
    if not os.path.exists(full):
        return pd.DataFrame()
    with open(full, encoding="utf-8") as f:
        data = json.load(f)
    rows = []
    for mes, postos_data in data.get("dados", {}).items():
        for posto, bloco in postos_data.items():
            for linha in bloco.get("linhas", []):
                rows.append({"mes": mes, "posto": posto, **linha})
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["mes"] = df["mes"].astype(str)
    df["posto"] = df["posto"].astype(str)
    if "valorpago" in df.columns:
        df["valorpago"] = pd.to_numeric(df["valorpago"], errors="coerce").fillna(0.0)
    # normaliza coluna tipo
    for col in ("Tipo", "tipo", "plano", "Plano"):
        if col in df.columns and "tipo" not in df.columns:
            df.rename(columns={col: "tipo"}, inplace=True)
            break
    if "tipo" in df.columns:
        df["tipo"] = df["tipo"].astype(str).str.strip()
    return df


def _load_consolidado_df(rel_path: str) -> pd.DataFrame:
    full = _path(rel_path)
    if not os.path.exists(full):
        return pd.DataFrame()
    with open(full, encoding="utf-8") as f:
        data = json.load(f)
    rows = []
    for mes, postos_data in data.items():
        if not isinstance(postos_data, dict):
            continue
        for posto, campos in postos_data.items():
            if not isinstance(campos, dict):
                continue
            rows.append({"mes": mes, "posto": posto, **campos})
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["mes"] = df["mes"].astype(str)
    df["posto"] = df["posto"].astype(str)
    return df


def _filter(df: pd.DataFrame, postos: List[str], ini: str, fim: str) -> pd.DataFrame:
    if df.empty:
        return df
    mask = (df["mes"] >= ini) & (df["mes"] <= fim)
    if postos:
        mask &= df["posto"].isin(postos)
    return df[mask].copy()


# ─────────────────────────────────────────────────────────────
# Formatação
# ─────────────────────────────────────────────────────────────

def _fmt(v: float) -> str:
    """R$190.105,11"""
    try:
        return f"R${float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "R$0,00"


def _delta(v1: float, v2: float) -> str:
    """Retorna '+R$14.463,36 (+112,3%)' ou '-R$5.000,00 (-10,0%)'"""
    d = v2 - v1
    pct = (d / abs(v1) * 100) if v1 != 0 else 0.0
    sign = "+" if d >= 0 else ""
    return f"{sign}{_fmt(d)} ({sign}{pct:.1f}%)"


def _mes_label(mes: str) -> str:
    """'2026-01' → 'Jan/26'"""
    parts = mes.split("-")
    if len(parts) == 2:
        return f"{_MES_LABELS.get(parts[1], parts[1])}/{parts[0][2:]}"
    return mes


# ─────────────────────────────────────────────────────────────
# Intent detection
# ─────────────────────────────────────────────────────────────

def _detect_intent(pergunta: str) -> str:
    p = pergunta.lower()
    if any(w in p for w in [
        "compar", "diferença", "diferente", "variou", "variação", "variacao",
        "mudou", "aumentou", "diminuiu", "por que", "porque", "cresceu", "caiu",
        "subiu", "vs ", "versus", "entre ", "melhorou", "piorou", "queda",
        "alta", "baixa", "o que mudou", "o que aconteceu", "como ficou",
    ]):
        return "comparison"
    if any(w in p for w in [
        "maior", "menor", "top ", "ranking", "principal", "destaque",
        "mais alto", "mais baixo", "mais caro", "listar", "maiores despesas",
        "principais despesas",
    ]):
        return "ranking"
    if any(w in p for w in [
        "tendência", "tendencia", "evolução", "evolucao", "histórico", "historico",
        "ao longo", "progressão", "crescimento mensal", "mês a mês", "mes a mes",
    ]):
        return "trend"
    return "summary"


# ─────────────────────────────────────────────────────────────
# Month detection
# ─────────────────────────────────────────────────────────────

def _detect_meses(pergunta: str, meses_disp: List[str]) -> Tuple[str, str]:
    """Detecta dois meses para comparar. Fallback: dois últimos disponíveis."""
    if not meses_disp:
        return "", ""

    p = pergunta.lower()
    matched: List[str] = []

    # Padrão explícito YYYY-MM
    for m in re.finditer(r"\b(\d{4})[-/](\d{2})\b", p):
        slug = f"{m.group(1)}-{m.group(2)}"
        if slug in meses_disp and slug not in matched:
            matched.append(slug)

    # Nome de mês
    if len(matched) < 2:
        for mes in sorted(meses_disp):
            month_num = mes[5:7]
            for nome, num in sorted(_MES_NOMES.items(), key=lambda x: -len(x[0])):
                if num == month_num and re.search(rf"\b{re.escape(nome)}\b", p):
                    if mes not in matched:
                        matched.append(mes)
                    break

    # Fallback últimos dois meses
    if len(matched) < 2:
        matched = list(dict.fromkeys(matched + meses_disp))  # preserva ordem, sem dup

    m1 = matched[0] if len(matched) > 0 else meses_disp[0]
    m2 = matched[1] if len(matched) > 1 else meses_disp[-1]
    return m1, m2


# ─────────────────────────────────────────────────────────────
# Posto detection
# ─────────────────────────────────────────────────────────────

def _detect_posto(pergunta: str, postos: List[str]) -> Optional[str]:
    m = re.search(r"\bposto\s+([A-Y])\b", pergunta, re.IGNORECASE)
    if m:
        p = m.group(1).upper()
        return p if (not postos or p in postos) else None
    m = re.search(r"\b(?:em|no|do|para\s+o)\s+([A-Y])\b", pergunta, re.IGNORECASE)
    if m:
        p = m.group(1).upper()
        return p if (not postos or p in postos) else None
    return None


# ─────────────────────────────────────────────────────────────
# Retirada split
# ─────────────────────────────────────────────────────────────

def _split_retirada(
    df_desp: pd.DataFrame, incluir: bool
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Returns (df_operacional, df_retirada)."""
    if df_desp.empty or "tipo" not in df_desp.columns:
        return df_desp.copy(), pd.DataFrame()
    mask = df_desp["tipo"].str.lower().isin(_RETIRADA_TIPOS)
    df_ret = df_desp[mask].copy()
    df_op = df_desp.copy() if incluir else df_desp[~mask].copy()
    return df_op, df_ret


# ─────────────────────────────────────────────────────────────
# Outlier detection — IQR sobre variação absoluta E percentual
# ─────────────────────────────────────────────────────────────

def _find_outliers(
    diffs: List[Tuple[str, float, float, float]]
) -> Tuple[List, List]:
    """
    Recebe lista de (tipo, v1, v2, delta) e retorna (aumentos_outliers, reducoes_outliers).

    Algoritmo IQR: um item é outlier se sua variação absoluta OU percentual
    ultrapassa Q3 + 1.5×IQR da distribuição do grupo.
    Se não houver outliers estatísticos, retorna os 5 maiores de cada direção.
    Cada lado (aumento / redução) é analisado independentemente.
    """
    if not diffs:
        return [], []

    aumentos = [(t, v1, v2, d) for t, v1, v2, d in diffs if d > 0]
    reducoes = [(t, v1, v2, d) for t, v1, v2, d in diffs if d < 0]

    def _iqr_threshold(values: List[float]) -> float:
        if not values:
            return float("inf")
        s = sorted(values)
        n = len(s)
        q1 = s[max(0, n // 4)]
        q3 = s[min(n - 1, (3 * n) // 4)]
        iqr = q3 - q1
        return q3 + 1.5 * iqr if iqr > 0 else q3 * 1.5

    def _filter_outliers(group: List) -> List:
        if len(group) <= 3:
            return sorted(group, key=lambda x: abs(x[3]), reverse=True)

        abs_vals = [abs(d) for _, _, _, d in group]
        pct_vals = [abs(d / v1 * 100) if v1 != 0 else abs(d) * 100 for _, v1, _, d in group]

        thr_abs = _iqr_threshold(abs_vals)
        thr_pct = _iqr_threshold(pct_vals)

        outliers = [
            item for item, av, pv in zip(group, abs_vals, pct_vals)
            if av > thr_abs or pv > thr_pct
        ]

        # fallback: sem outliers estatísticos → top 5 por valor absoluto
        if not outliers:
            outliers = sorted(group, key=lambda x: abs(x[3]), reverse=True)[:5]

        return sorted(outliers, key=lambda x: abs(x[3]), reverse=True)

    return _filter_outliers(aumentos), _filter_outliers(reducoes)


# ─────────────────────────────────────────────────────────────
# Analytics blocks
# ─────────────────────────────────────────────────────────────

def _comparison_block(
    df_rec: pd.DataFrame,
    df_op: pd.DataFrame,
    df_ret: pd.DataFrame,
    mes1: str,
    mes2: str,
    postos: List[str],
    incluir_retirada: bool,
) -> List[str]:
    lbl1, lbl2 = _mes_label(mes1), _mes_label(mes2)
    lines: List[str] = [f"## COMPARATIVO {lbl1} × {lbl2}"]

    for posto in postos:
        rec1 = float(df_rec[(df_rec.mes == mes1) & (df_rec.posto == posto)]["valorpago"].sum())
        rec2 = float(df_rec[(df_rec.mes == mes2) & (df_rec.posto == posto)]["valorpago"].sum())
        d1 = float(df_op[(df_op.mes == mes1) & (df_op.posto == posto)]["valorpago"].sum())
        d2 = float(df_op[(df_op.mes == mes2) & (df_op.posto == posto)]["valorpago"].sum())
        res1 = rec1 - d1
        res2 = rec2 - d2
        mg1 = (res1 / rec1 * 100) if rec1 else 0.0
        mg2 = (res2 / rec2 * 100) if rec2 else 0.0

        lines.append(f"\n### Posto {posto}")
        lines.append(f"- Receita:   {_fmt(rec1)} → {_fmt(rec2)}  ({_delta(rec1, rec2)})")
        lines.append(f"- Despesa:   {_fmt(d1)} → {_fmt(d2)}  ({_delta(d1, d2)})")
        lines.append(f"- Resultado: {_fmt(res1)} → {_fmt(res2)}  ({_delta(res1, res2)})")
        lines.append(f"- Margem:    {mg1:.1f}% → {mg2:.1f}%  ({mg2 - mg1:+.1f} pp)")

        # variações por categoria
        if "tipo" in df_op.columns:
            g1 = df_op[(df_op.mes == mes1) & (df_op.posto == posto)].groupby("tipo")["valorpago"].sum()
            g2 = df_op[(df_op.mes == mes2) & (df_op.posto == posto)].groupby("tipo")["valorpago"].sum()
            all_tipos = g1.index.union(g2.index)
            diffs = [
                (t, float(g1.get(t, 0)), float(g2.get(t, 0)), float(g2.get(t, 0) - g1.get(t, 0)))
                for t in all_tipos
            ]
            if diffs:
                aumentos_out, reducoes_out = _find_outliers(diffs)
                if aumentos_out:
                    lines.append(f"\n#### Aumentos significativos ({lbl1} → {lbl2}):")
                    for tipo, v1, v2, _ in aumentos_out:
                        lines.append(f"  - {tipo}: {_fmt(v1)} → {_fmt(v2)}  ({_delta(v1, v2)})")
                if reducoes_out:
                    lines.append(f"\n#### Reduções significativas ({lbl1} → {lbl2}):")
                    for tipo, v1, v2, _ in reducoes_out:
                        lines.append(f"  - {tipo}: {_fmt(v1)} → {_fmt(v2)}  ({_delta(v1, v2)})")

        # retirada separada
        if not df_ret.empty:
            r1 = float(df_ret[(df_ret.mes == mes1) & (df_ret.posto == posto)]["valorpago"].sum())
            r2 = float(df_ret[(df_ret.mes == mes2) & (df_ret.posto == posto)]["valorpago"].sum())
            if r1 or r2:
                nota = "incluída nos totais" if incluir_retirada else "NÃO incluída nos totais"
                lines.append(f"\n#### Retirada/Campinho ({nota}):")
                lines.append(f"  - {_fmt(r1)} → {_fmt(r2)}  ({_delta(r1, r2)})")

    return lines


def _ranking_block(
    df_op: pd.DataFrame,
    postos: List[str],
    ini: str,
    fim: str,
    top_n: int = 15,
) -> List[str]:
    lines: List[str] = ["## RANKING DE DESPESAS POR CATEGORIA"]
    for posto in postos:
        df_p = df_op[(df_op.posto == posto)]
        if df_p.empty or "tipo" not in df_p.columns:
            lines.append(f"\n### Posto {posto}: sem dados")
            continue
        grp = df_p.groupby("tipo")["valorpago"].sum().sort_values(ascending=False)
        total = float(grp.sum())
        lines.append(f"\n### Posto {posto} ({_mes_label(ini)} a {_mes_label(fim)}) — Total: {_fmt(total)}")
        for tipo, val in grp.head(top_n).items():
            pct = float(val) / total * 100 if total else 0
            lines.append(f"  - {tipo}: {_fmt(float(val))} ({pct:.1f}%)")
    return lines


def _trend_block(
    df_rec: pd.DataFrame,
    df_op: pd.DataFrame,
    postos: List[str],
    ini: str,
    fim: str,
) -> List[str]:
    lines: List[str] = ["## TENDÊNCIA MENSAL"]
    for posto in postos:
        lines.append(f"\n### Posto {posto}")
        meses = sorted(
            set(df_rec[(df_rec.posto == posto)]["mes"].tolist())
        )
        prev_res: Optional[float] = None
        for mes in meses:
            rec = float(df_rec[(df_rec.mes == mes) & (df_rec.posto == posto)]["valorpago"].sum())
            d = float(df_op[(df_op.mes == mes) & (df_op.posto == posto)]["valorpago"].sum())
            res = rec - d
            mg = (res / rec * 100) if rec else 0
            trend = ""
            if prev_res is not None:
                trend = " ↑" if res > prev_res else (" ↓" if res < prev_res else " →")
            lines.append(
                f"  - {_mes_label(mes)}: rec={_fmt(rec)} desp={_fmt(d)} "
                f"result={_fmt(res)} mg={mg:.1f}%{trend}"
            )
            prev_res = res
    return lines


def _summary_block(
    df_rec: pd.DataFrame,
    df_op: pd.DataFrame,
    df_ret: pd.DataFrame,
    postos: List[str],
    ini: str,
    fim: str,
    incluir_retirada: bool,
) -> List[str]:
    lines: List[str] = [f"## RESUMO {_mes_label(ini)} a {_mes_label(fim)}"]
    for posto in postos:
        df_rec_p = df_rec[df_rec.posto == posto]
        df_op_p = df_op[df_op.posto == posto]

        rec = float(df_rec_p["valorpago"].sum())
        d = float(df_op_p["valorpago"].sum())
        res = rec - d
        mg = (res / rec * 100) if rec else 0
        r_total = float(df_ret[df_ret.posto == posto]["valorpago"].sum()) if not df_ret.empty else 0

        nota_desp = "(inclui retirada)" if incluir_retirada else "(exclui retirada)"
        lines.append(f"\n### Posto {posto}")
        lines.append(f"- Receita total:          {_fmt(rec)}")
        lines.append(f"- Despesa total {nota_desp}: {_fmt(d)}")
        lines.append(f"- Resultado:              {_fmt(res)}")
        lines.append(f"- Margem:                 {mg:.1f}%")
        if r_total and not incluir_retirada:
            lines.append(f"- Retirada/Campinho (separado, fora dos totais): {_fmt(r_total)}")

        # Top 8 categorias
        if "tipo" in df_op_p.columns and not df_op_p.empty:
            grp = df_op_p.groupby("tipo")["valorpago"].sum().sort_values(ascending=False)
            lines.append(f"\n#### Top categorias de despesa:")
            for tipo, val in grp.head(8).items():
                lines.append(f"  - {tipo}: {_fmt(float(val))}")

        # Série mensal
        meses = sorted(df_rec_p["mes"].unique().tolist())
        if len(meses) > 1:
            lines.append(f"\n#### Série mensal:")
            for mes in meses:
                r = float(df_rec_p[df_rec_p.mes == mes]["valorpago"].sum())
                de = float(df_op_p[df_op_p.mes == mes]["valorpago"].sum())
                rs = r - de
                mg_m = (rs / r * 100) if r else 0
                lines.append(
                    f"  - {_mes_label(mes)}: rec={_fmt(r)} desp={_fmt(de)} "
                    f"result={_fmt(rs)} mg={mg_m:.1f}%"
                )
    return lines


# ─────────────────────────────────────────────────────────────
# Builders por KPI
# ─────────────────────────────────────────────────────────────

def _build_receita_despesa(
    postos: List[str],
    ini: str,
    fim: str,
    pergunta: str,
    incluir_retirada: bool,
    json_dir_desp: str = "json_consolidado",
) -> str:
    posto_alvo = _detect_posto(pergunta, postos)
    target = [posto_alvo] if posto_alvo else postos

    df_rec = _load_linhas_df("json_consolidado/fin_receita_tipo.json")
    df_desp = _load_linhas_df(f"{json_dir_desp}/fin_despesa_tipo.json")

    df_rec = _filter(df_rec, target, ini, fim)
    df_desp = _filter(df_desp, target, ini, fim)

    if df_rec.empty and df_desp.empty:
        return "Dados não disponíveis para o período/postos selecionados."

    df_op, df_ret = _split_retirada(df_desp, incluir_retirada)
    intent = _detect_intent(pergunta)

    nota_ret = "incluída nos totais" if incluir_retirada else "EXCLUÍDA dos totais (exibida separadamente)"
    header = [
        f"KPI: Receita x Despesas",
        f"Postos: {', '.join(target)}  |  Período: {_mes_label(ini)} a {_mes_label(fim)}",
        f"Retirada: {nota_ret}",
        "",
    ]

    if intent == "comparison":
        meses_disp = sorted(df_rec["mes"].unique().tolist())
        mes1, mes2 = _detect_meses(pergunta, meses_disp)
        if mes1 and mes2 and mes1 != mes2:
            body = _comparison_block(df_rec, df_op, df_ret, mes1, mes2, target, incluir_retirada)
        else:
            body = _summary_block(df_rec, df_op, df_ret, target, ini, fim, incluir_retirada)
    elif intent == "ranking":
        body = _ranking_block(df_op, target, ini, fim)
    elif intent == "trend":
        body = _trend_block(df_rec, df_op, target, ini, fim)
    else:
        body = _summary_block(df_rec, df_op, df_ret, target, ini, fim, incluir_retirada)

    return "\n".join(header + body)


def _build_alimentacao(
    postos: List[str], ini: str, fim: str, pergunta: str, incluir_retirada: bool
) -> str:
    posto_alvo = _detect_posto(pergunta, postos)
    target = [posto_alvo] if posto_alvo else postos

    df = _load_consolidado_df("json_consolidado/consolidado_mensal_por_posto.json")
    df = _filter(df, target, ini, fim)
    if df.empty:
        return "Dados de alimentação não disponíveis."

    for col in ["mensalidade", "alimentacao"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    intent = _detect_intent(pergunta)
    meses = sorted(df["mes"].unique().tolist())
    lines = [
        "KPI: Custo Alimentação x Mensalidades",
        f"Postos: {', '.join(target)}  |  Período: {_mes_label(ini)} a {_mes_label(fim)}",
        "",
    ]

    if intent == "comparison" and len(meses) >= 2:
        mes1, mes2 = _detect_meses(pergunta, meses)
        lines.append(f"## COMPARATIVO {_mes_label(mes1)} × {_mes_label(mes2)}")
        for posto in target:
            r1 = df[(df.mes == mes1) & (df.posto == posto)]
            r2 = df[(df.mes == mes2) & (df.posto == posto)]
            m1 = float(r1["mensalidade"].sum()) if not r1.empty else 0
            m2 = float(r2["mensalidade"].sum()) if not r2.empty else 0
            a1 = float(r1["alimentacao"].sum()) if not r1.empty else 0
            a2 = float(r2["alimentacao"].sum()) if not r2.empty else 0
            pct1 = a1 / m1 * 100 if m1 else 0
            pct2 = a2 / m2 * 100 if m2 else 0
            lines.append(f"\n### Posto {posto}")
            lines.append(f"- Mensalidade: {_fmt(m1)} → {_fmt(m2)}  ({_delta(m1, m2)})")
            lines.append(f"- Alimentação: {_fmt(a1)} → {_fmt(a2)}  ({_delta(a1, a2)})")
            lines.append(f"- % Alim/Mens: {pct1:.1f}% → {pct2:.1f}%  ({pct2 - pct1:+.1f} pp)")
    else:
        lines.append("## RESUMO MENSAL")
        for _, r in df.sort_values(["mes", "posto"]).iterrows():
            mens = float(r.get("mensalidade", 0))
            ali = float(r.get("alimentacao", 0))
            pct = ali / mens * 100 if mens else 0
            lines.append(
                f"  - {_mes_label(r['mes'])} | Posto {r['posto']}: "
                f"mens={_fmt(mens)} ali={_fmt(ali)} %={pct:.1f}%"
            )

    return "\n".join(lines)


def _build_vendas(
    postos: List[str], ini: str, fim: str, pergunta: str, incluir_retirada: bool
) -> str:
    posto_alvo = _detect_posto(pergunta, postos)
    target = [posto_alvo] if posto_alvo else postos

    df = _load_consolidado_df("json_vendas/vendas_mensal.json")
    if df.empty:
        return "Dados de vendas não disponíveis."
    df = _filter(df, target, ini, fim)

    for col in ["valor_total", "qtd_vendas", "ticket_medio"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    intent = _detect_intent(pergunta)
    meses = sorted(df["mes"].unique().tolist())
    lines = [
        "KPI: Vendas",
        f"Postos: {', '.join(target)}  |  Período: {_mes_label(ini)} a {_mes_label(fim)}",
        "",
    ]

    if intent == "comparison" and len(meses) >= 2:
        mes1, mes2 = _detect_meses(pergunta, meses)
        lines.append(f"## COMPARATIVO {_mes_label(mes1)} × {_mes_label(mes2)}")
        for posto in target:
            r1 = df[(df.mes == mes1) & (df.posto == posto)]
            r2 = df[(df.mes == mes2) & (df.posto == posto)]
            vt1 = float(r1["valor_total"].sum()) if not r1.empty else 0
            vt2 = float(r2["valor_total"].sum()) if not r2.empty else 0
            qt1 = int(r1["qtd_vendas"].sum()) if not r1.empty else 0
            qt2 = int(r2["qtd_vendas"].sum()) if not r2.empty else 0
            tk1 = float(r1["ticket_medio"].mean()) if (not r1.empty and r1["ticket_medio"].sum()) else 0
            tk2 = float(r2["ticket_medio"].mean()) if (not r2.empty and r2["ticket_medio"].sum()) else 0
            lines.append(f"\n### Posto {posto}")
            lines.append(f"- Valor total:  {_fmt(vt1)} → {_fmt(vt2)}  ({_delta(vt1, vt2)})")
            lines.append(f"- Qtd vendas:   {qt1} → {qt2}  ({qt2 - qt1:+d})")
            lines.append(f"- Ticket médio: {_fmt(tk1)} → {_fmt(tk2)}  ({_delta(tk1, tk2)})")
    else:
        lines.append("## RESUMO MENSAL")
        for _, r in df.sort_values(["mes", "posto"]).iterrows():
            vt = float(r.get("valor_total", 0))
            qt = int(r.get("qtd_vendas", 0))
            tk = float(r.get("ticket_medio", 0))
            lines.append(
                f"  - {_mes_label(r['mes'])} | Posto {r['posto']}: "
                f"total={_fmt(vt)} qtd={qt} ticket={_fmt(tk)}"
            )

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────
# Qualidade Agenda — context builder especializado
# ─────────────────────────────────────────────────────────────

_POSTO_NOMES = {
    "A": "Anchieta", "B": "Bangu", "C": "Campinho", "D": "Del Castilho",
    "G": "Guadalupe", "I": "Nova Iguaçu", "J": "Jacarepaguá",
    "M": "Madureira", "N": "Nilópolis", "P": "Rio das Pedras",
    "R": "Realengo", "X": "Xerém", "Y": "Campo Grande (Y)",
}

_POSTO_ZONA = {
    "A": "Rio de Janeiro / Corredor Japeri",
    "B": "Rio de Janeiro / Corredor Santa Cruz",
    "C": "Rio de Janeiro",
    "D": "Rio de Janeiro",
    "G": "Rio de Janeiro / Corredor Santa Cruz",
    "I": "Baixada Fluminense / Corredor Japeri",
    "J": "Rio de Janeiro",
    "M": "Rio de Janeiro",
    "N": "Baixada Fluminense / Corredor Japeri",
    "P": "Rio de Janeiro",
    "R": "Rio de Janeiro / Corredor Santa Cruz",
    "X": "Rio de Janeiro / Corredor Santa Cruz",
    "Y": "Rio de Janeiro / Corredor Santa Cruz",
}

_POSTO_VIZINHOS = {
    "A": ["M", "G", "B", "D"], "B": ["R", "A", "G"], "G": ["B", "R", "A", "C"],
    "R": ["B", "G", "A"], "N": ["I", "Y", "X"], "I": ["N", "X", "Y"],
    "X": ["I", "N", "Y"], "Y": ["N", "I", "X"], "C": ["J", "P", "G"],
    "D": ["M", "A"], "J": ["C", "P", "M"], "M": ["D", "A", "J"], "P": ["J", "C"],
}


def _build_qualidade_agenda(
    postos: List[str], ini: str, fim: str, pergunta: str, incluir_retirada: bool
) -> str:
    """Context builder completo para qualidade_agenda — envia TUDO para a IA."""

    # ── Carregar dados da agenda ──
    qa_path = _path("json_consolidado", "qualidade_agenda.json")
    if not os.path.exists(qa_path):
        return "Erro: qualidade_agenda.json não encontrado."
    with open(qa_path, encoding="utf-8") as f:
        qa = json.load(f)

    dados = qa.get("dados", [])
    cbos = qa.get("cbos", {})
    postos_info = qa.get("postos_info", {})
    meta = qa.get("meta", {})

    # Nomes dos postos (do JSON ou fallback)
    pn = {}
    for info in postos_info.values():
        pn[info.get("letra", "")] = info.get("nome", "")
    for k, v in _POSTO_NOMES.items():
        pn.setdefault(k, v)

    # Filtrar por postos selecionados
    postos_sel = postos if postos else sorted(set(d["posto"] for d in dados))
    postos_set = set(postos_sel)
    dados_filtrados = [d for d in dados if d.get("posto") in postos_set]

    if not dados_filtrados:
        return "Nenhum dado de agenda encontrado para os postos selecionados."

    lines = []

    # ── CABEÇALHO ──
    lines.append("QUALIDADE DA AGENDA MÉDICA — CAMIM")
    lines.append(f"Data de referência: {meta.get('data_referencia', 'N/A')}")
    lines.append(f"Postos selecionados: {', '.join(f'{p} ({pn.get(p, p)})' for p in sorted(postos_sel))}")

    # ── RESUMO GERAL ──
    total = len(dados_filtrados)
    n_ok = sum(1 for d in dados_filtrados if d["Status"] == "OK")
    n_al = sum(1 for d in dados_filtrados if d["Status"] == "ALERTA")
    n_cr = sum(1 for d in dados_filtrados if d["Status"] == "CRITICO")
    n_sv = sum(1 for d in dados_filtrados if d["Status"] == "SEM_VAGA")
    score = round((n_ok + n_al * 0.5) / total * 100) if total else 0

    lines.append(f"\nRESUMO GERAL:")
    lines.append(f"Score de Saúde: {score}/100")
    lines.append(f"Total combinações (posto × especialidade): {total}")
    lines.append(f"OK: {n_ok} ({round(n_ok/total*100)}%) | ALERTA: {n_al} ({round(n_al/total*100)}%) | CRÍTICO: {n_cr} ({round(n_cr/total*100)}%) | SEM VAGA: {n_sv} ({round(n_sv/total*100)}%)")

    # ── POR POSTO: cada especialidade com todos os dados ──
    por_posto = {}
    for d in dados_filtrados:
        por_posto.setdefault(d["posto"], []).append(d)

    lines.append(f"\nDETALHAMENTO COMPLETO POR POSTO:")

    for p in sorted(por_posto.keys()):
        pd_list = por_posto[p]
        nome = pn.get(p, p)
        zona = _POSTO_ZONA.get(p, "")
        vizinhos = _POSTO_VIZINHOS.get(p, [])
        viz_nomes = ", ".join(f"{v} ({pn.get(v, v)})" for v in vizinhos)

        p_ok = sum(1 for d in pd_list if d["Status"] == "OK")
        p_al = sum(1 for d in pd_list if d["Status"] == "ALERTA")
        p_cr = sum(1 for d in pd_list if d["Status"] == "CRITICO")
        p_sv = sum(1 for d in pd_list if d["Status"] == "SEM_VAGA")
        p_total = len(pd_list)
        p_score = round((p_ok + p_al * 0.5) / p_total * 100) if p_total else 0

        lines.append(f"\n## {nome} ({p}) — {zona}")
        lines.append(f"Score: {p_score}/100 | OK={p_ok} ALERTA={p_al} CRÍTICO={p_cr} SEM_VAGA={p_sv}")
        lines.append(f"Vizinhos geográficos: {viz_nomes or 'nenhum mapeado'}")

        # Ordenar: SEM_VAGA primeiro, depois CRITICO, ALERTA, OK
        prioridade = {"SEM_VAGA": 0, "CRITICO": 1, "ALERTA": 2, "OK": 3}
        for d in sorted(pd_list, key=lambda x: (prioridade.get(x["Status"], 9), x["Especialidade"])):
            esp = d["Especialidade"]
            st = d["Status"]
            dias = d.get("DiasAteProximaVaga")
            data_vaga = d.get("DataProximaVaga", "")
            vagas_disp = max(d.get("QuantidadeVagasDisponivelNaData", 0), 0)
            cap = max(d.get("QuantidadeVagasTotalMedicosAtendem", 0), 0)
            prazo_ans = d.get("prazoconsultaans", 0)
            prazo_camim = d.get("prazoconsultacamim", 0)
            pct_livre = round(d.get("ValorPercentualVagasLivres", 0))
            reserv = d.get("QuantidadeVagasReservadas", 0)

            if st == "SEM_VAGA":
                lines.append(f"  SEM_VAGA | {esp} | Sem agenda disponível | ANS={prazo_ans}d CAMIM={prazo_camim}d")
            elif st == "CRITICO":
                gap = (dias or 0) - prazo_ans
                lines.append(f"  CRÍTICO  | {esp} | {dias}d até vaga (data: {data_vaga}) | Gap: +{gap}d além ANS | Vagas: {vagas_disp}/{cap} ({pct_livre}% livre) | ANS={prazo_ans}d CAMIM={prazo_camim}d")
            elif st == "ALERTA":
                lines.append(f"  ALERTA   | {esp} | {dias}d até vaga (data: {data_vaga}) | Vagas: {vagas_disp}/{cap} ({pct_livre}% livre) | ANS={prazo_ans}d CAMIM={prazo_camim}d")
            else:
                lines.append(f"  OK       | {esp} | {dias}d até vaga (data: {data_vaga}) | Vagas: {vagas_disp}/{cap} ({pct_livre}% livre) | ANS={prazo_ans}d")

    # ── VISÃO POR ESPECIALIDADE — quais postos estão ruins para cada uma ──
    lines.append(f"\nVISÃO POR ESPECIALIDADE:")
    lines.append("(Para cada especialidade, lista a situação em todos os postos selecionados)")

    # Agrupar por especialidade
    por_esp = {}
    for d in dados_filtrados:
        por_esp.setdefault(d["Especialidade"], []).append(d)

    # Ordenar: especialidades com mais problemas primeiro
    def _esp_gravidade(items):
        return sum(1 for d in items if d["Status"] in ("CRITICO", "SEM_VAGA")) * 100 + \
               sum(1 for d in items if d["Status"] == "ALERTA")

    for esp in sorted(por_esp.keys(), key=lambda e: -_esp_gravidade(por_esp[e])):
        items = por_esp[esp]
        n_prob = sum(1 for d in items if d["Status"] in ("CRITICO", "SEM_VAGA"))
        n_alert = sum(1 for d in items if d["Status"] == "ALERTA")
        n_ok_e = sum(1 for d in items if d["Status"] == "OK")
        total_e = len(items)

        # Calcular CBOS thresholds
        cb = cbos.get(esp, {})
        prazo_ans = cb.get("prazoconsultaans", 0)
        prazo_camim = cb.get("prazoconsultacamim", 0)

        sistemico = n_prob >= 3
        tag = " *** PROBLEMA SISTÊMICO ***" if sistemico else ""

        lines.append(f"\n### {esp} (ANS={prazo_ans}d CAMIM={prazo_camim}d) — Crítico/SemVaga: {n_prob}/{total_e} postos | Alerta: {n_alert} | OK: {n_ok_e}{tag}")

        for d in sorted(items, key=lambda x: (prioridade.get(x["Status"], 9), x["posto"])):
            nome_p = pn.get(d["posto"], d["posto"])
            st = d["Status"]
            dias = d.get("DiasAteProximaVaga")
            vagas = max(d.get("QuantidadeVagasDisponivelNaData", 0), 0)
            cap = max(d.get("QuantidadeVagasTotalMedicosAtendem", 0), 0)
            data_v = d.get("DataProximaVaga", "")

            if st == "SEM_VAGA":
                lines.append(f"  {nome_p} ({d['posto']}): SEM VAGA")
            else:
                lines.append(f"  {nome_p} ({d['posto']}): {st} — {dias}d até vaga ({data_v}), {vagas}/{cap} vagas livres")

        # Sugestões de remanejamento para postos críticos desta especialidade
        criticos_esp = [d for d in items if d["Status"] in ("CRITICO", "SEM_VAGA")]
        ok_esp = [d for d in items if d["Status"] == "OK"]
        if criticos_esp and ok_esp:
            alt_str = ", ".join(f"{pn.get(d['posto'], d['posto'])} ({d.get('DiasAteProximaVaga', '?')}d, {max(d.get('QuantidadeVagasDisponivelNaData',0),0)} vagas)" for d in ok_esp[:3])
            lines.append(f"  -> Alternativas OK: {alt_str}")

    # ── PROBLEMAS SISTÊMICOS (3+ postos críticos na mesma especialidade) ──
    lines.append(f"\nPROBLEMAS SISTÊMICOS (especialidades críticas em 3+ postos):")
    tem_sistemico = False
    for esp, items in por_esp.items():
        n_prob = sum(1 for d in items if d["Status"] in ("CRITICO", "SEM_VAGA"))
        if n_prob >= 3:
            tem_sistemico = True
            postos_prob = [f"{pn.get(d['posto'], d['posto'])}" for d in items if d["Status"] in ("CRITICO", "SEM_VAGA")]
            lines.append(f"  {esp}: {n_prob} postos afetados ({', '.join(postos_prob)}) — remanejamento insuficiente, necessita contratação")
    if not tem_sistemico:
        lines.append("  Nenhum problema sistêmico identificado.")

    # ── DADOS CRUZADOS — churn, vendas, MRR ──
    lines.append(f"\nINDICADORES OPERACIONAIS CRUZADOS:")

    # Growth / MRR / Churn
    gw_path = _path("json_consolidado", "growth_dashboard.json")
    if os.path.exists(gw_path):
        try:
            with open(gw_path, encoding="utf-8") as f:
                gw = json.load(f)
            gw_dados = gw.get("dados", {})
            meses = sorted(gw_dados.keys())
            if meses:
                ultimo_mes = meses[-1]
                penultimo = meses[-2] if len(meses) > 1 else None
                for p in sorted(postos_sel):
                    nome_p = pn.get(p, p)
                    d_atual = gw_dados.get(ultimo_mes, {}).get(p, {})
                    d_ant = gw_dados.get(penultimo, {}).get(p, {}) if penultimo else {}
                    mrr = d_atual.get("mrr_count", 0)
                    cancel = d_atual.get("cancelamentos", 0)
                    base_ant = d_ant.get("mrr_count", 0)
                    churn_pct = round(cancel / base_ant * 100, 1) if base_ant > 0 else 0
                    lines.append(f"  {nome_p} ({p}): {mrr} contratos ativos (ref {ultimo_mes}) | {cancel} cancelamentos | Churn: {churn_pct}%")
        except Exception:
            lines.append("  (Dados de growth/churn indisponíveis)")
    else:
        lines.append("  (growth_dashboard.json não encontrado)")

    # Consultas / Faltas
    cons_path = _path("json_consolidado", "consultas_mensal_status_consolidado.json")
    if os.path.exists(cons_path):
        try:
            with open(cons_path, encoding="utf-8") as f:
                cons = json.load(f)
            # Pegar último mês disponível
            meses_c = sorted(cons.keys())
            if meses_c:
                ult_c = meses_c[-1]
                lines.append(f"\n  Consultas — mês referência: {ult_c}")
                for p in sorted(postos_sel):
                    cd = cons.get(ult_c, {}).get(p, {})
                    if cd:
                        pct_falta = cd.get("pct_falta", 0)
                        total_c = cd.get("total", 0)
                        lines.append(f"  {pn.get(p, p)} ({p}): {total_c} consultas, {round(pct_falta, 1)}% faltas")
        except Exception:
            pass

    # ── GLOSSÁRIO ──
    lines.append(f"\nGLOSSÁRIO:")
    lines.append("- Score Saúde: 0-100. Fórmula: (OK×1 + ALERTA×0.5) / total × 100. Quanto maior melhor.")
    lines.append("- OK: Dias até próxima vaga ≤ prazo ANS (dentro da regulação)")
    lines.append("- ALERTA: Dias > prazo ANS mas ≤ prazo CAMIM (acima da regulação, dentro do interno)")
    lines.append("- CRÍTICO: Dias > prazo CAMIM (acima de ambos os prazos)")
    lines.append("- SEM_VAGA: Nenhuma vaga disponível para agendamento nesta especialidade")
    lines.append("- Prazo ANS: prazo regulatório máximo da ANS para 1ª consulta na especialidade")
    lines.append("- Prazo CAMIM: prazo interno da CAMIM, geralmente mais tolerante que ANS")
    lines.append("- Gap: dias excedentes além do prazo ANS (DiasAteProximaVaga - prazoconsultaans)")
    lines.append("- Problema Sistêmico: especialidade em estado CRITICO/SEM_VAGA em 3+ postos — indica necessidade de contratação, não apenas remanejamento")
    lines.append("- Vagas X/Y: X = vagas disponíveis na data, Y = capacidade total de atendimento na agenda do dia")
    lines.append("- Churn: cancelamentos do mês ÷ base de contratos do mês anterior × 100")
    lines.append("- MRR count: número de contratos de mensalidade ativos no mês")
    lines.append("- Vizinhos: postos geograficamente próximos, úteis para remanejamento de profissionais")

    # ── ZONAS GEOGRÁFICAS ──
    lines.append(f"\nZONAS GEOGRÁFICAS:")
    lines.append("- Baixada Fluminense: Nilópolis (N), Nova Iguaçu (I) — apenas 2 postos")
    lines.append("- Cidade do Rio de Janeiro: A, R, B, Y, G, X, C, D, J, M, P")
    lines.append("- Corredor Japeri (linha de trem): Nilópolis (N), Nova Iguaçu (I), Anchieta (A)")
    lines.append("- Corredor Santa Cruz (linha de trem): Realengo (R), Bangu (B), Campo Grande Y (Y), Guadalupe (G), Xerém (X)")

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────
# Entry point público
# ─────────────────────────────────────────────────────────────

_BUILDERS = {
    "receita_despesa": lambda p, i, f, q, r: _build_receita_despesa(p, i, f, q, r, "json_consolidado"),
    "receita_despesa_rateio": lambda p, i, f, q, r: _build_receita_despesa(p, i, f, q, r, "json_rateio"),
    "alimentacao": _build_alimentacao,
    "vendas": _build_vendas,
    "qualidade_agenda": _build_qualidade_agenda,
}


def _normalizar_periodo(v: str) -> str:
    """Converte MM/YYYY → YYYY-MM; aceita YYYY-MM sem mudança."""
    v = (v or "").strip()
    if re.match(r"^\d{2}/\d{4}$", v):
        return v[3:] + "-" + v[:2]
    return v


def build_context(
    kpi: str,
    postos: List[str],
    periodo_ini: str,
    periodo_fim: str,
    pergunta: str,
    incluir_retirada: bool = False,
) -> str:
    """
    Retorna contexto pré-computado pelo pandas para o LLM.
    Nunca levanta exceção — retorna mensagem de erro legível.
    """
    fn = _BUILDERS.get(kpi)
    if not fn:
        return f"KPI '{kpi}' não reconhecido. Disponíveis: {', '.join(_BUILDERS)}."
    try:
        return fn(
            postos or [],
            _normalizar_periodo(periodo_ini),
            _normalizar_periodo(periodo_fim),
            pergunta or "",
            bool(incluir_retirada),
        )
    except Exception as exc:
        return f"Erro ao montar contexto para '{kpi}': {exc}"
