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
            diffs.sort(key=lambda x: abs(x[3]), reverse=True)
            if diffs:
                lines.append(f"\n#### Variações por categoria ({lbl1} → {lbl2}):")
                for tipo, v1, v2, diff in diffs[:12]:
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
# Entry point público
# ─────────────────────────────────────────────────────────────

_BUILDERS = {
    "receita_despesa": lambda p, i, f, q, r: _build_receita_despesa(p, i, f, q, r, "json_consolidado"),
    "receita_despesa_rateio": lambda p, i, f, q, r: _build_receita_despesa(p, i, f, q, r, "json_rateio"),
    "alimentacao": _build_alimentacao,
    "vendas": _build_vendas,
}


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
            periodo_ini or "",
            periodo_fim or "",
            pergunta or "",
            bool(incluir_retirada),
        )
    except Exception as exc:
        return f"Erro ao montar contexto para '{kpi}': {exc}"
