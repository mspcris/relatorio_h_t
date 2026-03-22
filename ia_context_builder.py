"""
ia_context_builder.py — Backend context builder para IA CAMIM.

Responsabilidade: ler os JSONs do disco com pandas, filtrar pelos postos
e período solicitados, e devolver uma string de contexto compacta e sempre
válida para o LLM. O browser não precisa mais enviar dados — só envia
{ kpi, postos, periodo_ini, periodo_fim, prompt }.

Estrutura dos JSONs suportados:
  dados/<mes>/<posto>/linhas: [{valorpago, tipo|Tipo|plano|...}]
  (conforme gerado por export_fin_full.py e export_governanca.py)
"""

import json
import os
import re
from typing import List, Optional

import pandas as pd

JSON_ROOT = os.getenv("JSON_ROOT", "/var/www")

# Postos que representam "retirada" (excluídos por padrão nas despesas)
_RETIRADA_TIPOS = {"retirada", "campinho", "campinho/retirada"}


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def _path(*parts: str) -> str:
    return os.path.join(JSON_ROOT, *parts)


def _load_linhas_df(rel_path: str) -> pd.DataFrame:
    """
    Carrega JSON com estrutura {dados: {mes: {posto: {linhas: [...]}}}}
    e retorna DataFrame flat com colunas: mes, posto + campos da linha.
    """
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
    return df


def _load_consolidado_df(rel_path: str) -> pd.DataFrame:
    """
    Carrega JSON com estrutura {mes: {posto: {campo: valor}}}
    (consolidado_mensal_por_posto, vendas_mensal, etc.)
    e retorna DataFrame flat.
    """
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


def _detect_posto(pergunta: str, postos_disponiveis: List[str]) -> Optional[str]:
    """Detecta se a pergunta menciona um posto específico."""
    m = re.search(r"\bposto\s+([A-Y])\b", pergunta, re.IGNORECASE)
    if m:
        p = m.group(1).upper()
        return p if (not postos_disponiveis or p in postos_disponiveis) else None
    # Busca letra isolada: "em X", "no X", "o X"
    m = re.search(r"\b(?:em|no|do|para\s+o)\s+([A-Y])\b", pergunta, re.IGNORECASE)
    if m:
        p = m.group(1).upper()
        return p if (not postos_disponiveis or p in postos_disponiveis) else None
    return None


def _r2(v) -> float:
    try:
        return round(float(v), 2)
    except Exception:
        return 0.0


def _fmt(v: float) -> str:
    return f"R${v:,.2f}"


# ─────────────────────────────────────────────────────────────
# Builders por KPI
# ─────────────────────────────────────────────────────────────

def _build_receita_despesa(
    postos: List[str], ini: str, fim: str, pergunta: str,
    json_dir_desp: str = "json_consolidado"
) -> str:
    posto_alvo = _detect_posto(pergunta, postos)
    target = [posto_alvo] if posto_alvo else postos

    df_desp = _load_linhas_df(f"{json_dir_desp}/fin_despesa_tipo.json")
    df_rec  = _load_linhas_df("json_consolidado/fin_receita_tipo.json")

    df_desp = _filter(df_desp, target, ini, fim)
    df_rec  = _filter(df_rec,  target, ini, fim)

    if df_desp.empty and df_rec.empty:
        return "Dados não disponíveis para o período/postos selecionados."

    # Normaliza coluna tipo
    for df in [df_desp, df_rec]:
        if "Tipo" in df.columns and "tipo" not in df.columns:
            df.rename(columns={"Tipo": "tipo"}, inplace=True)

    lines = [
        f"KPI: Receita x Despesas",
        f"Postos: {', '.join(target)}  |  Período: {ini} → {fim}",
        "",
    ]

    # ── Receita por mês/posto ──────────────────────────────
    if not df_rec.empty:
        lines.append("## Receita por mês e posto")
        grp = df_rec.groupby(["mes", "posto"])["valorpago"].sum().reset_index()
        for _, r in grp.sort_values(["mes", "posto"]).iterrows():
            lines.append(f"  {r['mes']} | {r['posto']}: {_fmt(r['valorpago'])}")

    # ── Despesa por mês/posto/tipo ─────────────────────────
    if not df_desp.empty and "tipo" in df_desp.columns:
        # Remove retirada
        mask_ret = df_desp["tipo"].str.lower().isin(_RETIRADA_TIPOS)
        df_ret   = df_desp[mask_ret].copy()
        df_desp2 = df_desp[~mask_ret].copy()

        lines.append("\n## Despesa por mês, posto e tipo (excluindo retirada)")
        grp_desp = df_desp2.groupby(["mes", "posto", "tipo"])["valorpago"].sum().reset_index()

        for (mes, posto), grp in grp_desp.sort_values(["mes", "posto"]).groupby(["mes", "posto"]):
            total = grp["valorpago"].sum()
            lines.append(f"  {mes} | {posto} | Total: {_fmt(total)}")
            # Top 15 tipos
            for _, r in grp.nlargest(15, "valorpago").iterrows():
                lines.append(f"    - {r['tipo']}: {_fmt(r['valorpago'])}")

        if not df_ret.empty:
            lines.append("\n## Retirada/Campinho (destacado)")
            grp_ret = df_ret.groupby(["mes", "posto"])["valorpago"].sum().reset_index()
            for _, r in grp_ret.sort_values(["mes", "posto"]).iterrows():
                lines.append(f"  {r['mes']} | {r['posto']}: {_fmt(r['valorpago'])}")

    # ── Resultado por mês/posto ────────────────────────────
    if not df_rec.empty and not df_desp.empty:
        lines.append("\n## Resultado (Receita − Despesa) por mês e posto")
        rec_grp  = df_rec.groupby(["mes", "posto"])["valorpago"].sum()
        desp_grp = df_desp.groupby(["mes", "posto"])["valorpago"].sum()
        idx = rec_grp.index.union(desp_grp.index)
        for (mes, posto) in sorted(idx):
            rec  = _r2(rec_grp.get((mes, posto), 0))
            desp = _r2(desp_grp.get((mes, posto), 0))
            res  = _r2(rec - desp)
            mg   = round(res / rec * 100, 2) if rec else 0
            lines.append(f"  {mes} | {posto}: rec={_fmt(rec)} desp={_fmt(desp)} resultado={_fmt(res)} margem={mg}%")

    return "\n".join(lines)


def _build_alimentacao(postos: List[str], ini: str, fim: str, pergunta: str) -> str:
    posto_alvo = _detect_posto(pergunta, postos)
    target = [posto_alvo] if posto_alvo else postos

    df = _load_consolidado_df("json_consolidado/consolidado_mensal_por_posto.json")
    df = _filter(df, target, ini, fim)

    if df.empty:
        return "Dados de alimentação não disponíveis."

    for col in ["mensalidade", "alimentacao"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    lines = [
        f"KPI: Custo Alimentação x Mensalidades",
        f"Postos: {', '.join(target)}  |  Período: {ini} → {fim}",
        "",
        "## Mensalidade, Alimentação e % por mês/posto",
    ]

    for _, r in df.sort_values(["mes", "posto"]).iterrows():
        mens = _r2(r.get("mensalidade", 0))
        ali  = _r2(r.get("alimentacao", 0))
        pct  = round(ali / mens * 100, 2) if mens else 0
        lines.append(f"  {r['mes']} | {r['posto']}: mens={_fmt(mens)} ali={_fmt(ali)} %ali={pct}%")

    return "\n".join(lines)


def _build_vendas(postos: List[str], ini: str, fim: str, pergunta: str) -> str:
    posto_alvo = _detect_posto(pergunta, postos)
    target = [posto_alvo] if posto_alvo else postos

    df = _load_consolidado_df("json_vendas/vendas_mensal.json")
    # vendas_mensal pode ter por_posto aninhado — tenta direto
    if df.empty:
        return "Dados de vendas não disponíveis."

    df = _filter(df, target, ini, fim)

    for col in ["valor_total", "qtd_vendas", "ticket_medio"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    lines = [
        f"KPI: Vendas",
        f"Postos: {', '.join(target)}  |  Período: {ini} → {fim}",
        "",
        "## Vendas por mês/posto",
    ]
    for _, r in df.sort_values(["mes", "posto"]).iterrows():
        vt = _r2(r.get("valor_total", 0))
        qt = int(r.get("qtd_vendas", 0))
        tk = _r2(r.get("ticket_medio", 0))
        lines.append(f"  {r['mes']} | {r['posto']}: total={_fmt(vt)} qtd={qt} ticket={_fmt(tk)}")

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────
# Entry point público
# ─────────────────────────────────────────────────────────────

_BUILDERS = {
    "receita_despesa":        lambda p, i, f, q: _build_receita_despesa(p, i, f, q, "json_consolidado"),
    "receita_despesa_rateio": lambda p, i, f, q: _build_receita_despesa(p, i, f, q, "json_rateio"),
    "alimentacao":            _build_alimentacao,
    "vendas":                 _build_vendas,
}


def build_context(
    kpi: str,
    postos: List[str],
    periodo_ini: str,
    periodo_fim: str,
    pergunta: str,
) -> str:
    """
    Retorna string de contexto compacta e sempre válida para o LLM.
    Nunca levanta exceção — retorna mensagem de erro se algo falhar.
    """
    fn = _BUILDERS.get(kpi)
    if not fn:
        return f"KPI '{kpi}' não reconhecido. Disponíveis: {', '.join(_BUILDERS)}."
    try:
        return fn(postos or [], periodo_ini or "", periodo_fim or "", pergunta or "")
    except Exception as exc:
        return f"Erro ao montar contexto para '{kpi}': {exc}"
