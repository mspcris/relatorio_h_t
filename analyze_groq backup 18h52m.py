# analyze.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os, re, json, time, textwrap, argparse, pathlib, html, unicodedata
from datetime import date, datetime, timezone
from typing import List, Dict, Tuple, Optional, Any

from dotenv import load_dotenv
from groq import Groq
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from threading import Lock

# -------------------------- paths e modelo --------------------------
BASE = pathlib.Path(__file__).parent.resolve()
# substitua as 3 linhas atuais de INP_DIR/OUT_DIR por:
INP_DIR = pathlib.Path(os.getenv("INP_DIR", BASE / "json_consolidado"))
OUT_DIR = pathlib.Path(os.getenv("OUT_DIR", BASE / "json_retorno_groq"))
OUT_DIR.mkdir(exist_ok=True)

FILES = {
    "geral":     INP_DIR / "consolidado_mensal.json",
    "por_posto": INP_DIR / "consolidado_mensal_por_posto.json",  # opcional
}

MODEL = os.getenv("GROQ_MODEL", "openai/gpt-oss-120b")

# -------------------------- contexto do negócio --------------------------
BUSINESS_CTX = (
    "Somos uma rede de clínicas ambulatoriais com plano de saúde próprio em modelo de franquia. "
    "Não oferecemos internação, cirurgias ou parto. Atendimento exclusivamente ambulatorial. "
    "As análises devem refletir esse escopo e evitar suposições sobre hospitalização."
)

# -------------------------- esquemas JSON --------------------------
MENS_SCHEMA = {
    "type": "object",
    "required": ["resumo", "insights", "alertas", "recomendacoes"],
    "properties": {
        "resumo": {"type": "string", "minLength": 8},
        "insights": {"type": "array", "items": {"type": "string"}},
        "alertas": {"type": "array", "items": {"type": "string"}},
        "recomendacoes": {"type": "array", "items": {"type": "string"}}
    },
    "additionalProperties": False
}

MED_SCHEMA = {
    "type": "object",
    "required": ["resumo", "picos_e_vales", "percentual_med_mens", "recomendacoes"],
    "properties": {
        "resumo": {"type": "string", "minLength": 8},
        "picos_e_vales": {"type": "array", "items": {"type": "string"}},
        "percentual_med_mens": {
            "type": "object",
            "required": ["serie"],
            "properties": {
                "serie": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": ["mes", "pct"],
                        "properties": {
                            "mes": {"type": "string", "pattern": r"^\d{4}-\d{2}$"},
                            "pct": {"type": "number"}
                        },
                        "additionalProperties": False
                    }
                }
            },
            "additionalProperties": False
        },
        "recomendacoes": {"type": "array", "items": {"type": "string"}}
    },
    "additionalProperties": False
}

ALIM_SCHEMA = {
    "type": "object",
    "required": ["resumo", "picos_e_vales", "percentual_alim_mens", "recomendacoes"],
    "properties": {
        "resumo": {"type": "string", "minLength": 8},
        "picos_e_vales": {"type": "array", "items": {"type": "string"}},
        "percentual_alim_mens": {
            "type": "object",
            "required": ["serie"],
            "properties": {
                "serie": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": ["mes", "pct"],
                        "properties": {
                            "mes": {"type": "string", "pattern": r"^\d{4}-\d{2}$"},
                            "pct": {"type": "number"}
                        },
                        "additionalProperties": False
                    }
                }
            },
            "additionalProperties": False
        },
        "recomendacoes": {"type": "array", "items": {"type": "string"}}
    },
    "additionalProperties": False
}

# Mesmo contrato que a página espera quando envia "blocks"
MENS_EXT_SCHEMA = MENS_SCHEMA

# -------------------------- sanitização PT-BR --------------------------
def _clean_pt(s: str) -> str:
    s = html.unescape(s or "")
    s = unicodedata.normalize("NFC", s)
    s = s.replace("\u00A0", " ").replace("\u200B", "")
    s = re.sub(r"([.,;:?!])(?!\s|$)", r"\1 ", s)
    s = re.sub(r"([a-zá-úç])([A-ZÁ-ÚÇ])", r"\1 \2", s)
    for h in ["Resumo", "Alertas", "Recomendações", "Recomendacoes", "Picos e vales"]:
        s = re.sub(rf"\b{h}(?=\S)", f"{h}: ", s)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s

def _sanitize_obj(o: Any) -> Any:
    if isinstance(o, dict):
        return {k: _sanitize_obj(v) for k, v in o.items()}
    if isinstance(o, list):
        return [_sanitize_obj(x) for x in o]
    if isinstance(o, str):
        return _clean_pt(o)
    return o

# -------------------------- util de meses --------------------------
def ym_list(start_ym: str, end_ym: str) -> List[str]:
    ys, ms = map(int, start_ym.split("-")); ye, me = map(int, end_ym.split("-"))
    y, m = ys, ms; out = []
    while (y < ye) or (y == ye and m <= me):
        out.append(f"{y:04d}-{m:02d}")
        m = 1 if m == 12 else m + 1
        if m == 1 and out[-1].endswith("-12"): y += 1
    return out

def month_today() -> str:
    t = date.today(); return f"{t.year:04d}-{t.month:02d}"

def last_complete_month(all_months: List[str]) -> str:
    cur = month_today()
    past = [m for m in sorted(all_months) if m < cur]
    if not past: raise RuntimeError("Sem mês completo anterior ao atual.")
    return past[-1]

def includes_current_month(from_ym: str, to_ym: str) -> bool:
    return to_ym >= month_today()

# -------------------------- helpers de JSON robusto --------------------------
def first_json_block(txt: str) -> Optional[str]:
    m = re.search(r"```json\s*(\{.*?\}|\[.*?\])\s*```", txt, flags=re.S)
    cand = m.group(1) if m else None
    if cand:
        try: json.loads(cand); return cand
        except Exception: cand = None
    def _scan(op: str, cl: str) -> Optional[str]:
        depth = 0; start = None
        for i, ch in enumerate(txt):
            if ch == op:
                if depth == 0: start = i
                depth += 1
            elif ch == cl and depth:
                depth -= 1
                if depth == 0 and start is not None:
                    seg = txt[start:i+1]
                    try: json.loads(seg); return seg
                    except Exception: pass
        return None
    return _scan("{","}") or _scan("[","]")

# -------------------------- Groq: JSON schema + fallback --------------------------
def system_prompt() -> str:
    return (
        "Você é um analista financeiro sênior. Entregue respostas técnicas e acionáveis. Você está prestando consultoria para profissionais de empresa familiar em segunda geração. Nenhum economista ou contador está à mesa, explique tecnicamente sempre seguido de uma explicação compreensível para quem não é economista. Anualmente os planos de saúde recebem reajuste autorizado pela ANS, logo, geralmente em junho o índice é liberado o que me faz cobrar o reajuste de junho e maio em julho e agosto, o que pode explicar leves picos. As mensalidades são nossa principal fonte de arrecadação. Os dados aqui analisados são todos em relação ao que recebemos de mensalidades. Não temos alta complexidade. Não atendemos emergência. Não temos internação. Logo, nosso custo é com salário dos médicos. O atendimento oferecido por nosso plano é ambulatorial. Se houver como, verifique nosso site em www.camim.com.br para responder com melhor precisão sobre nós."
        "Considere sazonalidade, tendência, outliers e riscos. "
        "Retorne somente JSON válido, sem markdown. "
        + BUSINESS_CTX
    )

def ask_json(client: Groq, user: str, schema: dict, retries: int = 3, wait_s: float = 1.5) -> dict:
    for _ in range(retries):
        try:
            resp = client.chat.completions.create(
                model=MODEL, temperature=0.1, max_completion_tokens=4096,
                tool_choice="none",
                response_format={"type": "json_schema", "json_schema": {"name": "StructuredOut", "schema": schema, "strict": True}},
                messages=[{"role": "system", "content": system_prompt()},
                          {"role": "user", "content": user}],
            )
            raw = (resp.choices[0].message.content or "").strip()
            obj = json.loads(raw)
            return _sanitize_obj(obj)
        except Exception:
            time.sleep(wait_s)

    for _ in range(retries):
        try:
            resp = client.chat.completions.create(
                model=MODEL, temperature=0.1, max_completion_tokens=4096,
                tool_choice="none", response_format={"type": "json_object"},
                messages=[{"role": "system", "content": "Retorne apenas um objeto JSON válido. " + BUSINESS_CTX},
                          {"role": "user", "content": user}],
            )
            txt = (resp.choices[0].message.content or "").strip()
            blk = first_json_block(txt) or txt
            obj = json.loads(blk)
            return _sanitize_obj(obj)
        except Exception:
            time.sleep(wait_s)

    raise RuntimeError("Falha ao obter JSON válido do modelo.")

# -------------------------- prompts tradicionais --------------------------
def prompt_mensalidades(series: Dict[str, dict], meses: List[str]) -> str:
    dados = {m: series.get(m, {}) for m in meses}
    return textwrap.dedent(f"""
    CONTEXTO:
    - {BUSINESS_CTX}
    - Série mensal com chaves: mensalidade, medico, alimentacao.
    - Meses analisados: {meses[0]}..{meses[-1]} (apenas meses completos).
    DADOS:
    {json.dumps(dados, ensure_ascii=False)}
    TAREFA:
    1) Analise exclusivamente as MENSALIDADES.
    2) Traga tendências, sazonalidade, outliers e variações mensais >±10%.
    3) Liste até 5 recomendações objetivas.
    SAIDA_JSON:
    {{"resumo":"string","insights":["string"],"alertas":["string"],"recomendacoes":["string"]}}
    """).strip()

def prompt_medico(series: Dict[str, dict], meses: List[str]) -> str:
    dados = {m: series.get(m, {}) for m in meses}
    return textwrap.dedent(f"""
    CONTEXTO:
    - {BUSINESS_CTX}
    - Série mensal com chaves: mensalidade, medico, alimentacao.
    - Meses: {meses[0]}..{meses[-1]} (apenas meses completos).
    DADOS:
    {json.dumps(dados, ensure_ascii=False)}
    TAREFA:
    1) Analise CUSTO MÉDICO em valor.
    2) Calcule e analise a % MÉDICO/MENSALIDADE para cada mês.
    3) Destaque picos/vales e meses atípicos e drivers prováveis.
    4) Liste ações para reduzir pressão de custo.
    SAIDA_JSON:
    {{"resumo":"string","picos_e_vales":["string"],"percentual_med_mens":{{"serie":[{{"mes":"YYYY-MM","pct":0.0}}]}}, "recomendacoes":["string"]}}
    """).strip()

def prompt_alimentacao(series: Dict[str, dict], meses: List[str]) -> str:
    dados = {m: series.get(m, {}) for m in meses}
    return textwrap.dedent(f"""
    CONTEXTO:
    - {BUSINESS_CTX}
    - Série mensal com chaves: mensalidade, medico, alimentacao.
    - Meses: {meses[0]}..{meses[-1]} (apenas meses completos).
    DADOS:
    {json.dumps(dados, ensure_ascii=False)}
    TAREFA:
    1) Analise CUSTO DE ALIMENTAÇÃO em valor.
    2) Calcule e analise a % ALIMENTAÇÃO/MENSALIDADE para cada mês.
    3) Aponte tendências, picos/vales e hipóteses.
    4) Liste recomendações objetivas.
    SAIDA_JSON:
    {{"resumo":"string","picos_e_vales":["string"],"percentual_alim_mens":{{"serie":[{{"mes":"YYYY-MM","pct":0.0}}]}}, "recomendacoes":["string"]}}
    """).strip()

# -------------------------- prompt integrado para "blocks" --------------------------
def prompt_from_blocks(blocks: List[Dict[str, Any]]) -> str:
    def _short(o): 
        try: 
            return json.dumps(o, ensure_ascii=False)
        except Exception:
            return str(o)

    sections = []
    for b in blocks or []:
        sc = b.get("scope")
        if sc in ("kpi_sel1", "kpi_sel2"):
            sections.append(f"{sc}: { _short(b.get('kpis', {})) }  período={_short(b.get('periodo'))} posto={b.get('posto')}")
        elif sc == "tabela_12m":
            keep = ["mes","receita","qtd","ticket","perf_mom","r_mom","q_mom","t_mom","r_yoy","q_yoy","t_yoy","logret","dpreco","dvolume","dinter"]
            rows = [{k:r.get(k) for k in keep} for r in (b.get("rows") or [])]
            sections.append(f"{sc}: { _short(rows) }")
        elif sc in ("chart_valores","chart_qtd_ticket","chart_perf","chart_yoy","chart_mix","chart_scatter"):
            pack = {k:b.get(k) for k in ('periodo','posto','series','perf','yoy','dpreco','dvolume') if b.get(k) is not None}
            sections.append(f"{sc}: { _short(pack) }")
        elif sc == "chart_pareto":
            pack = {k:b.get(k) for k in ("labels","valores","total") if k in b}
            sections.append(f"{sc}: { _short(pack) }")
        elif sc == "chart_yoy_posto":
            pack = {k:b.get(k) for k in ("postos","yoy_med") if k in b}
            sections.append(f"{sc}: { _short(pack) }")
        else:
            sections.append(f"{sc or 'desconhecido'}: { _short(b) }")

    corpo = "\n".join(f"- {s}" for s in sections) or "(sem blocos)"
    return textwrap.dedent(f"""
    CONTEXTO:
    {BUSINESS_CTX}
    Você receberá blocos heterogêneos vindos de uma UI (KPIs, Tabela 12m e séries de gráficos).

    TAREFA:
    1) Faça um diagnóstico integrado de MENSALIDADES usando TODAS as evidências.
       - Tendência, sazonalidade, outliers, mudanças >±10% MoM e YoY.
       - Conecte ΔPreço e ΔVolume à variação de receita.
       - Use LogRet/volatilidade para comentar estabilidade/risco.
       - Se houver Pareto: comente concentração por posto.
       - Se houver YoY por posto: destaque top 3 e bottom 3.
    2) Liste 3–7 insights e até 5 alertas.
    3) Liste 3–7 recomendações práticas.
    4) NÃO invente números que não estejam nos blocos.

    SAÍDA JSON (apenas JSON):
    {{"resumo":"string","insights":["string"],"alertas":["string"],"recomendacoes":["string"]}}

    DADOS:
    {corpo}
    """).strip()

# -------------------------- dados e série por posto --------------------------
def read_json(path: pathlib.Path):
    with open(path, "r", encoding="utf-8") as f: 
        return json.load(f)

def load_data() -> Tuple[Dict[str, dict], Dict[str, Dict[str, dict]]]:
    if not FILES["geral"].exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {FILES['geral']}")
    geral = read_json(FILES["geral"])
    por_posto = {}
    if FILES["por_posto"].exists():
        por_posto = read_json(FILES["por_posto"])
    return geral, por_posto

def aggregate_all_from_por_posto(por_posto: Dict[str, Dict[str, dict]]) -> Dict[str, dict]:
    out = {}
    for mes, postos in por_posto.items():
        tot = {"mensalidade":0.0,"medico":0.0,"alimentacao":0.0}
        for _, m in postos.items():
            tot["mensalidade"] += float(m.get("mensalidade",0) or 0)
            tot["medico"]      += float(m.get("medico",0) or 0)
            tot["alimentacao"] += float(m.get("alimentacao",0) or 0)
        out[mes] = tot
    return out

def series_for(geral: Dict[str, dict], por_posto: Dict[str, Dict[str, dict]], posto: str) -> Dict[str, dict]:
    if posto == "ALL":
        return geral if geral else aggregate_all_from_por_posto(por_posto)
    s: Dict[str, dict] = {}
    for mes, postos in por_posto.items():
        m = postos.get(posto)
        if m: 
            s[mes] = {
                "mensalidade": float(m.get("mensalidade",0) or 0),
                "medico": float(m.get("medico",0) or 0),
                "alimentacao": float(m.get("alimentacao",0) or 0)
            }
    return s

# -------------------------- saída --------------------------
def out_path(from_ym: str, to_ym: str, kind: str, posto: str) -> pathlib.Path:
    safe_posto = re.sub(r"[^A-Za-z0-9_-]+", "_", posto)
    return OUT_DIR / f"{from_ym}_{to_ym}_{safe_posto}_{kind}.json"

def need_range(from_ym: str, to_ym: str, posto: str) -> bool:
    for k in ("mensalidades", "medico", "alimentacao"):
        if not out_path(from_ym, to_ym, k, posto).exists():
            return True
    return False

# -------------------------- núcleo tradicional --------------------------
def run_one_range(client: Groq, geral: Dict[str, dict], por_posto: Dict[str, Dict[str, dict]],
                  from_ym: str, to_ym: str, posto: str, force: bool=False):
    meses = ym_list(from_ym, to_ym)
    series = series_for(geral, por_posto, posto)
    if includes_current_month(from_ym, to_ym): 
        force = True

    def _save(kind: str, prompt_builder, schema: dict):
        p = out_path(from_ym, to_ym, kind, posto)
        if force or not p.exists():
            obj = ask_json(client, prompt_builder(series, meses), schema)
            p.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
            print("OK:", p.name)

    _save("mensalidades", prompt_mensalidades, MENS_SCHEMA)
    _save("medico",        prompt_medico,        MED_SCHEMA)
    _save("alimentacao",   prompt_alimentacao,   ALIM_SCHEMA)

def precompute_all(force: bool=False, start_ym: str="2024-01", posto: str="ALL"):
    geral, por_posto = load_data()
    base_series = series_for(geral, por_posto, posto)
    all_months = sorted(base_series.keys())
    if not all_months: 
        raise RuntimeError("Sem meses na série base.")
    last_complete = last_complete_month(all_months)

    if start_ym not in all_months:
        all_ge = [m for m in all_months if m >= start_ym]
        if not all_ge: 
            raise RuntimeError("Sem meses >= start.")
        start_ym = all_ge[0]

    months = [m for m in all_months if start_ym <= m <= last_complete]
    print(f"Meses considerados: {months[0]} .. {months[-1]}  (último completo: {last_complete}) [posto={posto}]")

    load_dotenv(); api_key = os.getenv("GROQ_API_KEY")
    if not api_key: 
        raise RuntimeError("GROQ_API_KEY ausente no ambiente/.env")
    client = Groq(api_key=api_key)

    n = len(months)
    for i in range(n):
        for j in range(i, n):
            from_ym, to_ym = months[i], months[j]
            if not force and not need_range(from_ym, to_ym, posto): 
                continue
            print(f"-- Range {from_ym}..{to_ym} [{posto}]")
            run_one_range(client, geral, por_posto, from_ym, to_ym, posto, force=force)

    meta = {
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "model": MODEL, "first_month": months[0], "last_complete_month": months[-1],
        "posto": posto, "note": "Somente meses completos; mês corrente força regen."
    }
    (OUT_DIR / "_meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    print("Meta atualizado:", (OUT_DIR / "_meta.json").name)

# -------------------------- API on-demand --------------------------
app = FastAPI(title="IA-Groq-Analises")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
_lock = Lock()

class Reqs(BaseModel):
    from_ym: str
    to_ym:   str
    posto:   str







@app.post("/ia/analisar")
async def ia_analisar(payload: dict = Body(...)):
    if isinstance(payload, dict) and "blocks" in payload:
        load_dotenv(); api_key = os.getenv("GROQ_API_KEY")
        if not api_key: raise HTTPException(500, "GROQ_API_KEY ausente.")
        client = Groq(api_key=api_key)
        user = prompt_from_blocks(payload.get("blocks", []))
        return ask_json(client, user, MENS_EXT_SCHEMA)







        # legado (se quiser manter)
        data = Reqs(**payload)
        # ... resto igual
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(500, f"Erro: {e}")
















# -------------------------- CLI --------------------------
def parse_args():
    p = argparse.ArgumentParser(description="Pré-gerar análises IA por range de meses.")
    p.add_argument("--force", action="store_true", help="Regerar mesmo se já existir.")
    p.add_argument("--start", default="2024-01", help="Mês inicial mínimo (default: 2024-01).")
    p.add_argument("--only", nargs=3, metavar=("FROM_YYYY-MM", "TO_YYYY-MM", "POSTO"),
                   help="Gerar somente um range e um posto específico (POSTO ou ALL).")
    p.add_argument("--posto", default="ALL", help="Posto para pré-geração (default: ALL).")
    p.add_argument("--serve", action="store_true", help="Inicia API on-demand.")
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", type=int, default=8000)
    return p.parse_args()

def main():
    args = parse_args()
    if args.serve:
        import uvicorn
        uvicorn.run("analyze_groq:app", host=args.host, port=args.port, reload=False)
        return
    if args.only:
        from_ym, to_ym, posto = args.only
        geral, por_posto = load_data()
        load_dotenv(); api_key = os.getenv("GROQ_API_KEY")
        if not api_key: 
            raise RuntimeError("GROQ_API_KEY ausente no ambiente/.env")
        client = Groq(api_key=api_key)
        run_one_range(client, geral, por_posto, from_ym, to_ym, posto, force=args.force)
        meta = {"updated_at_utc": datetime.now(timezone.utc).isoformat(), "model": MODEL, "note": "Execução 'only'.", "posto": posto}
        (OUT_DIR / "_meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
        return
    precompute_all(force=args.force, start_ym=args.start, posto=args.posto)

if __name__ == "__main__":
    main()
