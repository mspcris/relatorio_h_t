#!/usr/bin/env python3
"""
auditoria_llm_relatorio.py

LLM Camada 2 — API top-tier, 1×/dia. Lê auditoria_financeira.json e produz
um relatório executivo em markdown analisando padrões sistêmicos:
  - tendências dos últimos 24m
  - concentração de anomalias por posto / plano_principal
  - itens de maior severidade

Tech:  OpenAI via SDK (llm_client_openai.py). Modelo configurável por env
       OPENAI_AUDITORIA_MODEL (default: gpt-5-mini). Custo típico < R$0,30/dia.
Cron:  0 6 * * *  (após o motor das 03:00)

Saída:
  /opt/relatorio_h_t/json_consolidado/auditoria_relatorio_diario.md
  /opt/relatorio_h_t/json_consolidado/_etl_meta_auditoria_llm_relatorio.json

Uso manual:
  /opt/relatorio_h_t/.venv/bin/python auditoria_llm_relatorio.py
"""
from __future__ import annotations

import json
import os
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime

# Carrega .env do auth (onde está ANTHROPIC_API_KEY)
from dotenv import load_dotenv
for _candidate in ("/etc/camim-auth.env",
                   "/opt/relatorio_h_t/.env",
                   os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")):
    if os.path.exists(_candidate):
        load_dotenv(_candidate, override=False)

JSON_PATH = "/opt/relatorio_h_t/json_consolidado/auditoria_financeira.json"
OUT_MD    = "/opt/relatorio_h_t/json_consolidado/auditoria_relatorio_diario.md"
META_OUT  = "/opt/relatorio_h_t/json_consolidado/_etl_meta_auditoria_llm_relatorio.json"

# Limita o que mandamos pra API (evita prompt gigante e custo)
TOP_N_ANOMALIAS = int(os.environ.get("LLM_RELATORIO_TOP", "40"))


def _construir_briefing(data: dict) -> str:
    anomalias = [a for a in data.get("anomalias", []) if not a.get("verificado")]
    benford   = data.get("benford", {}) or {}
    scores    = data.get("scores_postos", {}) or {}

    # Top severidade: zscore primeiro, depois maior valor
    prio = {"zscore_robusto": 0, "mm_pct": 1, "fornecedor_novo": 2,
            "gap_temporal": 3, "nao_recorrente_pct": 4}
    anomalias.sort(key=lambda a: (prio.get(a.get("regra_tipo"), 99),
                                  -float(a.get("valor_atual") or 0)))
    top = anomalias[:TOP_N_ANOMALIAS]

    # Distribuições
    por_posto = Counter(a["posto"] for a in anomalias)
    por_regra = Counter(a["regra_tipo"] for a in anomalias)
    por_tipo  = Counter(str(a.get("tipo_label") or a.get("id_conta_tipo")) for a in anomalias)

    # Benford resumo
    benford_resumo = []
    for p in sorted((benford.get("por_posto") or {}).keys()):
        bp = benford["por_posto"][p].get("saidas") or {}
        benford_resumo.append(f"  {p}: cor={bp.get('cor','?')}, MAD={bp.get('mad','?')}, n={bp.get('n',0)}")

    rede = benford.get("rede", {}) or {}
    cor_botao = benford.get("cor_botao", "?")

    # Lista enxuta dos top
    bullets = []
    for a in top:
        e = a.get("evidencia", {})
        e_str = ", ".join(f"{k}={v}" for k, v in e.items())
        valor = float(a.get("valor_atual") or 0)
        bullets.append(
            f"- [{a['posto']}] {a.get('tipo_label')}: R$ {valor:,.2f}".replace(",","X").replace(".",",").replace("X",".")
            + f" em {a['mes_ref']} — {a.get('regra_nome')} ({e_str})"
        )

    return f"""# Briefing — Auditoria Financeira CAMIM

Janela: últimos {data.get('janela_meses','?')} meses
Geração do motor: {data.get('generated_at','?')}

## Saúde dos postos (score 0-100)
{json.dumps(scores, ensure_ascii=False, indent=2)}

## Benford — saídas, por posto
Cor do botão (pior MAD): **{cor_botao}**
Rede inteira: cor={rede.get('saidas',{}).get('cor','?')}, MAD={rede.get('saidas',{}).get('mad','?')}, n={rede.get('saidas',{}).get('n',0)}

Por posto:
{chr(10).join(benford_resumo)}

## Distribuição das anomalias abertas
- Por posto: {dict(por_posto)}
- Por regra: {dict(por_regra)}
- Top 10 tipos de conta: {dict(por_tipo.most_common(10))}

## Top {len(top)} anomalias abertas (severidade × valor)
{chr(10).join(bullets)}
"""


SYSTEM_PROMPT = """Você é um analista financeiro sênior de uma rede de clínicas
médicas (CAMIM, 13 postos). Recebe um briefing diário de auditoria e produz um
relatório executivo curto e direto em PT-BR.

Princípios:
- Texto em markdown bem formatado (use ## para seções, listas onde fizer sentido)
- Foco em insights, não em parafrasear o briefing
- Apontar padrões: postos com maior risco, categorias problemáticas, tendências
- Sugerir 2-3 ações concretas que o gestor financeiro deveria fazer hoje
- Não compare postos entre si (têm portes muito diferentes); compare cada posto consigo mesmo
- Se Benford de algum posto está vermelho, comente especificamente
- Não invente números além dos que estão no briefing
- Limite total: ~600 palavras
"""


def main() -> int:
    started_at = datetime.now()
    if not os.path.exists(JSON_PATH):
        print(f"[skip] {JSON_PATH} não existe ainda", file=sys.stderr)
        return 0

    with open(JSON_PATH, "r", encoding="utf-8") as fh:
        data = json.load(fh)

    briefing = _construir_briefing(data)

    try:
        from llm_client_openai import LLMClientOpenAI, LLMConfig
    except Exception as exc:
        print(f"[err] não consegui importar llm_client_openai: {exc}",
              file=sys.stderr)
        return 1

    if not os.environ.get("OPENAI_API_KEY"):
        print("[err] OPENAI_API_KEY ausente; nada a fazer", file=sys.stderr)
        return 1

    # Modelo configurável; default gpt-5-mini (sobrescreve OPENAI_MODEL global
    # do client pra não interferir com outros usos da chave OpenAI).
    modelo = os.environ.get("OPENAI_AUDITORIA_MODEL", "gpt-5-mini")
    # max_tokens alto: gpt-5-mini e o-series gastam parte (às vezes a maior)
    # com "thinking interno" antes de emitir resposta. 2000 era curto demais.
    cli = LLMClientOpenAI(LLMConfig(model=modelo, temperature=0.3, max_tokens=8000))
    print(f"[call] modelo={cli.config.model}  briefing={len(briefing)} chars")
    t0 = time.time()
    relatorio = cli.gerar_texto(
        prompt=briefing,
        system_prompt=SYSTEM_PROMPT,
        temperature=0.3,
        max_tokens=8000,
    )
    elapsed = time.time() - t0

    cabecalho = (
        f"# Relatório executivo — Auditoria Financeira\n"
        f"_Gerado em {started_at.isoformat(timespec='seconds')} "
        f"({cli.config.model}, {elapsed:.1f}s)_\n\n"
    )

    tmp = OUT_MD + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write(cabecalho + relatorio)
    os.replace(tmp, OUT_MD)

    finished_at = datetime.now()
    meta = {
        "script":      "auditoria_llm_relatorio",
        "started_at":  started_at.isoformat(timespec="seconds"),
        "finished_at": finished_at.isoformat(timespec="seconds"),
        "duracao_segundos": round((finished_at - started_at).total_seconds(), 2),
        "modelo":      cli.config.model,
        "usage":       cli.last_usage,
        "out":         OUT_MD,
    }
    with open(META_OUT + ".tmp", "w", encoding="utf-8") as fh:
        json.dump(meta, fh, ensure_ascii=False, indent=2)
    os.replace(META_OUT + ".tmp", META_OUT)

    print(f"[ok] {OUT_MD}  {elapsed:.1f}s  usage={cli.last_usage}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
