#!/usr/bin/env python3
"""
auditoria_llm_local.py

LLM Camada 1 — local, 24/7. Lê auditoria_financeira.json, encontra anomalias
sem texto LLM ainda, gera 1–2 linhas explicativas em PT-BR via Ollama (CPU)
e enriquece o JSON in place.

Tech:  Ollama com Llama 3.2 3B (instalado na VM).
       Acesso via HTTP em http://127.0.0.1:11434/api/generate.
Cron:  */60 min (definido em cron/relatorio_ht)
Custo: zero (local, dados não saem da VM).

Bootstrap (uma vez na VM):
    curl -fsSL https://ollama.com/install.sh | sh
    systemctl enable --now ollama
    ollama pull llama3.2:3b      # ~2GB
    # ou llama3.2:1b se priorizar throughput

Variáveis de ambiente (opcionais):
    OLLAMA_URL   default http://127.0.0.1:11434
    OLLAMA_MODEL default llama3.2:3b
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime

JSON_PATH = "/opt/relatorio_h_t/json_consolidado/auditoria_financeira.json"
OLLAMA_URL   = os.environ.get("OLLAMA_URL", "http://127.0.0.1:11434")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "llama3.2:3b")
TIMEOUT      = int(os.environ.get("OLLAMA_TIMEOUT", "60"))
MAX_ITENS    = int(os.environ.get("LLM_LOCAL_MAX", "200"))


def _prompt_para_anomalia(a: dict) -> str:
    e = a.get("evidencia", {}) or {}
    evid_lines = "\n".join(f"  - {k}: {v}" for k, v in e.items())
    valor = f"R$ {float(a.get('valor_atual') or 0):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"""Você é um auditor financeiro. Em PT-BR, escreva 1 ou 2 frases curtas
explicando a anomalia abaixo. Não use bullets, não use markdown, apenas texto
direto. Foque no fato e na evidência. Não invente nada além do dado.

Posto: {a.get('posto')}
Tipo de conta: {a.get('tipo_label')}
Mês de referência: {a.get('mes_ref')}
Valor lançado: {valor}
Regra disparada: {a.get('regra_nome')}
Evidência:
{evid_lines}

Resposta:"""


def _chama_ollama(prompt: str) -> str:
    body = json.dumps({
        "model":  OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False,
        "options": {"temperature": 0.2, "num_predict": 120},
    }).encode("utf-8")
    req = urllib.request.Request(
        f"{OLLAMA_URL}/api/generate",
        data=body, headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    txt = (data.get("response") or "").strip()
    # tira aspas/saudações desnecessárias
    if txt.startswith('"') and txt.endswith('"'):
        txt = txt[1:-1]
    return txt[:500]


def main() -> int:
    if not os.path.exists(JSON_PATH):
        print(f"[skip] {JSON_PATH} não existe ainda", file=sys.stderr)
        return 0

    with open(JSON_PATH, "r", encoding="utf-8") as fh:
        data = json.load(fh)

    anomalias = data.get("anomalias", []) or []
    pendentes = [a for a in anomalias
                 if not a.get("verificado") and not a.get("llm_resumo")]
    pendentes = pendentes[:MAX_ITENS]

    if not pendentes:
        print("[ok] sem itens novos para LLM local")
        return 0

    print(f"[start] {len(pendentes)} item(ns) para resumir via {OLLAMA_MODEL}")
    t0 = time.time()
    enriquecidos = 0
    falhas = 0
    for a in pendentes:
        try:
            resumo = _chama_ollama(_prompt_para_anomalia(a))
            if resumo:
                a["llm_resumo"]  = resumo
                a["llm_modelo"]  = OLLAMA_MODEL
                a["llm_gerado"]  = datetime.now().isoformat(timespec="seconds")
                enriquecidos += 1
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as exc:
            falhas += 1
            print(f"[warn] {a.get('chave','?')[:8]}.. falhou: {exc}",
                   file=sys.stderr)
        except Exception as exc:
            falhas += 1
            print(f"[err] {a.get('chave','?')[:8]}.. {type(exc).__name__}: {exc}",
                   file=sys.stderr)

    if enriquecidos:
        tmp = JSON_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False, indent=2, default=str)
        os.replace(tmp, JSON_PATH)

    print(f"[done] enriquecidos={enriquecidos} falhas={falhas} "
          f"elapsed={time.time()-t0:.1f}s")
    return 0 if falhas < len(pendentes) else 1


if __name__ == "__main__":
    sys.exit(main())
