"""Ponto único de saída das chamadas de IA do relatorio_h_t para a OpenRouter (2026-09-29).

Pedido do Cristiano: tudo que usava GPT-OSS-120B pela Groq e modelos GPT direto da
OpenAI passa pela OpenRouter, com UMA CHAVE POR PROJETO (esta é a do relatorio_h_t,
`OPENROUTER_API_KEY` no .env) — é a chave que faz o centro de custo de IA bater.

- `cliente()` devolve um `openai.OpenAI` apontado para a OpenRouter (mesma API).
- `modelo()` traduz o nome antigo: "gpt-4.1" → "openai/gpt-4.1"; nomes que já têm
  prefixo de fornecedor ("openai/gpt-oss-120b") passam direto.
- `EXTRA_BODY` manda a OpenRouter escolher o provedor MAIS BARATO do modelo
  (`provider.sort = price`). Decisão do Cristiano: "pode colocar tudo no mais barato".
  Dado sensível e privacidade dos provedores: ver docs/divida_tecnica_ia.md.
- Sem `OPENROUTER_API_KEY` no ambiente, `ativo()` é False e cada cliente volta ao
  provedor antigo (Groq/OpenAI direto) — rodar local sem a chave não quebra nada.
"""
from __future__ import annotations

import os
import re

BASE_URL = "https://openrouter.ai/api/v1"
EXTRA_BODY = {"provider": {"sort": "price"}}


def _chave() -> str:
    return (os.getenv("OPENROUTER_API_KEY") or "").strip().strip("'\"")


def ativo() -> bool:
    return bool(_chave())


def cliente():
    from openai import OpenAI
    return OpenAI(
        api_key=_chave(),
        base_url=BASE_URL,
        default_headers={"HTTP-Referer": "https://kpi.camim.com.br", "X-Title": "relatorio_h_t"},
    )


def modelo(nome: str) -> str:
    """"gpt-4.1" → "openai/gpt-4.1"; "claude-sonnet-4-20250514" → "anthropic/claude-sonnet-4"."""
    nome = (nome or "").strip()
    if "/" in nome:
        return nome
    if nome.startswith("claude-"):
        nome = re.sub(r"-\d{8}$", "", nome)          # tira a data de versão da Anthropic
        nome = re.sub(r"-(\d)-(\d)$", r"-\1.\2", nome)  # claude-sonnet-4-5 → claude-sonnet-4.5
        return f"anthropic/{nome}"
    return f"openai/{nome}"
