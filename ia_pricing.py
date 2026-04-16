"""
ia_pricing.py — Tabela de preços por 1M tokens (USD) para cálculo de custo das chamadas LLM.

Valores aproximados de tabela pública (atualize quando mudar a política).
Formato: PRICES[modelo_lower] = (input_usd_per_1M, output_usd_per_1M)
"""

from typing import Optional, Tuple


PRICES: dict[str, Tuple[float, float]] = {
    # ── OpenAI ──────────────────────────────────────────────────────────────
    "gpt-4o":                (2.50, 10.00),
    "gpt-4o-mini":           (0.15,  0.60),
    "gpt-4.1":               (2.00,  8.00),
    "gpt-4.1-mini":          (0.40,  1.60),
    "gpt-4.1-nano":          (0.10,  0.40),
    "o1":                    (15.00, 60.00),
    "o1-mini":               (3.00, 12.00),
    "o3-mini":               (1.10,  4.40),

    # ── Anthropic ───────────────────────────────────────────────────────────
    "claude-sonnet-4-20250514":   (3.00, 15.00),
    "claude-opus-4-20250514":    (15.00, 75.00),
    "claude-3-5-sonnet-20241022": (3.00, 15.00),
    "claude-3-5-haiku-20241022":  (0.80,  4.00),
    "claude-3-opus-20240229":    (15.00, 75.00),

    # ── Groq (preços MUITO menores, a título informativo) ───────────────────
    "openai/gpt-oss-120b":        (0.15,  0.75),
    "llama-3.3-70b-versatile":    (0.59,  0.79),
    "llama-3.1-8b-instant":       (0.05,  0.08),
    "mixtral-8x7b-32768":         (0.24,  0.24),
}


def estimar_custo_usd(model: Optional[str],
                      prompt_tokens: Optional[int],
                      completion_tokens: Optional[int]) -> Optional[float]:
    """Calcula custo em USD com base na tabela PRICES.

    Retorna None se modelo/tokens ausentes ou modelo desconhecido.
    """
    if not model or prompt_tokens is None or completion_tokens is None:
        return None

    key = model.strip().lower()
    precos = PRICES.get(key)
    if precos is None:
        # tenta por prefixo (ex.: "gpt-4o-2024-08-06" → "gpt-4o")
        for k, v in PRICES.items():
            if key.startswith(k):
                precos = v
                break
    if precos is None:
        return None

    p_in, p_out = precos
    custo = (prompt_tokens * p_in + completion_tokens * p_out) / 1_000_000.0
    return round(custo, 6)
