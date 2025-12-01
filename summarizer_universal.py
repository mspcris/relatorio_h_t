# summarizer_universal.py
"""
Este módulo implementa um “resumidor universal” de texto, reutilizável em qualquer página.
Objetivos:
1) Gerar um resumo curto, sempre em PT-BR, focado em ser executivo e direto.
2) Padronizar tamanho do resumo (poucas frases), controlando expectativas do usuário.
3) Abstrair o prompt de resumo em um único lugar, para ajuste fino centralizado.
4) Encadear sanitização antes/depois da chamada ao LLM.
5) Reduzir duplicação de lógica de resumo entre páginas (mensalidades, médicos, etc.).
6) Servir como building block para o orquestrador, sem acoplar com layout ou HTML.

Use `resumir_texto_conciso` antes de devolver `resumo_curto` para o front.
"""

from __future__ import annotations

from typing import Optional

from llm_client import LLMClient, LLMConfig
from sanitizer_texto_universal import sanitizar_texto_bruto


def resumir_texto_conciso(
    texto: str,
    *,
    instrucoes_extras: Optional[str] = None,
    max_frases: int = 3,
) -> str:
    """
    Gera um resumo conciso e em PT-BR.

    - max_frases: usado apenas como guideline no prompt.
    - instrucoes_extras: permite forçar um flavor específico (ex.: “foco em riscos”).
    """
    base = (
        "Você é um analista executivo. Resuma o texto a seguir em PT-BR, "
        f"em no máximo {max_frases} frases curtas, objetivas e sem bullet points. "
        "Evite frases genéricas; traga apenas o que é realmente relevante para negócio."
    )

    if instrucoes_extras:
        base += " Instruções adicionais: " + instrucoes_extras.strip()

    prompt = f"{base}\n\n[TEXTO-BASE]\n{texto}"

    client = LLMClient(LLMConfig())
    bruto = client.gerar_texto(prompt, system_prompt="Resumidor executivo conciso.")
    return sanitizar_texto_bruto(bruto)
