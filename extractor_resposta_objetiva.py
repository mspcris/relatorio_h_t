# extractor_resposta_objetiva.py
"""
Este módulo implementa o “agente pedagogo” focado em resposta objetiva.
Objetivos:
1) Receber a pergunta original do usuário e o texto completo retornado pela IA.
2) Ignorar todo o resto (contexto, explicações, gráficos, plano 30/60/90).
3) Extrair uma resposta direta à pergunta, em PT-BR, no formato de 1–2 frases.
4) Manter o recorte super conciso (< ~300 caracteres), sem enrolação.
5) Blindar a experiência do usuário contra respostas prolixas do modelo base.
6) Ser independente de página (médicos, mensalidades, clima, etc.) e de layout.

Use `extrair_resposta_objetiva` sempre que quiser popular o campo `objective_answer`.
"""

from __future__ import annotations

from typing import Optional

from llm_client import LLMClient, LLMConfig
from sanitizer_texto_universal import sanitizar_texto_bruto


def extrair_resposta_objetiva(
    pergunta_usuario: str,
    texto_completo: str,
    *,
    max_caracteres: int = 300,
    modo_simples: bool = True,
) -> str:
    """
    Dado (pergunta, texto_completo), retorna uma resposta direta à pergunta.

    Se o texto não contiver elementos suficientes, a função retorna uma
    resposta honesta (“não é possível afirmar…”), mantendo o mesmo formato.
    """
    pergunta = pergunta_usuario.strip() if pergunta_usuario else ""
    contexto = texto_completo.strip() if texto_completo else ""

    if not pergunta or not contexto:
        return "Não foi possível extrair uma resposta objetiva com as informações disponíveis."

    prompt = f"""
Você é um assistente pedagógico corporativo.
Sua única tarefa é responder a PERGUNTA DO USUÁRIO com base no TEXTO COMPLETO fornecido.

REGRAS DE NEGÓCIO:
- Responda SEMPRE em português (PT-BR).
- Use no máximo 2 frases, diretas e sem rodeios.
- Tamanho máximo aproximado: {max_caracteres} caracteres.
- Não repita a pergunta.
- Não explique sua lógica, apenas responda.
- Se o texto não permitir uma conclusão, diga claramente que não é possível afirmar.

[PERGUNTA DO USUÁRIO]
{pergunta}

[TEXTO COMPLETO]
{contexto}
""".strip()

    system = (
        "Você é um 'agente pedagogo' focado em objetividade. "
        "Nunca rodeia, nunca devolve relatório. Apenas responde à pergunta."
    )

    client = LLMClient(LLMConfig())
    bruto = client.gerar_texto(prompt, system_prompt=system)
    resposta = sanitizar_texto_bruto(bruto)

    if modo_simples and len(resposta) > max_caracteres:
        resposta = resposta[: max_caracteres].rstrip() + "…"

    return resposta
