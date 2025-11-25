# sanitizer_texto_universal.py
"""
Este módulo concentra a higienização e normalização de textos vindos da IA.
Objetivos principais:
1) Remover marcas de código (```), tabelas ASCII e ruídos comuns de LLM.
2) Desfazer quebras de linha estranhas e espaços duplicados, mantendo legibilidade.
3) Transformar saídas mistas (markdown leve) em texto simples mais previsível.
4) Minimizar a chance de “lixo visual” na exibição ou em novos prompts encadeados.
5) Proteger o pipeline contra variações de formato entre modelos e versões.
6) Oferecer uma função única `sanitizar_texto_bruto` utilizada por todos os agentes.

Use este módulo sempre que receber texto do LLM antes de qualquer pós-processamento.
"""

from __future__ import annotations

import re


def sanitizar_texto_bruto(texto: str) -> str:
    """
    Normaliza e limpa texto vindo do LLM, devolvendo string “mais limpa”.

    Não tenta interpretar semântica, apenas retira ruído visual e formatações inúteis.
    """
    if not texto:
        return ""

    s = str(texto)

    # Normaliza quebras
    s = s.replace("\r\n", "\n").replace("\r", "\n")

    # Remove blocos de código markdown ```...```
    s = re.sub(r"```[\s\S]*?```", " ", s)

    # Remove linhas de borda de tabelas ASCII (─, ┌, ┐ etc.)
    s = re.sub(r"^\s*[+\-─═┌┐└┘┼┤├┬┴]+\s*$", " ", s, flags=re.MULTILINE)

    # Remove headings markdown duplicados (###) sem perder o texto
    s = re.sub(r"^#{1,6}\s*", "", s, flags=re.MULTILINE)

    # Remove markdown de negrito / itálico simples
    s = re.sub(r"\*\*(.*?)\*\*", r"\1", s)
    s = re.sub(r"__(.*?)__", r"\1", s)
    s = re.sub(r"_([^_]+)_", r"\1", s)

    # Remove linhas de "tabela" markdown pura
    s = re.sub(r"^\s*\|[- :]+\|\s*$", " ", s, flags=re.MULTILINE)

    # | col1 | col2 | -> "col1 — col2"
    def _table_line(m: re.Match) -> str:
        inner = m.group(1)
        cols = [c.strip() for c in inner.split("|") if c.strip()]
        return " — ".join(cols)

    s = re.sub(r"^\s*\|(.*?)\|\s*$", _table_line, s, flags=re.MULTILINE)

    # Tira espaços antes de quebra de linha
    s = re.sub(r"[ \t]+\n", "\n", s)

    # Garante apenas 2 quebras consecutivas no máximo
    s = re.sub(r"\n{3,}", "\n\n", s)

    # Espaço após pontuação importante, se faltar
    s = re.sub(r"([.,;:!?])(?!\s|$)", r"\1 ", s)

    # Espaços múltiplos → simples
    s = re.sub(r"[ \t]{2,}", " ", s)

    return s.strip()
