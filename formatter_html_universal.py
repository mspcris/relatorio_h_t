# formatter_html_universal.py
"""
Este módulo implementa o “formatter visual” para gerar HTML bonito no backend.
Objetivos:
1) Receber um texto analítico (completo) + contexto mínimo (produto/página/período).
2) Gerar um HTML estruturado, responsivo e consistente com o estilo do dashboard.
3) Incluir headings, parágrafos, listas, tabelas e ícones (usando classes FontAwesome).
4) Padronizar o visual entre páginas (médicos, mensalidades, vendas etc.).
5) Evitar que o front precise montar ou reorganizar blocos de texto.
6) Ser 100% stateless e reutilizável: entrada → HTML pronto para injetar na UI.

O HTML gerado deve ser “self-contained”, sem <html>, <body>, apenas o conteúdo.
"""

from __future__ import annotations

from typing import Optional, Dict, Any

from llm_client import LLMClient, LLMConfig
from sanitizer_texto_universal import sanitizar_texto_bruto


HTML_SYSTEM_PROMPT = """
Você é um 'formatter' de relatórios para um dashboard corporativo (AdminLTE + FontAwesome).
Sua função é transformar texto analítico em HTML LIMPO, organizado e profissional.

REGRAS DE FORMATAÇÃO:
- NÃO inclua <html>, <head> ou <body>. Apenas o conteúdo (section/div).
- Use apenas tags simples: <section>, <div>, <h2>, <h3>, <p>, <ul>, <ol>, <li>, <table>, <thead>, <tbody>, <tr>, <th>, <td>, <strong>, <em>, <span>, <i>.
- Use classes Bootstrap/AdminLTE quando fizer sentido: 'mb-2', 'mb-3', 'mt-2', 'mt-3', 'text-muted', 'small', 'table', 'table-sm', 'table-striped'.
- Títulos principais: <h2> com ícones FontAwesome (<i class="fas fa-chart-line"></i>, etc.).
- Subtítulos: <h3>.
- Use listas (<ul>/<li>) para insights, alertas, riscos e oportunidades.
- Use tabelas apenas quando houver números ou comparações estruturadas.
- Nunca use markdown, ASCII art ou blocos de código.
- Texto sempre em PT-BR.
""".strip()


def gerar_html_relatorio(
    *,
    produto: str,
    pergunta_usuario: Optional[str],
    texto_completo: str,
    meta: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Gera o HTML final, bonito, a partir de texto analítico + contexto.

    - produto: ex. "mensalidades", "medicos", "vendas".
    - pergunta_usuario: opcional; se existir, criar seção específica de resposta.
    - texto_completo: conteúdo vindo do agente analítico.
    - meta: dicionário livre com período, posto, etc.
    """
    meta = meta or {}
    texto_base = sanitizar_texto_bruto(texto_completo)

    periodo = meta.get("periodo_txt") or ""
    posto = meta.get("posto_txt") or ""
    page_label = meta.get("page_label") or produto

    contexto_header = f"Relatório de {page_label}"
    if periodo:
        contexto_header += f" — {periodo}"
    if posto:
        contexto_header += f" — Posto {posto}"

    pergunta_bloco = ""
    if pergunta_usuario:
        pergunta_bloco = (
            f"Se houver na análise informações específicas para a pergunta "
            f"\"{pergunta_usuario.strip()}\", destaque-as em uma subseção."
        )

    prompt = f"""
Gere um HTML de relatório executivo para o seguinte contexto:

[CONTEXTUALIZAÇÃO]
Produto/página: {produto}
Título sugerido: {contexto_header}
{pergunta_bloco}

[TEXTO ANALÍTICO COMPLETO]
{texto_base}

ESTRUTURE O HTML ASSIM (como guideline, não precisa seguir à risca):
- <section class="mb-2"> com um <h2> principal e breve contextualização.
- Bloco "Resumo Executivo" com 3–5 bullets principais (<ul>).
- Bloco "Análises de Gráficos / KPIs" (quando o texto mencionar gráficos).
- Bloco "Riscos e Oportunidades".
- Bloco "Recomendações (30/60/90 dias)" se pertinente ao texto.
- Se a pergunta do usuário existir, uma subseção "Resposta direta à pergunta" em <p>.

Não invente números que não estejam sugeridos no texto.
Não repita blocos idênticos.
""".strip()

    client = LLMClient(LLMConfig())
    bruto = client.gerar_texto(prompt, system_prompt=HTML_SYSTEM_PROMPT)
    html = bruto.strip()

    # LLM pode às vezes devolver texto puro; garantimos algo minimamente consistente.
    if "<h2" not in html and "<h3" not in html:
        html = f'<section class="mb-2"><h2><i class="fas fa-chart-line"></i> {contexto_header}</h2><p>{html}</p></section>'

    return html
