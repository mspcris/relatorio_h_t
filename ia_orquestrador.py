# ia_orquestrador.py
"""
Este módulo é o coração do fluxo multiagente de IA no backend.
Responsabilidades:
1) Receber o payload bruto do front (/ia/analisar), independentemente da página.
2) Orquestrar 3 estágios lógicos: analista (conteúdo cheio), pedagogo (resposta objetiva),
   e formatter (HTML bonito), chamando o LLM de forma coordenada.
3) Padronizar o formato de saída para TODAS as páginas: html, resumo_curto,
   full_text e objective_answer dentro de `content_mode=json`.
4) Encapsular sanitização e fallback (ex.: tratar respostas em JSON ou texto puro).
5) Minimizar diferenças entre páginas (médicos, mensalidades, vendas, etc.).
6) Servir como único entrypoint a ser chamado pelo controller HTTP (Flask/FastAPI).

Use `processar_requisicao_ia(payload)` no endpoint /ia/analisar.
"""

from __future__ import annotations

from typing import Dict, Any, Tuple, Optional
import json
import os
from pathlib import Path

from llm_client import LLMClient, LLMConfig
from sanitizer_texto_universal import sanitizar_texto_bruto
from summarizer_universal import resumir_texto_conciso
from extractor_resposta_objetiva import extrair_resposta_objetiva
from formatter_html_universal import gerar_html_relatorio


# Localização do superprompt; ajuste o path conforme sua estrutura
SUPERPROMPT_PATH = os.getenv("SUPERPROMPT_UNIVERSAL_PATH", "superprompt_universal.md")


def _carregar_superprompt() -> str:
    p = Path(SUPERPROMPT_PATH)
    if not p.exists():
        # Fallback seguro
        return (
            "Você é um analista de dados sênior. Gere uma análise de negócio em PT-BR, "
            "focando em insights, riscos e recomendações. Sem ASCII, sem markdown de tabela."
        )
    return p.read_text(encoding="utf-8")


def _montar_prompt_analista(payload: Dict[str, Any]) -> str:
    """
    Constrói o prompt do agente analista com base no payload vindo do front.

    - Usa `payload.get("prompt")` como núcleo (já montado pela página).
    - Anexa contexto técnico (blocks, kpis, tabela, macro, etc.) como JSON compactado.
    """
    superprompt = _carregar_superprompt()
    user_prompt = payload.get("prompt") or "Analise os dados e gere um relatório executivo."

    contexto = {
        "produto": payload.get("produto"),
        "page": payload.get("meta", {}).get("page") if isinstance(payload.get("meta"), dict) else None,
        "contexto": payload.get("contexto"),
        "qa": payload.get("qa"),
        "kpis": payload.get("kpis"),
        "tabela": payload.get("tabela"),
        "charts": payload.get("charts"),
        "blocks": payload.get("blocks"),
        "macro": payload.get("macro"),
    }

    contexto_str = json.dumps(contexto, ensure_ascii=False, separators=(",", ":"))

    prompt = f"""
{superprompt}

[INSTRUÇÕES ESPECÍFICAS DA PÁGINA]
{user_prompt}

[CONTEXT DATA EM JSON]
{contexto_str}
""".strip()

    return prompt


def _rodar_agente_analista(payload: Dict[str, Any]) -> str:
    """
    Executa o agente analista principal.
    Retorna SEMPRE texto puro (já sanitizado) para os próximos estágios.
    """
    prompt = _montar_prompt_analista(payload)
    client = LLMClient(LLMConfig())

    bruto = client.gerar_texto(
        prompt,
        system_prompt="Você é o AGENTE ANALISTA. Foque em explicar o que está acontecendo nos dados.",
        temperature=0.15,
        max_tokens=5500,
    )

    texto = sanitizar_texto_bruto(bruto)
    return texto


def _extrair_html_e_resumo_de_resposta_analista(texto_analista: str) -> Tuple[str, str]:
    """
    Caso o agente analista tenha devolvido JSON com {html, resumo_curto}, extrai.
    Caso contrário, assume que é só texto plano e gera um resumo curto via summarizer.
    Retorna (html_base, resumo_curto).
    """
    s = texto_analista.strip()
    if not s:
        return "", ""

    # Tenta interpretar como JSON primeiro
    if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
        try:
            obj = json.loads(s)
            data = obj.get("data", obj)

            html = (
                data.get("html")
                or data.get("conteudo")
                or data.get("content")
                or ""
            )
            resumo = (
                data.get("resumo_curto")
                or data.get("summary")
                or data.get("resumo")
                or ""
            )

            # Se html vier vazio mas summary estiver presente, usa summary como html simples.
            if not html and resumo:
                html = f"<p>{resumo}</p>"

            return html.strip(), resumo.strip()
        except Exception:
            # Se falhar o parse, tratamos como texto simples
            pass

    # Se chegamos aqui, consideramos tudo texto plano.
    resumo = resumir_texto_conciso(s, max_frases=3)
    # html_base simples (pode ser refinado pelo formatter depois)
    html_base = f"<p>{s}</p>"
    return html_base, resumo


def processar_requisicao_ia(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Função principal chamada pelo endpoint HTTP.

    Fluxo:
    1) Rodar AGENTE ANALISTA → texto_analista (full_text_base).
    2) Tentar extrair html/resumo, se o analista já devolver JSON; senão, gerar resumo.
    3) Rodar AGENTE PEDAGOGO → objective_answer (1–2 frases).
    4) Rodar FORMATTER → html_pretty (painel bonito, estruturado).
    5) Devolver payload padronizado para o front (content_mode=json).
    """
    user_query: Optional[str] = payload.get("qa") or payload.get("userQuery")
    produto: str = payload.get("produto") or payload.get("meta", {}).get("page") or "desconhecido"

    # Tenta extrair período/posto para contexto de layout
    filtros = payload.get("filtros", {}).get("deep") or {}
    periodo_txt = ""
    if filtros.get("from") and filtros.get("to"):
        periodo_txt = f"{filtros['from']} → {filtros['to']}"
    posto_txt = filtros.get("posto") or "ALL"

    meta_layout = {
        "periodo_txt": periodo_txt,
        "posto_txt": posto_txt,
        "page_label": produto,
    }

    # 1) Agente analista (full_text_base)
    full_text_base = _rodar_agente_analista(payload)

    # 2) Extração html/resumo a partir da resposta do analista (caso venha em JSON)
    html_base, resumo_curto = _extrair_html_e_resumo_de_resposta_analista(full_text_base)

    # 3) Agente pedagogo → resposta objetiva
    objective_answer = extrair_resposta_objetiva(
        pergunta_usuario=user_query or "",
        texto_completo=full_text_base,
        max_caracteres=280,
        modo_simples=True,
    )

    # 4) Formatter HTML bonito
    html_pretty = gerar_html_relatorio(
        produto=produto,
        pergunta_usuario=user_query,
        texto_completo=full_text_base,
        meta=meta_layout,
    )

    # Caso o formatter falhe por algum motivo (vazio), fallback para html_base
    if not html_pretty.strip():
        html_pretty = html_base or "<p>(Sem conteúdo gerado pelo analista.)</p>"

    resposta = {
        "content_mode": "json",
        "data": {
            "html": html_pretty,
            "resumo_curto": resumo_curto,
            "full_text": full_text_base,
            "objective_answer": objective_answer,
        },
    }

    return resposta
