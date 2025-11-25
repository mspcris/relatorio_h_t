# orquestrador.py
# -*- coding: utf-8 -*-
"""
Pipeline multiagentes completo para a Camila.AI.

Fluxo:
1) Entrada do usuário → PT-BR
2) Tradução PT → EN (Agente Tradutor)
3) Processamento por TODOS os agentes especialistas
4) Writer consolida tudo em inglês
5) Tradutor EN → PT
6) Formatter final → resposta corporativa padronizada
"""

import os
from typing import Callable, Dict, Any
import logging

# Configuração básica de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ============================================================
# Carregador de prompts dos agentes
# ============================================================

def load_prompt(path: str) -> str:
    """Lê o prompt-base de um agente a partir da pasta /prompt."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read().strip()
    except Exception as e:
        logger.warning("Falha ao carregar prompt em %s: %s", path, str(e))
        return "PROMPT NÃO ENCONTRADO."


# ============================================================
# Classe Genérica de Agente
# ============================================================

class Agente:
    def __init__(self, nome: str, prompt_path: str, llm_call: Callable):
        self.nome = nome
        self.prompt_base = load_prompt(prompt_path)
        self.llm_call = llm_call

    async def executar(self, texto_en: str) -> str:
        prompt = f"""
You are the agent: {self.nome}.
Follow strictly your role described below:

PROMPT BASE:
{self.prompt_base}

INPUT (ENGLISH):
{texto_en}

Respond ONLY in English.
No apologies, no disclaimers.
"""
        return await self.llm_call(prompt, modelo=os.getenv("GROQ_MODEL", "openai/gpt-oss-120b"))


# ============================================================
# Formatter Final – padrão corporativo
# ============================================================

class FormatterFinal:
    """
    Produz uma resposta elegante, corporativa e sempre padronizada.
    """

    @staticmethod
    def formatar(texto: str) -> str:
        texto = texto.strip()

        return f"""
ANÁLISE EXECUTIVA
────────────────────────────────────────

{texto}

────────────────────────────────────────
Gerado por Camila.AI — Pipeline Multiagentes
""".strip()


# ============================================================
# Writer – consolida respostas dos agentes
# ============================================================

class AgenteWriter:
    def __init__(self, llm_call):
        self.llm_call = llm_call
        self.prompt_base = load_prompt("prompt/agente_writer.txt")

    async def consolidar(self, respostas_agentes: Dict[str, str]) -> str:
        conteudo = "\n\n".join([f"[{k}]\n{v}" for k, v in respostas_agentes.items()])

        prompt = f"""
You are the WRITER agent.

PROMPT BASE:
{self.prompt_base}

Your task:
- Analyze ALL specialist agents' reports
- Synthesize them into a single, coherent, elegant executive analysis
- ALWAYS in English
- Very clear structure
- Corporate tone
- No disclaimers
- No bullet point overflow; keep it elegant

INPUT FROM AGENTS:
{conteudo}

Write the final combined analysis in English ONLY.
"""

        return await self.llm_call(prompt, modelo=os.getenv("GROQ_MODEL", "openai/gpt-oss-120b"))


# ============================================================
# Tradutor
# ============================================================

class AgenteTradutor:
    def __init__(self, llm_call):
        self.llm_call = llm_call
        self.prompt_base = load_prompt("prompt/agente_tradutor.txt")

    async def pt_para_en(self, txt: str) -> str:
        prompt = f"""
{self.prompt_base}

Translate the following text to English, preserving meaning and style:

Portuguese:
{txt}

English:
"""
        return await self.llm_call(prompt, modelo=os.getenv("GROQ_MODEL", "openai/gpt-oss-120b"))

    async def en_para_pt(self, txt: str) -> str:
        prompt = f"""
{self.prompt_base}

Translate the following text to Brazilian Portuguese, preserving elegance and corporate tone:

English:
{txt}

Portuguese:
"""
        return await self.llm_call(prompt, modelo=os.getenv("GROQ_MODEL", "openai/gpt-oss-120b"))


# ============================================================
# ORQUESTRADOR PRINCIPAL
# ============================================================

class OrquestradorIA:
    def __init__(self, llm_call: Callable):
        """
        llm_call(prompt: str, modelo: str) → str
        """
        self.llm_call = llm_call

        # Instanciar agentes
        self.agente_tradutor = AgenteTradutor(llm_call)

        # Especialistas
        self.agente_financas = Agente("Financas",      "prompt/agente_financas.txt", llm_call)
        self.agente_contabil = Agente("Contabil",      "prompt/agente_contabil.txt", llm_call)
        self.agente_familiar = Agente("Familiar",      "prompt/agente_familiar.txt", llm_call)
        self.agente_marketing = Agente("Marketing",    "prompt/agente_marketing.txt", llm_call)
        self.agente_compliance = Agente("Compliance",  "prompt/agente_compliance.txt", llm_call)

        # Estilo
        self.agente_estilo = Agente("Estilo",          "prompt/agente_estilo.txt", llm_call)

        # Writer final
        self.agente_writer = AgenteWriter(llm_call)

        # Formatter
        self.formatter = FormatterFinal()

    # --------------------------------------------------------------

    async def responder(self, pergunta_pt: str) -> Dict[str, Any]:
        """
        Pipeline completo:
        1) Traduz pergunta para EN
        2) Executa agentes especialistas
        3) Writer junta tudo
        4) Traduz resultado final para PT
        5) Formatter cria layout corporativo
        """

        # 1) Tradução PT→EN
        pergunta_en = await self.agente_tradutor.pt_para_en(pergunta_pt)

        # 2) Rodar todos agentes especialistas
        respostas: Dict[str, str] = {}
        respostas["Finanças"]   = await self.agente_financas.executar(pergunta_en)
        respostas["Contábil"]   = await self.agente_contabil.executar(pergunta_en)
        respostas["Familiar"]   = await self.agente_familiar.executar(pergunta_en)
        respostas["Marketing"]  = await self.agente_marketing.executar(pergunta_en)
        respostas["Compliance"] = await self.agente_compliance.executar(pergunta_en)
        respostas["Estilo"]     = await self.agente_estilo.executar(pergunta_en)

        # 3) Writer → análise combinada
        resposta_writer_en = await self.agente_writer.consolidar(respostas)

        # 4) Traduzir EN → PT
        resposta_final_pt = await self.agente_tradutor.en_para_pt(resposta_writer_en)

        # 5) Formatador corporativo final
        resposta_formatada = self.formatter.formatar(resposta_final_pt)

        # Log de debug do pipeline para inspeção na VM
        logger.info(
            "PIPELINE_DEBUG: %s",
            {
                "pergunta_en": pergunta_en,
                "agentes": list(respostas.keys()),
                "writer_en_preview": resposta_writer_en[:200],
            },
        )

        return {
            "resposta_final_pt": resposta_formatada,
            "pipeline_debug": {
                "pergunta_en": pergunta_en,
                "respostas_agentes": respostas,
                "resposta_writer_en": resposta_writer_en,
            },
        }
