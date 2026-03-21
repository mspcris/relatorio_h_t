import os
from flask import Blueprint, request, jsonify

from llm_client_openai import LLMClientOpenAI

ia_bp = Blueprint("ia", __name__)

_llm = None

SYSTEM_PROMPT = """Você é analista de dados da CAMIM — rede de clínicas médicas.

REGRAS OBRIGATÓRIAS:
1. Use APENAS os dados fornecidos no contexto JSON. NUNCA invente, estime ou use conhecimento externo para gerar valores monetários.
2. Se a pergunta mencionar um posto específico (ex: "em X", "no posto A"), use EXCLUSIVAMENTE o campo despesas_tipo_por_posto[posto] e evolucao_por_posto[posto] daquele posto.
3. O campo despesas_tipo_mensal_consolidado contém o total de TODOS os postos listados em "postos". NÃO o use para responder sobre um único posto.
4. Se os dados de um posto não estiverem disponíveis no contexto, informe isso claramente.
5. Responda em português brasileiro de forma objetiva e com os valores exatos do contexto."""


def get_llm():
    global _llm
    if _llm is None:
        _llm = LLMClientOpenAI()
    return _llm


# Rota legada — mantida para KPIs que já usam /api/ia/pergunta
@ia_bp.route("/api/ia/pergunta", methods=["POST"])
def pergunta():
    data = request.json or {}
    pergunta_txt = data.get("pergunta", "")
    contexto = data.get("contexto", "")
    prompt = f"Contexto do relatório:\n\n{contexto}\n\nPergunta do usuário:\n\n{pergunta_txt}"
    resposta = get_llm().gerar_texto(prompt=prompt, system_prompt=SYSTEM_PROMPT)
    return jsonify({"resposta": resposta})
