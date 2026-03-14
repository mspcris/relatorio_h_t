from flask import Blueprint, request, jsonify

from llm_client_openai import LLMClientOpenAI

ia_bp = Blueprint("ia", __name__)

llm = LLMClientOpenAI()


@ia_bp.route("/api/ia/pergunta", methods=["POST"])
def pergunta():

    data = request.json or {}

    pergunta = data.get("pergunta", "")
    contexto = data.get("contexto", "")

    system_prompt = """
Você é analista de dados da CAMIM.

Seu trabalho é analisar KPIs de saúde financeira e operacional
de uma operadora de planos de saúde.

Responda de forma objetiva.
"""

    resposta = llm.gerar_texto(
        prompt=f"""
Contexto do relatório:

{contexto}

Pergunta do usuário:

{pergunta}
""",
        system_prompt=system_prompt,
    )

    return jsonify({"resposta": resposta})