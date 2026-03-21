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
Você é analista de dados da CAMIM — rede de clínicas médicas.

REGRAS OBRIGATÓRIAS:
1. Use APENAS os dados fornecidos no contexto JSON. NUNCA invente, estime ou use conhecimento externo para gerar valores monetários.
2. Se a pergunta mencionar um posto específico (ex: "em X", "no posto A"), use EXCLUSIVAMENTE o campo despesas_tipo_por_posto[posto] e evolucao_por_posto[posto] daquele posto.
3. O campo despesas_tipo_mensal_consolidado contém o total de TODOS os postos listados em "postos". NÃO o use para responder sobre um único posto.
4. Se os dados de um posto não estiverem disponíveis no contexto, informe isso claramente.
5. Responda em português brasileiro de forma objetiva e com os valores exatos do contexto.
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