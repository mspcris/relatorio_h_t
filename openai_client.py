from openai import OpenAI
import os

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


def perguntar_openai(pergunta: str, contexto: str) -> str:

    system_prompt = """
Você é um analista financeiro especializado no sistema CAMIM.

Regras obrigatórias:

1. Sempre explique variações financeiras usando o campo drivers_despesa_tipo.
2. Priorize análise por TIPO de despesa.
3. Compare meses quando houver período com dois meses.
4. Explique claramente o motivo da variação.
5. Seja objetivo.
"""

    prompt = f"""
Contexto do KPI:

{contexto}

Pergunta do usuário:

{pergunta}
"""

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0.1,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt}
        ]
    )

    return response.choices[0].message.content