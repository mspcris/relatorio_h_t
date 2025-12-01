# ia_router.py
"""
Este módulo expõe o endpoint HTTP /ia/analisar integrado ao orquestrador.
Objetivos:
1) Receber a requisição JSON do front (chat.js) de qualquer página.
2) Encaminhar o payload bruto para `processar_requisicao_ia` (backend multiagente).
3) Padronizar a resposta HTTP no formato esperado pelo front (JSON).
4) Centralizar logging básico e tratamento de erros genéricos de IA.
5) Isolar a lógica HTTP da lógica de orquestração, facilitando manutenção.
6) Permitir plug & play em um app Flask existente via Blueprint.

Adapte para FastAPI ou outro framework se necessário.
"""

from __future__ import annotations

from flask import Blueprint, request, jsonify

from ia_orquestrador import processar_requisicao_ia


bp_ia = Blueprint("ia", __name__)


@bp_ia.route("/ia/analisar", methods=["POST"])
def analisar_ia():
    """
    Endpoint principal de IA.

    Espera JSON no corpo e devolve JSON no padrão:
    {
      "content_mode": "json",
      "data": { "html": "...", "resumo_curto": "...", "full_text": "...", "objective_answer": "..." }
    }
    """
    try:
        payload = request.get_json(silent=True) or {}
        resposta = processar_requisicao_ia(payload)
        return jsonify(resposta), 200
    except Exception as e:
        # Log estruturado seria ideal aqui
        return jsonify(
            {
                "content_mode": "free_text",
                "text": f"Falha ao processar análise de IA: {e}",
            }
        ), 500
