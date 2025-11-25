# ================================================================
# analyze_groq.py — PIPELINE MULTIAGENTES (VERSÃO LIMPA)
# Flag de Deploy: BUILD-2025-02-XX-01
# ================================================================
# Este arquivo substitui completamente qualquer versão anterior.
# Ele chama exclusivamente o OrquestradorIA, ignora totalmente o
# pipeline antigo (ask_json_only_prompt, extratores etc.) e mantém
# o endpoint minimalista necessário para o ChatIA.
# ================================================================

from __future__ import annotations

import os
from typing import Optional

from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from groq import Groq
from orquestrador import OrquestradorIA


# ================================================================
# FastAPI CONFIG
# ================================================================
app = FastAPI(title="IA-Groq-Pipeline-MultiAgentes")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://teste-ia.camim.com.br"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)


# ================================================================
# SANITIZAÇÃO
# ================================================================
def clean_text(s: str) -> str:
    import unicodedata, html, re
    s = html.unescape(s or "")
    s = unicodedata.normalize("NFC", s)
    s = s.replace("\u00A0", " ").replace("\u200B", "")
    s = re.sub(r"([.,;:?!])(?!\s|$)", r"\1 ", s)
    s = re.sub(r"\s{2,}", " ", s)
    return s.strip()


# ================================================================
# PAYLOAD
# ================================================================
class AnalyzePayload(BaseModel):
    prompt: Optional[str] = None


# ================================================================
# FUNÇÃO UNIVERSAL DE CHAMADA GROQ
# ================================================================
async def llm_call(prompt: str, modelo: str) -> str:
    """
    Camada única de chamada ao LLM da Groq.
    Atualiza logs futuros, padroniza temperatura e tokens.
    """
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        raise RuntimeError("GROQ_API_KEY ausente no ambiente (.env)")

    client = Groq(api_key=api_key)

    resp = client.chat.completions.create(
        model=modelo,
        messages=[{"role": "user", "content": prompt}],
        max_tokens=4096,
        temperature=0.1,
    )
    return (resp.choices[0].message.content or "").strip()


# ================================================================
# INSTÂNCIA DO ORQUESTRADOR MULTIAGENTES
# ================================================================
orquestrador = OrquestradorIA(llm_call)


# ================================================================
# HEALTHCHECK SIMPLES
# ================================================================
@app.get("/health")
async def health():
    return {
        "ok": True,
        "service": "IA-Groq Multiagentes",
        "deploy_flag": "BUILD-2025-02-XX-01",
    }


# ================================================================
# ENDPOINT PRINCIPAL USADO PELO FRONT
# ================================================================
@app.post("/ia/analisar")
async def ia_analisar(payload: AnalyzePayload = Body(...)):
    """
    ÚNICO endpoint consumido pelo chat.js.
    Aqui enviamos o prompt para o pipeline multiagentes.
    Esse endpoint deve sempre retornar:
        { ok, entrada, resposta }
    """

    if not payload.prompt or not payload.prompt.strip():
        raise HTTPException(400, "prompt ausente")

    try:
        resultado = await orquestrador.responder(payload.prompt)
        texto_final = clean_text(resultado["resposta_final_pt"])

        resposta_formatada = (
            "<div style='font-family:Roboto,Arial; line-height:1.45;'>"
            "<h2 style='margin:0 0 12px;'>Análise Executiva</h2>"
            "<hr/>"
            f"<div>{texto_final}</div>"
            "<hr/>"
            "<div style='color:#777; font-size:12px;'>Camila.AI — Pipeline Multiagentes</div>"
            "</div>"
        )

        return {
            "ok": True,
            "entrada": payload.prompt,
            "resposta": resposta_formatada,
            "deploy_flag": "BUILD-2025-02-XX-01"
        }

    except Exception as e:
        raise HTTPException(500, f"Erro interno: {e}")
