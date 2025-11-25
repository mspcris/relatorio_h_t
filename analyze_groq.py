# ================================================================
# analyze_groq.py — PIPELINE MULTIAGENTES (OPÇÃO A - FULL PAYLOAD)
# Compatível com chat.js (envia prompt + blocks + prefs + produto)
# ================================================================

from __future__ import annotations

import os
from typing import Optional, List, Dict, Any

from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, HTTPException, Body, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from groq import Groq
from orquestrador import OrquestradorIA


# ================================================================
# FASTAPI CONFIG
# ================================================================
app = FastAPI(title="IA-Groq-Pipeline-MultiAgentes-FULL")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://teste-ia.camim.com.br"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)


# ================================================================
# MODELOS — compatíveis com chat.js
# ================================================================
class AnalyzePayload(BaseModel):
    prompt: Optional[str] = None
    produto: Optional[str] = None
    contexto: Optional[str] = None
    blocks: Optional[List[Dict[str, Any]]] = None
    prefs: Optional[Dict[str, Any]] = None


# ================================================================
# SANITIZAÇÃO
# ================================================================
def clean_text(s: str) -> str:
    import unicodedata, html, re
    if not s:
        return ""
    s = html.unescape(s)
    s = unicodedata.normalize("NFC", s)
    s = s.replace("\u00A0", " ").replace("\u200B", "")
    s = re.sub(r"([.,;:?!])(?!\s|$)", r"\1 ", s)
    s = re.sub(r"\s{2,}", " ", s)
    return s.strip()


# ================================================================
# CHAMADA UNIVERSAL AO MODELO
# ================================================================
async def llm_call(prompt: str, modelo: str) -> str:
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        raise RuntimeError("GROQ_API_KEY ausente")

    client = Groq(api_key=api_key)

    resp = client.chat.completions.create(
        model=modelo,
        messages=[{"role": "user", "content": prompt}],
        max_tokens=4096,
        temperature=0.1,
    )
    return (resp.choices[0].message.content or "").strip()


# ================================================================
# INSTÂNCIA DO ORQUESTRADOR
# ================================================================
orquestrador = OrquestradorIA(llm_call)


# ================================================================
# HEALTHCHECK
# ================================================================
@app.get("/health")
async def health():
    return {
        "ok": True,
        "service": "IA-Groq Multiagentes FULL",
        "deploy_flag": "BUILD-2025-02-XX-01"
    }


# ================================================================
# ENDPOINT PRINCIPAL — AGORA COMPATÍVEL COM chat.js
# ================================================================
@app.post("/ia/analisar")
async def ia_analisar(req: Request, payload: AnalyzePayload = Body(...)):
    """
    Suporta:
      - prompt (texto do usuário)
      - produto (ex: medicos, alimentacao, vendas…)
      - blocks (dados estruturados dos gráficos)
      - prefs (temperatura, tokens, modo json, etc.)
      - contexto (relatorio, qa, deep…)
    """

    if not payload.prompt and not payload.blocks:
        raise HTTPException(400, "prompt ou blocks ausentes")

    try:
        resultado = await orquestrador.responder(payload.model_dump())

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
            "entrada": payload.model_dump(),
            "resposta": resposta_formatada,
            "deploy_flag": "BUILD-2025-02-XX-01"
        }

    except Exception as e:
        raise HTTPException(500, f"Erro interno: {e}")
