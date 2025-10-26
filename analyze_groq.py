# analyze.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os, json, pathlib, html, re, unicodedata, tempfile, shutil
from typing import Any, List, Optional

from dotenv import load_dotenv
from groq import Groq
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, field_validator

# ========================== Config & Paths ==========================
BASE = pathlib.Path(__file__).parent.resolve()
MODEL = os.getenv("GROQ_MODEL", "openai/gpt-oss-120b")

PROMPTS_DIR = pathlib.Path(os.getenv("PROMPTS_DIR", "/var/appdata/prompts")).resolve()
PROMPTS_DIR.mkdir(parents=True, exist_ok=True)

# ========================== Sanitização PT-BR ==========================
def _clean_pt(s: str) -> str:
    s = html.unescape(s or "")
    s = unicodedata.normalize("NFC", s).replace("\u00A0", " ").replace("\u200B", "")
    s = re.sub(r"([.,;:?!])(?!\s|$)", r"\1 ", s)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s

def _sanitize_obj(o: Any) -> Any:
    if isinstance(o, dict):  return {k: _sanitize_obj(v) for k, v in o.items()}
    if isinstance(o, list):  return [_sanitize_obj(x) for x in o]
    if isinstance(o, str):   return _clean_pt(o)
    return o

# ========================== Util: extrair primeiro JSON ==========================
def first_json_block(txt: str) -> Optional[str]:
    m = re.search(r"```json\s*(\{.*?\}|\[.*?\])\s*```", txt, flags=re.S)
    cand = m.group(1) if m else None
    if cand:
        try:
            json.loads(cand); return cand
        except Exception:
            pass
    depth = 0; start = None
    for i, ch in enumerate(txt):
        if ch == "{":
            if depth == 0: start = i
            depth += 1
        elif ch == "}" and depth:
            depth -= 1
            if depth == 0 and start is not None:
                seg = txt[start:i+1]
                try:
                    json.loads(seg); return seg
                except Exception:
                    pass
    return None

# ========================== Modelos (API) ==========================
class AnalyzePayload(BaseModel):
    prompt: str
    blocks: list | None = None
    prefs: dict | None = None

class PromptItem(BaseModel):
    id: str = Field(..., min_length=1, max_length=64)
    name: str = Field(..., min_length=1, max_length=256)
    template: str = Field("", max_length=100_000)
    updatedAt: str = Field(..., min_length=1, max_length=64)

class PromptState(BaseModel):
    list: List[PromptItem] = Field(default_factory=list)
    activeId: Optional[str] = None

    @field_validator("activeId")
    @classmethod
    def _active_must_exist(cls, v, info):
        if v is None:
            return v
        lst = info.data.get("list") or []
        if not any(p.id == v for p in lst):
            # permite valor “órfão”, o front pode ajustar; evita 400 desnecessário
            return v
        return v

# ========================== IO de arquivos (prompts) ==========================
def _safe_page_id(page_id: str) -> str:
    # somente [a-zA-Z0-9_-] para evitar path traversal
    if not re.fullmatch(r"[a-zA-Z0-9_\-]{1,128}", page_id or ""):
        raise HTTPException(400, "pageId inválido.")
    return page_id

def _prompts_path(page_id: str) -> pathlib.Path:
    return PROMPTS_DIR / f"{page_id}.json"

def _atomic_write_json(path: pathlib.Path, obj: dict) -> None:
    tmp = None
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd, tmp_name = tempfile.mkstemp(prefix=path.name + ".", dir=str(path.parent))
        tmp = pathlib.Path(tmp_name)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False)
        os.replace(tmp, path)
    finally:
        try:
            if tmp and tmp.exists():
                tmp.unlink(missing_ok=True)
        except Exception:
            pass

def _read_json_or_empty(path: pathlib.Path) -> dict:
    if not path.exists():
        return {"list": [], "activeId": None}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        # arquivo corrompido: retorna estrutura vazia para não bloquear operação
        return {"list": [], "activeId": None}

# ========================== Chamada Groq ==========================
def ask_json_only_prompt(client: Groq, user_prompt: str, blocks: list | None, prefs: dict | None) -> dict:
    msgs = [{"role": "user", "content": user_prompt}]
    if blocks:
        msgs.append({"role": "user", "content": json.dumps({"blocks": blocks}, ensure_ascii=False)})

    temperature = float(prefs.get("temperature", 0.1)) if isinstance(prefs, dict) else 0.1
    max_tokens  = int(prefs.get("max_completion_tokens", 4096)) if isinstance(prefs, dict) else 4096

    try:
        resp = client.chat.completions.create(
            model=MODEL,
            temperature=temperature,
            max_completion_tokens=max_tokens,
            tool_choice="none",
            response_format={"type": "json_object"},
            messages=msgs,
        )
        raw = (resp.choices[0].message.content or "").strip()
        obj = json.loads(raw)
        return _sanitize_obj(obj)
    except Exception:
        pass

    resp = client.chat.completions.create(
        model=MODEL,
        temperature=temperature,
        max_completion_tokens=max_tokens,
        tool_choice="none",
        messages=msgs,
    )
    txt = (resp.choices[0].message.content or "").strip()
    blk = first_json_block(txt) or txt
    try:
        obj = json.loads(blk)
    except Exception:
        raise RuntimeError("Modelo não retornou JSON válido.")
    return _sanitize_obj(obj)

# ========================== FastAPI ==========================
app = FastAPI(title="IA-Groq-Analises")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)

@app.get("/health")
async def health():
    return {"ok": True, "model": MODEL, "prompts_dir": str(PROMPTS_DIR)}

# --------- IA principal ---------
@app.post("/ia/analisar")
async def ia_analisar(payload: AnalyzePayload):
    load_dotenv()
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        raise HTTPException(500, "GROQ_API_KEY ausente.")
    client = Groq(api_key=api_key)

    try:
        return ask_json_only_prompt(
            client=client,
            user_prompt=payload.prompt,
            blocks=payload.blocks or [],
            prefs=payload.prefs or {},
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Erro: {e}")

# --------- Persistência de prompts (compartilhado) ---------
@app.get("/prompts/{page_id}")
async def get_prompts(page_id: str):
    pid = _safe_page_id(page_id)
    path = _prompts_path(pid)
    data = _read_json_or_empty(path)
    # garantia de forma
    try:
        state = PromptState(**data)
        return json.loads(state.model_dump_json())
    except Exception:
        # se estruturalmente inválido, zera
        return {"list": [], "activeId": None}

@app.put("/prompts/{page_id}")
async def put_prompts(page_id: str, body: PromptState):
    pid = _safe_page_id(page_id)
    # saneamento leve do conteúdo
    clean_list = []
    for item in body.list:
        clean_list.append(PromptItem(
            id=item.id.strip(),
            name=_clean_pt(item.name)[:256],
            template=item.template,            # mantém texto integral; limite já aplicado no modelo
            updatedAt=item.updatedAt.strip()
        ))
    state = PromptState(list=clean_list, activeId=(body.activeId or None))
    path = _prompts_path(pid)
    try:
        _atomic_write_json(path, json.loads(state.model_dump_json()))
        return {"ok": True}
    except Exception as e:
        raise HTTPException(500, f"Falha ao gravar prompts: {e}")
