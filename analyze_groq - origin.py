# analyze_groq.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os, json, pathlib, html, re, unicodedata, tempfile
from typing import Any, List, Optional, Dict

from dotenv import load_dotenv
load_dotenv()  # carregue envs uma vez, no topo

from groq import Groq
from fastapi import FastAPI, HTTPException, Body, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator
from fastapi.middleware.cors import CORSMiddleware

# ---------------- FastAPI + CORS ----------------
app = FastAPI(title="IA-Groq-Analises")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://teste-ia.camim.com.br"],
    allow_credentials=True,
    allow_methods=["GET","POST","OPTIONS"],
    allow_headers=["*"],
)

# ---------------- Config ----------------
BASE = pathlib.Path(__file__).parent.resolve()
MODEL = os.getenv("GROQ_MODEL", "openai/gpt-oss-120b")
PROMPTS_DIR = pathlib.Path(os.getenv("PROMPTS_DIR", "/var/appdata/prompts")).resolve()
PROMPTS_DIR.mkdir(parents=True, exist_ok=True)

# Limites simples de chunk
MAX_BLOCKS_PER_PART = int(os.getenv("IA_MAX_BLOCKS_PER_PART", "4"))
MAX_PARTS = int(os.getenv("IA_MAX_PARTS", "6"))

# ========================== Sanitização PT-BR ==========================
def _clean_pt(s: str) -> str:
    s = html.unescape(s or "")
    s = unicodedata.normalize("NFC", s).replace("\u00A0", " ").replace("\u200B", "")
    s = re.sub(r"([.,;:?!])(?!\s|$)", r"\1 ", s)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s

def _strip_heavy_markup(s: str) -> str:
    if not s:
        return s
    s = re.sub(r"```[\s\S]*?```", " ", s)                 # code fences
    s = re.sub(r"^\s*\|.*\|\s*$", " ", s, flags=re.M)     # tabelas ASCII
    s = re.sub(r"\$\$[\s\S]*?\$\$|\\\[[\s\S]*?\\\]|\\\([\s\S]*?\\\)", " ", s)  # LaTeX
    s = re.sub(r"\n{3,}", "\n\n", s)
    return _clean_pt(s)

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
    prompt: Optional[str] = None
    blocks: Optional[List[Dict[str, Any]]] = None
    prefs: Optional[Dict[str, Any]] = None

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
        return v  # permite valor “órfão”; o front pode ajustar

# ========================== IO de arquivos (prompts) ==========================
def _safe_page_id(page_id: str) -> str:
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
        return {"list": [], "activeId": None}

# ========================== Probing opcional ==========================
def pick_probe_model(client: Groq, probe_models: List[str]) -> Optional[str]:
    for m in probe_models:
        try:
            r = client.chat.completions.create(
                model=m,
                messages=[{"role": "user", "content": "ping"}],
                max_tokens=1,
                temperature=0,
            )
            _ = r.choices[0].message.content
            return m
        except Exception:
            continue
    return None

# ========================== Helpers internos ==========================
def _normalize_blocks(blocks: Any) -> list[dict]:
    if not isinstance(blocks, list):
        return []
    out: list[dict] = []
    for b in blocks:
        if isinstance(b, dict):
            out.append(b)
    return out

def _chunk_blocks(blocks: list[dict], size: int = MAX_BLOCKS_PER_PART) -> List[list[dict]]:
    if size <= 0:
        size = 4
    parts = []
    for i in range(0, len(blocks), size):
        parts.append(blocks[i:i+size])
        if len(parts) >= MAX_PARTS:
            break
    return parts if parts else [[]]

def _nonempty_text(txt: str) -> str:
    s = _strip_heavy_markup(txt or "")
    return s if s else "sem conteúdo."

# ========================== Chamada Groq ==========================
def _call_groq_free(client: Groq, messages: list, temperature: float, max_tokens: int) -> str:
    resp = client.chat.completions.create(
        model=MODEL,
        temperature=temperature,
        max_tokens=max_tokens,
        tool_choice="none",
        messages=messages,
    )
    return (resp.choices[0].message.content or "").strip()

def ask_json_only_prompt(client: Groq, user_prompt: str, blocks: list | None, prefs: dict | None) -> dict:
    msgs = [{"role": "user", "content": user_prompt}]
    if blocks:
        msgs.append({"role": "user", "content": json.dumps({"blocks": blocks}, ensure_ascii=False)})

    # prefs
    temperature = float(prefs.get("temperature", 0.1)) if isinstance(prefs, dict) else 0.1
    # compat: aceitar chaves novas ou antigas
    max_tokens = int(prefs.get("max_tokens", prefs.get("max_completion_tokens", 4096))) if isinstance(prefs, dict) else 4096
    accept = (prefs.get("accept_format", "auto") if isinstance(prefs, dict) else "auto").lower()

    # ===== Caminho 1: forçar JSON apenas quando solicitado =====
    if accept == "json":
        try:
            resp = client.chat.completions.create(
                model=MODEL,
                temperature=temperature,
                max_tokens=max_tokens,
                tool_choice="none",
                response_format={"type": "json_object"},
                messages=msgs,
            )
            raw = (resp.choices[0].message.content or "").strip()
            obj = json.loads(raw)
            return {"content_mode": "json", "parse_status": "ok", "data": _sanitize_obj(obj)}
        except Exception as e:
            # quando o cliente exigiu JSON, falha dura
            raise RuntimeError(f"Modelo não retornou JSON válido: {e}")

    # ===== Caminho 2: forçar texto livre quando solicitado =====
    if accept == "free_text":
        resp = client.chat.completions.create(
            model=MODEL,
            temperature=temperature,
            max_tokens=max_tokens,
            tool_choice="none",
            messages=msgs,
        )
        txt = (resp.choices[0].message.content or "").strip()
        return {"content_mode": "free_text", "parse_status": "raw", "text": _clean_pt(txt)}

    # ===== Caminho 3: auto (heurística antiga, mas segura) =====
    # tenta JSON e, se falhar, cai para texto e tenta extrair JSON embutido
    try:
        resp = client.chat.completions.create(
            model=MODEL,
            temperature=temperature,
            max_tokens=max_tokens,
            tool_choice="none",
            response_format={"type": "json_object"},
            messages=msgs,
        )
        raw = (resp.choices[0].message.content or "").strip()
        obj = json.loads(raw)
        return {"content_mode": "json", "parse_status": "ok", "data": _sanitize_obj(obj)}
    except Exception:
        pass

    resp = client.chat.completions.create(
        model=MODEL,
        temperature=temperature,
        max_tokens=max_tokens,
        tool_choice="none",
        messages=msgs,
    )
    txt = (resp.choices[0].message.content or "").strip()

    blk = first_json_block(txt)
    if blk:
        try:
            obj = json.loads(blk)
            return {"content_mode": "json", "parse_status": "extracted", "data": _sanitize_obj(obj)}
        except Exception:
            pass

    return {"content_mode": "free_text", "parse_status": "raw", "text": _clean_pt(txt)}

# ========================== FastAPI ==========================
@app.get("/health")
async def health():
    load_dotenv()
    probe_models = [m.strip() for m in os.getenv("GROQ_PROBE_MODELS", "").split(",") if m.strip()]
    api_key = os.getenv("GROQ_API_KEY")
    probe_selected = None
    if api_key and probe_models:
        try:
            probe_selected = pick_probe_model(Groq(api_key=api_key), probe_models)
        except Exception:
            probe_selected = None
    return {"ok": True, "model": MODEL, "probe_models": probe_models, "probe_selected": probe_selected, "prompts_dir": str(PROMPTS_DIR)}

@app.get("/ia/healthz")
async def healthz():
    return {"ok": True}

# --------- IA principal ---------
class _AnalyzeResponse(BaseModel):
    content_mode: str
    parse_status: str
    data: Optional[dict] = None
    text: Optional[str] = None

@app.post("/ia/analisar")
async def ia_analisar(req: Request, payload: Optional[AnalyzePayload] = Body(None)) -> _AnalyzeResponse:
    # Suporte a text/plain
    ctype = req.headers.get("content-type", "")
    if payload is None and ctype.startswith("text/plain"):
        text = (await req.body()).decode("utf-8", errors="ignore")
        payload = AnalyzePayload(prompt=text, blocks=[])

    # Validação mínima: precisa de prompt ou blocks
    if payload is None or not ((payload.prompt and payload.prompt.strip()) or (payload.blocks and len(payload.blocks) > 0)):
        return JSONResponse({"detail": "missing prompt or blocks"}, status_code=422)

    load_dotenv()
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        raise HTTPException(500, "GROQ_API_KEY ausente.")
    client = Groq(api_key=api_key)

    try:
        blk_norm = _normalize_blocks(payload.blocks or [])
        out = ask_json_only_prompt(
            client=client,
            user_prompt=(payload.prompt or "").strip(),
            blocks=blk_norm,
            prefs=payload.prefs or {},
        )
        # Normaliza texto final para o front
        if out.get("content_mode") == "free_text":
            out["text"] = _strip_heavy_markup(out.get("text") or "")
        if out.get("content_mode") == "json":
            out["data"] = _sanitize_obj(out.get("data") or {})
        return _AnalyzeResponse(**out)
    except HTTPException:
        raise
    except Exception as e:
        # nunca quebra: retorna texto explicativo
        return _AnalyzeResponse(
            content_mode="free_text",
            parse_status="exception_fallback",
            text=_clean_pt(f"falha interna: {e}")
        )

# --------- Persistência de prompts (compartilhado) ---------
@app.get("/prompts/{page_id}")
async def get_prompts(page_id: str):
    pid = _safe_page_id(page_id)
    path = _prompts_path(pid)
    data = _read_json_or_empty(path)
    try:
        state = PromptState(**data)
        return json.loads(state.model_dump_json())
    except Exception:
        return {"list": [], "activeId": None}

@app.put("/prompts/{page_id}")
async def put_prompts(page_id: str, body: PromptState):
    pid = _safe_page_id(page_id)
    clean_list = []
    for item in body.list:
        clean_list.append(PromptItem(
            id=item.id.strip(),
            name=_clean_pt(item.name)[:256],
            template=item.template,
            updatedAt=item.updatedAt.strip()
        ))
    state = PromptState(list=clean_list, activeId=(body.activeId or None))
    path = _prompts_path(pid)
    try:
        _atomic_write_json(path, json.loads(state.model_dump_json()))
        return {"ok": True}
    except Exception as e:
        raise HTTPException(500, f"Falha ao gravar prompts: {e}")
