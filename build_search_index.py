"""Gera resumos + embeddings das páginas para a busca semântica da home.

Fluxo em duas etapas:

  1) python build_search_index.py summarize
     Lê search_index/pages.json. Para páginas internas, lê o HTML, extrai
     texto visível e pede ao GPT um resumo + lista de palavras-chave.
     Para páginas externas, usa description/keywords manuais.
     Salva em search_index/summaries.json (revisável).

  2) python build_search_index.py embed
     Lê search_index/summaries.json. Para cada página, gera embedding via
     OpenAI text-embedding-3-small de "title + summary + keywords".
     Salva em search_index/embeddings.json (consumido pelo /api/search).

Variáveis necessárias: OPENAI_API_KEY (mesmas do app).

Custo aproximado:
  - Resumos: ~40 páginas × ~5k tokens entrada + ~200 saída ≈ $0.04 com gpt-4o-mini
  - Embeddings: ~40 × ~300 tokens ≈ $0.0001 com text-embedding-3-small
"""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path
from typing import Any

from openai import OpenAI

ROOT = Path(__file__).resolve().parent


def _load_dotenv() -> None:
    """Carrega chaves do .env adjacente, sem depender da lib python-dotenv."""
    env_path = ROOT / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        if k and k not in os.environ:
            os.environ[k] = v


_load_dotenv()
INDEX_DIR = ROOT / "search_index"
PAGES_FILE = INDEX_DIR / "pages.json"
SUMMARIES_FILE = INDEX_DIR / "summaries.json"
EMBEDDINGS_FILE = INDEX_DIR / "embeddings.json"

SUMMARY_MODEL = os.getenv("SEARCH_SUMMARY_MODEL", "gpt-4o-mini")
EMBED_MODEL = os.getenv("SEARCH_EMBED_MODEL", "text-embedding-3-small")

SYSTEM_PROMPT = """Você é um sistema que analisa páginas de um dashboard interno da CAMIM (rede de clínicas) e produz um resumo em português voltado para busca semântica.

Para a página dada, retorne um JSON com este formato exato:
{
  "summary": "2-4 frases descrevendo o que a página faz, quais métricas mostra e em que contexto seria útil. Português direto, sem marketing.",
  "keywords": ["palavra1", "palavra2", "..."]
}

Regras:
- O summary deve mencionar JARGÃO de negócio que o usuário usaria ao buscar (ex: "pagamento de médico", "inadimplência", "ticket médio").
- Keywords são 8-15 termos específicos: nomes de KPIs, conceitos, tipos de dado, sinônimos coloquiais.
- Não invente funcionalidades que não estão no HTML. Se a página é sobre médicos, diga isso; não diga que é sobre vendas.
- Se a página tem um filtro importante (posto, mês, especialidade), inclua nas keywords.
- Saída APENAS o JSON, nada mais. Sem markdown, sem ```json."""


def _strip_html(html: str) -> str:
    # Remove scripts, styles, e tags. Mantém texto visível.
    html = re.sub(r"<script\b[^<]*(?:(?!</script>)<[^<]*)*</script>", " ", html, flags=re.I)
    html = re.sub(r"<style\b[^<]*(?:(?!</style>)<[^<]*)*</style>", " ", html, flags=re.I)
    html = re.sub(r"<!--.*?-->", " ", html, flags=re.S)
    html = re.sub(r"<[^>]+>", " ", html)
    html = re.sub(r"\s+", " ", html).strip()
    return html


def _client() -> OpenAI:
    key = os.getenv("OPENAI_API_KEY")
    if not key:
        raise RuntimeError("OPENAI_API_KEY não está definida no ambiente.")
    return OpenAI(api_key=key)


def _load_pages() -> list[dict[str, Any]]:
    data = json.loads(PAGES_FILE.read_text(encoding="utf-8"))
    return data["pages"]


def _summarize_internal(client: OpenAI, page: dict[str, Any]) -> dict[str, Any]:
    html_path = ROOT / page["html_file"]
    if not html_path.exists():
        raise FileNotFoundError(f"HTML não encontrado: {html_path}")
    raw = html_path.read_text(encoding="utf-8", errors="ignore")
    text = _strip_html(raw)
    # Limita a ~12k chars (≈ 3-4k tokens) — sidebar/header repetem em todas, conteúdo único costuma caber.
    if len(text) > 12000:
        text = text[:12000]
    prompt = (
        f"Página: {page['title']} (arquivo {page['html_file']}, URL {page['url']})\n\n"
        f"Conteúdo visível:\n{text}\n"
    )
    resp = client.chat.completions.create(
        model=SUMMARY_MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
        temperature=0.2,
        response_format={"type": "json_object"},
    )
    return json.loads(resp.choices[0].message.content)


def cmd_summarize() -> None:
    pages = _load_pages()
    client = _client()
    out: list[dict[str, Any]] = []
    # Carrega summaries existentes para permitir retomar de onde parou.
    existing: dict[str, dict[str, Any]] = {}
    if SUMMARIES_FILE.exists():
        for item in json.loads(SUMMARIES_FILE.read_text(encoding="utf-8")):
            existing[item["key"]] = item
    for i, page in enumerate(pages, 1):
        key = page["key"]
        prev = existing.get(key)
        if prev and prev.get("html_hash") == _html_hash(page) and prev.get("summary"):
            print(f"[{i}/{len(pages)}] {key}: cache hit, pulando")
            out.append(prev)
            continue
        print(f"[{i}/{len(pages)}] {key} ({page['type']})... ", end="", flush=True)
        try:
            if page["type"] == "external":
                summary = page.get("description", "")
                keywords = page.get("keywords", [])
            else:
                gen = _summarize_internal(client, page)
                summary = gen.get("summary", "")
                keywords = gen.get("keywords", [])
            entry = {
                "key": key,
                "title": page["title"],
                "url": page["url"],
                "type": page["type"],
                "summary": summary,
                "keywords": keywords,
                "html_hash": _html_hash(page),
            }
            out.append(entry)
            print("ok")
        except Exception as e:
            print(f"ERRO: {e}")
            if prev:
                out.append(prev)
    SUMMARIES_FILE.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nSalvo em {SUMMARIES_FILE} ({len(out)} páginas)")


def _html_hash(page: dict[str, Any]) -> str:
    """Hash do HTML interno (ou da description externa) para detectar mudanças."""
    import hashlib
    if page["type"] == "external":
        s = (page.get("description", "") + "|" + ",".join(page.get("keywords", []))).encode()
    else:
        path = ROOT / page["html_file"]
        if not path.exists():
            return ""
        s = path.read_bytes()
    return hashlib.sha256(s).hexdigest()[:16]


def cmd_embed() -> None:
    if not SUMMARIES_FILE.exists():
        raise SystemExit(f"Arquivo {SUMMARIES_FILE} não existe. Rode 'summarize' primeiro.")
    summaries = json.loads(SUMMARIES_FILE.read_text(encoding="utf-8"))
    client = _client()
    texts = [
        f"{s['title']}. {s['summary']} Palavras-chave: {', '.join(s.get('keywords', []))}"
        for s in summaries
    ]
    print(f"Gerando embeddings de {len(texts)} páginas com {EMBED_MODEL}...")
    resp = client.embeddings.create(model=EMBED_MODEL, input=texts)
    out = []
    for s, item in zip(summaries, resp.data):
        out.append({
            "key": s["key"],
            "title": s["title"],
            "url": s["url"],
            "type": s["type"],
            "summary": s["summary"],
            "keywords": s.get("keywords", []),
            "embedding": item.embedding,
        })
    EMBEDDINGS_FILE.write_text(json.dumps({"model": EMBED_MODEL, "pages": out}, ensure_ascii=False), encoding="utf-8")
    print(f"Salvo em {EMBEDDINGS_FILE}")


def main() -> None:
    if len(sys.argv) < 2 or sys.argv[1] not in {"summarize", "embed"}:
        print(__doc__)
        sys.exit(1)
    cmd = sys.argv[1]
    if cmd == "summarize":
        cmd_summarize()
    else:
        cmd_embed()


if __name__ == "__main__":
    main()
