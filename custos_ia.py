"""
custos_ia.py — Núcleo do dashboard "Custos com IA" (Mais Serviços, admin-only).

Objetivo:
  - OpenAI: custo REAL por projeto, por MÊS, via Costs API da organização.
            Requer Admin key (sk-admin-...) com escopo `api.usage.read`, lida de
            OPENAI_ADMIN_KEY. A OPENAI_API_KEY comum (sk-proj-...) NÃO acessa o
            endpoint de custos da organização.
  - Groq:   NÃO tem API pública de custo (endpoints dão 404). O custo por projeto
            vem de um PRINT da tela console.groq.com/settings/organization/projects
            (lido por visão da OpenAI) OU é digitado à mão (sem gastar visão).

Fechamento mensal:
  Cada mês tem seus próprios arquivos (openai_YYYY-MM.json / groq_YYYY-MM.json),
  então o histórico fica preservado mês a mês. Um mês pode ser "fechado"
  (closed_months.json) — quando fechado, o ETL horário não o sobrescreve mais
  (o registro fica congelado). Meses anteriores podem ser re-buscados sob demanda.

Armazenamento: arquivos JSON em CUSTOS_IA_DIR (default ./data/custos_ia no dev,
/opt/relatorio_h_t/data/custos_ia na VM). Nunca versionados (dados financeiros).

Tudo é somente-leitura no provedor (GET na Costs API, grátis). A leitura do print
usa uma chamada de visão da OpenAI (centavos), disparada à mão — nunca em lote.
"""

from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

import requests

_BRT = timezone(timedelta(hours=-3))
OPENAI_API_BASE = "https://api.openai.com/v1"
_HTTP_TIMEOUT = 30
_MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
_CLOSED_FILE = "closed_months.json"


# ─────────────────────────────────────────────────────────────────────────────
# Datas / meses
# ─────────────────────────────────────────────────────────────────────────────
def month_now() -> str:
    return datetime.now(_BRT).strftime("%Y-%m")


def _now_iso() -> str:
    return datetime.now(_BRT).replace(microsecond=0).isoformat()


def valid_month(m: Optional[str]) -> str:
    """Normaliza/valida 'YYYY-MM'; default = mês corrente (BRT)."""
    m = (m or "").strip()
    return m if _MONTH_RE.match(m) else month_now()


def _month_bounds_utc(month: str) -> tuple[int, int]:
    """(start_ts, end_ts) em UTC para o mês 'YYYY-MM'. end = início do mês seguinte."""
    y, mo = (int(x) for x in month.split("-"))
    start = datetime(y, mo, 1, tzinfo=timezone.utc)
    nxt = datetime(y + 1, 1, 1, tzinfo=timezone.utc) if mo == 12 \
        else datetime(y, mo + 1, 1, tzinfo=timezone.utc)
    return int(start.timestamp()), int(nxt.timestamp())


# ─────────────────────────────────────────────────────────────────────────────
# Storage
# ─────────────────────────────────────────────────────────────────────────────
def _data_dir() -> str:
    d = os.environ.get("CUSTOS_IA_DIR")
    if not d:
        d = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "custos_ia")
    os.makedirs(d, exist_ok=True)
    return d


def _path(name: str) -> str:
    return os.path.join(_data_dir(), name)


def _provider_path(provider: str, month: str) -> str:
    return _path(f"{provider}_{month}.json")


def _write_json_atomic(path: str, payload: dict) -> None:
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, allow_nan=False, default=str)
    os.replace(tmp, path)


def _read_json(path: str) -> Optional[dict]:
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Fechamento de mês
# ─────────────────────────────────────────────────────────────────────────────
def _closed_data() -> dict:
    return _read_json(_path(_CLOSED_FILE)) or {"months": {}}


def is_closed(month: str) -> bool:
    return valid_month(month) in _closed_data().get("months", {})


def close_month(month: str, by: Optional[str] = None) -> dict:
    month = valid_month(month)
    data = _closed_data()
    data.setdefault("months", {})[month] = {"closed_at": _now_iso(), "closed_by": by}
    _write_json_atomic(_path(_CLOSED_FILE), data)
    return data["months"][month]


def reopen_month(month: str) -> None:
    month = valid_month(month)
    data = _closed_data()
    data.get("months", {}).pop(month, None)
    _write_json_atomic(_path(_CLOSED_FILE), data)


def list_months() -> list[str]:
    """Meses com algum dado (ou fechados), incluindo o corrente, em ordem desc."""
    months: set[str] = {month_now()}
    try:
        for fn in os.listdir(_data_dir()):
            m = re.match(r"^(?:openai|groq)_(\d{4}-\d{2})\.json$", fn)
            if m:
                months.add(m.group(1))
    except FileNotFoundError:
        pass
    months.update(_closed_data().get("months", {}).keys())
    return sorted(months, reverse=True)


# ─────────────────────────────────────────────────────────────────────────────
# OpenAI — Costs API
# ─────────────────────────────────────────────────────────────────────────────
def _openai_admin_key() -> Optional[str]:
    k = (os.environ.get("OPENAI_ADMIN_KEY") or "").strip()
    return k or None


def _openai_project_names(admin_key: str) -> dict[str, str]:
    """Mapa project_id -> nome legível. Tolerante a falha (devolve {})."""
    names: dict[str, str] = {}
    url = f"{OPENAI_API_BASE}/organization/projects"
    params: dict = {"limit": 100}
    headers = {"Authorization": f"Bearer {admin_key}"}
    try:
        for _ in range(10):
            r = requests.get(url, headers=headers, params=params, timeout=_HTTP_TIMEOUT)
            if r.status_code != 200:
                break
            body = r.json()
            for p in body.get("data", []):
                if p.get("id"):
                    names[p["id"]] = p.get("name") or p["id"]
            if not body.get("has_more"):
                break
            params["after"] = body.get("last_id")
    except requests.RequestException:
        pass
    return names


def fetch_openai_costs(admin_key: str, start_time: int, end_time: int) -> dict:
    """Custo da organização agrupado por projeto, em buckets diários, no intervalo.

    Retorna {"projects": {pid: usd}, "daily": [{date, amount}], "total_usd", "currency"}.
    Lança RuntimeError amigável em 401/403 (key sem escopo).
    """
    url = f"{OPENAI_API_BASE}/organization/costs"
    headers = {"Authorization": f"Bearer {admin_key}"}
    params: dict = {
        "start_time": start_time,
        "end_time": end_time,
        "bucket_width": "1d",
        "group_by": ["project_id"],
        "limit": 180,
    }
    projects: dict[str, float] = {}
    daily: list[dict] = []
    currency = "usd"

    for _ in range(40):
        r = requests.get(url, headers=headers, params=params, timeout=_HTTP_TIMEOUT)
        if r.status_code in (401, 403):
            raise RuntimeError(
                "A Admin key da OpenAI não tem permissão para a Costs API "
                "(escopo api.usage.read). Verifique OPENAI_ADMIN_KEY."
            )
        r.raise_for_status()
        body = r.json()
        for bucket in body.get("data", []):
            bdate = datetime.fromtimestamp(
                bucket.get("start_time", 0), tz=timezone.utc
            ).strftime("%Y-%m-%d")
            bucket_total = 0.0
            for res in bucket.get("results", []):
                amt = res.get("amount") or {}
                val = float(amt.get("value") or 0.0)
                currency = amt.get("currency") or currency
                pid = res.get("project_id") or "—"
                projects[pid] = projects.get(pid, 0.0) + val
                bucket_total += val
            daily.append({"date": bdate, "amount": round(bucket_total, 4)})
        if not body.get("has_more"):
            break
        params["page"] = body.get("next_page")

    return {
        "projects": {k: round(v, 4) for k, v in projects.items()},
        "daily": daily,
        "total_usd": round(sum(projects.values()), 4),
        "currency": currency,
    }


def build_openai_snapshot(month: Optional[str] = None) -> dict:
    """Monta o snapshot da OpenAI para o mês. Sempre retorna dict gravável."""
    month = valid_month(month)
    snap: dict = {
        "provider": "openai", "ok": False, "error": None,
        "updated_at": _now_iso(), "month": month,
        "total_usd": 0.0, "currency": "usd", "projects": [], "daily": [],
    }
    admin_key = _openai_admin_key()
    if not admin_key:
        snap["error"] = ("OPENAI_ADMIN_KEY ausente. Crie uma Admin key em "
                         "platform.openai.com (Settings → API keys → Admin keys).")
        return snap
    try:
        start, end = _month_bounds_utc(month)
        raw = fetch_openai_costs(admin_key, start, end)
        names = _openai_project_names(admin_key)
        projetos = [
            {"id": pid, "name": names.get(pid, pid), "amount_usd": amount}
            for pid, amount in raw["projects"].items()
        ]
        projetos.sort(key=lambda x: x["amount_usd"], reverse=True)
        snap.update(ok=True, total_usd=raw["total_usd"], currency=raw["currency"],
                    projects=projetos, daily=raw["daily"])
    except Exception as e:  # noqa: BLE001
        snap["error"] = str(e)
    return snap


def save_openai_snapshot(month: Optional[str] = None, *, force: bool = False) -> dict:
    """Gera e grava o snapshot da OpenAI do mês. Mês fechado não é sobrescrito
    (a menos que force=True)."""
    month = valid_month(month)
    if is_closed(month) and not force:
        existing = load_openai_snapshot(month)
        if existing:
            return existing
    snap = build_openai_snapshot(month)
    # Não congele um erro por cima de um snapshot bom já existente.
    if not snap.get("ok"):
        existing = load_openai_snapshot(month)
        if existing and existing.get("ok"):
            existing["last_error"] = snap.get("error")
            existing["last_error_at"] = _now_iso()
            _write_json_atomic(_provider_path("openai", month), existing)
            return existing
    _write_json_atomic(_provider_path("openai", month), snap)
    return snap


def load_openai_snapshot(month: Optional[str] = None) -> Optional[dict]:
    return _read_json(_provider_path("openai", valid_month(month)))


# ─────────────────────────────────────────────────────────────────────────────
# Groq — print (visão) ou digitação manual
# ─────────────────────────────────────────────────────────────────────────────
_GROQ_PERIOD_NOTE = (
    'Valor da coluna "MONTHLY SPEND" da Groq. A Groq não deixa claro se é o '
    "mês-calendário corrente ou o ciclo de cobrança — confira no painel deles."
)

_GROQ_VISION_PROMPT = (
    "Esta é uma captura de tela da página de Projetos do console da Groq "
    "(console.groq.com/settings/organization/projects). A tabela tem colunas "
    "NAME e MONTHLY SPEND. Extraia TODOS os projetos visíveis e o gasto mensal "
    "de cada um. Responda APENAS com JSON válido, sem comentários, no formato: "
    '{"currency":"USD","projects":[{"name":"...","monthly_spend_usd":0.00}]}. '
    'Converta valores como "$62.61 USD" para o número 62.61. Use ponto como '
    "separador decimal. Não invente projetos; só os que aparecem na imagem."
)


def _groq_snapshot(projetos: list[dict], *, source: str, month: str) -> dict:
    total = round(sum(p.get("amount_usd", 0.0) for p in projetos), 4)
    projetos = sorted(projetos, key=lambda x: x.get("amount_usd", 0.0), reverse=True)
    return {
        "provider": "groq", "ok": True, "error": None,
        "source": source,                       # "print" | "manual"
        "updated_at": _now_iso(), "month": month,
        "period_note": _GROQ_PERIOD_NOTE,
        "total_usd": total, "currency": "usd", "projects": projetos,
    }


def save_groq_manual(projects: list[dict], month: Optional[str] = None) -> dict:
    """Grava custos da Groq digitados à mão (sem gastar visão)."""
    month = valid_month(month)
    norm = []
    for p in projects:
        nome = (str(p.get("name") or "")).strip() or "—"
        try:
            amount = round(float(p.get("amount_usd") or 0.0), 4)
        except (TypeError, ValueError):
            amount = 0.0
        norm.append({"id": nome, "name": nome, "amount_usd": amount})
    snap = _groq_snapshot(norm, source="manual", month=month)
    return save_groq_snapshot(snap)


def extract_groq_from_image(image_bytes: bytes, mime: str = "image/png",
                            openai_key: Optional[str] = None,
                            model: Optional[str] = None,
                            month: Optional[str] = None) -> dict:
    """Lê um print da tela de Projects da Groq via visão da OpenAI. NÃO grava."""
    import base64
    from openai import OpenAI

    key = (openai_key or os.environ.get("OPENAI_API_KEY") or "").strip()
    if not key:
        raise RuntimeError("OPENAI_API_KEY ausente para a leitura por visão.")
    model = model or os.environ.get("CUSTOS_IA_VISION_MODEL", "gpt-4.1")
    data_uri = f"data:{mime};base64,{base64.b64encode(image_bytes).decode('ascii')}"

    client = OpenAI(api_key=key)
    resp = client.chat.completions.create(
        model=model, temperature=0,
        response_format={"type": "json_object"},
        messages=[{
            "role": "user",
            "content": [
                {"type": "text", "text": _GROQ_VISION_PROMPT},
                {"type": "image_url", "image_url": {"url": data_uri, "detail": "high"}},
            ],
        }],
    )
    parsed = json.loads(resp.choices[0].message.content or "{}")
    projetos = []
    for p in parsed.get("projects", []):
        try:
            amount = round(float(p.get("monthly_spend_usd") or 0.0), 4)
        except (TypeError, ValueError):
            amount = 0.0
        nome = (p.get("name") or "").strip() or "—"
        projetos.append({"id": nome, "name": nome, "amount_usd": amount})
    return _groq_snapshot(projetos, source="print", month=valid_month(month))


_GROQ_AMOUNT_RE = re.compile(r"\$\s*([\d][\d.,]*)\s*USD", re.I)
_GROQ_DATE_RE = re.compile(r"\b\d{1,2}/\d{1,2}/\d{2,4}\b")
_GROQ_TIME_RE = re.compile(r"\b\d{1,2}:\d{2}(:\d{2})?\b")
_GROQ_SKIP_EXACT = {
    "projects", "create new project", "view", "name", "created at",
    "monthly spend", "rate limits",
}


def parse_groq_text(text: str) -> list[dict]:
    """Extrai [{name, amount_usd}] do TEXTO copiado da tela Projects da Groq.

    Tolerante ao formato bagunçado do copia-e-cola: nome e gasto costumam vir
    em linhas separadas (nome numa linha; 'dd/mm/aaaa, hh:mm:ss \\t $X.XX USD'
    em outra). Casa cada valor com o nome imediatamente anterior; se nome e
    valor vierem na mesma linha, separa pelo que vem antes da data.
    """
    projetos: list[dict] = []
    last_name: Optional[str] = None
    for raw in (text or "").splitlines():
        l = raw.strip()
        if not l:
            continue
        low = l.lower()
        if low in _GROQ_SKIP_EXACT or low.startswith("projects allow"):
            continue
        if "monthly spend" in low or "created at" in low:
            continue
        m = _GROQ_AMOUNT_RE.search(l)
        if m:
            try:
                amount = round(float(m.group(1).replace(",", "")), 4)
            except ValueError:
                amount = 0.0
            pre = _GROQ_TIME_RE.sub("", _GROQ_DATE_RE.sub("", l[:m.start()]))
            pre = pre.replace(",", " ").strip(" \t:")
            # só trata 'pre' como nome se tiver letra (evita pegar data/hora soltas)
            name = pre if (pre and pre.lower() != "view"
                           and re.search(r"[A-Za-zÀ-ÿ]", pre)) else (last_name or "—")
            projetos.append({"id": name, "name": name, "amount_usd": amount})
            last_name = None
            continue
        if _GROQ_DATE_RE.search(l):  # linha só de data, sem valor → ignora
            continue
        last_name = l  # candidato a nome do próximo projeto
    return projetos


def save_groq_text(text: str, month: Optional[str] = None) -> dict:
    """Faz parse do texto colado da Groq e grava o snapshot do mês."""
    projetos = parse_groq_text(text)
    snap = _groq_snapshot(projetos, source="texto", month=valid_month(month))
    return save_groq_snapshot(snap)


def save_groq_snapshot(snap: dict) -> dict:
    month = valid_month(snap.get("month"))
    _write_json_atomic(_provider_path("groq", month), snap)
    return snap


def load_groq_snapshot(month: Optional[str] = None) -> Optional[dict]:
    return _read_json(_provider_path("groq", valid_month(month)))


# ─────────────────────────────────────────────────────────────────────────────
# Meses recentes / backfill
# ─────────────────────────────────────────────────────────────────────────────
DEFAULT_HISTORY_MONTHS = 6


def recent_months(n: int = DEFAULT_HISTORY_MONTHS) -> list[str]:
    """Últimos n meses (corrente + anteriores), em ordem crescente."""
    y, m = (int(x) for x in month_now().split("-"))
    out = []
    for _ in range(n):
        out.append(f"{y:04d}-{m:02d}")
        m -= 1
        if m == 0:
            m = 12
            y -= 1
    return list(reversed(out))


def backfill_openai(n_months: int = DEFAULT_HISTORY_MONTHS, *, force: bool = False) -> list[dict]:
    """Busca e grava o snapshot da OpenAI para cada um dos últimos n meses.
    Mês fechado é pulado (salvo force=True). Só faz GET (sem custo)."""
    res = []
    for m in recent_months(n_months):
        snap = save_openai_snapshot(m, force=force)
        res.append({"month": m, "ok": snap.get("ok"),
                    "total_usd": snap.get("total_usd"), "error": snap.get("error")})
    return res


# ─────────────────────────────────────────────────────────────────────────────
# Projetos lógicos (vínculo OpenAI ↔ Groq)
# ─────────────────────────────────────────────────────────────────────────────
def _mapping_path() -> str:
    return _path("mapping.json")


def load_mapping() -> dict:
    """{"labels": {"openai::camila.ai": "Camila", "groq::Camila Atendimento": "Camila"}}"""
    return _read_json(_mapping_path()) or {"labels": {}}


def save_mapping(labels: dict) -> dict:
    clean = {str(k): str(v).strip() for k, v in (labels or {}).items() if str(v).strip()}
    data = {"labels": clean}
    _write_json_atomic(_mapping_path(), data)
    return data


def distinct_projects(months: Optional[list[str]] = None) -> list[dict]:
    """Todos os projetos distintos (por provedor) vistos nos meses dados,
    com o rótulo unificado atual. Alimenta o editor de vínculos."""
    months = months or list_months()
    labels = load_mapping().get("labels", {})
    seen: dict = {}
    for m in months:
        for prov, snap in (("openai", load_openai_snapshot(m)), ("groq", load_groq_snapshot(m))):
            for p in (snap or {}).get("projects", []):
                nm = p.get("name") or p.get("id")
                if nm:
                    seen[(prov, nm)] = True
    out = []
    for (prov, name) in sorted(seen, key=lambda x: (x[0], x[1].lower())):
        out.append({"provider": prov, "name": name,
                    "label": labels.get(f"{prov}::{name}") or name})
    return out


def unify_month(month: Optional[str] = None) -> list[dict]:
    """Custo por projeto LÓGICO no mês: junta OpenAI + Groq pelo rótulo unificado."""
    month = valid_month(month)
    labels = load_mapping().get("labels", {})
    agg: dict = {}
    for prov, snap in (("openai", load_openai_snapshot(month)), ("groq", load_groq_snapshot(month))):
        key = f"{prov}_usd"
        for p in (snap or {}).get("projects", []):
            nm = p.get("name") or p.get("id")
            lab = labels.get(f"{prov}::{nm}") or nm
            row = agg.setdefault(lab, {"openai_usd": 0.0, "groq_usd": 0.0})
            row[key] += float(p.get("amount_usd") or 0.0)
    rows = []
    for lab, v in agg.items():
        rows.append({
            "label": lab,
            "openai_usd": round(v["openai_usd"], 4),
            "groq_usd": round(v["groq_usd"], 4),
            "total_usd": round(v["openai_usd"] + v["groq_usd"], 4),
        })
    rows.sort(key=lambda x: x["total_usd"], reverse=True)
    return rows


def project_matrix(n: int = DEFAULT_HISTORY_MONTHS) -> dict:
    """Matriz projeto (lógico) × mês, últimos n meses."""
    months = recent_months(n)
    cells: dict = {}
    ordem: dict = {}
    for m in months:
        for row in unify_month(m):
            lab = row["label"]
            cells.setdefault(lab, {})[m] = row["total_usd"]
            ordem[lab] = ordem.get(lab, 0.0) + row["total_usd"]
    # oculta projetos zerados em todos os meses
    labels = sorted([l for l in ordem if ordem[l] > 0], key=lambda l: ordem[l], reverse=True)
    totals = {m: round(sum(cells.get(l, {}).get(m, 0.0) for l in labels), 4) for m in months}
    return {"months": months, "labels": labels, "cells": cells, "totals": totals}


# ─────────────────────────────────────────────────────────────────────────────
# Assinaturas de IA (mensalidades fixas: Anthropic, ChatGPT Plus, etc.)
# ─────────────────────────────────────────────────────────────────────────────
def _subs_path() -> str:
    return _path("subscriptions.json")


def load_subscriptions() -> list[dict]:
    return (_read_json(_subs_path()) or {}).get("items", [])


def save_subscriptions(items: list[dict]) -> list[dict]:
    norm = []
    for it in (items or []):
        name = (str(it.get("name") or "")).strip()
        if not name:
            continue
        try:
            val = round(float(it.get("monthly_usd") or 0.0), 4)
        except (TypeError, ValueError):
            val = 0.0
        since = it.get("since")
        norm.append({
            "id": str(it.get("id") or int(time.time() * 1000)),
            "name": name,
            "provider": (str(it.get("provider") or "")).strip(),
            "monthly_usd": val,
            "since": valid_month(since) if since else None,
            "active": bool(it.get("active", True)),
        })
    _write_json_atomic(_subs_path(), {"items": norm})
    return norm


def subscriptions_for(month: Optional[str] = None) -> list[dict]:
    """Assinaturas ativas que valem para o mês (since <= mês)."""
    month = valid_month(month)
    out = []
    for s in load_subscriptions():
        if not s.get("active"):
            continue
        since = s.get("since")
        if since and since > month:
            continue
        out.append(s)
    return out


def subs_total(month: Optional[str] = None) -> float:
    return round(sum(float(s.get("monthly_usd") or 0.0) for s in subscriptions_for(month)), 4)


# ─────────────────────────────────────────────────────────────────────────────
# Dashboard
# ─────────────────────────────────────────────────────────────────────────────
def _empty_openai(month: str) -> dict:
    return {
        "provider": "openai", "ok": False,
        "error": "Ainda não gerado para este mês (rode o ETL ou clique Atualizar).",
        "total_usd": 0.0, "currency": "usd", "projects": [], "daily": [],
        "updated_at": None, "month": month,
    }


def _empty_groq(month: str) -> dict:
    return {
        "provider": "groq", "ok": False,
        "error": "Sem dados deste mês — envie um print ou digite manualmente.",
        "source": None, "total_usd": 0.0, "currency": "usd", "projects": [],
        "updated_at": None, "month": month, "period_note": _GROQ_PERIOD_NOTE,
    }


def monthly_history(n: int = DEFAULT_HISTORY_MONTHS) -> list[dict]:
    """Resumo por mês (OpenAI + Groq + assinaturas), últimos n meses — gráfico."""
    hist = []
    for m in recent_months(n):
        o = load_openai_snapshot(m) or {}
        g = load_groq_snapshot(m) or {}
        ot = float(o.get("total_usd") or 0.0)
        gt = float(g.get("total_usd") or 0.0)
        st = subs_total(m)
        hist.append({
            "month": m,
            "openai_usd": round(ot, 4),
            "groq_usd": round(gt, 4),
            "subs_usd": round(st, 4),
            "total_usd": round(ot + gt + st, 4),
            "closed": is_closed(m),
        })
    return hist


def available_months() -> list[str]:
    """Últimos 6 meses ∪ meses com dado, desc — para o seletor (permite colar
    Groq de meses passados mesmo sem dado ainda)."""
    return sorted(set(recent_months(DEFAULT_HISTORY_MONTHS)) | set(list_months()), reverse=True)


def load_dashboard(month: Optional[str] = None) -> dict:
    """Payload do dashboard para um mês (default: corrente)."""
    month = valid_month(month)
    openai = load_openai_snapshot(month) or _empty_openai(month)
    groq = load_groq_snapshot(month) or _empty_groq(month)
    subs = subscriptions_for(month)
    subs_t = round(sum(float(s.get("monthly_usd") or 0.0) for s in subs), 4)
    total = round((openai.get("total_usd") or 0.0) + (groq.get("total_usd") or 0.0) + subs_t, 4)
    return {
        "generated_at": _now_iso(),
        "month": month,
        "is_current_month": (month == month_now()),
        "closed": is_closed(month),
        "available_months": available_months(),
        "total_usd": total,
        "providers": {"openai": openai, "groq": groq},
        "subscriptions": {"items": subs, "total_usd": subs_t,
                          "all": load_subscriptions()},
        "unified": unify_month(month),
        "matrix": project_matrix(DEFAULT_HISTORY_MONTHS),
        "history": monthly_history(DEFAULT_HISTORY_MONTHS),
    }


if __name__ == "__main__":
    snap = save_openai_snapshot()
    print(json.dumps(snap, ensure_ascii=False, indent=2, default=str))
