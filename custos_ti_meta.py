"""
custos_ti_meta.py — Entrada de custos da Meta (WhatsApp Business) no Custos de TI.

Dois caminhos, porque a Meta expõe DUAS coisas diferentes e só uma tem API:

1) COBRANÇA REAL (o que caiu no cartão)  →  SEM API pública.
   É a tela Business Manager → Cobrança e pagamentos → Atividade de pagamento
   (business.facebook.com/latest/billing_hub/payment_activity). A Graph API não
   publica essas transações: `/{business-id}/business_invoices` só existe para
   contas em linha de crédito/faturamento mensal, não para conta paga no cartão.
   Solução: colar o texto da tela — `parse_payment_activity()`. Mesmo molde do
   import por texto da Groq (custos_ia.parse_groq_text), e de graça: parsing
   local, nenhuma chamada a LLM.

2) CUSTO ESTIMADO DAS MENSAGENS  →  TEM API.
   `GET /{waba-id}?fields=pricing_analytics(...)` com metric_types=[COST,VOLUME]
   devolve o gasto aproximado por dia, categoria de preço e tier, na moeda da
   WABA. É o endpoint alinhado ao modelo POR MENSAGEM (vigente desde jul/2025);
   `conversation_analytics` é o modelo antigo, por conversa, e fica de reserva.
   Precisa de token de System User com `whatsapp_business_management`.

   Atenção: a Meta não devolve `cost` para WABA que usa a linha de crédito de um
   Solution Partner. A conta da CAMIM é paga direto no cartão (Visa ···· 6852,
   moeda USD), então o custo vem.

   Serve para EXPLICAR o mês (quanto foi marketing vs utility vs autenticação),
   não para fechar a conta — o valor cobrado no cartão inclui ajustes, créditos
   e impostos que não aparecem aqui.

Os dois viram `ti_lancamento` com `origem` distinta ('meta_texto' e 'meta_api')
e nunca se misturam: a home soma só os lançamentos de cobrança real. O que vem
da API entra como `previsto` e fica marcado como estimativa.

Tudo aqui é somente-leitura no lado da Meta (GET). Nenhuma chamada deste módulo
envia mensagem nem gera custo — ver REGRA CRÍTICA de custos reais no CLAUDE.md.
"""
from __future__ import annotations

import os
import re
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import requests

GRAPH_VERSION = os.environ.get("META_GRAPH_VERSION", "v21.0")
GRAPH_BASE = f"https://graph.facebook.com/{GRAPH_VERSION}"
_HTTP_TIMEOUT = 30

# ─────────────────────────────────────────────────────────────────────────────
# 1) Parser do texto colado da "Atividade de pagamento"
# ─────────────────────────────────────────────────────────────────────────────

# 28011176295236020-28028364550183862
_TXN_RE = re.compile(r"^\d{10,}-\d{10,}$")

_MESES = {
    "jan": 1, "janeiro": 1, "fev": 2, "fevereiro": 2, "feb": 2,
    "mar": 3, "marco": 3, "março": 3, "march": 3,
    "abr": 4, "abril": 4, "apr": 4,
    "mai": 5, "maio": 5, "may": 5,
    "jun": 6, "junho": 6, "june": 6,
    "jul": 7, "julho": 7, "july": 7,
    "ago": 8, "agosto": 8, "aug": 8, "august": 8,
    "set": 9, "setembro": 9, "sep": 9, "sept": 9, "september": 9,
    "out": 10, "outubro": 10, "oct": 10, "october": 10,
    "nov": 11, "novembro": 11, "november": 11,
    "dez": 12, "dezembro": 12, "dec": 12, "december": 12,
}

# "1 de ago de 2026" / "1 de agosto de 2026" / "1 ago 2026"
_DATA_EXT_RE = re.compile(
    r"\b(\d{1,2})\s*(?:de\s+)?([A-Za-zÀ-ÿ]{3,10})\.?\s*(?:de\s+)?(\d{4})\b"
)
_DATA_NUM_RE = re.compile(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b")

# US$225,53 · $1,234.56 · R$ 89,90
_VALOR_RE = re.compile(r"(US\$|R\$|\$|USD|BRL|EUR|€)\s*([\d][\d.,]*)", re.I)

_BANDEIRA_RE = re.compile(
    r"\b(visa|mastercard|master|amex|american\s+express|elo|hipercard|"
    r"discover|diners|jcb)\b",
    re.I,
)
_ULT4_RE = re.compile(r"(?:[·•*.·•∙]\s*){2,}\s*(\d{4})\b")

_MOEDA_POR_SIMBOLO = {
    "us$": "USD", "usd": "USD", "$": "USD",
    "r$": "BRL", "brl": "BRL",
    "€": "EUR", "eur": "EUR",
}

_STATUS_PAGO = {"pago", "paid", "concluído", "concluido", "completo", "aprovado"}

# Linhas de cabeçalho/UI que aparecem no copia-e-cola e não são dado.
_SKIP_EXATO = {
    "id da transação", "id da transacao", "data", "valor", "ação", "acao",
    "forma de pagamento", "status do pagamento", "pesquisar", "saldo atual",
    "conta do whatsapp business", "transaction id", "amount", "date",
    "payment method", "payment status", "action",
}


def _norm(s: str) -> str:
    return (s or "").strip().lower()


def parse_money(txt: str) -> Optional[float]:
    """Converte '225,53' / '1.234,56' / '1,234.56' / '75' em float.

    Regra: se há os dois separadores, o ÚLTIMO é o decimal. Com um só,
    3 dígitos depois = milhar; 1 ou 2 dígitos = decimal.
    """
    t = re.sub(r"[^\d.,]", "", txt or "")
    if not t:
        return None
    if "." in t and "," in t:
        corte = max(t.rfind("."), t.rfind(","))
        inteiro = re.sub(r"[.,]", "", t[:corte])
        frac = re.sub(r"\D", "", t[corte + 1:])
        return float(f"{inteiro or 0}.{frac or 0}")
    sep = "." if "." in t else ("," if "," in t else None)
    if sep is None:
        return float(t)
    partes = t.split(sep)
    if len(partes) > 2:                      # 1.234.567 → só milhar
        return float("".join(partes))
    if len(partes[1]) == 3:                  # 1.234 / 1,234 → milhar
        return float("".join(partes))
    return float(f"{partes[0] or 0}.{partes[1] or 0}")


def parse_data(txt: str) -> Optional[date]:
    """Reconhece '1 de ago de 2026', '01/08/2026' e 'Aug 1, 2026'."""
    m = _DATA_EXT_RE.search(txt or "")
    if m:
        mes = _MESES.get(_norm(m.group(2)))
        if mes:
            try:
                return date(int(m.group(3)), mes, int(m.group(1)))
            except ValueError:
                return None
    # "Aug 1, 2026" — mês antes do dia
    m = re.search(r"\b([A-Za-zÀ-ÿ]{3,10})\.?\s+(\d{1,2}),?\s+(\d{4})\b", txt or "")
    if m:
        mes = _MESES.get(_norm(m.group(1)))
        if mes:
            try:
                return date(int(m.group(3)), mes, int(m.group(2)))
            except ValueError:
                return None
    m = _DATA_NUM_RE.search(txt or "")
    if m:
        try:                                  # pt-BR: dd/mm/aaaa
            return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except ValueError:
            return None
    return None


def parse_payment_activity(texto: str) -> dict:
    """Extrai as transações do texto copiado da tela de Atividade de pagamento.

    A tela copia em ordem imprevisível (às vezes uma célula por linha, às vezes
    a linha inteira separada por TAB), então a âncora é o ID da transação: tudo
    que vem depois de um ID e antes do próximo pertence àquela transação.

    Retorna {"items": [...], "ignoradas": int, "moeda": "USD"}.
    Cada item: {external_id, data, valor, moeda, bandeira, ultimos4, status}.
    """
    # TAB e pipe viram quebra de linha → trata os dois formatos de cola igual.
    linhas = re.split(r"[\t\n\r|]+", texto or "")

    itens: list[dict] = []
    atual: Optional[dict] = None

    def _fecha():
        # Transação sem valor legível não entra — melhor faltar que inventar.
        nonlocal atual
        if atual and atual.get("valor") is not None:
            itens.append(atual)
        atual = None

    for bruta in linhas:
        linha = bruta.strip().strip("​")   # o copy da Meta injeta ZWSP
        if not linha:
            continue
        baixa = _norm(linha)
        if baixa in _SKIP_EXATO or baixa.startswith("pesquise pelo"):
            continue

        if _TXN_RE.match(linha):
            _fecha()
            atual = {"external_id": linha, "data": None, "valor": None,
                     "moeda": "USD", "bandeira": None, "ultimos4": None,
                     "status": "pago"}
            continue

        if atual is None:
            # Cabeçalho, saldo, nome da conta: qualquer coisa antes do 1º ID.
            # É aqui que o "Saldo atual $0,00" é descartado sem virar despesa.
            continue

        if atual.get("valor") is None:
            mv = _VALOR_RE.search(linha)
            if mv:
                v = parse_money(mv.group(2))
                if v is not None:
                    atual["valor"] = v
                    atual["moeda"] = _MOEDA_POR_SIMBOLO.get(_norm(mv.group(1)), "USD")

        if atual.get("data") is None:
            d = parse_data(linha)
            if d:
                atual["data"] = d

        mb = _BANDEIRA_RE.search(linha)
        if mb and not atual.get("bandeira"):
            atual["bandeira"] = _bandeira_canonica(mb.group(1))
        m4 = _ULT4_RE.search(linha)
        if m4 and not atual.get("ultimos4"):
            atual["ultimos4"] = m4.group(1)
        elif not atual.get("ultimos4") and mb:
            # "Visa 6852" sem os pontinhos
            resto = linha[mb.end():]
            m4b = re.search(r"\b(\d{4})\b", resto)
            if m4b:
                atual["ultimos4"] = m4b.group(1)

        if baixa in _STATUS_PAGO:
            atual["status"] = "pago"
        elif baixa in ("pendente", "pending", "em processamento", "processing"):
            atual["status"] = "previsto"
        elif baixa in ("falhou", "failed", "recusado", "declined"):
            atual["status"] = "falhou"

    _fecha()

    # Transações recusadas não são custo — filtradas aqui, não na tela.
    validas = [i for i in itens if i["status"] != "falhou"]
    moedas = sorted({i["moeda"] for i in validas})
    return {
        "items": validas,
        "ignoradas": len(itens) - len(validas),
        "moeda": moedas[0] if len(moedas) == 1 else "USD",
        "moedas_multiplas": len(moedas) > 1,
    }


def _bandeira_canonica(b: str) -> str:
    b = _norm(b)
    if b in ("master", "mastercard"):
        return "Mastercard"
    if b in ("amex", "american express"):
        return "Amex"
    return b.capitalize()


# ─────────────────────────────────────────────────────────────────────────────
# 2) Graph API — conversation_analytics (custo ESTIMADO por conversa)
# ─────────────────────────────────────────────────────────────────────────────
def meta_config() -> dict:
    """Credenciais da integração, vindas do ambiente. Sem elas, só o modo texto."""
    return {
        "waba_id": (os.environ.get("META_WABA_ID") or "").strip(),
        "token": (os.environ.get("META_ACCESS_TOKEN") or "").strip(),
        "business_id": (os.environ.get("META_BUSINESS_ID") or "").strip(),
    }


def meta_api_disponivel() -> bool:
    cfg = meta_config()
    return bool(cfg["waba_id"] and cfg["token"])


def _bounds_utc(competencia: str) -> tuple[int, int]:
    ano, mes = (int(x) for x in competencia.split("-"))
    ini = datetime(ano, mes, 1, tzinfo=timezone.utc)
    fim = (datetime(ano + 1, 1, 1, tzinfo=timezone.utc) if mes == 12
           else datetime(ano, mes + 1, 1, tzinfo=timezone.utc))
    return int(ini.timestamp()), int(fim.timestamp())


_SEM_CREDENCIAL = (
    "Integração da Meta não configurada. Defina META_WABA_ID e META_ACCESS_TOKEN "
    "(token de System User com a permissão whatsapp_business_management) no "
    ".env da VM e reinicie o camim-auth."
)


def _graph_analytics(campo: str, raiz: str) -> tuple[Optional[list], Optional[str]]:
    """GET num campo de analytics da WABA. Devolve (data_points, erro)."""
    cfg = meta_config()
    if not cfg["waba_id"] or not cfg["token"]:
        return None, _SEM_CREDENCIAL
    try:
        r = requests.get(
            f"{GRAPH_BASE}/{cfg['waba_id']}",
            params={"fields": campo, "access_token": cfg["token"]},
            timeout=_HTTP_TIMEOUT,
        )
        corpo = r.json() if r.content else {}
        if r.status_code != 200:
            erro = (corpo.get("error") or {}).get("message") or r.text[:300]
            return None, f"Graph API {r.status_code}: {erro}"
    except requests.RequestException as e:
        return None, f"Falha ao falar com a Graph API: {e}"
    except ValueError:
        return None, "A Graph API respondeu algo que não é JSON."

    pontos = []
    for bloco in (corpo.get(raiz, {}) or {}).get("data", []):
        pontos.extend(bloco.get("data_points", []) or [])
    return pontos, None


def _vazio(competencia: str, fonte: str) -> dict:
    return {"ok": False, "error": None, "fonte": fonte, "competencia": competencia,
            "total": 0.0, "moeda": "USD", "volume": 0,
            "por_categoria": {}, "por_tier": {}, "por_telefone": {}, "diario": []}


_TELEFONES_CACHE: dict = {}


def listar_telefones(forcar: bool = False) -> dict:
    """{numero_normalizado: rótulo} dos telefones da WABA. Cacheado em memória.

    O `pricing_analytics` devolve o número em `phone_number` (ex.: '552124559600');
    isto traduz para '+55 21 2455-9600 — Central de Atendimento'.
    """
    if _TELEFONES_CACHE and not forcar:
        return _TELEFONES_CACHE
    cfg = meta_config()
    if not cfg["waba_id"] or not cfg["token"]:
        return {}
    try:
        r = requests.get(f"{GRAPH_BASE}/{cfg['waba_id']}/phone_numbers",
                         params={"access_token": cfg["token"], "limit": 50},
                         timeout=_HTTP_TIMEOUT)
        if r.status_code != 200:
            return {}
        for p in (r.json() or {}).get("data", []):
            mostrado = p.get("display_phone_number") or p.get("id") or ""
            nome = (p.get("verified_name") or "").strip()
            _TELEFONES_CACHE[_so_digitos(mostrado)] = (
                f"{mostrado} — {nome}" if nome else mostrado)
    except requests.RequestException:
        return {}
    return _TELEFONES_CACHE


def _so_digitos(s: str) -> str:
    return re.sub(r"\D", "", s or "")


def rotulo_telefone(numero: str) -> str:
    """Nome amigável do número, caindo no próprio número quando não conhecido."""
    d = _so_digitos(numero)
    mapa = listar_telefones()
    return mapa.get(d) or mapa.get(d[-11:]) or (numero or "—")


def fetch_pricing_analytics(competencia: str, granularity: str = "DAILY") -> dict:
    """Custo aproximado das MENSAGENS do mês (modelo por mensagem, jul/2025+).

    `GET /{waba-id}?fields=pricing_analytics.start().end().granularity(DAILY)
        .metric_types(["COST","VOLUME"])
        .dimensions(["PHONE","PRICING_CATEGORY","TIER"])`

    ATENÇÃO à granularidade: `MONTHLY` devolve **200 com data_points vazio** —
    sem erro nenhum, medido em 2026-08-02 num mês que tinha US$ 1.100 de gasto.
    Só `DAILY` traz dado. Não trocar para MONTHLY "para economizar pontos".

    Só GET. Nunca levanta — devolve sempre um dict com `ok`/`error`.
    """
    out = _vazio(competencia, "pricing_analytics")
    ini, fim = _bounds_utc(competencia)
    campo = (
        f"pricing_analytics.start({ini}).end({fim})"
        f".granularity({granularity})"
        f'.metric_types(["COST","VOLUME"])'
        f'.dimensions(["PHONE","PRICING_CATEGORY","TIER"])'
    )
    pontos, erro = _graph_analytics(campo, "pricing_analytics")
    if erro:
        out["error"] = erro
        return out

    por_dia: dict[str, dict] = {}
    for p in pontos or []:
        custo = float(p.get("cost") or 0.0)
        volume = int(p.get("volume") or 0)
        cat = p.get("pricing_category") or "—"
        tier = p.get("tier") or "—"
        tel = _so_digitos(p.get("phone_number") or "") or "—"

        out["por_categoria"][cat] = round(out["por_categoria"].get(cat, 0.0) + custo, 4)
        out["por_tier"][tier] = round(out["por_tier"].get(tier, 0.0) + custo, 4)

        # por telefone, quebrado por categoria — é o que responde
        # "quanto o 2455 gastou de marketing?"
        linha = out["por_telefone"].setdefault(tel, {
            "numero": tel, "rotulo": rotulo_telefone(tel),
            "total": 0.0, "volume": 0, "categorias": {}})
        linha["total"] = round(linha["total"] + custo, 4)
        linha["volume"] += volume
        c = linha["categorias"].setdefault(cat, {"custo": 0.0, "volume": 0})
        c["custo"] = round(c["custo"] + custo, 4)
        c["volume"] += volume

        out["total"] += custo
        out["volume"] += volume
        if p.get("start"):
            dia = datetime.fromtimestamp(int(p["start"]), tz=timezone.utc)\
                          .strftime("%Y-%m-%d")
            slot = por_dia.setdefault(dia, {"data": dia, "valor": 0.0, "volume": 0})
            slot["valor"] = round(slot["valor"] + custo, 4)
            slot["volume"] += volume

    out["total"] = round(out["total"], 4)
    out["diario"] = [por_dia[k] for k in sorted(por_dia)]
    out["ok"] = True
    if not pontos:
        out["error"] = ("A Meta respondeu, mas não veio nenhum ponto de custo "
                        "para este mês.")
    return out


def fetch_conversation_analytics(competencia: str,
                                 granularity: str = "DAILY") -> dict:
    """Custo por CONVERSA (modelo antigo, pré-jul/2025). Reserva do pricing_analytics.

    Mantido porque WABA antiga pode continuar respondendo aqui e vir vazia no
    pricing_analytics. Mesmo formato de saída, com `volume` = nº de conversas.
    """
    out = _vazio(competencia, "conversation_analytics")
    ini, fim = _bounds_utc(competencia)
    campo = (
        f"conversation_analytics.start({ini}).end({fim})"
        f".granularity({granularity})"
        f'.metric_types(["COST","CONVERSATION"])'
        f'.dimensions(["CONVERSATION_CATEGORY"])'
    )
    pontos, erro = _graph_analytics(campo, "conversation_analytics")
    if erro:
        out["error"] = erro
        return out

    por_dia: dict[str, dict] = {}
    for p in pontos or []:
        custo = float(p.get("cost") or 0.0)
        conversas = int(p.get("conversation") or 0)
        cat = p.get("conversation_category") or "—"
        out["por_categoria"][cat] = round(out["por_categoria"].get(cat, 0.0) + custo, 4)
        out["total"] += custo
        out["volume"] += conversas
        if p.get("start"):
            dia = datetime.fromtimestamp(int(p["start"]), tz=timezone.utc)\
                          .strftime("%Y-%m-%d")
            slot = por_dia.setdefault(dia, {"data": dia, "valor": 0.0, "volume": 0})
            slot["valor"] = round(slot["valor"] + custo, 4)
            slot["volume"] += conversas

    out["total"] = round(out["total"], 4)
    out["diario"] = [por_dia[k] for k in sorted(por_dia)]
    out["ok"] = True
    if not pontos:
        out["error"] = ("A Meta respondeu, mas não há dados de custo para este "
                        "mês (WABA sem conversas ou janela fora do retido).")
    return out


def fetch_custo_mensagens(competencia: str, granularity: str = "DAILY") -> dict:
    """Custo do mês pela Graph API: tenta o modelo novo, cai no antigo se vier zero.

    A escolha fica registrada em `fonte` para a tela dizer de onde veio o número.
    """
    novo = fetch_pricing_analytics(competencia, granularity)
    if novo.get("ok") and novo.get("total"):
        return novo
    antigo = fetch_conversation_analytics(competencia, granularity)
    if antigo.get("ok") and antigo.get("total"):
        antigo["nota"] = ("Veio do conversation_analytics (modelo por conversa). "
                          "O pricing_analytics não devolveu custo neste mês.")
        return antigo
    # Nenhum dos dois trouxe valor — devolve o do modelo atual, com o erro dele.
    return novo


def testar_credencial() -> dict:
    """Valida o token lendo os dados básicos da WABA. Só GET, resposta pequena.

    Serve para separar "token errado / sem permissão" de "não há custo no mês",
    que na tela pareceriam o mesmo problema.
    """
    cfg = meta_config()
    if not cfg["waba_id"] or not cfg["token"]:
        return {"ok": False, "error": _SEM_CREDENCIAL}
    try:
        r = requests.get(
            f"{GRAPH_BASE}/{cfg['waba_id']}",
            params={"fields": "id,name,currency,timezone_id,account_review_status",
                    "access_token": cfg["token"]},
            timeout=_HTTP_TIMEOUT,
        )
        corpo = r.json() if r.content else {}
    except requests.RequestException as e:
        return {"ok": False, "error": f"Falha de rede: {e}"}
    except ValueError:
        return {"ok": False, "error": "A Graph API respondeu algo que não é JSON."}

    if r.status_code != 200:
        err = corpo.get("error") or {}
        msg = err.get("message") or r.text[:300]
        dica = ""
        if err.get("code") == 190:
            dica = " → token inválido ou expirado; gere outro no System User."
        elif err.get("code") in (10, 200, 803):
            dica = (" → o token existe mas não tem acesso a esta WABA. Confira se "
                    "a permissão whatsapp_business_management está marcada E se a "
                    "conta do WhatsApp foi atribuída ao System User em Ativos.")
        return {"ok": False, "error": f"Graph API {r.status_code}: {msg}{dica}"}

    return {"ok": True, "waba": {"id": corpo.get("id"), "nome": corpo.get("name"),
                                 "moeda": corpo.get("currency"),
                                 "status": corpo.get("account_review_status")}}


def descobrir_wabas() -> dict:
    """Lista as WABAs do Business, para ajudar a preencher META_WABA_ID.

    Usa /{business-id}/owned_whatsapp_business_accounts. Só GET, tolerante a erro.
    """
    cfg = meta_config()
    if not cfg["token"] or not cfg["business_id"]:
        return {"ok": False, "error": "META_ACCESS_TOKEN e META_BUSINESS_ID são necessários.",
                "items": []}
    try:
        r = requests.get(
            f"{GRAPH_BASE}/{cfg['business_id']}/owned_whatsapp_business_accounts",
            params={"access_token": cfg["token"], "limit": 50},
            timeout=_HTTP_TIMEOUT,
        )
        corpo = r.json() if r.content else {}
        if r.status_code != 200:
            return {"ok": False, "items": [],
                    "error": (corpo.get("error") or {}).get("message") or r.text[:300]}
        return {"ok": True, "items": corpo.get("data", []), "error": None}
    except requests.RequestException as e:
        return {"ok": False, "items": [], "error": str(e)}


if __name__ == "__main__":  # diagnóstico rápido: python custos_ti_meta.py < extrato.txt
    import json
    import sys

    print(json.dumps(parse_payment_activity(sys.stdin.read()),
                     ensure_ascii=False, indent=2, default=str))
