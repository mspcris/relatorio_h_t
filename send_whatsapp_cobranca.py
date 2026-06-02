"""
send_whatsapp_cobranca.py
Engine de envio de cobrança WhatsApp.

Lê campanhas ativas do SQLite, conecta posto a posto no SQL Server,
aplica os filtros da campanha e envia as mensagens.

Uso:
    python send_whatsapp_cobranca.py [--dry-run] [--campanha ID] [--limit N]

Flags:
    --dry-run        Simula sem enviar de fato nem gravar no banco
    --campanha ID    Roda apenas a campanha com este ID (default: todas ativas)
    --limit N        Máximo de mensagens enviadas no total (0 = sem limite)
"""

import os
import re
import sys
import time
import uuid
import logging
import argparse
from datetime import datetime, date, timezone
from urllib.parse import quote_plus

import requests
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

import wpp_cobranca_db as db
from wpp_cobranca_sql import (
    get_conn_posto,
    build_where,
    source_sql,
    where_extras,
    modo_envio as campanha_modo_envio,
    MODO_CLIENTES,
    MODO_CLIENTE_NOVO,
    MODO_FALTA_MEDICO,
    get_query_cliente_novo,
)

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------
BASE_DIR      = os.path.dirname(os.path.abspath(__file__))
WPP_API_URL   = os.getenv("WAPP_API_URL",    "https://whatsapp-api.camim.com.br")
WPP_TOKEN     = os.getenv("WAPP_TOKEN",      "")
CHAT_API_URL  = os.getenv("CHAT_API_URL",   "")
CHAT_FROM     = os.getenv("WAPP_CHAT_FROM", "")
CHAT_QUEUE_ID = os.getenv("WAPP_QUEUE_ID",  "")
POSTOS_ALL    = list("ANXYBRPCDGIMJ")

# Cache dos textos dos templates: {nome: body_text}
_TEMPLATE_BODIES: dict = {}


def _load_template_bodies() -> None:
    """Carrega o texto completo de cada template aprovado da API."""
    global _TEMPLATE_BODIES
    try:
        r = requests.get(
            f"{WPP_API_URL}/templates",
            headers={"Authorization": f"Bearer {WPP_TOKEN}"},
            timeout=10,
        )
        r.raise_for_status()
        for t in r.json().get("items", []):
            for comp in t.get("components", []):
                if comp.get("type") == "BODY":
                    _TEMPLATE_BODIES[t["name"]] = comp.get("text", "")
                    break
        log_tmp = logging.getLogger(__name__)
        log_tmp.info("Templates carregados: %s", list(_TEMPLATE_BODIES.keys()))
    except Exception as e:
        logging.getLogger(__name__).warning("Não foi possível carregar templates: %s", e)


def _expandir_template(template_name: str, params: dict) -> str:
    """Expande as variáveis {{key}} do template com os valores reais.

    Se o template ainda não está no cache (módulo importado pelo Flask sem
    rodar main(), ou template novo aprovado depois do startup), tenta
    carregar lazily uma vez antes de cair no fallback "k: v".
    """
    body = _TEMPLATE_BODIES.get(template_name, "")
    if not body:
        # cache miss → tenta recarregar (template pode ter sido aprovado depois)
        _load_template_bodies()
        body = _TEMPLATE_BODIES.get(template_name, "")
    if not body:
        # ainda sem template → fallback de último recurso (debug-friendly)
        return "  ".join(f"{k}: {v}" for k, v in params.items() if v)
    for key, val in params.items():
        body = body.replace(f"{{{{{key}}}}}", str(val))
    return body

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(BASE_DIR, "whatsapp_cobranca.log"),
                            encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Query com filtros da campanha
# ---------------------------------------------------------------------------

def buscar_faturas(cursor, campanha: dict) -> list[dict]:
    where, params = build_where(campanha)
    src = source_sql(campanha)
    extra = where_extras(campanha)
    if campanha_modo_envio(campanha) == MODO_CLIENTE_NOVO:
        sql, qparams = get_query_cliente_novo()
        cursor.execute(sql, qparams)
        cols = [c[0] for c in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]
    if campanha_modo_envio(campanha) == MODO_CLIENTES:
        sql = f"""
            SELECT
                idcliente        AS idreceita,
                matricula,
                nomecadastro     AS nome,
                codigoendereco   AS posto,
                telefone_efetivo AS telefonewhatsapp,
                CONVERT(VARCHAR, dataadmissao, 103) AS ref,
                NULL             AS valor,
                NULL             AS venc,
                0                AS diasdebito,
                tipo_cliente,
                situacao_efetiva,
                planotipo
            FROM {src}
            WHERE {where}{extra}
            ORDER BY dataadmissao ASC
        """
    else:
        sql = f"""
            SELECT
                idreceita,
                matricula,
                nomecadastro     AS nome,
                codigoendereco   AS posto,
                telefonewhatsapp,
                descricao        AS ref,
                valordevido      AS valor,
                datareferencia   AS venc,
                diasdebito
            FROM {src}
            WHERE {where}{extra}
            ORDER BY datareferencia ASC
        """
    cursor.execute(sql, params)
    cols = [c[0] for c in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]

# ---------------------------------------------------------------------------
# Limpeza de telefone
# ---------------------------------------------------------------------------

_INVALIDOS = ("NÃO TEM", "NAO TEM", "SEM CELULAR", "CLIQUE AQUI", "S/N", "0000-0000")


def limpar_telefone(raw: str | None) -> str | None:
    if not raw:
        return None
    raw = raw.strip()
    if not raw:
        return None
    raw_upper = raw.upper()
    for inv in _INVALIDOS:
        if inv in raw_upper:
            return None
    primeiro = re.split(r"[/,]", raw)[0].strip()
    digits = re.sub(r"\D", "", primeiro)
    if len(digits) < 8:
        return None
    if digits.startswith("55"):
        return digits if len(digits) in (12, 13) else None
    return ("55" + digits) if len(digits) in (10, 11) else None

# ---------------------------------------------------------------------------
# Janela de envio
# ---------------------------------------------------------------------------

class JanelaEnvio:
    def __init__(self, hora_inicio: str, hora_fim: str, dias_semana: str):
        self.h_ini = self._p(hora_inicio)
        self.h_fim = self._p(hora_fim)
        self.dias  = {int(d.strip()) for d in dias_semana.split(",") if d.strip().isdigit()}

    @staticmethod
    def _p(s: str):
        h, m = s.strip().split(":")
        return (int(h), int(m))

    def ok(self) -> bool:
        n = datetime.now()
        return n.weekday() in self.dias and self.h_ini <= (n.hour, n.minute) < self.h_fim

    def motivo(self) -> str:
        n = datetime.now()
        nomes = ["seg","ter","qua","qui","sex","sáb","dom"]
        if n.weekday() not in self.dias:
            dias_ok = ",".join(nomes[d] for d in sorted(self.dias))
            return f"dia_nao_permitido ({nomes[n.weekday()]}, permitidos: {dias_ok})"
        return f"fora_do_horario ({n.strftime('%H:%M')}, janela: {self.h_ini[0]:02d}:{self.h_ini[1]:02d}–{self.h_fim[0]:02d}:{self.h_fim[1]:02d})"

# ---------------------------------------------------------------------------
# Formatação
# ---------------------------------------------------------------------------

def fmt_valor(v) -> str:
    if v is None:
        return "0,00"
    if isinstance(v, str):
        return v.strip()
    return f"{float(v):,.2f}".replace(".", "X").replace(",", ".").replace("X", ",")


def fmt_venc(v) -> str:
    if v is None:
        return ""
    if isinstance(v, (datetime, date)):
        return v.strftime("%d/%m/%Y")
    try:
        return datetime.fromisoformat(str(v)).strftime("%d/%m/%Y")
    except Exception:
        return str(v)

# ---------------------------------------------------------------------------
# Envio via API
# ---------------------------------------------------------------------------

def montar_payload_chat(telefone: str, nome: str, texto: str,
                         queue_id: str | None = None,
                         from_user_id: str | None = None,
                         phone_number_id: str | None = None,
                         display_phone_number: str | None = None,
                         must_close_ticket: bool = False) -> tuple[dict, str]:
    """Monta o envelope do webhook pra api-chat sem disparar request.
    Útil pra acumular em lote (POST /webhooks/batch) ou disparar 1-a-1.
    Retorna (payload_dict, hash_id_gerado)."""
    hash_id  = uuid.uuid4().hex[:24]
    ts       = datetime.now().astimezone().isoformat(timespec="seconds")
    fila_id  = queue_id or CHAT_QUEUE_ID
    remetente = from_user_id or CHAT_FROM
    if remetente and not remetente.startswith("chat:"):
        remetente = "chat:" + remetente

    msg = {
        "id":       hash_id,
        "from":     remetente,
        "queue_id": fila_id,
        "text":     {"body": texto},
        "type":     "text",
        "timestamp": ts,
    }
    # Campo opcional acordado com o dev sênior (Robson, 26/05/2026):
    # quando True, o chat fecha o ticket automaticamente após registrar.
    # Não enviar a chave (em vez de False) preserva o comportamento default.
    if must_close_ticket:
        msg["must_close_ticket"] = True

    payload = {
        "entry": [{
            "id": hash_id,
            "changes": [{
                "field": "messages",
                "value": {
                    "contacts": [{"wa_id": telefone, "profile": {"name": nome}}],
                    "messages": [msg],
                    "metadata": {
                        "phone_number_id": phone_number_id or "",
                        "display_phone_number": display_phone_number or "",
                    },
                    "messaging_product": "whatsapp",
                },
            }],
        }],
        "object": "whatsapp_business_account",
    }
    return payload, hash_id


def enviar_chat_batch(payloads: list[dict]) -> tuple[str, dict | str]:
    """POST /webhooks/batch com até 100 items. Endpoint introduzido pelo dev
    sênior pra reduzir N POSTs sequenciais (e pressão na api-chat) quando
    o cron processa centenas de envios por rodada.

    Quando 1 ≤ len(payloads) ≤ 100, faz 1 request. Pra mais, o caller deve
    fatiar (limite 200 hard, 100 confortável). Retorna (status, body).

    `status` ∈ {'accepted_batch', 'erro_batch:...', 'skipped'}."""
    if not payloads:
        return "skipped", {}
    try:
        r = requests.post(
            f"{CHAT_API_URL}/webhooks/batch",
            json={"items": payloads},
            timeout=30,
        )
        r.raise_for_status()
        try:
            return "accepted_batch", r.json()
        except Exception:
            return "accepted_batch", (r.text or "")[:500]
    except requests.HTTPError as e:
        body = ""
        try: body = (e.response.text or "")[:200]
        except Exception: pass
        return f"erro_batch:HTTP {e.response.status_code} {body}", {}
    except Exception as e:
        return f"erro_batch:{type(e).__name__}: {str(e)[:200]}", {}


def enviar_via_chat(telefone: str, nome: str, template: str, params: dict,
                    queue_id: str | None = None,
                    from_user_id: str | None = None,
                    phone_number_id: str | None = None,
                    display_phone_number: str | None = None,
                    must_close_ticket: bool = False) -> tuple[str, str | None]:
    """Envia webhook 1-a-1 pra api-chat (cria ticket + conversa). Mantido pra
    compatibilidade com callers que processam 1 envio por vez (ex: tela
    /wpp/teste). Pra envios em lote, prefira montar_payload_chat + enviar_chat_batch."""
    texto = _expandir_template(template, params)
    payload, hash_id = montar_payload_chat(
        telefone, nome, texto,
        queue_id=queue_id, from_user_id=from_user_id,
        phone_number_id=phone_number_id,
        display_phone_number=display_phone_number,
        must_close_ticket=must_close_ticket,
    )
    try:
        r = requests.post(
            f"{CHAT_API_URL}/webhooks/whatsapp",
            json=payload, timeout=15,
        )
        r.raise_for_status()
        return "accepted_chat", hash_id
    except requests.HTTPError as e:
        return f"erro_chat:HTTP {e.response.status_code}", None
    except Exception as e:
        return f"erro_chat:{str(e)[:100]}", None


def enviar_via_meta(telefone: str, template: str, params: dict,
                     header_image_url: str | None = None,
                     from_phone: str | None = None) -> tuple[str, str | None]:
    """Envia template diretamente pela API Meta/WhatsApp Business.

    Quando o template tem HEADER do tipo IMAGE, a wrapper API exige o campo
    `data.HEADER.imageUrl` (mesmo que a imagem seja "fixa" no template
    aprovado — a Meta exige ser passada por mensagem). Se header_image_url
    não vier, ainda assim mandamos `HEADER: {}` (a wrapper retorna 400/500
    indicando isso pra ficar visível no log e na lista de não-enviados).

    `from_phone` (opcional, só usado quando o remetente não é o default da
    conta Meta — ex.: 552135296666 para o Couto/3529).
    """
    data_field = {"BODY": params}
    if header_image_url:
        data_field["HEADER"] = {"imageUrl": header_image_url}

    payload = {
        "template": template,
        "people": [{
            "phone": telefone,
            "data":  data_field,
        }],
    }
    if from_phone:
        payload["from"] = from_phone
    try:
        r = requests.post(
            f"{WPP_API_URL}/templates/send",
            headers={"Authorization": f"Bearer {WPP_TOKEN}"},
            json=payload,
            timeout=15,
        )
        r.raise_for_status()
        data = r.json()
        wamid = data.get("id") or data.get("wamid")
        return "accepted_meta", wamid
    except requests.HTTPError as e:
        body = ""
        try:
            body = e.response.text[:150]
        except Exception:
            pass
        return f"erro_meta:HTTP {e.response.status_code} {body}", None
    except Exception as e:
        return f"erro_meta:{str(e)[:100]}", None


def enviar(telefone: str, nome: str, template: str, params: dict,
           dry_run: bool, queue_id: str | None = None,
           from_user_id: str | None = None,
           usar_chat: bool = True,
           usar_meta: bool = False,
           header_image_url: str | None = None,
           from_phone: str | None = None,
           phone_number_id: str | None = None) -> tuple[str, str | None]:
    if dry_run:
        return "dry_run", None

    status_final = None
    wamid_final = None

    if usar_chat:
        # display_phone_number cai pro default 2455 quando from_phone é None
        # (campanha de Altamiro omite `from` no payload Meta porque é o número
        # default da conta — mas o chat ainda precisa do display).
        display_chat = from_phone or "552124559600"
        s, w = enviar_via_chat(telefone, nome, template, params,
                               queue_id=queue_id, from_user_id=from_user_id,
                               phone_number_id=phone_number_id,
                               display_phone_number=display_chat)
        status_final = s
        wamid_final = w
        if "erro" in s:
            log.warning("  enviar_via_chat falhou: %s", s)

    if usar_meta:
        s, w = enviar_via_meta(telefone, template, params,
                               header_image_url=header_image_url,
                               from_phone=from_phone)
        if wamid_final is None:
            wamid_final = w
        # Meta tem prioridade no status final
        if "erro" not in s:
            status_final = s if not status_final or "erro" in status_final else "accepted"
        else:
            log.warning("  enviar_via_meta falhou: %s", s)
            if status_final is None:
                status_final = s

    return status_final or "erro:nenhum_canal", wamid_final


def montar_params_template(template_name: str, fatura: dict) -> dict:
    """
    Extrai as variáveis {{key}} do corpo do template e mapeia com os campos da fatura.
    Funciona dinamicamente para qualquer template — não precisa de hardcode por nome.
    """
    # nome: só primeiro nome (templates novos como mensalidade_vencida usam
    # forma de tratamento informal — "Olá João" soa melhor que "Olá João da Silva")
    nome_completo = str(fatura.get("nome") or "").strip()
    primeiro_nome = nome_completo.split()[0] if nome_completo else ""

    # matricula com letra do posto sufixada (ex: 123456A = matricula 123456 do
    # idendereco A=Anchieta). Padrão da CAMIM pra identificar unicamente a
    # matrícula entre os 13 postos. Só sufixa quando posto é uma letra (single
    # char alfa) — defesa contra valores vazios/numéricos que apareçam.
    matricula_raw = str(fatura.get("matricula") or "").strip()
    posto = str(fatura.get("posto") or "").strip().upper()
    matricula_com_letra = (
        f"{matricula_raw}{posto}"
        if matricula_raw and len(posto) == 1 and posto.isalpha()
        else matricula_raw
    )

    CAMPO_MAP = {
        "ref":          str(fatura.get("ref") or ""),
        "valor":        fatura.get("_valor_fmt", ""),
        "venc":         fatura.get("_venc_fmt", ""),
        "matricula":    matricula_com_letra,
        "idreceita":    str(fatura.get("idreceita") or ""),
        "nome":         primeiro_nome,
        # Campos do modo clientes_admissao
        "admissao":     str(fatura.get("ref") or ""),   # ref = dataadmissao formatada
        "tipo_cliente": str(fatura.get("tipo_cliente") or ""),
        "situacao":     str(fatura.get("situacao_efetiva") or ""),
        "planotipo":    str(fatura.get("planotipo") or ""),
        # Template seja_bem_vindo (modo cliente_novo)
        "plano":        str(fatura.get("plano") or ""),
        "cobrador":     str(fatura.get("cobrador") or ""),
        "referencia":   str(fatura.get("referencia") or ""),
        "linkapp":      "https://app.camim.com.br/",
    }
    body = _TEMPLATE_BODIES.get(template_name, "")
    if body:
        keys = re.findall(r'\{\{(\w+)\}\}', body)
        return {k: CAMPO_MAP.get(k, "") for k in keys}
    # fallback caso os bodies ainda não tenham sido carregados
    return {k: v for k, v in CAMPO_MAP.items() if k in ("ref", "valor", "venc")}

# ---------------------------------------------------------------------------
# Execução de uma campanha
# ---------------------------------------------------------------------------

def rodar_campanha(campanha: dict, dry_run: bool, limit_restante: int,
                   rodada_em: str, telefones_rodada: set[str]) -> int:
    """Retorna quantidade de mensagens enviadas (ou simuladas)."""

    janela = JanelaEnvio(
        campanha.get("hora_inicio", "08:00"),
        campanha.get("hora_fim", "20:00"),
        campanha.get("dias_semana", "0,1,2,3,4"),
    )

    if not dry_run and not janela.ok():
        log.warning(f"  [{campanha['nome']}] Fora da janela: {janela.motivo()}")
        return 0

    # Regra por campanha com histórico global:
    # cada campanha respeita seu próprio intervalo_dias, considerando o último
    # envio accepted do telefone em qualquer campanha.
    intervalo    = int(campanha.get("intervalo_dias") or 7)
    # Bit por campanha: quando 1, ignora o intervalo global cross-campanha e
    # usa "enviar 1x por contato NESTA campanha". Default 0 = comportamento
    # histórico (respeita o intervalo global). Ver wpp_cobranca_db.ignorar_intervalo.
    ignorar_intervalo = bool(campanha.get("ignorar_intervalo"))
    template     = campanha.get("template", "notificacao_de_fatura")
    postos       = campanha.get("postos") or []
    queue_id     = campanha.get("queue_id") or None
    from_user_id = campanha.get("from_user_id") or None
    usar_chat    = bool(campanha.get("enviar_chat", 1))
    usar_meta    = bool(campanha.get("enviar_meta", 0))
    header_url   = campanha.get("header_image_url") or None
    must_close   = bool(campanha.get("must_close_ticket", 0))
    from_phone   = db.from_phone_por_numero_saida(campanha.get("numero_saida"))
    phone_number_id = db.phone_number_id_por_numero_saida(campanha.get("numero_saida"))
    # Lote: até 200 mensagens por iteração (limite hard do endpoint batch).
    # Fluxo de 2 fases por lote — fase 1 dispara chat batch, espera um buffer,
    # fase 2 dispara Meta 1-a-1 e grava envios. O buffer dá uma janela pro
    # ticket no chat ser registrado ANTES da mensagem chegar no WhatsApp do
    # cliente (senão uma resposta imediata pode virar ticket órfão).
    # Reduzido de 5min → 30s em 02/06/2026 ("tempo do arrependimento"): 5min por
    # lote/posto serializava a rodada inteira em horas. 30s é buffer suficiente
    # e não afeta custo nem destinatários (só a janela de registro no chat).
    LOTE_SIZE = 200
    WAIT_BEFORE_META_SEC = 30  # 30s de buffer pro chat processar (era 5min)

    if not postos:
        log.warning(f"  [{campanha['nome']}] Nenhum posto configurado.")
        return 0

    enviados_campanha   = 0
    hoje = date.today()
    # Dedup por contato DENTRO desta campanha — usado só quando ignorar_intervalo=1.
    # Evita 2 msgs pro mesmo telefone que aparece em várias matrículas/postos.
    telefones_camp: set[str] = set()

    # Buffer de lote em duas fases. Cada item: (chat_payload, fatura,
    # telefone, params). Meta é disparado APENAS dentro de _processar_lote,
    # depois da espera de 5 min — garante que o ticket no chat já está
    # registrado quando a mensagem chega no WhatsApp do cliente.
    lote_buffer: list[tuple] = []

    def _processar_lote():
        """Executa as 2 fases do lote atual:
        1) api-chat batch (1 request com até LOTE_SIZE itens) — registra
           tickets no chat externo;
        2) aguarda WAIT_BEFORE_META_SEC pra o chat fazer o flush interno;
        3) Meta 1-a-1 — entrega no WhatsApp do cliente;
        4) grava status final em envios/nao_enviados.

        Lógica de status final combinada (Meta prioritário, chat fallback)
        igual ao fluxo antigo via enviar() — mantém compatibilidade com
        relatórios e dedup global por telefone."""
        if not lote_buffer:
            return
        tamanho = len(lote_buffer)

        # === FASE 1: api-chat batch ==================================
        chat_status = "skipped"
        chat_ok = True   # default True quando não usa chat (pra logic de status)
        if usar_chat and not dry_run:
            payloads = [item[0] for item in lote_buffer if item[0] is not None]
            chat_status, _b = enviar_chat_batch(payloads)
            chat_ok = chat_status.startswith("accepted")
            log.info(
                f"  [{campanha['nome']}] LOTE chat ({len(payloads)} msgs): {chat_status}"
            )

        # === FASE 2: aguarda 5 min antes do Meta =====================
        # Só faz sentido esperar se DOIS canais vão ser usados; senão segue
        # direto pra fase 3.
        if usar_chat and usar_meta and not dry_run:
            log.info(
                f"  [{campanha['nome']}] Aguardando {WAIT_BEFORE_META_SEC}s "
                f"pro chat processar antes do Meta ({tamanho} clientes)…"
            )
            time.sleep(WAIT_BEFORE_META_SEC)
            # Janela pode ter fechado durante a espera (campanha com hora_fim
            # apertada). Aborta Meta do lote — mensagens do chat já saíram,
            # registra como 'janela_fechou_entre_fases' pra rastreio.
            if not janela.ok():
                log.warning(
                    f"  [{campanha['nome']}] Janela fechou durante espera — "
                    "Meta do lote abortado. Chat já enviou."
                )
                for chat_payload, f_item, tel, _params in lote_buffer:
                    db.registrar_nao_enviado(
                        campanha["id"], posto, f_item, rodada_em, tel,
                        "janela_fechou_entre_fases",
                    )
                lote_buffer.clear()
                return

        # === FASE 3: Meta 1-a-1 + grava envios =======================
        for chat_payload, f_item, tel, params_item in lote_buffer:
            status_meta = None
            wamid_meta  = None
            if usar_meta and not dry_run:
                s_meta, w = enviar_via_meta(
                    tel, template, params_item,
                    header_image_url=header_url, from_phone=from_phone,
                )
                status_meta = s_meta
                wamid_meta  = w
                if "erro" in s_meta:
                    log.warning("  enviar_via_meta falhou: %s", s_meta)

            # Status final: Meta vence quando OK; chat segura quando Meta cai
            if status_meta is not None:
                if "erro" not in status_meta:
                    status_final = status_meta
                elif chat_ok:
                    status_final = "accepted_chat"
                else:
                    status_final = status_meta
            else:
                status_final = "accepted_chat" if chat_ok else chat_status

            if "erro" in status_final:
                db.registrar_nao_enviado(
                    campanha["id"], posto, f_item, rodada_em, tel,
                    f"erro_api:{status_final}",
                )
                log.warning(
                    f"    {tel} | {(f_item.get('nome') or '')[:25]} | → {status_final}"
                )
            else:
                db.registrar_envio(
                    campanha["id"], posto, f_item, tel, template,
                    status_final, wamid_meta,
                )
                log.info(
                    f"    {tel} | {(f_item.get('nome') or '')[:25]} | "
                    f"{f_item.get('diasdebito',0)}d | {f_item.get('ref','')} | "
                    f"R${f_item.get('_valor_fmt','')} | {f_item.get('_venc_fmt','')} | → {status_final}"
                )
        lote_buffer.clear()

    for posto in postos:
        sql_conn = get_conn_posto(posto)
        if not sql_conn:
            log.warning(f"  [{campanha['nome']}] Posto {posto}: sem conexão.")
            continue

        cursor = sql_conn.cursor()
        try:
            faturas = buscar_faturas(cursor, campanha)
        except Exception as e:
            log.error(f"  [{campanha['nome']}] Posto {posto}: erro na query: {e}")
            cursor.close()
            sql_conn.close()
            continue

        log.info(f"  [{campanha['nome']}] Posto {posto}: {len(faturas)} faturas.")

        for fatura in faturas:
            if limit_restante and enviados_campanha >= limit_restante:
                log.info(f"  Limite atingido.")
                break

            if not dry_run and not janela.ok():
                log.warning(f"  [{campanha['nome']}] Janela encerrada durante loop.")
                break

            fatura["_valor_fmt"] = fmt_valor(fatura.get("valor"))
            fatura["_venc_fmt"]  = fmt_venc(fatura.get("venc"))

            # Filtro de cadastros de teste no SQL Server da CAMIM.
            # Incidente 2026-05-26: cliente real 'MATRICULA DE TESTE DE BANGU'
            # (mat 90333 posto B) recebeu envio com {{nome}}='MATRICULA' porque
            # montar_params_template pega o primeiro nome via split()[0].
            # Filtro abaixo bloqueia qualquer nome cujas palavras incluam
            # 'TESTE' (word-boundary) ou comece com 'MATRICULA' — esses são os
            # padrões observados pra registros legados de teste na base.
            nome_real = str(fatura.get("nome") or "").strip()
            nome_upper = nome_real.upper()
            if re.search(r"\bTESTE\b", nome_upper) or nome_upper.startswith("MATRICULA "):
                log.warning(
                    f"    {fatura.get('matricula')} | {nome_real[:40]} | "
                    "→ skip:nome_de_teste"
                )
                if not dry_run:
                    db.registrar_nao_enviado(
                        campanha["id"], posto, fatura,
                        rodada_em, None, "nome_de_teste",
                    )
                continue

            raw_tel  = fatura.get("telefonewhatsapp")
            telefone = limpar_telefone(raw_tel)

            if not telefone:
                if not dry_run:
                    db.registrar_nao_enviado(campanha["id"], posto, fatura,
                                             rodada_em, None, "sem_telefone_valido")
                continue

            # Dedup por contato — duas políticas conforme o bit ignorar_intervalo:
            if ignorar_intervalo:
                # Campanha one-shot (ex.: indique e ganhe): IGNORA o intervalo
                # global (envia mesmo a quem recebeu cobrança nos últimos dias) e
                # trava em "1x por contato NESTA campanha" — telefones_camp evita
                # repetir na rodada (mesmo telefone em várias matrículas) e
                # ja_enviado_na_campanha evita reenviar em rodadas/dias seguintes.
                # NÃO toca telefones_rodada de propósito: precisa ser transparente
                # pro dedup das outras campanhas (João recebe cobrança E indique).
                if telefone in telefones_camp:
                    continue
                if db.ja_enviado_na_campanha(campanha["id"], telefone):
                    telefones_camp.add(telefone)
                    if not dry_run:
                        db.registrar_nao_enviado(
                            campanha["id"], posto, fatura, rodada_em, telefone,
                            "ja_enviado_campanha",
                        )
                    log.info(
                        f"    {telefone} | {(fatura.get('nome') or '')[:25]} | "
                        f"{fatura.get('ref','')} | → ja_enviado_campanha (envio único)"
                    )
                    continue
            else:
                # Controle global na rodada (cross-campanha).
                if telefone in telefones_rodada:
                    log.info(
                        f"    {telefone} | {(fatura.get('nome') or '')[:25]} | "
                        f"{fatura.get('diasdebito',0)}d | {fatura.get('ref','')} | "
                        "→ bloqueado_rodada_global"
                    )
                    continue

                ultimo = db.ultimo_envio_aceito(telefone)
                if ultimo:
                    dias_desde = (hoje - datetime.fromisoformat(ultimo).date()).days
                    if dias_desde < intervalo:
                        telefones_rodada.add(telefone)
                        log.info(
                            f"    {telefone} | {(fatura.get('nome') or '')[:25]} | "
                            f"{fatura.get('diasdebito',0)}d | {fatura.get('ref','')} | "
                            f"→ bloqueado_intervalo_global:{dias_desde}d<{intervalo}d"
                        )
                        continue

            params = montar_params_template(template, fatura)
            nome_cliente = str(fatura.get("nome") or "")

            if dry_run:
                log.info(
                    f"    {telefone} | {nome_cliente[:25]} | "
                    f"{fatura.get('diasdebito',0)}d | {fatura.get('ref','')} | "
                    "→ dry_run"
                )
                (telefones_camp if ignorar_intervalo else telefones_rodada).add(telefone)
                enviados_campanha += 1
                continue

            # ============================================================
            # Coleta o payload chat (sem disparar). Meta é disparado SÓ
            # dentro de _processar_lote, depois da espera de 5 min.
            # ============================================================
            chat_payload = None
            if usar_chat:
                texto = _expandir_template(template, params)
                # display_phone_number pro chat: cai pro default 2455 quando
                # from_phone é None (campanha do Altamiro omite `from`)
                display_chat = from_phone or "552124559600"
                chat_payload, _hash = montar_payload_chat(
                    telefone, nome_cliente, texto,
                    queue_id=queue_id, from_user_id=from_user_id,
                    phone_number_id=phone_number_id,
                    display_phone_number=display_chat,
                    must_close_ticket=must_close,
                )

            lote_buffer.append((chat_payload, fatura, telefone, params))
            (telefones_camp if ignorar_intervalo else telefones_rodada).add(telefone)
            enviados_campanha += 1

            if len(lote_buffer) >= LOTE_SIZE:
                _processar_lote()

        # Flush final do posto — processa o que sobrou no buffer.
        if not dry_run:
            _processar_lote()

        cursor.close()
        sql_conn.close()

    return enviados_campanha

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Engine de cobrança WhatsApp")
    parser.add_argument("--dry-run",   action="store_true")
    parser.add_argument("--campanha",  type=int, default=0,
                        help="ID da campanha (0 = todas ativas)")
    parser.add_argument("--limit",     type=int, default=0,
                        help="Máximo de mensagens no total (0 = sem limite)")
    args = parser.parse_args()

    # Valida variáveis obrigatórias
    faltando = [v for v in ("CHAT_API_URL", "WAPP_CHAT_FROM", "WAPP_QUEUE_ID", "WAPP_TOKEN")
                if not os.getenv(v)]
    if faltando:
        log.error("Variáveis de ambiente obrigatórias não definidas: %s", ", ".join(faltando))
        sys.exit(1)

    _load_template_bodies()

    rodada_em = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")

    if args.campanha:
        campanhas = [db.get_campanha(args.campanha)]
        campanhas = [c for c in campanhas if c]
    else:
        campanhas = [c for c in db.listar_campanhas() if c.get("ativa")]
        # Processa primeiro as campanhas cuja JANELA FECHA MAIS CEDO.
        #
        # Motivo: a rodada é serial e cada lote dorme WAIT_BEFORE_META_SEC
        # (5 min) entre o batch do chat e o Meta. Com as campanhas pesadas
        # (atraso / pré-vencimento, milhares de contatos em lotes de 200) na
        # frente, uma única rodada leva 4h+ e só alcança uma campanha de
        # janela curta DEPOIS que ela já fechou → 0 envios todo dia útil
        # (starvation). Visto em 2026-06-02: campanha 43 (janela 08:00–12:00)
        # só foi alcançada às 12:14 → "fora_do_horario" → 0 enviados, dia após
        # dia. Ordenar por hora_fim asc dá prioridade a quem tem o prazo mais
        # apertado; as de janela longa (até 20:00) têm folga de sobra.
        #
        # Trade-off conhecido: o dedup global por telefone na rodada
        # (telefones_rodada / intervalo de 7d) é sensível à ordem. Quando um
        # mesmo telefone é elegível em duas campanhas, a de janela mais curta
        # passa a "ganhar" o contato. Aceitável porque janela curta = envio
        # deliberadamente time-boxed pelo operador.
        #
        # sort estável: preserva a ordem anterior (numero_saida, id) entre
        # campanhas de mesmo hora_fim.
        def _hora_fim_key(c: dict) -> tuple[int, int]:
            try:
                h, m = str(c.get("hora_fim") or "20:00").split(":")
                return (int(h), int(m))
            except Exception:
                return (20, 0)
        campanhas.sort(key=_hora_fim_key)

    if not campanhas:
        log.info("Nenhuma campanha ativa encontrada.")
        return

    log.info(f"Rodada {rodada_em} | campanhas={len(campanhas)} | dry_run={args.dry_run}")

    total_enviado = 0
    telefones_rodada: set[str] = set()
    for campanha in campanhas:
        modo = campanha_modo_envio(campanha)

        # SAFETY DUPLA pra modo 'falta_medico'. Esse modo é disparado via API
        # direta pelo /medico_falta — NUNCA pelo cron de cobrança.
        #
        # Camada 1 (essa): filtro explícito no loop principal.
        # Camada 2 (wpp_cobranca_sql.modo_envio): 'falta_medico' está na
        # whitelist de modos válidos — sem isso a função silenciosamente
        # devolve MODO_ATRASO (= 'atraso') e ESSE filtro aqui nunca dispara,
        # como aconteceu em 2026-05-06 (1.204 envios errados, R$ 421,40).
        #
        # Comparamos pelo VALOR BRUTO da campanha (não pela função normalizada)
        # pra evitar ficar refém da whitelist: mesmo se um modo novo for
        # criado e não estiver na whitelist, esse filtro continua funcionando.
        modo_raw = str(campanha.get("modo_envio") or "").strip().lower()
        if modo_raw == MODO_FALTA_MEDICO or modo == MODO_FALTA_MEDICO:
            log.info(f"  [{campanha['nome']}] modo=falta_medico — disparo é via API "
                     f"do /medico_falta, cron pula essa campanha.")
            continue

        if modo == "pre_vencimento":
            regra = (
                f"ref+{campanha.get('dias_ref_min', 4)}"
                f"–{campanha.get('dias_ref_max') if campanha.get('dias_ref_max') is not None else '∞'}d"
            )
        elif modo == MODO_CLIENTE_NOVO:
            regra = "primeiro-pagamento-últimos-7d"
        elif modo == MODO_CLIENTES:
            regra = (
                f"adm={campanha.get('adm_data_ini', '?')}"
                f"–{campanha.get('adm_data_fim', '?')}"
            )
        else:
            regra = (
                f"atraso={campanha.get('dias_atraso_min')}–"
                f"{campanha.get('dias_atraso_max') or '∞'}d"
            )
        log.info(f"Campanha [{campanha['id']}] {campanha['nome']} | "
                 f"modo={modo} | "
                 f"postos={campanha.get('postos')} | "
                 f"{regra} | "
                 f"intervalo={campanha.get('intervalo_dias')}d")

        limit_restante = max(0, args.limit - total_enviado) if args.limit else 0
        enviados = rodar_campanha(
            campanha, args.dry_run, limit_restante, rodada_em, telefones_rodada
        )
        total_enviado += enviados
        log.info(f"  [{campanha['nome']}] enviados nesta campanha: {enviados}")

        if args.limit and total_enviado >= args.limit:
            log.info("Limite global atingido.")
            break

    log.info(f"Total enviado na rodada: {total_enviado}")


if __name__ == "__main__":
    main()
