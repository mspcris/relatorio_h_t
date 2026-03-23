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
from wpp_cobranca_sql import get_conn_posto, VIEW_NAME, build_where

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
    """Expande as variáveis {{key}} do template com os valores reais."""
    body = _TEMPLATE_BODIES.get(template_name, "")
    if not body:
        # fallback: monta texto simples
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
    sql = f"""
        SELECT
            idreceita,
            matricula,
            nomecadastro     AS nome,
            codigoendereco   AS posto,
            telefonewhatsapp,
            descricao        AS ref,
            valordevido      AS valor,
            datadevencimento AS venc,
            diasdebito
        FROM {VIEW_NAME}
        WHERE {where}
          AND situacao <> 'Pré-Cadastro'
          AND descricao LIKE '%/20[0-9][0-9]'
        ORDER BY datadevencimento ASC
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

def enviar(telefone: str, nome: str, template: str, params: dict,
           dry_run: bool) -> tuple[str, str | None]:
    if dry_run:
        return "dry_run", None

    hash_id = uuid.uuid4().hex[:24]
    texto   = _expandir_template(template, params)
    ts      = datetime.now().astimezone().isoformat(timespec="seconds")

    payload = {
        "entry": [{
            "id": hash_id,
            "changes": [{
                "field": "messages",
                "value": {
                    "contacts": [{"wa_id": telefone, "profile": {"name": nome}}],
                    "messages": [{
                        "id":       hash_id,
                        "from":     CHAT_FROM,
                        "queue_id": CHAT_QUEUE_ID,
                        "text":     {"body": texto},
                        "type":     "text",
                        "timestamp": ts,
                    }],
                    "metadata": {"phone_number_id": "", "display_phone_number": ""},
                    "messaging_product": "whatsapp",
                },
            }],
        }],
        "object": "whatsapp_business_account",
    }

    try:
        r = requests.post(
            f"{CHAT_API_URL}/webhooks/whatsapp",
            json=payload,
            timeout=15,
        )
        r.raise_for_status()
        return "accepted", hash_id
    except requests.HTTPError as e:
        return f"erro:HTTP {e.response.status_code}", None
    except Exception as e:
        return f"erro:{str(e)[:100]}", None


def montar_params_template(template_name: str, fatura: dict) -> dict:
    """
    Extrai as variáveis {{key}} do corpo do template e mapeia com os campos da fatura.
    Funciona dinamicamente para qualquer template — não precisa de hardcode por nome.
    """
    CAMPO_MAP = {
        "ref":       str(fatura.get("ref") or ""),
        "valor":     fatura.get("_valor_fmt", ""),
        "venc":      fatura.get("_venc_fmt", ""),
        "matricula": str(fatura.get("matricula") or ""),
        "idreceita": str(fatura.get("idreceita") or ""),
        "nome":      str(fatura.get("nome") or ""),
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
                   rodada_em: str) -> int:
    """Retorna quantidade de mensagens enviadas (ou simuladas)."""

    janela = JanelaEnvio(
        campanha.get("hora_inicio", "08:00"),
        campanha.get("hora_fim", "20:00"),
        campanha.get("dias_semana", "0,1,2,3,4"),
    )

    if not dry_run and not janela.ok():
        log.warning(f"  [{campanha['nome']}] Fora da janela: {janela.motivo()}")
        return 0

    intervalo = int(campanha.get("intervalo_dias") or 7)
    template  = campanha.get("template", "notificacao_de_fatura")
    postos    = campanha.get("postos") or []

    if not postos:
        log.warning(f"  [{campanha['nome']}] Nenhum posto configurado.")
        return 0

    enviados_campanha   = 0
    telefones_run: set[str] = set()
    hoje = date.today()

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

            raw_tel  = fatura.get("telefonewhatsapp")
            telefone = limpar_telefone(raw_tel)

            if not telefone:
                if not dry_run:
                    db.registrar_nao_enviado(campanha["id"], posto, fatura,
                                             rodada_em, None, "sem_telefone_valido")
                continue

            if telefone in telefones_run:
                continue

            ultimo = db.ultimo_envio_aceito(telefone)
            if ultimo:
                dias_desde = (hoje - datetime.fromisoformat(ultimo).date()).days
                if dias_desde < intervalo:
                    telefones_run.add(telefone)
                    continue

            params = montar_params_template(template, fatura)
            nome_cliente = str(fatura.get("nome") or "")
            status, wamid = enviar(telefone, nome_cliente, template, params, dry_run)
            telefones_run.add(telefone)

            nivel = logging.INFO if "erro" not in status else logging.WARNING
            log.log(nivel,
                f"    {telefone} | {(fatura.get('nome') or '')[:25]} | "
                f"{fatura.get('diasdebito',0)}d | {fatura.get('ref','')} | "
                f"R${fatura['_valor_fmt']} | {fatura['_venc_fmt']} | → {status}"
            )

            if not dry_run:
                if "erro" in status:
                    db.registrar_nao_enviado(campanha["id"], posto, fatura,
                                             rodada_em, telefone,
                                             f"erro_api:{status}")
                else:
                    db.registrar_envio(campanha["id"], posto, fatura,
                                       telefone, template, status, wamid)
                time.sleep(0.3)

            enviados_campanha += 1

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

    if not campanhas:
        log.info("Nenhuma campanha ativa encontrada.")
        return

    log.info(f"Rodada {rodada_em} | campanhas={len(campanhas)} | dry_run={args.dry_run}")

    total_enviado = 0
    for campanha in campanhas:
        log.info(f"Campanha [{campanha['id']}] {campanha['nome']} | "
                 f"postos={campanha.get('postos')} | "
                 f"atraso={campanha.get('dias_atraso_min')}–{campanha.get('dias_atraso_max') or '∞'}d | "
                 f"intervalo={campanha.get('intervalo_dias')}d")

        limit_restante = max(0, args.limit - total_enviado) if args.limit else 0
        enviados = rodar_campanha(campanha, args.dry_run, limit_restante, rodada_em)
        total_enviado += enviados
        log.info(f"  [{campanha['nome']}] enviados nesta campanha: {enviados}")

        if args.limit and total_enviado >= args.limit:
            log.info("Limite global atingido.")
            break

    log.info(f"Total enviado na rodada: {total_enviado}")


if __name__ == "__main__":
    main()
