"""
campinho_crm.py — criação de CRM em Campinho via API camila3.

Local-first: app.py grava o CRM no Postgres f3 (crm_local) e responde na hora;
este módulo sobe pra Campinho em background, com retry até conseguir.

Fluxo de upload (mesmo caminho do sistema CRM, crm_routes.api_criar_crm):
  1. GET  {CAMILA3_API_URL}/crm/cliente/{matricula}{letra}  → id_cliente do
     titular + dependentes (letra = posto do CLIENTE, não o da agenda)
  2. POST {CAMILA3_API_URL}/crm  → sp_CRM_Insert em Campinho; retorna
     id_cliente_historico + protocolo

O idUsuario do sp_CRM_Insert é resolvido em sis_usuario do banco C (Campinho)
no momento da CRIAÇÃO (app.py), pelo login_campinho do operador — mesmo gate
de vínculo do confirmar presença.
"""
import logging
import os
import threading
import time
import unicodedata

import requests

log = logging.getLogger(__name__)

# letra do posto → idEndereco em Campinho (Cad_Endereco; igual ao projeto crm)
POSTO_ID_ENDERECO = {
    "A": 3, "B": 7, "C": 2, "D": 20, "G": 5, "I": 6, "J": 4,
    "M": 25, "N": 12, "P": 26, "R": 1, "X": 21, "Y": 51,
}

_lookup_cache = {"data": None, "ts": 0.0}
_LOOKUP_TTL = 600  # 10 min

# CRM "externo" (criado direto no sistema CRM ou no F3/ERP) que acende o bit
# da agenda: motivo ORIENTAÇÃO AO CLIENTE + tipo FINANCEIRO (Campinho).
# O ETL marca agenda_dia.crm_externo a cada ciclo com esses filtros.
def crm_externo_ids() -> tuple[int, int]:
    """(id_motivo, id_tipo) — configurável por env, defaults validados 2026-06-12."""
    return (int(os.getenv("CRM_EXTERNO_ID_MOTIVO", "7")),     # ORIENTAÇÃO AO CLIENTE
            int(os.getenv("CRM_EXTERNO_ID_TIPO", "85")))      # FINANCEIRO


def _base_url() -> str:
    return os.getenv("CAMILA3_API_URL", "https://camila3.ia.camim.com.br").rstrip("/")


def _headers() -> dict:
    return {"x-api-key": os.getenv("CAMILA3_API_KEY", "")}


def get_lookup() -> dict:
    """Motivos e tipos de CRM (Cad_ClienteHistoricoMotivo/Tipo, via camila3).
    Cache em memória; se a API estiver fora, devolve o último cache."""
    now = time.time()
    if _lookup_cache["data"] and now - _lookup_cache["ts"] < _LOOKUP_TTL:
        return _lookup_cache["data"]
    try:
        base, headers = _base_url(), _headers()
        r_mot = requests.get(f"{base}/crm/cad_cliente_historico_motivo",
                             headers=headers, timeout=15)
        r_mot.raise_for_status()
        r_tip = requests.get(f"{base}/crm/cad_cliente_historico_tipo",
                             headers=headers, timeout=15)
        r_tip.raise_for_status()
        data = {
            "motivos": [{"id": m["id_cliente_historico_motivo"], "motivo": m["crmmotivo"]}
                        for m in r_mot.json()],
            "tipos": [{"id": t["id_cliente_historico_tipo"], "tipo": t["crmtipo"]}
                      for t in r_tip.json()],
        }
        _lookup_cache.update(data=data, ts=now)
        return data
    except Exception as e:
        log.error("Falha lookup camila3: %s", e)
        if _lookup_cache["data"]:
            return _lookup_cache["data"]
        raise


def get_crm_externo(matricula, letra: str, data_iso: str) -> dict | None:
    """Detalhe do CRM externo (criado no CRM/F3) pro modal da agenda: busca os
    CRMs da matrícula via camila3 e devolve o mais recente do dia `data_iso`
    com motivo/tipo de orientação financeira. None se não houver."""
    id_motivo, id_tipo = crm_externo_ids()
    base, headers = _base_url(), _headers()
    r = requests.get(f"{base}/crm/existentes/{matricula}{letra}",
                     headers=headers, timeout=20)
    r.raise_for_status()
    for c in r.json().get("crms", []):   # já vem ordenado por datahora DESC
        if c.get("id_motivo") != id_motivo or c.get("id_tipo") != id_tipo:
            continue
        if not str(c.get("data_hora") or "").startswith(data_iso):
            continue
        return {
            "paciente":             c.get("paciente"),
            "titular":              c.get("titular"),
            "matricula":            c.get("matricula"),
            "posto_cliente":        letra,
            "historico":            c.get("historico"),
            "pessoa":               c.get("pessoa"),
            "telefone":             c.get("telefone_whatsapp_cliente"),
            "protocolo":            c.get("protocolo"),
            "data_hora":            c.get("data_hora"),
            "id_cliente_historico": c.get("id_cliente_historico"),
        }
    return None


def _norm_nome(s: str) -> str:
    s = unicodedata.normalize("NFD", s or "")
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return " ".join(s.upper().split())


def _upload(crm: dict) -> tuple[bool, dict | str]:
    """Sobe um CRM (dict do crm_local) pra Campinho.
    ok=True  → info {"id_cliente_historico", "protocolo"}
    ok=False → mensagem de erro (str)."""
    id_usuario = crm.get("id_usuario_campinho")
    if not id_usuario:
        return False, "CRM sem id_usuario_campinho (operador sem vínculo na criação?)."

    letra = (crm.get("posto_cliente") or crm.get("posto") or "").strip().upper()[:1]
    id_endereco = POSTO_ID_ENDERECO.get(letra)
    if not id_endereco:
        return False, f"Posto do cliente '{letra}' sem idEndereco mapeado."

    matricula = crm.get("matricula")
    if matricula is None:
        return False, "CRM sem matrícula."

    base, headers = _base_url(), _headers()

    # 1) resolve titular/dependentes no posto do cliente
    try:
        r = requests.get(f"{base}/crm/cliente/{matricula}{letra}",
                         headers=headers, timeout=20)
        r.raise_for_status()
        cliente = r.json()
    except Exception as e:
        return False, f"Falha ao buscar cliente na camila3: {e}"

    titular = cliente.get("titular") or {}
    id_cliente = titular.get("id_cliente") or titular.get("idcliente")
    if not id_cliente:
        return False, "Não foi possível resolver id_cliente da matrícula no posto."
    nome_titular = titular.get("nome") or ""

    # Paciente da agenda pode ser dependente — casa por nome normalizado.
    id_dependente = None
    paciente = crm.get("paciente") or nome_titular
    alvo = _norm_nome(paciente)
    if alvo and alvo != _norm_nome(nome_titular):
        for dep in (cliente.get("dependentes") or []):
            if _norm_nome(dep.get("nome") or "") == alvo:
                id_dependente = dep.get("id_dependente") or dep.get("iddependente")
                break

    # 2) grava em Campinho (sp_CRM_Insert)
    payload = {
        "id_usuario":                      int(id_usuario),
        "id_cliente":                      id_cliente,
        "id_dependente":                   id_dependente,
        "matricula":                       str(matricula),
        "titular":                         nome_titular,
        "paciente":                        paciente,
        "id_endereco_cliente":             id_endereco,
        "id_endereco_reclamacao_origem":   id_endereco,
        "id_endereco_reclamacao_resposta": id_endereco,
        "id_tipo":                         int(crm["id_tipo"]),
        "id_motivo":                       int(crm["id_motivo"]),
        "pessoa":                          crm.get("pessoa") or None,
        "telefone_whatsapp_cliente":       crm.get("telefone") or None,
        "historico":                       crm["historico"],
        "relato_cliente":                  None,
    }
    try:
        r = requests.post(f"{base}/crm", headers=headers, json=payload, timeout=30)
        r.raise_for_status()
        d = r.json()
    except Exception as e:
        body = getattr(getattr(e, "response", None), "text", "") or ""
        return False, f"Falha POST /crm camila3: {e} {body[:300]}"

    if isinstance(d, list):
        d = d[0] if d else {}
    return True, {
        "id_cliente_historico": d.get("id_cliente_historico"),
        "protocolo":            d.get("protocolo"),
    }


def sync_crm(crm_id: int):
    """Tenta subir um CRM e grava o resultado no crm_local."""
    import f3_db
    crm = f3_db.crm_get_by_id(crm_id)
    if not crm or crm["sync_status"] == "enviado":
        return
    if not f3_db.crm_claim(crm_id):
        return  # outro worker está subindo este CRM agora
    ok, info = _upload(crm)
    if ok:
        f3_db.crm_mark_sync(crm_id, True,
                            id_cliente_historico=info.get("id_cliente_historico"),
                            protocolo=info.get("protocolo"))
        log.info("CRM %s enviado pra Campinho (protocolo %s).", crm_id, info.get("protocolo"))
    else:
        f3_db.crm_mark_sync(crm_id, False, erro=str(info))
        log.warning("CRM %s falhou upload: %s", crm_id, info)


def sync_async(crm_id: int):
    """Dispara o upload em background logo após o save local."""
    threading.Thread(target=sync_crm, args=(crm_id,), daemon=True).start()


_worker_started = False
_worker_lock = threading.Lock()


def start_retry_worker(interval_seconds: int = 120):
    """Re-tenta CRMs pendentes/com erro até subirem. Idempotente."""
    global _worker_started
    with _worker_lock:
        if _worker_started:
            return
        _worker_started = True

    def _loop():
        while True:
            time.sleep(interval_seconds)
            try:
                import f3_db
                for crm_id in f3_db.crm_pendentes_ids():
                    sync_crm(crm_id)
            except Exception as e:
                log.error("Retry worker CRM: %s", e)

    threading.Thread(target=_loop, daemon=True).start()
