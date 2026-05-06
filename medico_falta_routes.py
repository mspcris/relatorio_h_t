"""medico_falta_routes.py — Cadastro de Falta do Médico (Cad_MedicoFalta).

Página em /medico_falta. Permite:
  - Listar faltas já cadastradas (vw_Cad_MedicoFalta) por posto e período
  - Cadastrar nova falta:
      * total (00:00–23:59) ou parcial (HH:MM–HH:MM)
      * INSERT em Cad_MedicoFalta — a vw_Cad_LancamentoProntuarioComDesistencia
        já reflete a falta nos atendimentos via JOIN, sem UPDATE manual
      * Auditoria em Sis_Historico (idTabela=50)
  - Enviar template `notificacao_de_falta_do_medico` aos pacientes via:
      * api-chat (registra conversa no Camila.ai chat) E Meta API (envia mensagem)
      * Reusa send_whatsapp_cobranca.enviar() do projeto wpp-cobrança
      * Registra cada envio em whatsapp_cobranca.db.envios (campanha_id da
        campanha "Falta de Médico", criada idempotentemente com modo_envio
        'falta_medico' — assim o cron de cobrança ignora)
"""

from __future__ import annotations

import os
import re
import uuid
import logging
from datetime import date, datetime, timedelta

import pyodbc
import requests
from flask import Blueprint, jsonify, request

# Reusa helpers do módulo medico_novo (sem duplicar lógica de auth/conexão/audit)
from medico_novo_routes import (
    _check_admin, _conn_for_posto, _require_posto_in_acl,
    _resolver_idusuario_no_posto, _audit, ERR_SEM_VINCULO,
)

logger = logging.getLogger(__name__)

medico_falta_bp = Blueprint("medico_falta_bp", __name__)

ID_TABELA_CAD_MEDICO_FALTA = 50  # de Sis_HistoricoTabela
ID_COMANDO_INCLUSAO = 1
ID_COMANDO_EXCLUSAO = 3

# Identificadores da campanha "Falta de Médico" no whatsapp_cobranca.db.
# IMPORTANTE: template, from_user_id e queue_id são SEMPRE lidos da campanha
# (não hardcoded e não do .env) — admin altera pelo painel sem mexer no código.
WPP_MODO_ENVIO_FALTA = "falta_medico"  # cron de cobrança ignora esse modo
WPP_CAMPANHA_NOME = "Falta de Médico (avisos automáticos)"
# Defaults usados apenas se a campanha precisar ser criada pela primeira vez
WPP_TEMPLATE_DEFAULT_AO_CRIAR = "aviso_de_fechamento_de_agenda"
WPP_FROM_USER_DEFAULT_AO_CRIAR = "cmg8cum8g0519jbbm6r9l93f7"


# Mapeia letra do posto → idEndereco, descrição, telefone (puxado de cad_endereco em runtime)
def _posto_endereco(con: pyodbc.Connection, letra: str) -> tuple[int | None, str, str]:
    cur = con.cursor()
    cur.execute(
        "SELECT TOP 1 idEndereco, descricao, Telefone FROM cad_endereco "
        "WHERE codigo = ? AND AtendimentoAtivoPosto = 1",
        letra.strip().upper(),
    )
    row = cur.fetchone()
    if not row:
        return None, "", ""
    return int(row[0]), (row[1] or "").strip(), (row[2] or "").strip()


# Roteamento Meta: postos do grupo Couto saem pelo número 3529-6666
# (from=552135296666). Demais postos omitem o `from` e saem pelo número
# default da conta (2455-9600). Tentamos antes resolver pelo letra/grupo
# (mais confiável que cad_endereco.Telefone, cuja formatação varia por filial).
COUTO_POSTOS = frozenset({"C", "D", "J", "M", "P"})
WPP_FROM_COUTO = "552135296666"


def _resolve_wpp_from_phone(letra_posto: str | None) -> str | None:
    """Retorna o `from` Meta para o posto, ou None pra usar o default da conta.

    Posto do grupo Couto (C, D, J, M, P) → 3529-6666 (552135296666).
    Demais → None (omite `from`, sai pelo 2455-9600 default).
    """
    if (letra_posto or "").strip().upper() in COUTO_POSTOS:
        return WPP_FROM_COUTO
    return None


def _numero_saida_humano(letra_posto: str | None) -> str:
    """String legível do número que sai pra ir no MotivoDesistencia/CRM."""
    if (letra_posto or "").strip().upper() in COUTO_POSTOS:
        return "3529-6666"
    return "2455-9600"


# ---------------------------------------------------------------------------
# Integração com api_fin_receita (camila3) — registra CRM e append no MotivoDesistencia
# ---------------------------------------------------------------------------

CAMILA3_API_URL = os.getenv("CAMILA3_API_URL", "")
CAMILA3_API_KEY = os.getenv("CAMILA3_API_KEY", "")

# IDs fixos no banco C (lookup confirmado em 2026-05-06):
CRM_TIPO_OUTROS = 1
CRM_MOTIVO_ORIENTACAO_AO_CLIENTE = 7


def _chat_create_ticket(telefone: str, nome: str, texto: str,
                         queue_id: str | None, from_user_id: str | None,
                         display_phone_number: str | None,
                         phone_number_id: str | None = None) -> dict:
    """POST /webhooks/chat — cria ticket sincronamente e devolve {id, link}.

    Por que escolhemos esse e não o legado /webhooks/whatsapp?
    No legado, quando o cliente responde, o chat ABRE TICKET NOVO (perde
    contexto). No /webhooks/chat a resposta agrega no MESMO ticket, que é
    o ponto inteiro do CRM.

    Bug conhecido (em curso pelo dev senior do chat — 2026-05-06): o
    /webhooks/chat tagga 'Contato 2455' independente do display_phone_number
    do payload. A entrega Meta sai pelo número certo (3529 quando Couto),
    mas o painel do chat mostra a tag errada até a correção do lado dele.
    """
    chat_url = os.getenv("CHAT_API_URL", "").rstrip("/")
    if not chat_url:
        return {"ok": False, "error": "CHAT_API_URL não configurado"}
    if not from_user_id:
        return {"ok": False, "error": "from_user_id obrigatório"}
    remetente = from_user_id if from_user_id.startswith("chat:") else f"chat:{from_user_id}"
    ext_id = uuid.uuid4().hex[:24]
    ts = datetime.now().astimezone().isoformat(timespec="seconds")
    payload = {
        "entry": [{
            "id": ext_id,
            "changes": [{
                "field": "messages",
                "value": {
                    "contacts": [{"wa_id": telefone, "profile": {"name": nome}}],
                    "messages": [{
                        "id": ext_id, "from": remetente, "queue_id": queue_id,
                        "text": {"body": texto}, "type": "text", "timestamp": ts,
                    }],
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
    try:
        r = requests.post(f"{chat_url}/webhooks/chat", json=payload, timeout=20)
        r.raise_for_status()
        d = r.json() if r.content else {}
        return {"ok": True, "ticket_id": d.get("id"), "link": d.get("link"),
                "external_id": ext_id}
    except requests.HTTPError as e:
        return {"ok": False, "error": f"HTTP {e.response.status_code}",
                "external_id": ext_id}
    except Exception as e:
        return {"ok": False, "error": str(e)[:200], "external_id": ext_id}


def _camila3_append_observacao(id_endereco: int, id_lancamento_servico: int,
                                texto: str, id_usuario: int | None = None) -> dict:
    """POST /f3/{id_endereco}/lancamento/{id}/observacao — append no MotivoDesistencia."""
    if not (CAMILA3_API_URL and CAMILA3_API_KEY):
        return {"ok": False, "error": "CAMILA3_API_URL/KEY não configurados"}
    payload = {"texto": texto}
    if id_usuario:
        payload["id_usuario"] = int(id_usuario)
    url = f"{CAMILA3_API_URL.rstrip('/')}/f3/{int(id_endereco)}/lancamento/{int(id_lancamento_servico)}/observacao"
    try:
        r = requests.post(url, headers={"x-api-key": CAMILA3_API_KEY},
                          json=payload, timeout=15)
        if 200 <= r.status_code < 300:
            return {"ok": True, **(r.json() if r.content else {})}
        return {"ok": False, "error": f"HTTP {r.status_code}: {r.text[:200]}"}
    except Exception as e:
        return {"ok": False, "error": str(e)[:200]}


def _camila3_create_crm(payload: dict) -> dict:
    """POST /crm — cria registro de atendimento via sp_CRM_Insert (banco C)."""
    if not (CAMILA3_API_URL and CAMILA3_API_KEY):
        return {"ok": False, "error": "CAMILA3_API_URL/KEY não configurados"}
    url = f"{CAMILA3_API_URL.rstrip('/')}/crm"
    try:
        r = requests.post(url, headers={"x-api-key": CAMILA3_API_KEY},
                          json=payload, timeout=20)
        if 200 <= r.status_code < 300:
            return {"ok": True, **(r.json() if r.content else {})}
        return {"ok": False, "error": f"HTTP {r.status_code}: {r.text[:200]}"}
    except Exception as e:
        return {"ok": False, "error": str(e)[:200]}


def _chat_get_ticket_number(ticket_id: str) -> int | None:
    """Resolve ticketNumber humano (#107800) a partir do id interno (cuid).

    /webhooks/chat já devolve o id direto na resposta — esse helper só faz
    a tradução id→ticketNumber via PK no MySQL do chat. Lookup leve, single
    query, sem retry (o ticket existe no momento em que /webhooks/chat
    devolveu HTTP 201).
    """
    if not ticket_id:
        return None
    host = os.getenv("CHAT_MYSQL_HOST", "")
    user = os.getenv("CHAT_MYSQL_USER", "")
    pwd  = os.getenv("CHAT_MYSQL_PASSWORD", "")
    db   = os.getenv("CHAT_MYSQL_DATABASE", "")
    if not (host and user and pwd and db):
        logger.warning("CHAT_MYSQL_* não configurado — sem lookup de ticketNumber")
        return None
    try:
        import pymysql
        conn = pymysql.connect(
            host=host, user=user, password=pwd, database=db,
            charset="utf8mb4", connect_timeout=5, read_timeout=5, autocommit=True,
        )
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT ticketNumber FROM Ticket WHERE id = %s", (ticket_id,))
                row = cur.fetchone()
                return int(row[0]) if row and row[0] is not None else None
        finally:
            conn.close()
    except Exception as e:
        logger.warning("lookup ticketNumber falhou (id=%s): %s",
                       ticket_id, str(e)[:200])
        return None


def _limpar_telefone(raw: str | None) -> str | None:
    """Reproduz a regra de limpeza de telefone do send_whatsapp_cobranca."""
    if not raw:
        return None
    digs = re.sub(r"\D", "", str(raw))
    # Pega o último número válido (campo costuma ter vários separados)
    if not digs:
        return None
    if len(digs) < 10:
        return None
    if not digs.startswith("55"):
        digs = "55" + digs
    return digs


_ALTAMIRO_POSTOS = frozenset({"A", "B", "G", "I", "N", "R", "X", "Y"})
# COUTO_POSTOS já definido lá em cima

WPP_CAMPANHA_NOME_ALTAMIRO = "Falta de Médico — Altamiro (2455-9600)"
WPP_CAMPANHA_NOME_COUTO    = "Falta de Médico — Couto (3529-6666)"


def _ensure_campanhas_falta_medico() -> None:
    """Idempotente: garante que existem as 2 campanhas (Altamiro/2455 e Couto/3529).

    - Se nenhuma campanha de modo 'falta_medico' existe → cria a Altamiro.
    - Se existe a campanha legada (postos=[], numero_saida default) → migra ela
      pra Altamiro (postos = A,B,G,I,N,R,X,Y) preservando id/template/from_user.
    - Se não existe campanha cobrindo Couto → cria a Couto duplicando config
      base da Altamiro mas com numero_saida=3529-6666 e postos=C,D,J,M,P.
    """
    import sqlite3, json as _json
    from datetime import datetime as _dt

    db_path = os.getenv("WAPP_CTRL_DB", "/opt/camim-auth/whatsapp_cobranca.db")
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM campanhas WHERE modo_envio = ? ORDER BY id",
            (WPP_MODO_ENVIO_FALTA,),
        )
        rows = list(cur.fetchall())

        # Bootstrap: nenhuma campanha falta_medico ainda → cria Altamiro
        agora = _dt.now().isoformat(timespec="seconds")
        if not rows:
            cur.execute(
                """INSERT INTO campanhas
                      (nome, template, postos, dias_atraso_min, dias_atraso_max,
                       incluir_cancelados, sem_email, sexo, idade_min, idade_max,
                       nao_recorrente, hora_inicio, hora_fim, dias_semana,
                       intervalo_dias, ativa, created_at, updated_at,
                       modo_envio, dias_ref_min, dias_ref_max,
                       from_user_id, enviar_chat, enviar_meta, queue_id, numero_saida)
                   VALUES (?, ?, ?, 0, NULL,
                           0, 0, NULL, NULL, NULL,
                           0, '00:00', '23:59', '0,1,2,3,4,5,6',
                           1, 1, ?, ?,
                           ?, 0, NULL,
                           ?, 1, 1, NULL, '2455-9600')""",
                (WPP_CAMPANHA_NOME_ALTAMIRO, WPP_TEMPLATE_DEFAULT_AO_CRIAR,
                 _json.dumps(sorted(_ALTAMIRO_POSTOS)),
                 agora, agora,
                 WPP_MODO_ENVIO_FALTA, WPP_FROM_USER_DEFAULT_AO_CRIAR),
            )
            conn.commit()
            cur.execute(
                "SELECT * FROM campanhas WHERE modo_envio = ? ORDER BY id",
                (WPP_MODO_ENVIO_FALTA,),
            )
            rows = list(cur.fetchall())

        # Migração: campanha legada com postos=[] vira Altamiro
        for r in rows:
            try:
                postos_atuais = _json.loads(r["postos"] or "[]")
            except Exception:
                postos_atuais = []
            ns = (r["numero_saida"] or "2455-9600").strip()
            if (not postos_atuais) and ns == "2455-9600":
                cur.execute(
                    "UPDATE campanhas SET postos = ?, nome = ?, updated_at = ? WHERE id = ?",
                    (_json.dumps(sorted(_ALTAMIRO_POSTOS)),
                     WPP_CAMPANHA_NOME_ALTAMIRO, agora, int(r["id"])),
                )
                conn.commit()

        # Garantir campanha Couto: nenhuma com numero_saida 3529 ainda?
        cur.execute(
            "SELECT * FROM campanhas WHERE modo_envio = ? AND numero_saida = ?",
            (WPP_MODO_ENVIO_FALTA, "3529-6666"),
        )
        if not cur.fetchone():
            # duplica config base da campanha existente (template/from_user/queue_id)
            base = rows[0]
            cur.execute(
                """INSERT INTO campanhas
                      (nome, template, postos, dias_atraso_min, dias_atraso_max,
                       incluir_cancelados, sem_email, sexo, idade_min, idade_max,
                       nao_recorrente, hora_inicio, hora_fim, dias_semana,
                       intervalo_dias, ativa, created_at, updated_at,
                       modo_envio, dias_ref_min, dias_ref_max,
                       from_user_id, enviar_chat, enviar_meta, queue_id, numero_saida)
                   VALUES (?, ?, ?, 0, NULL,
                           0, 0, NULL, NULL, NULL,
                           0, '00:00', '23:59', '0,1,2,3,4,5,6',
                           1, 1, ?, ?,
                           ?, 0, NULL,
                           ?, ?, ?, ?, '3529-6666')""",
                (WPP_CAMPANHA_NOME_COUTO, base["template"],
                 _json.dumps(sorted(COUTO_POSTOS)),
                 agora, agora,
                 WPP_MODO_ENVIO_FALTA, base["from_user_id"],
                 int(base["enviar_chat"] or 1), int(base["enviar_meta"] or 1),
                 base["queue_id"]),
            )
            conn.commit()


def _campanha_falta_medico_por_posto(posto: str) -> dict:
    """Retorna a campanha cobrindo o posto. Cria as duas (Altamiro/Couto) se
    ainda não existem (idempotente).

    Match por inclusão na lista `postos` da campanha. Se nenhuma cobre o
    posto, fallback pra primeira campanha falta_medico ativa (defensivo —
    não quebra o disparo, mas loga warning).
    """
    import sqlite3, json as _json

    _ensure_campanhas_falta_medico()
    posto_u = (posto or "").strip().upper()
    db_path = os.getenv("WAPP_CTRL_DB", "/opt/camim-auth/whatsapp_cobranca.db")
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM campanhas WHERE modo_envio = ? AND ativa = 1 ORDER BY id",
            (WPP_MODO_ENVIO_FALTA,),
        )
        rows = list(cur.fetchall())

        chosen = None
        for r in rows:
            try:
                postos_lista = [str(p).strip().upper() for p in _json.loads(r["postos"] or "[]")]
            except Exception:
                postos_lista = []
            if posto_u in postos_lista:
                chosen = r
                break

        if not chosen and rows:
            logger.warning(
                "Nenhuma campanha falta_medico cobre o posto %s — caindo na 1ª (id=%s)",
                posto_u, rows[0]["id"],
            )
            chosen = rows[0]
        if not chosen:
            raise RuntimeError("Nenhuma campanha falta_medico ativa no banco.")

        return {
            "id": int(chosen["id"]),
            "template": chosen["template"],
            "from_user_id": chosen["from_user_id"] or None,
            "queue_id": chosen["queue_id"] or None,  # NULL = sem fila → Camila atende
            "enviar_chat": bool(chosen["enviar_chat"]),
            "enviar_meta": bool(chosen["enviar_meta"]),
            "numero_saida": (chosen["numero_saida"] or "2455-9600").strip(),
        }


def _registrar_envio_log(campanha_id: int, posto: str, telefone: str,
                          paciente: str, template: str, status: str,
                          wamid: str | None, ref_extra: str = "",
                          chat_ticket_id: str | None = None) -> int | None:
    """Insere em whatsapp_cobranca.db.envios. Retorna o id do envio inserido.

    `chat_ticket_id` (cuid devolvido pelo /webhooks/chat) é gravado pra
    permitir lookup direto na tela 'Ver conversa' sem depender de wamid
    (que o chat NÃO armazena pra outgoing).
    """
    import sqlite3
    db_path = os.getenv("WAPP_CTRL_DB", "/opt/camim-auth/whatsapp_cobranca.db")
    with sqlite3.connect(db_path) as conn:
        from datetime import datetime as _dt
        cur = conn.execute(
            """INSERT INTO envios
                  (campanha_id, posto, telefone, idreceita, matricula, nome,
                   ref, valor, venc, dias_atraso, template, status, wamid,
                   chat_ticket_id, enviado_em)
               VALUES (?, ?, ?, '', '', ?, ?, '', '', NULL, ?, ?, ?, ?, ?)""",
            (campanha_id, posto, telefone, paciente, ref_extra,
             template, status, wamid, chat_ticket_id,
             _dt.now().isoformat(timespec="seconds")),
        )
        conn.commit()
        return cur.lastrowid


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@medico_falta_bp.get("/api/medico_falta/lookups")
def api_lookups():
    """Motivos de falta e (TODO) lista resumida de funcionários do posto."""
    email, postos, login_campinho = _check_admin()
    if not email:
        return jsonify({"error": "unauthorized"}), 401
    if not login_campinho:
        return jsonify({"error": ERR_SEM_VINCULO, "sem_vinculo": True}), 403
    posto = request.args.get("posto", "")
    erro = _require_posto_in_acl(posto, postos)
    if erro:
        return jsonify({"error": erro}), 400
    try:
        with _conn_for_posto(posto) as con:
            id_usuario_op = _resolver_idusuario_no_posto(con, login_campinho)
            if not id_usuario_op:
                return jsonify({"error": ERR_SEM_VINCULO, "sem_vinculo_posto": True}), 403
            cur = con.cursor()
            cur.execute(
                "SELECT idMedicoFaltaMotivo, Motivo FROM vw_Cad_MedicoFaltaMotivo "
                "WHERE Desativado = 0 ORDER BY Motivo"
            )
            motivos = [{"id": int(r[0]), "label": (r[1] or "").strip()} for r in cur.fetchall()]
        return jsonify({"motivos": motivos, "id_usuario_op": id_usuario_op})
    except Exception as e:
        logger.exception("lookups (medico_falta) falhou no posto %s", posto)
        return jsonify({"error": str(e)[:300]}), 500


@medico_falta_bp.get("/api/medico_falta/especialidades_medico")
def api_especialidades_medico():
    """Lista as especialidades em que o médico atende no posto, lendo cad_especialidade.

    Um médico pode ter N linhas em cad_especialidade (uma por especialidade/horário/sala).
    O retorno é distinct por nome de especialidade, ignorando linhas desativadas
    e linhas com janela de exibição já encerrada (DataFimExibicao < hoje).
    """
    email, postos, login_campinho = _check_admin()
    if not email:
        return jsonify({"error": "unauthorized"}), 401
    if not login_campinho:
        return jsonify({"error": ERR_SEM_VINCULO, "sem_vinculo": True}), 403
    posto = request.args.get("posto", "")
    erro = _require_posto_in_acl(posto, postos)
    if erro:
        return jsonify({"error": erro}), 400
    try:
        idmedico = int(request.args.get("idmedico") or 0)
    except ValueError:
        return jsonify({"error": "idmedico inválido"}), 400
    if not idmedico:
        return jsonify({"error": "idmedico obrigatório"}), 400

    try:
        with _conn_for_posto(posto) as con:
            cur = con.cursor()
            cur.execute(
                """SELECT DISTINCT LTRIM(RTRIM(Especialidade)) AS Esp
                     FROM cad_especialidade
                    WHERE idmedico = ?
                      AND ISNULL(Desativado, 0) = 0
                      AND Especialidade IS NOT NULL
                      AND LTRIM(RTRIM(Especialidade)) <> ''
                      AND (DataFimExibicao IS NULL OR DataFimExibicao >= CAST(GETDATE() AS DATE))
                    ORDER BY Esp""",
                idmedico,
            )
            especs = [(r[0] or "").strip() for r in cur.fetchall() if (r[0] or "").strip()]
        return jsonify({"especialidades": especs, "total": len(especs)})
    except Exception as e:
        logger.exception("especialidades_medico falhou no posto %s", posto)
        return jsonify({"error": str(e)[:300]}), 500


@medico_falta_bp.get("/api/medico_falta/list")
def api_list():
    """Lista faltas ativas no posto, filtrando por intervalo de DataFalta."""
    email, postos, login_campinho = _check_admin()
    if not email:
        return jsonify({"error": "unauthorized"}), 401
    if not login_campinho:
        return jsonify({"error": ERR_SEM_VINCULO, "sem_vinculo": True}), 403
    posto = request.args.get("posto", "")
    erro = _require_posto_in_acl(posto, postos)
    if erro:
        return jsonify({"error": erro}), 400

    # Intervalo padrão: hoje até +30 dias. Aceita query string ?ini=YYYY-MM-DD&fim=YYYY-MM-DD
    try:
        ini_raw = (request.args.get("ini") or "").strip()
        fim_raw = (request.args.get("fim") or "").strip()
        ini = datetime.strptime(ini_raw, "%Y-%m-%d").date() if ini_raw else date.today()
        fim = datetime.strptime(fim_raw, "%Y-%m-%d").date() if fim_raw else (ini + timedelta(days=30))
    except ValueError:
        return jsonify({"error": "datas inválidas (use YYYY-MM-DD)"}), 400

    try:
        with _conn_for_posto(posto) as con:
            cur = con.cursor()
            # Atenção: vw_Cad_MedicoFalta tem `idFalta` (PK real da Cad_MedicoFalta,
            # ex.: 173472) E `idMedicoFalta` (que é o idMedico, ex.: 1741).
            # Nomes confusos do banco — sempre usar idFalta para identificar a falta.
            cur.execute(
                """SELECT TOP 200 idFalta, idMedico, Medico, Especialidade,
                                  DataFalta, [Hora Inicial], [Hora Final],
                                  Motivo, [Nome Usuario], [Usuário Avisado],
                                  Observacao, ClinicaFechouAgenda, MedicoFechouAgenda,
                                  QuantidadePacienteAgendado, DataHoraInclusao
                     FROM vw_Cad_MedicoFalta
                     WHERE Desativado = 0
                       AND DataFalta >= ? AND DataFalta <= ?
                     ORDER BY DataFalta DESC, Medico""",
                ini, fim,
            )
            out = []
            for r in cur.fetchall():
                out.append({
                    "id_falta": int(r[0]),
                    "id_medico": int(r[1]),
                    "medico": (r[2] or "").strip(),
                    "especialidade": (r[3] or "").strip(),
                    "data_falta": r[4].isoformat() if r[4] else None,
                    "hora_inicio": r[5],
                    "hora_fim": r[6],
                    "motivo": (r[7] or "").strip(),
                    "usuario_cadastrou": (r[8] or "").strip(),
                    "usuario_avisado": (r[9] or "").strip(),
                    "observacao": (r[10] or "").strip() if r[10] else "",
                    "clinica_fechou": bool(r[11]),
                    "medico_fechou": bool(r[12]),
                    "qtd_pacientes": int(r[13]) if r[13] is not None else None,
                    "data_inclusao": r[14].isoformat() if r[14] else None,
                })
        return jsonify({"faltas": out, "ini": ini.isoformat(), "fim": fim.isoformat()})
    except Exception as e:
        logger.exception("list (medico_falta) falhou no posto %s", posto)
        return jsonify({"error": str(e)[:300]}), 500


@medico_falta_bp.get("/api/medico_falta/agendamentos")
def api_agendamentos():
    """Lista pacientes agendados pro médico no dia (e horário, se parcial).
    Usado tanto para mostrar a previsão de impacto quanto para depois enviar WhatsApp.
    """
    email, postos, login_campinho = _check_admin()
    if not email:
        return jsonify({"error": "unauthorized"}), 401
    if not login_campinho:
        return jsonify({"error": ERR_SEM_VINCULO, "sem_vinculo": True}), 403
    posto = request.args.get("posto", "")
    erro = _require_posto_in_acl(posto, postos)
    if erro:
        return jsonify({"error": erro}), 400
    try:
        idmedico = int(request.args.get("idmedico") or 0)
    except ValueError:
        return jsonify({"error": "idmedico inválido"}), 400
    if not idmedico:
        return jsonify({"error": "idmedico obrigatório"}), 400
    data_str = (request.args.get("data") or "").strip()
    try:
        d = datetime.strptime(data_str, "%Y-%m-%d").date()
    except ValueError:
        return jsonify({"error": "data inválida (use YYYY-MM-DD)"}), 400
    hora_ini = (request.args.get("hora_ini") or "00:00").strip()
    hora_fim = (request.args.get("hora_fim") or "23:59").strip()
    # Especialidade é o que define a falta (médico pode ter N especialidades).
    # Se não vier, mostra TODOS os agendamentos do médico no intervalo (legado).
    especialidade = (request.args.get("especialidade") or "").strip()

    try:
        with _conn_for_posto(posto) as con:
            cur = con.cursor()
            # vw_Cad_LancamentoProntuarioComDesistencia é pesada, mas tem tudo que precisamos.
            # NOLOCK + READ UNCOMMITTED para acelerar.
            cur.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
            # CLAUDE.md: views da CAMIM usam SET DATEFORMAT dmy → passar data como DD/MM/YYYY string
            d_str = d.strftime("%d/%m/%Y")
            d_next = (d + timedelta(days=1)).strftime("%d/%m/%Y")
            sql = """SELECT idLancamentoServico, Matricula, Paciente, idadePaciente,
                          TelefoneResidencial, HoraPrevistaConsulta, Servico, Especialidade
                     FROM vw_Cad_LancamentoProntuarioComDesistencia WITH (NOLOCK)
                     WHERE idMedico = ?
                       AND DataConsulta >= ?
                       AND DataConsulta <  ?
                       AND Desistencia = 0
                       AND HoraPrevistaConsulta >= ?
                       AND HoraPrevistaConsulta <= ?"""
            params = [idmedico, d_str, d_next, hora_ini, hora_fim]
            if especialidade:
                sql += " AND LTRIM(RTRIM(Especialidade)) = ?"
                params.append(especialidade)
            sql += " ORDER BY HoraPrevistaConsulta"
            cur.execute(sql, params)
            out = []
            for r in cur.fetchall():
                out.append({
                    "id_lancamento_servico": int(r[0]),
                    "matricula": str(r[1]).strip() if r[1] is not None else "",
                    "paciente": (r[2] or "").strip(),
                    "idade": int(r[3]) if r[3] is not None else None,
                    "telefone_raw": str(r[4]).strip() if r[4] is not None else "",
                    "hora": r[5],
                    "servico": (r[6] or "").strip(),
                    "especialidade": (r[7] or "").strip(),
                })
        return jsonify({"agendamentos": out, "total": len(out)})
    except Exception as e:
        logger.exception("agendamentos (medico_falta) falhou no posto %s", posto)
        return jsonify({"error": str(e)[:300]}), 500


@medico_falta_bp.post("/api/medico_falta/insert")
def api_insert():
    """INSERT em Cad_MedicoFalta + auditoria. Retorna idFalta + lista de pacientes afetados.
    O envio de WhatsApp é responsabilidade do frontend (chama endpoint dedicado depois).
    """
    email, postos, login_campinho = _check_admin()
    if not email:
        return jsonify({"error": "unauthorized"}), 401
    if not login_campinho:
        return jsonify({"error": ERR_SEM_VINCULO, "sem_vinculo": True}), 403

    data = request.get_json(silent=True) or {}
    posto = (data.get("posto") or "").strip().upper()
    erro = _require_posto_in_acl(posto, postos)
    if erro:
        return jsonify({"error": erro}), 400

    try:
        idmedico = int(data.get("idmedico") or 0)
    except (TypeError, ValueError):
        idmedico = 0
    if not idmedico:
        return jsonify({"error": "idmedico obrigatório"}), 400

    data_falta_str = (data.get("data_falta") or "").strip()
    try:
        data_falta = datetime.strptime(data_falta_str, "%Y-%m-%d").date()
    except ValueError:
        return jsonify({"error": "data_falta inválida (use YYYY-MM-DD)"}), 400

    parcial = bool(data.get("parcial", False))
    hora_ini = (data.get("hora_inicio") or "00:00").strip() if parcial else "00:00"
    hora_fim = (data.get("hora_fim") or "23:59").strip() if parcial else "23:59"
    if not (re.match(r"^\d{2}:\d{2}$", hora_ini) and re.match(r"^\d{2}:\d{2}$", hora_fim)):
        return jsonify({"error": "hora no formato HH:MM"}), 400

    try:
        id_motivo = int(data.get("id_motivo") or 0)
    except (TypeError, ValueError):
        id_motivo = 0
    if not id_motivo:
        return jsonify({"error": "motivo obrigatório"}), 400

    motivo_label = (data.get("motivo_label") or "").strip()[:250] or None
    # Especialidade vinda do select (cad_especialidade) — obrigatória.
    # É ela que define a unicidade da falta: o mesmo médico pode ter várias
    # especialidades, faltando em uma e atendendo em outra no mesmo dia.
    especialidade = (data.get("especialidade") or "").strip()[:50]
    if not especialidade:
        return jsonify({
            "error": "Especialidade obrigatória. O médico pode atender em várias — "
                     "selecione a que está com falta.",
            "campo": "especialidade",
        }), 400
    observacao = (data.get("observacao") or "").strip() or None
    clinica_fechou = 1 if data.get("clinica_fechou") else 0
    medico_fechou = 1 if data.get("medico_fechou") else 0
    # XOR obrigatório: exatamente um dos dois (nem 0, nem 2)
    if (clinica_fechou + medico_fechou) != 1:
        return jsonify({
            "error": "Marque exatamente UM responsável: 'Clínica fechou a agenda' OU 'Médico fechou a agenda' (não os dois, e não nenhum).",
            "campo": "responsavel_fechamento",
        }), 400

    # Timestamps: passar objeto datetime para evitar bug de SET DATEFORMAT dmy em parâmetros
    h_ini = datetime.strptime(hora_ini, "%H:%M").time()
    h_fim = datetime.strptime(hora_fim, "%H:%M").time()
    dh_ini = datetime.combine(data_falta, h_ini)
    dh_fim = datetime.combine(data_falta, h_fim)

    try:
        with _conn_for_posto(posto) as con:
            id_usuario_op = _resolver_idusuario_no_posto(con, login_campinho)
            if not id_usuario_op:
                return jsonify({"error": ERR_SEM_VINCULO, "sem_vinculo_posto": True}), 403
            cur = con.cursor()

            # Anti-duplicidade: não permite 2 faltas ativas para o mesmo
            # médico+especialidade+dia. Médico com 5 especialidades pode estar
            # faltando em só uma — outras especialidades seguem atendendo.
            cur.execute(
                """SELECT TOP 1 idFalta FROM Cad_MedicoFalta
                    WHERE idMedico = ?
                      AND DataFalta = ?
                      AND LTRIM(RTRIM(ISNULL(Especialidade,''))) = ?
                      AND Desativado = 0""",
                idmedico, data_falta, especialidade,
            )
            existente = cur.fetchone()
            if existente:
                return jsonify({
                    "error": (
                        f"Já existe falta cadastrada (idFalta={int(existente[0])}) "
                        f"para esse médico em {data_falta.strftime('%d/%m/%Y')} "
                        f"na especialidade {especialidade}. "
                        f"Edite ou desative a existente antes de criar outra."
                    ),
                    "duplicada": True,
                    "id_falta_existente": int(existente[0]),
                }), 409

            # Conta pacientes agendados no intervalo (para QuantidadePacienteAgendado).
            # Filtra pela especialidade da falta — se médico tem outras, elas
            # continuam atendendo e não devem entrar nessa contagem.
            cur.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
            d_str = data_falta.strftime("%d/%m/%Y")
            d_next = (data_falta + timedelta(days=1)).strftime("%d/%m/%Y")
            cur.execute(
                """SELECT COUNT(*)
                     FROM vw_Cad_LancamentoProntuarioComDesistencia WITH (NOLOCK)
                     WHERE idMedico = ?
                       AND DataConsulta >= ? AND DataConsulta <  ?
                       AND Desistencia = 0
                       AND HoraPrevistaConsulta >= ? AND HoraPrevistaConsulta <= ?
                       AND LTRIM(RTRIM(Especialidade)) = ?""",
                idmedico, d_str, d_next, hora_ini, hora_fim, especialidade,
            )
            qtd_pacientes = int(cur.fetchone()[0])

            # INSERT na Cad_MedicoFalta com OUTPUT INTO @t (compatível com triggers caso futuras)
            sql = """
            DECLARE @t TABLE (idFalta INT);
            INSERT INTO Cad_MedicoFalta
                (idMedico, idUsuario, DataHora, DatahoraFim, DataFalta,
                 Desativado, Motivo, Especialidade, idMedicoFaltaMotivo,
                 Observacao, QuantidadeHorarioComFalta,
                 ClinicaFechouAgenda, MedicoFechouAgenda,
                 QuantidadePacienteAgendado, AvisoFaltaMedicoAplicativo,
                 DataHoraInclusao)
            OUTPUT INSERTED.idFalta INTO @t
            VALUES (?, ?, ?, ?, ?,
                    0, ?, ?, ?,
                    ?, ?,
                    ?, ?,
                    ?, 0,
                    GETDATE());
            SELECT idFalta FROM @t;
            """
            params = (
                idmedico, id_usuario_op, dh_ini, dh_fim, data_falta,
                motivo_label, especialidade, id_motivo,
                observacao, qtd_pacientes,
                clinica_fechou, medico_fechou,
                qtd_pacientes,
            )
            cur.execute(sql, params)
            while cur.description is None:
                if not cur.nextset():
                    break
            row = cur.fetchone()
            if not row or row[0] is None:
                con.rollback()
                return jsonify({"error": "INSERT Cad_MedicoFalta não retornou idFalta"}), 500
            id_falta = int(row[0])

            # Agendamentos afetados (para o frontend disparar WhatsApp depois).
            # Filtra pela especialidade da falta — pacientes de outras especialidades
            # do mesmo médico não foram afetados.
            cur.execute(
                """SELECT idLancamentoServico, Paciente, HoraPrevistaConsulta,
                          TelefoneResidencial, Especialidade
                     FROM vw_Cad_LancamentoProntuarioComDesistencia WITH (NOLOCK)
                     WHERE idMedico = ?
                       AND DataConsulta >= ? AND DataConsulta <  ?
                       AND Desistencia = 0
                       AND HoraPrevistaConsulta >= ? AND HoraPrevistaConsulta <= ?
                       AND LTRIM(RTRIM(Especialidade)) = ?
                     ORDER BY HoraPrevistaConsulta""",
                idmedico, d_str, d_next, hora_ini, hora_fim, especialidade,
            )
            agendamentos = [
                {
                    "id_lancamento_servico": int(r[0]),
                    "paciente": (r[1] or "").strip(),
                    "hora": r[2],
                    "telefone_raw": str(r[3]).strip() if r[3] is not None else "",
                    "especialidade": (r[4] or "").strip(),
                }
                for r in cur.fetchall()
            ]

            _audit(con, id_falta, ID_TABELA_CAD_MEDICO_FALTA, ID_COMANDO_INCLUSAO,
                   id_usuario_op,
                   f"Inclusão Falta Médica via RH&T (medico={idmedico}, "
                   f"data={data_falta.isoformat()} {hora_ini}-{hora_fim}, "
                   f"motivo={motivo_label or id_motivo}, pacientes={qtd_pacientes})")
            con.commit()

        return jsonify({
            "ok": True,
            "id_falta": id_falta,
            "qtd_pacientes_afetados": qtd_pacientes,
            "agendamentos": agendamentos,
        })
    except Exception as e:
        logger.exception("INSERT Cad_MedicoFalta falhou no posto %s", posto)
        return jsonify({"error": str(e)[:400]}), 500


@medico_falta_bp.post("/api/medico_falta/desativar")
def api_desativar():
    """Soft-delete da falta: UPDATE Cad_MedicoFalta SET desativado=1.
    Registra em Sis_Historico (idTabela=50, idComando=3=Exclusão).
    """
    email, postos, login_campinho = _check_admin()
    if not email:
        return jsonify({"error": "unauthorized"}), 401
    if not login_campinho:
        return jsonify({"error": ERR_SEM_VINCULO, "sem_vinculo": True}), 403
    data = request.get_json(silent=True) or {}
    posto = (data.get("posto") or "").strip().upper()
    erro = _require_posto_in_acl(posto, postos)
    if erro:
        return jsonify({"error": erro}), 400
    try:
        id_falta = int(data.get("id_falta") or 0)
    except (TypeError, ValueError):
        id_falta = 0
    if not id_falta:
        return jsonify({"error": "id_falta obrigatório"}), 400

    try:
        with _conn_for_posto(posto) as con:
            id_usuario_op = _resolver_idusuario_no_posto(con, login_campinho)
            if not id_usuario_op:
                return jsonify({"error": ERR_SEM_VINCULO, "sem_vinculo_posto": True}), 403
            cur = con.cursor()
            # Confirma que existe (qualquer estado) — log detalhado pra debug
            cur.execute(
                "SELECT idMedico, DataFalta, Desativado FROM Cad_MedicoFalta WHERE idFalta = ?",
                id_falta,
            )
            row = cur.fetchone()
            if not row:
                logger.warning("desativar: idFalta=%s NÃO existe no posto %s", id_falta, posto)
                return jsonify({
                    "error": f"idFalta={id_falta} não existe no posto {posto} — pode ter sido criada em outro posto",
                }), 404
            if bool(row[2]):  # Desativado
                logger.warning("desativar: idFalta=%s já está desativada no posto %s", id_falta, posto)
                return jsonify({"error": f"idFalta={id_falta} já está desativada"}), 400
            idmedico, data_falta = int(row[0]), row[1]
            # Soft-delete
            cur.execute(
                "UPDATE Cad_MedicoFalta SET Desativado = 1 WHERE idFalta = ?",
                id_falta,
            )
            _audit(con, id_falta, ID_TABELA_CAD_MEDICO_FALTA, ID_COMANDO_EXCLUSAO,
                   id_usuario_op,
                   f"Exclusão (soft) Falta Médica via RH&T (medico={idmedico}, "
                   f"data={data_falta.strftime('%d/%m/%Y') if data_falta else '?'})")
            con.commit()
        return jsonify({"ok": True, "id_falta": id_falta})
    except Exception as e:
        logger.exception("desativar falta %s falhou", id_falta)
        return jsonify({"error": str(e)[:400]}), 500


@medico_falta_bp.post("/api/medico_falta/enviar_wpp")
def api_enviar_wpp():
    """Dispara o template `notificacao_de_falta_do_medico` aos pacientes afetados.
    Lê dados de cad_cliente / cad_clientedependente (NomeSocial > Nome,
    TelefoneWhatsApp), monta os 5 params do template e chama enviar() do
    send_whatsapp_cobranca (chat + Meta). Registra cada envio em envios.

    Body JSON: { posto, id_falta }
    """
    email, postos, login_campinho = _check_admin()
    if not email:
        return jsonify({"error": "unauthorized"}), 401
    if not login_campinho:
        return jsonify({"error": ERR_SEM_VINCULO, "sem_vinculo": True}), 403

    data = request.get_json(silent=True) or {}
    posto = (data.get("posto") or "").strip().upper()
    erro = _require_posto_in_acl(posto, postos)
    if erro:
        return jsonify({"error": erro}), 400
    try:
        id_falta = int(data.get("id_falta") or 0)
    except (TypeError, ValueError):
        id_falta = 0
    if not id_falta:
        return jsonify({"error": "id_falta obrigatório"}), 400

    try:
        # Reusa função de envio do projeto wpp-cobrança
        import importlib
        send_mod = importlib.import_module("send_whatsapp_cobranca")
    except Exception as e:
        logger.exception("falha ao importar send_whatsapp_cobranca")
        return jsonify({"error": f"módulo de envio indisponível: {e}"}), 500

    try:
        # Escolhe a campanha pelo posto da falta. Resolvida ANTES do primeiro
        # `with`/SQL block porque `wpp_from_phone_camp` é referenciada lá dentro
        # ao montar o envio. Mover esse bloco pra cá corrige o UnboundLocalError.
        camp = _campanha_falta_medico_por_posto(posto)
        campanha_id = camp["id"]
        wpp_template = camp["template"]
        wpp_from_user = camp["from_user_id"]
        wpp_queue_id = camp["queue_id"]
        wpp_usar_chat = camp["enviar_chat"]
        wpp_usar_meta = camp["enviar_meta"]
        try:
            from wpp_cobranca_db import from_phone_por_numero_saida as _from_phone_resolver
            wpp_from_phone_camp = _from_phone_resolver(camp["numero_saida"])
        except Exception:
            wpp_from_phone_camp = None

        with _conn_for_posto(posto) as con:
            id_usuario_op = _resolver_idusuario_no_posto(con, login_campinho)
            if not id_usuario_op:
                return jsonify({"error": ERR_SEM_VINCULO, "sem_vinculo_posto": True}), 403

            # 1) Dados da falta
            cur = con.cursor()
            cur.execute(
                """SELECT mf.idMedico, m.Nome AS NomeMedico, mf.DataFalta,
                          mf.DataHora, mf.DatahoraFim,
                          mf.MedicoFechouAgenda, mf.ClinicaFechouAgenda,
                          mfm.Motivo, mf.Observacao, mf.Especialidade
                     FROM Cad_MedicoFalta mf
                     LEFT JOIN cad_medico m ON m.idmedico = mf.idMedico
                     LEFT JOIN vw_Cad_MedicoFaltaMotivo mfm ON mfm.idMedicoFaltaMotivo = mf.idMedicoFaltaMotivo
                    WHERE mf.idFalta = ?""",
                id_falta,
            )
            f = cur.fetchone()
            if not f:
                return jsonify({"error": f"falta idFalta={id_falta} não existe no posto {posto}"}), 404
            idmedico, nome_medico_raw, data_falta = f[0], (f[1] or "").strip(), f[2]
            dh_ini, dh_fim = f[3], f[4]
            medico_fechou, clinica_fechou = bool(f[5]), bool(f[6])
            motivo_label = (f[7] or "").strip() or "—"
            especialidade_falta = (f[9] or "").strip()

            # 2) Posto: descrição + telefone (telefone determina o `from` Meta)
            id_endereco_posto, posto_descricao, posto_telefone = _posto_endereco(con, posto)
            # Roteia pelo `numero_saida` da campanha escolhida (que por sua
            # vez foi escolhida pelos `postos` da campanha — admin controla
            # qual grupo cai em qual número via painel /wpp).
            wpp_from_phone = wpp_from_phone_camp
            if wpp_from_phone is None:
                # Fallback de segurança: se a campanha não tem numero_saida
                # mapeado, cai pra regra hardcoded (Couto = 3529).
                wpp_from_phone = _resolve_wpp_from_phone(posto)

            # 3) "Médico ou Clínica?" — bit que estiver marcado define a string
            # Se nenhum marcado, default = "Médico"
            medico_ou_clinica = "Clínica" if clinica_fechou and not medico_fechou else "Médico"

            # 4) Data formatada para o template
            data_str = data_falta.strftime("%d/%m/%Y") if data_falta else ""

            # 5) Lista pacientes afetados no intervalo
            cur.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
            d_str = data_falta.strftime("%d/%m/%Y")
            d_next = (data_falta + timedelta(days=1)).strftime("%d/%m/%Y")
            hi = dh_ini.strftime("%H:%M") if dh_ini else "00:00"
            hf = dh_fim.strftime("%H:%M") if dh_fim else "23:59"
            sql_pac = """SELECT idLancamentoServico, idCliente, idDependente, Paciente,
                          HoraPrevistaConsulta, TelefoneResidencial, Especialidade
                     FROM vw_Cad_LancamentoProntuarioComDesistencia WITH (NOLOCK)
                     WHERE idMedico = ?
                       AND DataConsulta >= ? AND DataConsulta <  ?
                       AND Desistencia = 0
                       AND HoraPrevistaConsulta >= ? AND HoraPrevistaConsulta <= ?"""
            params_pac = [idmedico, d_str, d_next, hi, hf]
            # Filtra pela especialidade da falta — pacientes de outras especialidades
            # do mesmo médico não foram afetados por essa falta.
            if especialidade_falta:
                sql_pac += " AND LTRIM(RTRIM(Especialidade)) = ?"
                params_pac.append(especialidade_falta)
            cur.execute(sql_pac, params_pac)
            agendamentos = cur.fetchall()

        # 6) Para cada paciente:
        #    a) resolve titular + dados do paciente (matricula, idEnderecoCliente, telefone)
        #    b) cria ticket no chat via /webhooks/chat (foreground → devolve ticket_id) e
        #       envia template via Meta — sequencial (preciso do ticket_id antes do registro)
        #    c) faz o append no MotivoDesistencia da linha do prontuário (Cad_LancamentoServico)
        #    d) cria CRM (motivo ORIENTAÇÃO AO CLIENTE / tipo OUTROS) — 1 por paciente
        #    Tudo (c)/(d) é best-effort: se falhar, registra no resultado mas o envio
        #    já foi feito (não dá pra desfazer WhatsApp).
        # String humana do número de saída — vem direto da campanha
        # (ex.: "2455-9600" ou "3529-6666"). Usado pra anotar na
        # MotivoDesistencia e no histórico do CRM.
        numero_saida_str = (camp.get("numero_saida") or "2455-9600").strip()
        # Display phone pro chat tagging — formato Meta sem hífen,
        # derivado do numero_saida da campanha
        display_phone = "552135296666" if numero_saida_str == "3529-6666" else "552124559600"
        # phone_number_id (ID interno Meta) — vai no metadata.phone_number_id
        # do payload do chat. O dev sênior do chat passou esses IDs em
        # 2026-05-06 pra resolver o bug da tag "Contato 2455" sair errada
        # quando o número de saída é Couto.
        try:
            from wpp_cobranca_db import phone_number_id_por_numero_saida as _pn_id_resolver
            phone_number_id_camp = _pn_id_resolver(numero_saida_str)
        except Exception:
            phone_number_id_camp = None
        with _conn_for_posto(posto) as con:
            cur = con.cursor()
            enviados, falhados, sem_telefone = [], [], []
            for ag in agendamentos:
                id_lancamento_servico, idcli, iddep, paciente_view, hora, tel_view, especialidade = ag

                # --- (a) resolve titular SEMPRE pra ter matricula + idEndereco ---
                cur.execute(
                    "SELECT TOP 1 Nome, NomeSocial, TelefoneWhatsApp, TelefoneCelular, "
                    "       Matricula, idEndereco "
                    "FROM cad_cliente WHERE idCliente = ?",
                    int(idcli),
                )
                tit_row = cur.fetchone()
                if not tit_row:
                    falhados.append({"paciente": paciente_view, "erro": "titular não encontrado em cad_cliente"})
                    continue
                tit_nome, tit_nome_social, tit_tel_wpp, tit_tel_cel, matricula_raw, id_endereco_cliente = tit_row
                titular_resolvido = (tit_nome_social or tit_nome or "").strip()
                matricula_base = re.sub(r"\D", "", str(matricula_raw or ""))

                # Override paciente info se for dependente
                if iddep and int(iddep) > 0:
                    cur.execute(
                        "SELECT TOP 1 NomeSocial, Nome, TelefoneWhatsApp, TelefoneCelular "
                        "FROM cad_clientedependente WHERE idCliente = ? AND idDependente = ?",
                        int(idcli), int(iddep),
                    )
                    dep = cur.fetchone()
                    if dep:
                        nome_social, nome_pad, tel_wpp, tel_cel = dep
                        nome_paciente = (nome_social or nome_pad or paciente_view or "").strip()
                    else:
                        nome_paciente = titular_resolvido or (paciente_view or "").strip()
                        tel_wpp, tel_cel = tit_tel_wpp, tit_tel_cel
                else:
                    nome_paciente = titular_resolvido or (paciente_view or "").strip()
                    tel_wpp, tel_cel = tit_tel_wpp, tit_tel_cel

                tel_limpo = _limpar_telefone(tel_wpp) or _limpar_telefone(tel_cel)
                if not tel_limpo:
                    sem_telefone.append({"paciente": nome_paciente})
                    _registrar_envio_log(
                        campanha_id, posto, "", nome_paciente, wpp_template,
                        "erro:sem_telefone", None,
                        ref_extra=f"falta {id_falta}{posto}",
                    )
                    continue

                # Parâmetros do template (template recriado em 2026-05-01 com nomes minúsculos):
                #   {{paciente}} {{medico}} {{data_consulta}} {{local}}
                #   {{resp_fechamento}} {{motivo}}
                params = {
                    "paciente": nome_paciente,
                    "medico": nome_medico_raw,
                    "data_consulta": data_str,
                    "local": posto_descricao,
                    "resp_fechamento": medico_ou_clinica,
                    "motivo": motivo_label,
                }
                texto_renderizado = send_mod._expandir_template(wpp_template, params)

                # --- (b) chat /webhooks/chat (sincrono — devolve ticket_id;
                # resposta do cliente cai no MESMO ticket, não cria novo) + Meta ---
                # Bug em curso: tag 'Contato' vem 2455 mesmo passando 3529 em
                # display_phone_number. Dev senior do chat corrige amanhã
                # (2026-05-06). Entrega Meta segue indo pelo número certo.
                ticket_id: str | None = None      # id interno (cuid)
                ticket_number: int | None = None  # número humano (#107800)
                if wpp_usar_chat:
                    chat_res = _chat_create_ticket(
                        telefone=tel_limpo,
                        nome=nome_paciente,
                        texto=texto_renderizado,
                        queue_id=wpp_queue_id,
                        from_user_id=wpp_from_user,
                        display_phone_number=display_phone,
                        phone_number_id=phone_number_id_camp,
                    )
                    if chat_res.get("ok"):
                        ticket_id = chat_res.get("ticket_id")
                    else:
                        logger.warning("chat ticket falhou: %s", chat_res.get("error"))

                wamid: str | None = None
                meta_status = "skipped_meta"
                if wpp_usar_meta:
                    meta_status, wamid = send_mod.enviar_via_meta(
                        telefone=tel_limpo,
                        template=wpp_template,
                        params=params,
                        from_phone=wpp_from_phone,
                    )

                # Status consolidado pro log de envios. Quando os 2 canais
                # (chat + Meta) deram OK, normaliza pra 'accepted' — o painel
                # /wpp conta envios pelo prefixo 'accepted%', e contadores
                # antigos (resumo_campanha, enviados_hoje, etc.) batiam só em
                # 'accepted' exato. Status compostos só sobrevivem em casos
                # de falha parcial (ex.: 'accepted_chat' quando Meta falhou).
                chat_ok = bool(ticket_id) if wpp_usar_chat else True
                meta_ok = ("erro" not in (meta_status or "")) if wpp_usar_meta else True
                if chat_ok and meta_ok:
                    status = "accepted"
                elif meta_ok and not chat_ok:
                    status = "accepted_meta"   # Meta saiu, chat falhou
                elif chat_ok and not meta_ok:
                    status = "accepted_chat"   # Chat OK, Meta falhou
                else:
                    status = meta_status if wpp_usar_meta else "erro:chat_sem_ticket"

                _registrar_envio_log(
                    campanha_id, posto, tel_limpo, nome_paciente, wpp_template,
                    status, wamid,
                    ref_extra=f"falta {id_falta}{posto}",
                    chat_ticket_id=ticket_id,
                )

                envio_falhou = "erro" in (status or "")
                if envio_falhou:
                    falhados.append({"paciente": nome_paciente, "telefone": tel_limpo, "erro": status})
                    # Não escreve em MotivoDesistencia/CRM se nem chegou a sair
                    continue

                # Resolve ticketNumber humano (#107800) por PK no MySQL do chat.
                # /webhooks/chat já devolveu o id (cuid) — só faltava traduzir.
                if ticket_id:
                    ticket_number = _chat_get_ticket_number(ticket_id)

                paciente_info = {"paciente": nome_paciente, "telefone": tel_limpo,
                                 "status": status, "wamid": wamid,
                                 "ticket_id": ticket_id, "ticket_number": ticket_number,
                                 "id_lancamento_servico": int(id_lancamento_servico)}

                # --- (c) append no MotivoDesistencia ---
                # Usa ticketNumber (#107799) — é o que aparece na UI do chat e
                # o que o operador consegue buscar/navegar. Cai pro id interno
                # (cuid) só se o lookup MySQL falhar.
                ticket_str = (f"#{ticket_number}" if ticket_number
                              else (str(ticket_id) if ticket_id else "?"))
                texto_obs = (f"Foi enviada mensagem pelo whatsapp: {numero_saida_str} "
                             f"no ticket-chat: {ticket_str}")
                obs_res = _camila3_append_observacao(
                    id_endereco=id_endereco_posto,
                    id_lancamento_servico=int(id_lancamento_servico),
                    texto=texto_obs,
                    id_usuario=id_usuario_op,
                )
                paciente_info["observacao_ok"] = bool(obs_res.get("ok"))
                if not obs_res.get("ok"):
                    paciente_info["observacao_erro"] = obs_res.get("error", "")[:200]
                    logger.warning("append observacao falhou (idLs=%s): %s",
                                   id_lancamento_servico, obs_res.get("error"))

                # --- (d) cria CRM (motivo ORIENTAÇÃO AO CLIENTE / tipo OUTROS) ---
                # historico no estilo do sp_CRM_Insert (vê manual_crm_api.html)
                historico_crm = (
                    "INCLUSÃO DE CRM\r\n"
                    f"PACIENTE: {nome_paciente}\r\n"
                    f"TELEFONE: {tel_limpo}\r\n"
                    "MOTIVO: ORIENTAÇÃO AO CLIENTE\r\n"
                    "TIPO: OUTROS\r\n"
                    f"PESSOA: {nome_medico_raw}\r\n"
                    "\r\n"
                    f"HISTÓRICO: Falta médica em {data_str} no posto {posto_descricao}. "
                    f"Mensagem WhatsApp enviada pelo {numero_saida_str}, "
                    f"ticket-chat {ticket_str}.\r\n"
                    f"TEXTO ENVIADO: {texto_renderizado}"
                )
                crm_payload = {
                    "id_usuario": int(id_usuario_op),
                    "id_cliente": int(idcli),
                    "id_dependente": int(iddep) if iddep and int(iddep) > 0 else None,
                    "matricula": matricula_base,
                    "titular": titular_resolvido,
                    "paciente": nome_paciente,
                    "id_endereco_cliente": int(id_endereco_cliente or id_endereco_posto or 0),
                    "id_endereco_reclamacao_origem": int(id_endereco_posto or 0),
                    "id_endereco_reclamacao_resposta": int(id_endereco_posto or 0),
                    "id_tipo": CRM_TIPO_OUTROS,
                    "id_motivo": CRM_MOTIVO_ORIENTACAO_AO_CLIENTE,
                    "pessoa": (nome_medico_raw or "")[:200] or None,
                    "tipo_pessoa": "MEDICO",
                    "id_pessoa": int(idmedico),
                    "telefone_whatsapp_cliente": tel_limpo,
                    "data_nascimento": None,
                    "historico": historico_crm,
                    "relato_cliente": (
                        f"Notificação automática de falta médica enviada "
                        f"via WhatsApp ({numero_saida_str})."
                    ),
                }
                crm_res = _camila3_create_crm(crm_payload)
                paciente_info["crm_ok"] = bool(crm_res.get("ok"))
                if crm_res.get("ok"):
                    paciente_info["crm_protocolo"] = crm_res.get("protocolo")
                    paciente_info["crm_id"] = crm_res.get("id_cliente_historico")
                else:
                    paciente_info["crm_erro"] = crm_res.get("error", "")[:200]
                    logger.warning("create CRM falhou (idCliente=%s, idLs=%s): %s",
                                   idcli, id_lancamento_servico, crm_res.get("error"))

                enviados.append(paciente_info)

        return jsonify({
            "ok": True,
            "id_falta": id_falta,
            "campanha_id": campanha_id,
            "total": len(agendamentos),
            "enviados": len(enviados),
            "falhados": len(falhados),
            "sem_telefone": len(sem_telefone),
            "observacoes_ok": sum(1 for e in enviados if e.get("observacao_ok")),
            "crms_ok":        sum(1 for e in enviados if e.get("crm_ok")),
            "numero_saida":   numero_saida_str,
            "detalhes_enviados": enviados,
            "detalhes_falhados": falhados,
            "detalhes_sem_telefone": sem_telefone,
        })
    except Exception as e:
        logger.exception("enviar_wpp falhou (id_falta=%s)", id_falta)
        return jsonify({"error": str(e)[:400]}), 500
