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
import logging
from datetime import date, datetime, timedelta

import pyodbc
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


# Mapeia letra do posto → idEndereco e descrição (puxado de cad_endereco em runtime)
def _posto_endereco(con: pyodbc.Connection, letra: str) -> tuple[int | None, str]:
    cur = con.cursor()
    cur.execute(
        "SELECT TOP 1 idEndereco, descricao FROM cad_endereco "
        "WHERE codigo = ? AND AtendimentoAtivoPosto = 1",
        letra.strip().upper(),
    )
    row = cur.fetchone()
    if not row:
        return None, ""
    return int(row[0]), (row[1] or "").strip()


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


def _get_or_create_campanha_falta_medico() -> dict:
    """Garante a campanha de log para faltas. Retorna dict com config viva
    (template, from_user_id, queue_id, enviar_chat, enviar_meta) — admin
    pode alterar via painel sem mexer no código.
    """
    import sqlite3
    db_path = os.getenv("WAPP_CTRL_DB", "/opt/camim-auth/whatsapp_cobranca.db")
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM campanhas WHERE modo_envio = ? LIMIT 1",
            (WPP_MODO_ENVIO_FALTA,),
        )
        row = cur.fetchone()
        if not row:
            from datetime import datetime as _dt
            agora = _dt.now().isoformat(timespec="seconds")
            cur.execute(
                """INSERT INTO campanhas
                      (nome, template, postos, dias_atraso_min, dias_atraso_max,
                       incluir_cancelados, sem_email, sexo, idade_min, idade_max,
                       nao_recorrente, hora_inicio, hora_fim, dias_semana,
                       intervalo_dias, ativa, created_at, updated_at,
                       modo_envio, dias_ref_min, dias_ref_max,
                       from_user_id, enviar_chat, enviar_meta, queue_id)
                   VALUES (?, ?, '[]', 0, NULL,
                           0, 0, NULL, NULL, NULL,
                           0, '00:00', '23:59', '0,1,2,3,4,5,6',
                           1, 1, ?, ?,
                           ?, 0, NULL,
                           ?, 1, 1, NULL)""",
                (WPP_CAMPANHA_NOME, WPP_TEMPLATE_DEFAULT_AO_CRIAR, agora, agora,
                 WPP_MODO_ENVIO_FALTA, WPP_FROM_USER_DEFAULT_AO_CRIAR),
            )
            conn.commit()
            cur.execute("SELECT * FROM campanhas WHERE id = ?", (cur.lastrowid,))
            row = cur.fetchone()
        return {
            "id": int(row["id"]),
            "template": row["template"],
            "from_user_id": row["from_user_id"] or None,
            "queue_id": row["queue_id"] or None,  # NULL = sem fila → Camila atende
            "enviar_chat": bool(row["enviar_chat"]),
            "enviar_meta": bool(row["enviar_meta"]),
        }


def _registrar_envio_log(campanha_id: int, posto: str, telefone: str,
                          paciente: str, template: str, status: str,
                          wamid: str | None, ref_extra: str = "") -> None:
    """Insere em whatsapp_cobranca.db.envios — adapta o pattern existente."""
    import sqlite3
    db_path = os.getenv("WAPP_CTRL_DB", "/opt/camim-auth/whatsapp_cobranca.db")
    with sqlite3.connect(db_path) as conn:
        from datetime import datetime as _dt
        conn.execute(
            """INSERT INTO envios
                  (campanha_id, posto, telefone, idreceita, matricula, nome,
                   ref, valor, venc, dias_atraso, template, status, wamid, enviado_em)
               VALUES (?, ?, ?, '', '', ?, ?, '', '', NULL, ?, ?, ?, ?)""",
            (campanha_id, posto, telefone, paciente, ref_extra,
             template, status, wamid, _dt.now().isoformat(timespec="seconds")),
        )
        conn.commit()


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
            cur.execute(
                """SELECT TOP 200 idMedicoFalta, idMedico, Medico, Especialidade,
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

    try:
        with _conn_for_posto(posto) as con:
            cur = con.cursor()
            # vw_Cad_LancamentoProntuarioComDesistencia é pesada, mas tem tudo que precisamos.
            # NOLOCK + READ UNCOMMITTED para acelerar.
            cur.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
            # CLAUDE.md: views da CAMIM usam SET DATEFORMAT dmy → passar data como DD/MM/YYYY string
            d_str = d.strftime("%d/%m/%Y")
            d_next = (d + timedelta(days=1)).strftime("%d/%m/%Y")
            cur.execute(
                """SELECT idLancamentoServico, Matricula, Paciente, idadePaciente,
                          TelefoneResidencial, HoraPrevistaConsulta, Servico, Especialidade
                     FROM vw_Cad_LancamentoProntuarioComDesistencia WITH (NOLOCK)
                     WHERE idMedico = ?
                       AND DataConsulta >= ?
                       AND DataConsulta <  ?
                       AND Desistencia = 0
                       AND HoraPrevistaConsulta >= ?
                       AND HoraPrevistaConsulta <= ?
                     ORDER BY HoraPrevistaConsulta""",
                idmedico, d_str, d_next, hora_ini, hora_fim,
            )
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
    especialidade = (data.get("especialidade") or "").strip()[:50] or None
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

            # Anti-duplicidade: não permite 2 faltas ativas para o mesmo médico no mesmo dia
            cur.execute(
                """SELECT TOP 1 idFalta FROM Cad_MedicoFalta
                    WHERE idMedico = ? AND DataFalta = ? AND Desativado = 0""",
                idmedico, data_falta,
            )
            existente = cur.fetchone()
            if existente:
                return jsonify({
                    "error": (
                        f"Já existe falta cadastrada (idFalta={int(existente[0])}) "
                        f"para esse médico em {data_falta.strftime('%d/%m/%Y')}. "
                        f"Edite ou desative a existente antes de criar outra."
                    ),
                    "duplicada": True,
                    "id_falta_existente": int(existente[0]),
                }), 409

            # Conta pacientes agendados no intervalo (para QuantidadePacienteAgendado)
            cur.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
            d_str = data_falta.strftime("%d/%m/%Y")
            d_next = (data_falta + timedelta(days=1)).strftime("%d/%m/%Y")
            cur.execute(
                """SELECT COUNT(*)
                     FROM vw_Cad_LancamentoProntuarioComDesistencia WITH (NOLOCK)
                     WHERE idMedico = ?
                       AND DataConsulta >= ? AND DataConsulta <  ?
                       AND Desistencia = 0
                       AND HoraPrevistaConsulta >= ? AND HoraPrevistaConsulta <= ?""",
                idmedico, d_str, d_next, hora_ini, hora_fim,
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

            # Agendamentos afetados (para o frontend disparar WhatsApp depois)
            cur.execute(
                """SELECT idLancamentoServico, Paciente, HoraPrevistaConsulta,
                          TelefoneResidencial, Especialidade
                     FROM vw_Cad_LancamentoProntuarioComDesistencia WITH (NOLOCK)
                     WHERE idMedico = ?
                       AND DataConsulta >= ? AND DataConsulta <  ?
                       AND Desistencia = 0
                       AND HoraPrevistaConsulta >= ? AND HoraPrevistaConsulta <= ?
                     ORDER BY HoraPrevistaConsulta""",
                idmedico, d_str, d_next, hora_ini, hora_fim,
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
                          mfm.Motivo, mf.Observacao
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

            # 2) Posto: descrição
            id_endereco_posto, posto_descricao = _posto_endereco(con, posto)

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
            cur.execute(
                """SELECT idLancamentoServico, idCliente, idDependente, Paciente,
                          HoraPrevistaConsulta, TelefoneResidencial, Especialidade
                     FROM vw_Cad_LancamentoProntuarioComDesistencia WITH (NOLOCK)
                     WHERE idMedico = ?
                       AND DataConsulta >= ? AND DataConsulta <  ?
                       AND Desistencia = 0
                       AND HoraPrevistaConsulta >= ? AND HoraPrevistaConsulta <= ?""",
                idmedico, d_str, d_next, hi, hf,
            )
            agendamentos = cur.fetchall()

        # Garante a campanha (idempotente). Toda config (template/from_user/queue_id/
        # enviar_chat/enviar_meta) vem dela — admin pode alterar via painel sem código.
        camp = _get_or_create_campanha_falta_medico()
        campanha_id = camp["id"]
        wpp_template = camp["template"]
        wpp_from_user = camp["from_user_id"]
        wpp_queue_id = camp["queue_id"]
        wpp_usar_chat = camp["enviar_chat"]
        wpp_usar_meta = camp["enviar_meta"]

        # 6) Para cada paciente: busca NomeSocial + TelefoneWhatsApp em cad_cliente OU cad_clientedependente,
        #    monta params, dispara, registra
        with _conn_for_posto(posto) as con:
            cur = con.cursor()
            enviados, falhados, sem_telefone = [], [], []
            for ag in agendamentos:
                _ils, idcli, iddep, paciente_view, hora, tel_view, especialidade = ag
                # Resolve dados do cliente/dep
                if iddep and int(iddep) > 0:
                    cur.execute(
                        "SELECT TOP 1 NomeSocial, Nome, TelefoneWhatsApp, TelefoneCelular "
                        "FROM cad_clientedependente WHERE idCliente = ? AND idDependente = ?",
                        int(idcli), int(iddep),
                    )
                else:
                    cur.execute(
                        "SELECT TOP 1 NomeSocial, Nome, TelefoneWhatsApp, TelefoneCelular "
                        "FROM cad_cliente WHERE idCliente = ?",
                        int(idcli),
                    )
                cli = cur.fetchone()
                if not cli:
                    falhados.append({"paciente": paciente_view, "erro": "cliente não encontrado"})
                    continue
                nome_social, nome_pad, tel_wpp, tel_cel = cli
                nome_paciente = (nome_social or nome_pad or paciente_view or "").strip()
                # Primeiro tenta TelefoneWhatsApp (campo dedicado); cai pra Celular se vazio
                tel_limpo = _limpar_telefone(tel_wpp) or _limpar_telefone(tel_cel)
                if not tel_limpo:
                    sem_telefone.append({"paciente": nome_paciente})
                    _registrar_envio_log(
                        campanha_id, posto, "", nome_paciente, wpp_template,
                        "erro:sem_telefone", None,
                        ref_extra=f"falta {id_falta}",
                    )
                    continue
                # Parâmetros do template `aviso_de_fechamento_de_agenda`
                # (template recriado em 2026-05-01 com nomes minúsculos):
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
                status, wamid = send_mod.enviar(
                    telefone=tel_limpo,
                    nome=nome_paciente,
                    template=wpp_template,
                    params=params,
                    dry_run=False,
                    queue_id=wpp_queue_id,           # da campanha (NULL = Camila atende)
                    from_user_id=wpp_from_user,      # da campanha
                    usar_chat=wpp_usar_chat,         # da campanha
                    usar_meta=wpp_usar_meta,         # da campanha
                )
                _registrar_envio_log(
                    campanha_id, posto, tel_limpo, nome_paciente, wpp_template,
                    status, wamid, ref_extra=f"falta {id_falta}",
                )
                if "erro" in (status or ""):
                    falhados.append({"paciente": nome_paciente, "telefone": tel_limpo, "erro": status})
                else:
                    enviados.append({"paciente": nome_paciente, "telefone": tel_limpo, "status": status, "wamid": wamid})

        return jsonify({
            "ok": True,
            "id_falta": id_falta,
            "campanha_id": campanha_id,
            "total": len(agendamentos),
            "enviados": len(enviados),
            "falhados": len(falhados),
            "sem_telefone": len(sem_telefone),
            "detalhes_enviados": enviados,
            "detalhes_falhados": falhados,
            "detalhes_sem_telefone": sem_telefone,
        })
    except Exception as e:
        logger.exception("enviar_wpp falhou (id_falta=%s)", id_falta)
        return jsonify({"error": str(e)[:400]}), 500
