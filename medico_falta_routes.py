"""medico_falta_routes.py — Cadastro de Falta do Médico (Cad_MedicoFalta).

Página em /medico_falta. Permite:
  - Listar faltas já cadastradas (vw_Cad_MedicoFalta) por posto e período
  - Cadastrar nova falta:
      * total (00:00–23:59) ou parcial (HH:MM–HH:MM)
      * INSERT em Cad_MedicoFalta — a vw_Cad_LancamentoProntuarioComDesistencia
        já reflete a falta nos atendimentos via JOIN, sem UPDATE manual
      * Auditoria em Sis_Historico (idTabela=50)
      * Opcional: enviar WhatsApp `notificacao_de_falta_do_medico` aos pacientes
        afetados (template Meta) — pendente, quando user passar a API
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
                    "matricula": (r[1] or "").strip() if r[1] else "",
                    "paciente": (r[2] or "").strip(),
                    "idade": int(r[3]) if r[3] is not None else None,
                    "telefone_raw": (r[4] or "").strip() if r[4] else "",
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
                    "telefone_raw": (r[3] or "").strip() if r[3] else "",
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
