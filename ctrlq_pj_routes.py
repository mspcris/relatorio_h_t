"""ctrlq_pj_routes.py — Justificativas de médico SEM contrato PJ (KPI Médicos · Qualidade).

Contexto (2026-08-17): a página `ctrlq_relatorio.html` lista os médicos com
contrato PJ. O Cristiano quer a aba oposta — quem NÃO tem PJ — e exigir do
gerente do posto uma justificativa MENSAL do porquê aquele médico ainda não
tem contrato. A tela mostra a última justificativa e permite ver as
anteriores.

Onde mora o dado: tabela `Cad_EspecialidadeJustificativaPJ` no SQL Server de
CADA posto (criada pelo Janderson; existe nos 13 postos, verificado):

    idEspecialidadeJustificativaPJ  int identity
    idEspecialidade                 int   → Cad_Especialidade (médico × especialidade no posto)
    DataHora                        datetime
    idUsuario                       int   → sis_usuario do posto
    Justificativa                   varchar(250)
    Desativado                      bit   default 0

A justificativa é POR MÉDICO na prática (a pergunta é "por que o Dr. X não
tem PJ"), mas a chave física é `idEspecialidade`. Por isso:
  * na LEITURA agrupamos por `idMedico` (join em Cad_Especialidade) — tanto faz
    em qual especialidade do médico a justificativa foi pendurada;
  * na ESCRITA gravamos no `idEspecialidade` que a página mandou (o da linha
    dedupada do KPI); se ela não mandar, pega a especialidade ativa mais
    antiga do médico.

REGRA — toda escrita no CAMIM leva `Sis_Historico` com o idUsuario de quem
clicou (resolvido pelo `login_campinho` do usuário do KPI na `sis_usuario`
do posto). A tabela nova NÃO está em `Sis_HistoricoTabela`; a auditoria vai
em idTabela=53 (`Cad_Especialidade`, "Quadro de especialidade") com
`id=idEspecialidade`, que é a entidade a que a justificativa pertence.
Comando 1 = Inclusão, 3 = Exclusão (desativar).

Rotas (todas exigem sessão do KPI + posto dentro do ACL do usuário):
  GET  /api/ctrlq/pj/justificativas?postos=A,B      lista ativas (+ pode_escrever por posto)
  POST /api/ctrlq/pj/justificativas                 {posto, id_medico|crm, id_especialidade?, justificativa}
  POST /api/ctrlq/pj/justificativas/<id>/desativar  {posto}  — só o autor ou admin
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import pyodbc
from flask import Blueprint, jsonify, request

from medico_novo_routes import (
    ERR_SEM_VINCULO,
    ID_COMANDO_EXCLUSAO,
    ID_COMANDO_INCLUSAO,
    ID_TABELA_CAD_ESPECIALIDADE,
    _audit,
    _conn_for_posto,
    _resolver_idusuario_no_posto,
)

logger = logging.getLogger(__name__)

ctrlq_pj_bp = Blueprint("ctrlq_pj_bp", __name__)

MAX_JUSTIFICATIVA = 250   # varchar(250) na tabela
MIN_JUSTIFICATIVA = 10    # menos que isso não explica nada ("ok", "123456")
_MAX_WORKERS = 13


@contextmanager
def _conexao(posto: str):
    """`with` do pyodbc só faz commit/rollback — não fecha. Fecha aqui, sempre
    (mesma lição do fd leak de 2026-08-10: não confiar no GC)."""
    con = _conn_for_posto(posto)
    try:
        yield con
    finally:
        try:
            con.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Sessão / ACL
# ---------------------------------------------------------------------------

def _sessao():
    """(email, postos_set, login_campinho, is_admin) ou (None, None, None, False)."""
    from auth_routes import decode_user
    from auth_db import SessionLocal, get_user_by_email
    email, postos = decode_user()
    if not email:
        return None, None, None, False
    db = SessionLocal()
    try:
        u = get_user_by_email(db, email)
        if not u:
            return None, None, None, False
        login = (getattr(u, "login_campinho", None) or "").strip() or None
        return email, set(postos or []), login, bool(getattr(u, "is_admin", False))
    finally:
        db.close()


def _postos_pedidos(acl: set) -> list:
    """`?postos=A,B` ∩ ACL. Sem parâmetro = todos do ACL."""
    raw = (request.args.get("postos") or "").strip().upper()
    if not raw:
        return sorted(acl)
    pedidos = {p.strip() for p in raw.split(",") if p.strip()}
    return sorted(p for p in pedidos if len(p) == 1 and p in acl)


# ---------------------------------------------------------------------------
# Leitura
# ---------------------------------------------------------------------------

_SQL_LISTA = """
SELECT j.idEspecialidadeJustificativaPJ, j.idEspecialidade, e.idMedico,
       m.nome, m.crm, e.Especialidade,
       j.DataHora, j.idUsuario, u.Usuario, u.Nome AS UsuarioNome,
       j.Justificativa,
       ISNULL(m.PessoaJuridica, 0) AS PessoaJuridica
FROM Cad_EspecialidadeJustificativaPJ j WITH (NOLOCK)
LEFT JOIN Cad_Especialidade e WITH (NOLOCK) ON e.idEspecialidade = j.idEspecialidade
LEFT JOIN cad_medico m        WITH (NOLOCK) ON m.idMedico = e.idMedico
LEFT JOIN sis_usuario u       WITH (NOLOCK) ON u.idUsuario = j.idUsuario
WHERE j.Desativado = 0
ORDER BY j.DataHora DESC, j.idEspecialidadeJustificativaPJ DESC
"""


def _row_to_dict(r) -> dict:
    dh = r.DataHora
    return {
        "id": int(r.idEspecialidadeJustificativaPJ),
        "id_especialidade": int(r.idEspecialidade),
        "id_medico": int(r.idMedico) if r.idMedico is not None else None,
        "medico": (r.nome or "").strip(),
        "crm": (str(r.crm) if r.crm is not None else "").strip(),
        "especialidade": (r.Especialidade or "").strip(),
        "data_hora": dh.strftime("%Y-%m-%dT%H:%M:%S") if isinstance(dh, datetime) else None,
        "competencia": dh.strftime("%Y-%m") if isinstance(dh, datetime) else None,
        "id_usuario": int(r.idUsuario) if r.idUsuario is not None else None,
        "usuario": (r.Usuario or "").strip(),
        "usuario_nome": (r.UsuarioNome or "").strip(),
        "justificativa": (r.Justificativa or "").strip(),
        "medico_pj_agora": bool(r.PessoaJuridica),
    }


def _ler_posto(posto: str, login_campinho: str | None) -> dict:
    """Lê as justificativas ativas de UM posto + resolve se o usuário pode escrever nele."""
    with _conexao(posto) as con:
        id_usuario = _resolver_idusuario_no_posto(con, login_campinho) if login_campinho else None
        cur = con.cursor()
        cur.execute(_SQL_LISTA)
        itens = [_row_to_dict(r) for r in cur.fetchall()]
    return {
        "itens": itens,
        "pode_escrever": id_usuario is not None,
        "id_usuario": id_usuario,
    }


@ctrlq_pj_bp.get("/api/ctrlq/pj/justificativas")
def api_listar():
    email, acl, login, is_admin = _sessao()
    if not email:
        return jsonify({"error": "unauthorized"}), 401
    postos = _postos_pedidos(acl)
    out, erros = {}, {}
    if postos:
        with ThreadPoolExecutor(max_workers=min(_MAX_WORKERS, len(postos))) as pool:
            futs = {pool.submit(_ler_posto, p, login): p for p in postos}
            for f in as_completed(futs):
                p = futs[f]
                try:
                    out[p] = f.result()
                except Exception as e:  # posto fora do ar NÃO pode virar "0 justificativas" calado
                    logger.warning("justificativas PJ: posto %s falhou: %s", p, e)
                    erros[p] = str(e)[:200]
    return jsonify({
        "postos": out,
        "erros": erros,
        "login_campinho": login,
        "is_admin": is_admin,
        "sem_vinculo": login is None,
        "gerado_em": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
    })


# ---------------------------------------------------------------------------
# Escrita
# ---------------------------------------------------------------------------

def _resolver_medico(cur, id_medico, crm):
    """Devolve (idMedico, nome, PessoaJuridica) ou None. Por id, senão por CRM
    (tem que ser exatamente 1 médico ativo com aquele CRM)."""
    if id_medico:
        cur.execute(
            "SELECT idMedico, nome, ISNULL(PessoaJuridica,0) FROM cad_medico WHERE idMedico = ?",
            int(id_medico),
        )
        r = cur.fetchone()
        return (int(r[0]), (r[1] or "").strip(), bool(r[2])) if r else None
    crm = (crm or "").strip()
    if not crm:
        return None
    cur.execute(
        "SELECT idMedico, nome, ISNULL(PessoaJuridica,0) FROM cad_medico "
        "WHERE LTRIM(RTRIM(CAST(crm AS varchar(50)))) = ? AND Desativado = 0",
        crm,
    )
    rows = cur.fetchall()
    if len(rows) != 1:
        return None
    r = rows[0]
    return (int(r[0]), (r[1] or "").strip(), bool(r[2]))


def _resolver_especialidade(cur, id_medico: int, id_especialidade) -> int | None:
    """Garante que idEspecialidade pertence ao médico; sem id, pega a ativa mais antiga."""
    if id_especialidade:
        cur.execute(
            "SELECT idEspecialidade FROM Cad_Especialidade WHERE idEspecialidade = ? AND idMedico = ?",
            int(id_especialidade), id_medico,
        )
        r = cur.fetchone()
        if r:
            return int(r[0])
    cur.execute(
        "SELECT TOP 1 idEspecialidade FROM Cad_Especialidade "
        "WHERE idMedico = ? AND Desativado = 0 ORDER BY Temporario ASC, idEspecialidade ASC",
        id_medico,
    )
    r = cur.fetchone()
    return int(r[0]) if r else None


@ctrlq_pj_bp.post("/api/ctrlq/pj/justificativas")
def api_criar():
    email, acl, login, is_admin = _sessao()
    if not email:
        return jsonify({"error": "unauthorized"}), 401
    if not login:
        return jsonify({"error": ERR_SEM_VINCULO, "sem_vinculo": True}), 403

    body = request.get_json(silent=True) or {}
    posto = (body.get("posto") or "").strip().upper()
    if len(posto) != 1 or posto not in acl:
        return jsonify({"error": f"posto '{posto}' fora do seu ACL"}), 400

    texto = " ".join((body.get("justificativa") or "").split())
    if len(texto) < MIN_JUSTIFICATIVA:
        return jsonify({"error": f"Justificativa muito curta (mínimo {MIN_JUSTIFICATIVA} caracteres)."}), 400
    if len(texto) > MAX_JUSTIFICATIVA:
        return jsonify({"error": f"Justificativa com {len(texto)} caracteres; o máximo é {MAX_JUSTIFICATIVA}."}), 400

    try:
        with _conexao(posto) as con:
            con.autocommit = False
            cur = con.cursor()
            id_usuario = _resolver_idusuario_no_posto(con, login)
            if not id_usuario:
                return jsonify({
                    "error": (f"Seu Login Campinho `{login}` não existe (ou está desativado) na "
                              f"sis_usuario do posto {posto}. Sem isso a justificativa ficaria sem "
                              f"responsável. Procure o Cristiano."),
                    "sem_vinculo_posto": True,
                }), 403

            med = _resolver_medico(cur, body.get("id_medico"), body.get("crm"))
            if not med:
                return jsonify({"error": "Médico não encontrado no cadastro deste posto."}), 404
            id_medico, nome_medico, pj = med
            if pj:
                # O KPI é foto de ontem; se hoje o médico já é PJ, não há o que justificar.
                return jsonify({
                    "error": f"{nome_medico} já consta como PJ no cadastro do posto {posto} — não precisa de justificativa.",
                    "medico_pj_agora": True,
                }), 409

            id_esp = _resolver_especialidade(cur, id_medico, body.get("id_especialidade"))
            if not id_esp:
                return jsonify({"error": "Médico sem especialidade ativa no posto; não há onde pendurar a justificativa."}), 409

            cur.execute(
                "INSERT INTO Cad_EspecialidadeJustificativaPJ (idEspecialidade, DataHora, idUsuario, Justificativa, Desativado) "
                "OUTPUT INSERTED.idEspecialidadeJustificativaPJ, INSERTED.DataHora "
                "VALUES (?, GETDATE(), ?, ?, 0)",
                id_esp, id_usuario, texto,
            )
            r = cur.fetchone()
            novo_id, dh = int(r[0]), r[1]
            _audit(
                con, id_esp, ID_TABELA_CAD_ESPECIALIDADE, ID_COMANDO_INCLUSAO, id_usuario,
                f"Justificativa PJ via RH&T (idJust={novo_id}, medico={id_medico}): {texto}",
            )
            con.commit()

        logger.info("justificativa PJ criada: posto=%s id=%s medico=%s por %s", posto, novo_id, id_medico, email)
        return jsonify({
            "ok": True,
            "item": {
                "id": novo_id,
                "id_especialidade": id_esp,
                "id_medico": id_medico,
                "medico": nome_medico,
                "data_hora": dh.strftime("%Y-%m-%dT%H:%M:%S") if isinstance(dh, datetime) else None,
                "competencia": dh.strftime("%Y-%m") if isinstance(dh, datetime) else None,
                "id_usuario": id_usuario,
                "usuario": login,
                "justificativa": texto,
                "posto": posto,
            },
        }), 201
    except pyodbc.Error as e:
        logger.exception("criar justificativa PJ falhou (posto %s)", posto)
        return jsonify({"error": f"Erro no SQL Server do posto {posto}: {str(e)[:200]}"}), 500


@ctrlq_pj_bp.post("/api/ctrlq/pj/justificativas/<int:jid>/desativar")
def api_desativar(jid: int):
    email, acl, login, is_admin = _sessao()
    if not email:
        return jsonify({"error": "unauthorized"}), 401
    if not login:
        return jsonify({"error": ERR_SEM_VINCULO, "sem_vinculo": True}), 403
    body = request.get_json(silent=True) or {}
    posto = (body.get("posto") or "").strip().upper()
    if len(posto) != 1 or posto not in acl:
        return jsonify({"error": f"posto '{posto}' fora do seu ACL"}), 400
    try:
        with _conexao(posto) as con:
            con.autocommit = False
            cur = con.cursor()
            id_usuario = _resolver_idusuario_no_posto(con, login)
            if not id_usuario:
                return jsonify({"error": f"Login Campinho `{login}` não existe na sis_usuario do posto {posto}."}), 403
            cur.execute(
                "SELECT idEspecialidade, idUsuario, Justificativa FROM Cad_EspecialidadeJustificativaPJ "
                "WHERE idEspecialidadeJustificativaPJ = ? AND Desativado = 0",
                jid,
            )
            r = cur.fetchone()
            if not r:
                return jsonify({"error": "Justificativa não encontrada (ou já desativada)."}), 404
            id_esp, autor, texto = int(r[0]), (int(r[1]) if r[1] is not None else None), (r[2] or "")
            if autor != id_usuario and not is_admin:
                return jsonify({"error": "Só quem escreveu a justificativa (ou um admin) pode desativá-la."}), 403
            cur.execute(
                "UPDATE Cad_EspecialidadeJustificativaPJ SET Desativado = 1 WHERE idEspecialidadeJustificativaPJ = ?",
                jid,
            )
            _audit(
                con, id_esp, ID_TABELA_CAD_ESPECIALIDADE, ID_COMANDO_EXCLUSAO, id_usuario,
                f"Justificativa PJ desativada via RH&T (idJust={jid}): {texto}",
            )
            con.commit()
        logger.info("justificativa PJ desativada: posto=%s id=%s por %s", posto, jid, email)
        return jsonify({"ok": True, "id": jid})
    except pyodbc.Error as e:
        logger.exception("desativar justificativa PJ falhou (posto %s)", posto)
        return jsonify({"error": f"Erro no SQL Server do posto {posto}: {str(e)[:200]}"}), 500
