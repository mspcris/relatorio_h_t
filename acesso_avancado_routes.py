"""acesso_avancado_routes.py — KPI "Acesso Avançado".

Lista, por posto, quais usuários do CAMIM possuem flags sensíveis de acesso
em `sis_usuario`:

  - verAbaAcessoAvancado                      → aba "Acesso Avançado" (cria
                                                superusuários, libera/bloqueia
                                                qualquer opção do sistema).
                                                SOMENTE superusuários deveriam ter.
  - FuncionarioVerSalario                     → vê o salário no cadastro de funcionário.
  - verPlanoDeContaSalarioContasPagar         → vê salário de funcionários no filtro
                                                do Contas a Pagar.
  - verPlanoDeContaSalarioContasPagarMedico   → vê o salário do médico.
  - AcessoLimparDataFim                        → limpa Data Fim da agenda médica pelo F3.
  - EspecialidadeAlterarDataFim                → limpa Data Fim pelo cadastro de
                                                especialidade (Ctrl-Q).

ACESSO RESTRITO: a página só é acessível a usuários com `all_pages=True`
(o gate é feito tanto no render da página em app.py quanto aqui na API).

Leitura (SELECT) + revogação de flags. A revogação (`/api/acesso_avancado/revogar`)
faz `UPDATE sis_usuario SET <flag> = 0` e SEMPRE registra em Sis_Historico, na
mesma transação, assinando com o `login_campinho` vinculado ao admin — igual ao
restante do sistema (medico_novo_routes). Sem vínculo, a escrita é bloqueada.
"""
import os
import logging

import pyodbc
from flask import Blueprint, jsonify, request

logger = logging.getLogger(__name__)

acesso_avancado_bp = Blueprint("acesso_avancado_bp", __name__)

ODBC_DRIVER = os.getenv("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")

# Colunas-bit de sis_usuario expostas pelo KPI, na ordem da grade.
# (chave JSON, rótulo curto pra UI)
FLAGS = [
    ("verAbaAcessoAvancado",                    "Acesso Avançado"),
    ("FuncionarioVerSalario",                   "Salário (cad. funcionário)"),
    ("verPlanoDeContaSalarioContasPagar",       "Salário func. (Contas a Pagar)"),
    ("verPlanoDeContaSalarioContasPagarMedico", "Salário médico"),
    ("AcessoLimparDataFim",                     "Limpar Data Fim (F3)"),
    ("EspecialidadeAlterarDataFim",             "Limpar Data Fim (Ctrl-Q)"),
]

_SELECT = """
    SELECT idUsuario,
           Usuario,
           Nome,
           ISNULL(Desativado, 0)                                 AS Desativado,
           ISNULL(verAbaAcessoAvancado, 0)                       AS verAbaAcessoAvancado,
           ISNULL(FuncionarioVerSalario, 0)                      AS FuncionarioVerSalario,
           ISNULL(verPlanoDeContaSalarioContasPagar, 0)          AS verPlanoDeContaSalarioContasPagar,
           ISNULL(verPlanoDeContaSalarioContasPagarMedico, 0)    AS verPlanoDeContaSalarioContasPagarMedico,
           ISNULL(AcessoLimparDataFim, 0)                        AS AcessoLimparDataFim,
           ISNULL(EspecialidadeAlterarDataFim, 0)                AS EspecialidadeAlterarDataFim
      FROM sis_usuario WITH (NOLOCK)
     ORDER BY Nome
"""


# ---------------------------------------------------------------------------
# Helpers de autenticação / conexão
# ---------------------------------------------------------------------------

def _check_user():
    """Retorna (email, postos_set, all_pages, login_campinho) se autenticado,
    senão (None, None, False, None).

    `login_campinho` é a string Usuario do sis_usuario vinculada ao admin do KPI
    (ex.: 'cristiano2.a'); obrigatória para assinar o Sis_Historico ao escrever.

    Importação tardia para evitar import circular com app.py / auth_routes.
    """
    from auth_routes import decode_user
    from auth_db import SessionLocal, get_user_by_email
    email, postos = decode_user()
    if not email:
        return None, None, False, None
    db = SessionLocal()
    try:
        u = get_user_by_email(db, email)
        if not u:
            return None, None, False, None
        all_pages = bool(getattr(u, "all_pages", True))
        login_campinho = (getattr(u, "login_campinho", None) or "").strip() or None
        return email, set(postos or []), all_pages, login_campinho
    finally:
        db.close()


def _conn_for_posto(posto: str) -> pyodbc.Connection:
    p = (posto or "").strip().upper()
    if not p or len(p) != 1:
        raise ValueError("posto inválido")
    host = os.getenv(f"DB_HOST_{p}", "").strip().strip("'\"")
    base = os.getenv(f"DB_BASE_{p}", "").strip().strip("'\"")
    if not host or not base:
        raise ValueError(f"posto {p} sem configuração no .env")
    user = os.getenv(f"DB_USER_{p}", "").strip().strip("'\"")
    pwd  = os.getenv(f"DB_PASSWORD_{p}", "").strip().strip("'\"")
    port = os.getenv(f"DB_PORT_{p}", "1433").strip().strip("'\"") or "1433"
    cs = (
        f"DRIVER={{{ODBC_DRIVER}}};"
        f"SERVER=tcp:{host},{port};DATABASE={base};"
        f"Encrypt=no;TrustServerCertificate=yes;"
        f"Connection Timeout=10;"
    )
    if user:
        cs += f"UID={user};PWD={pwd}"
    else:
        cs += "Trusted_Connection=yes"
    return pyodbc.connect(cs, timeout=15)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@acesso_avancado_bp.get("/api/acesso_avancado/postos")
def api_postos():
    """Postos do ACL do usuário que têm configuração de banco, com nome legível.
    Também devolve o catálogo de FLAGS pra UI montar os cabeçalhos."""
    email, postos, all_pages, _ = _check_user()
    if not email:
        return jsonify({"error": "unauthorized"}), 401
    if not all_pages:
        return jsonify({"error": "forbidden"}), 403
    try:
        from alarmes_db import POSTOS_NOMES
    except Exception:
        POSTOS_NOMES = {}
    out = []
    for p in sorted(postos or []):
        if not os.getenv(f"DB_HOST_{p}"):
            continue
        out.append({"letra": p, "nome": POSTOS_NOMES.get(p, p)})
    return jsonify({
        "postos": out,
        "flags": [{"key": k, "label": lbl} for k, lbl in FLAGS],
    })


@acesso_avancado_bp.get("/api/acesso_avancado/data")
def api_data():
    """Usuários e flags de acesso de UM posto (`?posto=X`).

    Front-end chama uma vez por posto (em paralelo) para montar uma grade por posto.
    """
    email, postos, all_pages, _ = _check_user()
    if not email:
        return jsonify({"error": "unauthorized"}), 401
    if not all_pages:
        return jsonify({"error": "forbidden"}), 403

    posto = (request.args.get("posto") or "").strip().upper()
    if not posto or len(posto) != 1:
        return jsonify({"error": "posto inválido"}), 400
    if posto not in (postos or set()):
        return jsonify({"error": f"posto {posto} fora do seu acesso"}), 403
    if not os.getenv(f"DB_HOST_{posto}"):
        return jsonify({"error": f"posto {posto} sem configuração"}), 400

    flag_keys = [k for k, _ in FLAGS]
    try:
        with _conn_for_posto(posto) as con:
            cur = con.cursor()
            cur.execute(_SELECT)
            usuarios = []
            for r in cur.fetchall():
                flags = {k: bool(getattr(r, k)) for k in flag_keys}
                usuarios.append({
                    "idUsuario":  int(r.idUsuario),
                    "usuario":    (r.Usuario or "").strip(),
                    "nome":       (r.Nome or "").strip(),
                    "desativado": bool(r.Desativado),
                    "flags":      flags,
                    "tem_algum":  any(flags.values()),
                })
    except Exception as e:
        logger.exception("acesso_avancado falhou no posto %s", posto)
        return jsonify({"error": str(e)[:300]}), 500

    return jsonify({"posto": posto, "usuarios": usuarios})


@acesso_avancado_bp.post("/api/acesso_avancado/revogar")
def api_revogar():
    """Revoga UMA flag sensível de UM usuário num posto: `UPDATE sis_usuario
    SET <flag> = 0 WHERE idUsuario = ?`.

    Registra OBRIGATORIAMENTE em Sis_Historico (idTabela=sis_usuario,
    idComando=Edição), na mesma transação, assinado pelo idUsuario vinculado ao
    admin (login_campinho). Sem vínculo → 403, nada é escrito.

    Body JSON: {posto, idUsuario, flag}. `flag` é validada contra a whitelist
    FLAGS antes de entrar no SQL (o nome da coluna é interpolado, então a
    whitelist é a barreira de injeção).
    """
    email, postos, all_pages, login_campinho = _check_user()
    if not email:
        return jsonify({"error": "unauthorized"}), 401
    if not all_pages:
        return jsonify({"error": "forbidden"}), 403

    data = request.get_json(silent=True) or {}
    posto = (data.get("posto") or "").strip().upper()
    flag = (data.get("flag") or "").strip()

    if not posto or len(posto) != 1:
        return jsonify({"error": "posto inválido"}), 400
    if posto not in (postos or set()):
        return jsonify({"error": f"posto {posto} fora do seu acesso"}), 403
    if not os.getenv(f"DB_HOST_{posto}"):
        return jsonify({"error": f"posto {posto} sem configuração"}), 400

    if flag not in {k for k, _ in FLAGS}:
        return jsonify({"error": "flag inválida"}), 400

    try:
        id_usuario_alvo = int(data.get("idUsuario"))
    except (TypeError, ValueError):
        return jsonify({"error": "idUsuario inválido"}), 400

    # Vínculo é obrigatório: sem ele não há quem assinar o Sis_Historico.
    from medico_novo_routes import (
        _audit, _resolver_idusuario_no_posto,
        ID_TABELA_SIS_USUARIO, ID_COMANDO_EDICAO, ERR_SEM_VINCULO,
    )
    if not login_campinho:
        return jsonify({"error": ERR_SEM_VINCULO, "sem_vinculo": True}), 403

    try:
        with _conn_for_posto(posto) as con:
            id_usuario_op = _resolver_idusuario_no_posto(con, login_campinho)
            if not id_usuario_op:
                return jsonify({"error": ERR_SEM_VINCULO, "sem_vinculo_posto": True}), 403

            cur = con.cursor()
            # Confirma alvo e captura estado atual (para detalhe e idempotência).
            # `flag` já validada contra a whitelist → seguro interpolar.
            cur.execute(
                f"SELECT TOP 1 Usuario, Nome, ISNULL({flag}, 0) FROM sis_usuario "
                f"WHERE idUsuario = ?",
                id_usuario_alvo,
            )
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "usuário não encontrado neste posto"}), 404
            usuario_alvo = (row[0] or "").strip()
            nome_alvo = (row[1] or "").strip()
            ja_zerado = not bool(row[2])

            if ja_zerado:
                # Nada a fazer; não gera Sis_Historico de uma não-mudança.
                return jsonify({
                    "ok": True, "no_change": True,
                    "idUsuario": id_usuario_alvo, "flag": flag,
                })

            cur.execute(
                f"UPDATE sis_usuario SET {flag} = 0 WHERE idUsuario = ?",
                id_usuario_alvo,
            )
            _audit(
                con, id_usuario_alvo, ID_TABELA_SIS_USUARIO, ID_COMANDO_EDICAO,
                id_usuario_op,
                f"Revogou {flag} (KPI Acesso Avancado) usuario={usuario_alvo}",
            )
            con.commit()
    except Exception as e:
        logger.exception("revogar acesso falhou no posto %s", posto)
        return jsonify({"error": str(e)[:400]}), 500

    return jsonify({
        "ok": True,
        "idUsuario": id_usuario_alvo,
        "usuario": usuario_alvo,
        "nome": nome_alvo,
        "flag": flag,
    })


def _postos_alvo(postos) -> list:
    """Postos do ACL do usuário que têm configuração de banco."""
    return sorted(p for p in (postos or set()) if os.getenv(f"DB_HOST_{p}"))


def _cascata_desativar(postos, login_campinho: str, target_login: str, executar: bool) -> list:
    """Percorre TODOS os postos do ACL e desativa `target_login` em cada um.

    Casa SEMPRE pelo login completo (com sufixo), que é idêntico entre postos —
    NUNCA por idUsuario (o id difere de posto para posto). Quando `executar` é
    True, faz `UPDATE Desativado = 1` + Sis_Historico na mesma transação; quando
    False, é só pré-visualização (não escreve nada).

    Cada posto precisa do vínculo Campinho do admin resolvido localmente para
    assinar o Sis_Historico; sem ele, o posto é pulado (status sem_vinculo_admin).
    """
    from medico_novo_routes import (
        _audit, _resolver_idusuario_no_posto,
        ID_TABELA_SIS_USUARIO, ID_COMANDO_EDICAO,
    )
    try:
        from alarmes_db import POSTOS_NOMES
    except Exception:
        POSTOS_NOMES = {}

    resultados = []
    for p in _postos_alvo(postos):
        item = {"posto": p, "posto_nome": POSTOS_NOMES.get(p, p),
                "login": target_login, "nome": None, "status": None}
        try:
            with _conn_for_posto(p) as con:
                id_usuario_op = _resolver_idusuario_no_posto(con, login_campinho)
                if not id_usuario_op:
                    item["status"] = "sem_vinculo_admin"
                    resultados.append(item)
                    continue

                cur = con.cursor()
                cur.execute(
                    "SELECT TOP 1 idUsuario, Nome, ISNULL(Desativado, 0) "
                    "FROM sis_usuario WHERE Usuario = ?",
                    target_login,
                )
                row = cur.fetchone()
                if not row:
                    item["status"] = "nao_encontrado"
                    resultados.append(item)
                    continue

                idu = int(row[0])
                item["nome"] = (row[1] or "").strip()
                if bool(row[2]):
                    item["status"] = "ja_desativado"
                    resultados.append(item)
                    continue

                if not executar:
                    item["status"] = "sera_desativado"
                    resultados.append(item)
                    continue

                cur.execute(
                    "UPDATE sis_usuario SET Desativado = 1 WHERE idUsuario = ?", idu)
                _audit(
                    con, idu, ID_TABELA_SIS_USUARIO, ID_COMANDO_EDICAO, id_usuario_op,
                    f"Desativou usuario={target_login} (KPI Acesso Avancado, cascata todos os postos)",
                )
                con.commit()
                item["status"] = "desativado"
        except Exception as e:
            logger.exception("cascata desativar falhou no posto %s", p)
            item["status"] = "erro"
            item["erro"] = str(e)[:200]
        resultados.append(item)
    return resultados


@acesso_avancado_bp.post("/api/acesso_avancado/desativar/preview")
def api_desativar_preview():
    """Pré-visualização (somente leitura) da desativação em cascata: mostra, por
    posto, qual usuário casou pelo login e o status, SEM escrever nada."""
    email, postos, all_pages, login_campinho = _check_user()
    if not email:
        return jsonify({"error": "unauthorized"}), 401
    if not all_pages:
        return jsonify({"error": "forbidden"}), 403

    data = request.get_json(silent=True) or {}
    login = (data.get("login") or "").strip()
    if not login:
        return jsonify({"error": "login obrigatório"}), 400

    from medico_novo_routes import ERR_SEM_VINCULO
    if not login_campinho:
        return jsonify({"error": ERR_SEM_VINCULO, "sem_vinculo": True}), 403

    resultados = _cascata_desativar(postos, login_campinho, login, executar=False)
    return jsonify({"login": login, "resultados": resultados})


@acesso_avancado_bp.post("/api/acesso_avancado/desativar")
def api_desativar():
    """Desativa `login` (UPDATE Desativado = 1) em TODOS os postos do ACL onde ele
    existir, cada um auditado em Sis_Historico. Exige `confirmar=true` (a UI mostra
    a pré-visualização antes). Casa pelo login completo, nunca por id.

    Body JSON: {login, confirmar:true}.
    """
    email, postos, all_pages, login_campinho = _check_user()
    if not email:
        return jsonify({"error": "unauthorized"}), 401
    if not all_pages:
        return jsonify({"error": "forbidden"}), 403

    data = request.get_json(silent=True) or {}
    login = (data.get("login") or "").strip()
    if not login:
        return jsonify({"error": "login obrigatório"}), 400
    if not data.get("confirmar"):
        return jsonify({"error": "confirmação obrigatória"}), 400

    from medico_novo_routes import ERR_SEM_VINCULO
    if not login_campinho:
        return jsonify({"error": ERR_SEM_VINCULO, "sem_vinculo": True}), 403

    resultados = _cascata_desativar(postos, login_campinho, login, executar=True)
    n_ok = sum(1 for r in resultados if r["status"] == "desativado")
    return jsonify({"ok": True, "login": login, "desativados": n_ok, "resultados": resultados})
