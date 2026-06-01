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

Somente leitura (SELECT) — não escreve no CAMIM, logo não exige auditoria
em Sis_Historico.
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
    """Retorna (email, postos_set, all_pages) se autenticado, senão (None, None, False).

    Importação tardia para evitar import circular com app.py / auth_routes.
    """
    from auth_routes import decode_user
    from auth_db import SessionLocal, get_user_by_email
    email, postos = decode_user()
    if not email:
        return None, None, False
    db = SessionLocal()
    try:
        u = get_user_by_email(db, email)
        if not u:
            return None, None, False
        all_pages = bool(getattr(u, "all_pages", True))
        return email, set(postos or []), all_pages
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
    email, postos, all_pages = _check_user()
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
    email, postos, all_pages = _check_user()
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
