"""medico_novo_routes.py — Inclusão Web de Médico (3 fases: cad_medico, cad_especialidade, sis_usuario).

Página em /medico_novo (registrada em mais_servicos). Backend conecta no SQL Server
do posto escolhido pelo admin (ACL existente: o admin só vê os postos liberados).

Fluxo:
  1) GET /medico_novo                              → render template
  2) GET /api/medico_novo/postos                   → postos liberados ao admin
  3) GET /api/medico_novo/lookups?posto=X          → Fin_ContaTipo, Fin_Plano, Fin_Forma, salas
  4) POST /api/medico_novo/medico                  → INSERT cad_medico → idmedico
  5) POST /api/medico_novo/especialidade           → INSERT cad_especialidade
  6) GET /api/medico_novo/check_usuario            → existe usuário com esse login no posto?
  7) POST /api/medico_novo/usuario                 → INSERT sis_usuario (com senha plain de 5 dígitos)
"""

from __future__ import annotations

import os
import re
import random
import logging
import unicodedata
from datetime import date, datetime, timedelta

import pyodbc
from flask import Blueprint, jsonify, request

logger = logging.getLogger(__name__)

medico_novo_bp = Blueprint("medico_novo_bp", __name__)

ODBC_DRIVER = os.getenv("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")

ESPECIALIDADES_PERMITIDAS = ("CLÍNICA GERAL", "PEDIATRIA")
DIAS_COL = {0: "Segunda", 1: "Terca", 2: "Quarta", 3: "Quinta", 4: "Sexta", 5: "Sabado", 6: "Domingo"}
DIAS_BIT = {0: "segunda", 1: "Terca", 2: "quarta", 3: "quinta", 4: "sexta", 5: "sabado", 6: "domingo"}

# Sis_HistoricoTabela / Sis_HistoricoComando (IDs descobertos via SELECT em 2026-05-01)
ID_TABELA_CAD_MEDICO         = 37
ID_TABELA_SIS_USUARIO        = 38
ID_TABELA_CAD_ESPECIALIDADE  = 53
ID_COMANDO_INCLUSAO          = 1
ID_COMANDO_EDICAO            = 2
ID_COMANDO_EXCLUSAO          = 3

COMPUTADOR_ORIGEM = "kpi.camim.com.br"

# Mensagem cruel quando o admin não tem login_campinho vinculado
ERR_SEM_VINCULO = (
    "Acesso bloqueado. Seu login no RH&T não tem vínculo com um usuário do "
    "CAMIM (Login Campinho). Sem vínculo, nada do que você fizer aqui fica "
    "auditável — e o sistema não tolera ação anônima. Cristiano pediu esse "
    "dado e você não passou. Resolva com ele e volte."
)


# ---------------------------------------------------------------------------
# Helpers de conexão / autenticação
# ---------------------------------------------------------------------------

def _check_admin():
    """Retorna (email, postos_set, login_campinho) se autenticado, senão (None, None, None).
    `login_campinho` é a string Usuario do sis_usuario (ex.: 'cristiano2.a').
    Importação tardia para evitar circular import com app.py.
    """
    from auth_routes import decode_user
    from auth_db import SessionLocal, get_user_by_email
    email, postos = decode_user()
    if not email:
        return None, None, None
    db = SessionLocal()
    try:
        u = get_user_by_email(db, email)
        if not u:
            return None, None, None
        login_campinho = (getattr(u, "login_campinho", None) or "").strip() or None
        return email, set(postos or []), login_campinho
    finally:
        db.close()


def _resolver_idusuario_no_posto(con: pyodbc.Connection, login_campinho: str) -> int | None:
    """Busca idUsuario na sis_usuario do posto-alvo pelo Usuario (login_campinho).
    Retorna None se não achar — significa que o admin não opera nesse posto.
    """
    if not login_campinho:
        return None
    cur = con.cursor()
    cur.execute(
        "SELECT TOP 1 idUsuario FROM sis_usuario WHERE Usuario = ? AND Desativado = 0",
        login_campinho,
    )
    row = cur.fetchone()
    return int(row[0]) if row else None


def _audit(con: pyodbc.Connection, id_alvo: int, id_tabela: int,
           id_comando: int, id_usuario: int, detalhe: str) -> None:
    """Insere em Sis_Historico para registrar auditoria. Chamar APÓS o INSERT/UPDATE/DELETE
    e ANTES do commit (roda na mesma transação)."""
    cur = con.cursor()
    cur.execute(
        """INSERT INTO Sis_Historico
                  (id, idTabela, idComando, idUsuario, DataHora, Detalhe, Computador)
           VALUES (?, ?, ?, ?, GETDATE(), ?, ?)""",
        id_alvo, id_tabela, id_comando, id_usuario, (detalhe or "")[:250], COMPUTADOR_ORIGEM,
    )


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
    return pyodbc.connect(cs, timeout=10)


def _require_posto_in_acl(posto: str, postos_acl: set) -> str | None:
    """Retorna mensagem de erro se inválido; None se OK."""
    p = (posto or "").strip().upper()
    if not p or len(p) != 1:
        return "posto inválido"
    if not postos_acl:
        return "sem postos liberados"
    if p not in postos_acl:
        return f"posto {p} fora do ACL do usuário"
    return None


def _strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "")
    return "".join(c for c in s if not unicodedata.combining(c))


def _encrypt_camim_password(plain: str, key: str | None = None) -> str:
    """Reproduz `TEvCriptografa.TextToCriptoHex` do componente Delphi `Ecrypto`
    usado pelo sistema CAMIM. Devolve o hex (uppercase) pronto para gravar em
    `sis_usuario.Senha`.

    Algoritmo (port direto do Pascal):
      1) Inverte a string `plain`
      2) Para cada char (1-indexed), XOR com  `(ord(key[(pos % len(key))]) + pos) & 0xFF`
         (Delphi `Copy(FKey, (SPos mod Length(FKey)) + 1, 1)` → key 0-indexed Python)
      3) Cada byte resultante vira 2 dígitos hex uppercase
    """
    if not plain:
        return ""
    if key is None:
        key = os.getenv("CAMIM_SENHA_KEY", "@#$%").strip().strip("'\"") or "@#$%"
    text = plain[::-1]
    klen = len(key)
    return "".join(
        f"{(ord(text[spos - 1]) ^ ((ord(key[spos % klen]) + spos) & 0xFF)) & 0xFF:02X}"
        for spos in range(1, len(text) + 1)
    )


def _valida_cpf(cpf: str) -> bool:
    """Valida CPF: 11 dígitos, não-todos-iguais, dígitos verificadores corretos."""
    cpf = re.sub(r"\D", "", cpf or "")
    if len(cpf) != 11 or len(set(cpf)) == 1:
        return False
    nums = [int(c) for c in cpf]
    # Dígito 1
    s1 = sum(nums[i] * (10 - i) for i in range(9))
    d1 = 0 if s1 % 11 < 2 else 11 - (s1 % 11)
    if d1 != nums[9]:
        return False
    # Dígito 2
    s2 = sum(nums[i] * (11 - i) for i in range(10))
    d2 = 0 if s2 % 11 < 2 else 11 - (s2 % 11)
    return d2 == nums[10]


def _gerar_login(nome_completo: str, posto: str = "") -> str:
    """'Cristiano Silva Souza' + 'A' → 'CRISTIANOSS.A' (caixa alta + sufixo do posto).

    Regra confirmada em 2026-05-01: Usuario gravado SEMPRE em caixa alta no
    sis_usuario, com sufixo `.<letra_do_posto>` (também maiúsculo).
    """
    nome = _strip_accents((nome_completo or "").strip())
    nome = re.sub(r"[^A-Za-z\s]", "", nome).lower()
    parts = [p for p in nome.split() if p]
    if not parts:
        return ""
    base = parts[0] if len(parts) == 1 else (parts[0] + "".join(p[0] for p in parts[1:]))
    suf = (posto or "").strip().upper()[:1]
    out = f"{base}.{suf}" if suf else base
    return out.upper()


def _fetch_id_endereco(con: pyodbc.Connection) -> int:
    cur = con.cursor()
    cur.execute("SELECT TOP 1 idEndereco FROM sis_empresa")
    row = cur.fetchone()
    if not row:
        raise RuntimeError("sis_empresa sem registros")
    return int(row[0])


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@medico_novo_bp.get("/api/medico_novo/postos")
def api_postos():
    """Postos liberados ao admin, com nomes legíveis. Inclui flag `tem_vinculo`
    indicando se o admin tem `login_campinho` cadastrado no posto.
    """
    email, postos, login_campinho = _check_admin()
    if not email:
        return jsonify({"error": "unauthorized"}), 401
    try:
        from alarmes_db import POSTOS_NOMES
    except Exception:
        POSTOS_NOMES = {}
    if not login_campinho:
        return jsonify({"postos": [], "sem_vinculo": True, "msg": ERR_SEM_VINCULO})
    out = []
    for p in sorted(postos or []):
        if not os.getenv(f"DB_HOST_{p}"):
            continue
        out.append({"letra": p, "nome": POSTOS_NOMES.get(p, p)})
    return jsonify({"postos": out, "login_campinho": login_campinho})


@medico_novo_bp.get("/api/medico_novo/buscar_medico")
def api_buscar_medico():
    """Busca médicos já cadastrados no posto (para fluxo de plantão extra).
    Retorna até 30 médicos cujo Nome OU ConselhoNumero contenham o termo `q`.
    """
    email, postos, login_campinho = _check_admin()
    if not email:
        return jsonify({"error": "unauthorized"}), 401
    posto = request.args.get("posto", "")
    erro = _require_posto_in_acl(posto, postos)
    if erro:
        return jsonify({"error": erro}), 400
    q = (request.args.get("q") or "").strip()
    if len(q) < 2:
        return jsonify({"medicos": []})
    like = f"%{q}%"
    try:
        with _conn_for_posto(posto) as con:
            cur = con.cursor()
            cur.execute(
                """SELECT TOP 30 idmedico, Nome, ConselhoProfissional, ConselhoNumero,
                                 especializacao, valormedico
                     FROM cad_medico
                     WHERE desativado = 0
                       AND (Nome LIKE ? OR ConselhoNumero LIKE ?)
                     ORDER BY Nome""",
                like, like,
            )
            out = []
            for r in cur.fetchall():
                out.append({
                    "idmedico": int(r[0]),
                    "nome": (r[1] or "").strip(),
                    "conselho": f"{(r[2] or '').strip()} {(r[3] or '').strip()}".strip(),
                    "especializacao": (r[4] or "").strip(),
                    "valor_medico": float(r[5]) if r[5] is not None else None,
                })
        return jsonify({"medicos": out})
    except Exception as e:
        logger.exception("buscar_medico falhou no posto %s", posto)
        return jsonify({"error": str(e)[:300]}), 500


@medico_novo_bp.get("/api/medico_novo/lookups")
def api_lookups():
    email, postos, login_campinho = _check_admin()
    if not email:
        return jsonify({"error": "unauthorized"}), 401
    if not login_campinho:
        return jsonify({"error": ERR_SEM_VINCULO, "sem_vinculo": True}), 403
    posto = request.args.get("posto", "")
    erro = _require_posto_in_acl(posto, postos)
    if erro:
        return jsonify({"error": erro}), 400
    p = posto.strip().upper()
    try:
        with _conn_for_posto(p) as con:
            # Valida que o admin tem usuário ativo no posto-alvo
            id_usuario_op = _resolver_idusuario_no_posto(con, login_campinho)
            if not id_usuario_op:
                return jsonify({
                    "error": (
                        f"Acesso bloqueado neste posto. Seu Login Campinho "
                        f"`{login_campinho}` não existe em sis_usuario do posto {p} "
                        f"(ou está desativado). Sem isso, qualquer ação aqui ficaria "
                        f"sem responsável e o sistema bloqueia. Procure o Cristiano."
                    ),
                    "sem_vinculo_posto": True,
                }), 403
            cur = con.cursor()
            cur.execute("SELECT IDCONTATIPO, TIPO FROM Fin_ContaTipo WHERE Desativado=0 ORDER BY TIPO")
            conta_tipos = [{"id": int(r[0]), "label": (r[1] or "").strip()} for r in cur.fetchall()]
            cur.execute("SELECT IDPLANO, DESCRICAO FROM Fin_Plano WHERE Desativado=0 ORDER BY DESCRICAO")
            planos = [{"id": int(r[0]), "label": (r[1] or "").strip()} for r in cur.fetchall()]
            cur.execute("SELECT IDFORMA, FORMA FROM Fin_Forma WHERE Desativado=0 ORDER BY FORMA")
            formas = [{"id": int(r[0]), "label": (r[1] or "").strip()} for r in cur.fetchall()]
            cur.execute(
                "SELECT idSala, NomeSala, ISNULL(Andar,'') AS Andar "
                "FROM vw_Cad_PrevisaoConsultorioSala WITH (NOLOCK) "
                "WHERE Desativado=0 ORDER BY NomeSala"
            )
            salas = [
                {"id": int(r[0]), "nome": (r[1] or "").strip(), "andar": (r[2] or "").strip()}
                for r in cur.fetchall()
            ]
        return jsonify({
            "conta_tipos": conta_tipos,
            "planos": planos,
            "formas": formas,
            "salas": salas,
            "especialidades": list(ESPECIALIDADES_PERMITIDAS),
        })
    except Exception as e:
        logger.exception("lookups falhou no posto %s", p)
        return jsonify({"error": str(e)[:300]}), 500


@medico_novo_bp.post("/api/medico_novo/medico")
def api_insert_medico():
    """Fase 1: INSERT em cad_medico. Retorna idmedico."""
    email, postos, login_campinho = _check_admin()
    if not email:
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json(silent=True) or {}
    posto = (data.get("posto") or "").strip().upper()
    erro = _require_posto_in_acl(posto, postos)
    if erro:
        return jsonify({"error": erro}), 400

    nome = (data.get("nome") or "").strip()
    if not nome:
        return jsonify({"error": "nome obrigatório"}), 400
    cpf = re.sub(r"\D", "", (data.get("cpf") or ""))
    if not _valida_cpf(cpf):
        return jsonify({"error": "CPF inválido (dígitos verificadores não conferem)"}), 400
    especializacao = (data.get("especializacao") or "").strip().upper()
    if especializacao not in ESPECIALIDADES_PERMITIDAS:
        return jsonify({"error": "especialização deve ser CLÍNICA GERAL ou PEDIATRIA"}), 400
    conselho_numero = (data.get("conselho_numero") or "").strip()
    if not conselho_numero:
        return jsonify({"error": "ConselhoNumero obrigatório"}), 400

    # SET DATEFORMAT dmy do SQL Server CAMIM corrompe strings ISO via pyodbc:
    # passar objeto `date` Python — pyodbc envia como tipo SQL DATE/DATETIME nativo.
    data_nasc_raw = (data.get("data_nascimento") or "").strip()
    data_nasc = None
    if data_nasc_raw:
        try:
            data_nasc = datetime.strptime(data_nasc_raw, "%Y-%m-%d").date()
        except ValueError:
            return jsonify({"error": "data_nascimento inválida (YYYY-MM-DD)"}), 400

    valor_medico = data.get("valor_medico")  # decimal/None
    sexo = (data.get("sexo") or "").strip().upper()[:1] or None  # 'M' ou 'F'

    payload = {
        "Nome": nome,
        "telefone": (data.get("telefone") or "").strip() or None,
        "TelefoneWhatsApp": (data.get("telefone_whatsapp") or "").strip() or None,
        "Email": (data.get("email") or "").strip() or None,
        "DataNascimento": data_nasc,
        "Especializacao": especializacao,
        "crm": conselho_numero,
        "ConselhoProfissional": (data.get("conselho_profissional") or "CRM").strip(),
        "ConselhoNumero": conselho_numero,
        "ConselhoUF": (data.get("conselho_uf") or "").strip().upper()[:2] or None,
        "CPF": cpf,
        "Sexo": sexo,
        "ValorMedico": valor_medico,
        "idContaTipoMedico": data.get("id_conta_tipo_medico"),
        "idPlanoMedico": data.get("id_plano_medico"),
        "idForma": data.get("id_forma"),
        "Endereco": (data.get("endereco") or "").strip() or None,
        "Numero": (data.get("numero") or "").strip() or None,
        "Complemento": (data.get("complemento") or "").strip() or None,
        "Bairro": (data.get("bairro") or "").strip() or None,
        "Cidade": (data.get("cidade") or "").strip() or None,
        "Estado": (data.get("estado") or "").strip().upper()[:2] or None,
        "CEP": re.sub(r"\D", "", (data.get("cep") or "")) or None,
        "bMedicoSolicitante": 1,
        "bMedicoExecutante": 1,
        "GerarPagamentoMedicoAutomatico": 1,
        "PessoaJuridica": 0,
    }

    # OUTPUT INTO @t é compatível com triggers; SCOPE_IDENTITY() retorna NULL
    # em alguns casos quando há triggers downstream (visto em cad_medico).
    sql = """
    DECLARE @t TABLE (idmedico INT);
    INSERT INTO cad_medico
        (Nome, telefone, TelefoneWhatsApp, Email, DataNascimento, especializacao,
         crm, ConselhoProfissional, ConselhoNumero, ConselhoUF, cpf, sexo,
         valormedico, idcontatipomedico, idplanomedico, idforma,
         Endereco, numero, Complemento, Bairro, Cidade, Estado, CEP,
         bMedicoSolicitante, bMedicoExecutante, GerarPagamentoMedicoAutomatico,
         PessoaJuridica, datainclusao)
    OUTPUT INSERTED.idmedico INTO @t
    VALUES
        (?, ?, ?, ?, ?, ?,
         ?, ?, ?, ?, ?, ?,
         ?, ?, ?, ?,
         ?, ?, ?, ?, ?, ?, ?,
         ?, ?, ?,
         ?, GETDATE());
    SELECT idmedico FROM @t;
    """
    params = (
        payload["Nome"], payload["telefone"], payload["TelefoneWhatsApp"], payload["Email"],
        payload["DataNascimento"], payload["Especializacao"],
        payload["crm"], payload["ConselhoProfissional"], payload["ConselhoNumero"],
        payload["ConselhoUF"], payload["CPF"], payload["Sexo"],
        payload["ValorMedico"], payload["idContaTipoMedico"],
        payload["idPlanoMedico"], payload["idForma"],
        payload["Endereco"], payload["Numero"], payload["Complemento"], payload["Bairro"],
        payload["Cidade"], payload["Estado"], payload["CEP"],
        payload["bMedicoSolicitante"], payload["bMedicoExecutante"],
        payload["GerarPagamentoMedicoAutomatico"], payload["PessoaJuridica"],
    )

    try:
        with _conn_for_posto(posto) as con:
            id_usuario_op = _resolver_idusuario_no_posto(con, login_campinho)
            if not id_usuario_op:
                return jsonify({"error": ERR_SEM_VINCULO, "sem_vinculo_posto": True}), 403
            cur = con.cursor()
            cur.execute(sql, params)
            # Pular result sets vazios do DECLARE/INSERT até o SELECT do @t
            while cur.description is None:
                if not cur.nextset():
                    break
            row = cur.fetchone()
            if not row or row[0] is None:
                con.rollback()
                return jsonify({"error": "INSERT cad_medico não retornou idmedico"}), 500
            idmedico = int(row[0])
            _audit(con, idmedico, ID_TABELA_CAD_MEDICO, ID_COMANDO_INCLUSAO,
                   id_usuario_op, f"Inclusão de Médico via RH&T (CPF={cpf})")
            con.commit()
        return jsonify({"ok": True, "idmedico": idmedico})
    except Exception as e:
        logger.exception("INSERT cad_medico falhou no posto %s", posto)
        return jsonify({"error": str(e)[:400]}), 500


@medico_novo_bp.post("/api/medico_novo/especialidade")
def api_insert_especialidade():
    """Fase 2: INSERT em cad_especialidade."""
    email, postos, login_campinho = _check_admin()
    if not email:
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json(silent=True) or {}
    posto = (data.get("posto") or "").strip().upper()
    erro = _require_posto_in_acl(posto, postos)
    if erro:
        return jsonify({"error": erro}), 400

    idmedico = data.get("idmedico")
    if not idmedico:
        return jsonify({"error": "idmedico obrigatório"}), 400
    especialidade = (data.get("especialidade") or "").strip().upper()
    if especialidade not in ESPECIALIDADES_PERMITIDAS:
        return jsonify({"error": "especialidade deve ser CLÍNICA GERAL ou PEDIATRIA"}), 400

    data_plantao = (data.get("data_plantao") or "").strip()  # YYYY-MM-DD
    try:
        d = datetime.strptime(data_plantao, "%Y-%m-%d").date()
    except Exception:
        return jsonify({"error": "data_plantao inválida (YYYY-MM-DD)"}), 400

    hora_ini = (data.get("hora_inicio") or "").strip()
    hora_fim = (data.get("hora_fim") or "").strip()
    if not re.match(r"^\d{2}:\d{2}$", hora_ini) or not re.match(r"^\d{2}:\d{2}$", hora_fim):
        return jsonify({"error": "horário no formato HH:MM"}), 400

    almoco = bool(data.get("almoco", True))
    almoco_ini = (data.get("almoco_inicio") or "12:00").strip()
    almoco_fim = (data.get("almoco_fim") or "13:00").strip()

    id_sala = data.get("id_sala")
    if id_sala in (None, ""):
        return jsonify({"error": "sala obrigatória"}), 400
    id_sala = int(id_sala)

    valor_medico = data.get("valor_medico")  # vem da fase 1
    idade_min = data.get("idade_minima")
    idade_max = data.get("idade_maxima")
    numero_rqe = (data.get("numero_rqe") or "").strip() or None
    descricao = (data.get("descricao") or "").strip()
    if len(descricao) > 400:
        descricao = descricao[:400]
    # Vagas do dia (preenche admin; em branco → NULL = sem limite definido).
    # NÃO confundir com Quantidadeweb/QuantidadeMaximaweb (sempre NULL: bloqueia
    # marcação pelo site Camim, correto para plantonista temporário).
    vagas_min = data.get("vagas_min")
    vagas_max = data.get("vagas_max")
    vagas_min = int(vagas_min) if vagas_min not in (None, "") else None
    vagas_max = int(vagas_max) if vagas_max not in (None, "") else None

    # Mapeia dia da semana → colunas
    # Python weekday: Mon=0, Tue=1, ..., Sun=6
    py_wd = d.weekday()
    bits = {nm: 0 for nm in ("segunda", "Terca", "quarta", "quinta", "sexta", "sabado", "domingo")}
    bits[DIAS_BIT[py_wd]] = 1
    col_sufixo = DIAS_COL[py_wd]  # ex: 'Sabado'
    # data_fim_exibicao = data_plantao + 7 dias
    data_fim_exib = d + timedelta(days=7)

    base_cols = [
        "idmedico", "Especialidade", "Visibility",
        "permitirf6", "F3ExibirNumeroEmBranco", "F6ConfirmarEspecialidadeMedico",
        "ExibirnoF3", "PermitirAgendamentoquenuncaconsultou",
        "GerarPagamentoMedicoAutomatico", "Temporario", "Desativado",
        "MedicoRecebePorComissao", "Maisde1Agendamento", "AutoAtendimentoExibir",
        "idademinima", "idademaxima", "numerorqe", "Descricao",
        "DataInicioExibicao", "DataFimExibicao", "DataPlantao",
        "segunda", "Terca", "quarta", "quinta", "sexta", "sabado", "domingo",
        "DomingoOrdemChegada", "segundaOrdemChegada", "tercaOrdemChegada",
        "quartaOrdemChegada", "quintaOrdemChegada", "sextaOrdemChegada", "sabadoOrdemChegada",
        f"{col_sufixo}Sala",
        f"{col_sufixo}HoraInicio", f"{col_sufixo}HoraFim",
        f"{col_sufixo}Almoco", f"{col_sufixo}Almocoinicio", f"{col_sufixo}Almocofim",
        f"ValorCusto{col_sufixo}",
        f"{col_sufixo}Quantidade", f"{col_sufixo}QuantidadeMaxima",
        # NÃO incluímos <dia>Quantidadeweb/QuantidadeMaximaweb: NULL bloqueia
        # marcação via site Camim (correto para plantonista temporário).
    ]

    # OrdemChegada do dia escolhido = 1 (plantonista marca por ordem de chegada).
    ordem_chegada = {dia: 0 for dia in ("Domingo", "segunda", "terca", "quarta", "quinta", "sexta", "sabado")}
    # Mapear DIAS_COL[py_wd] para o nome usado em <dia>OrdemChegada (case do exemplo)
    nome_oc_por_wd = {0: "segunda", 1: "terca", 2: "quarta", 3: "quinta", 4: "sexta", 5: "sabado", 6: "Domingo"}
    ordem_chegada[nome_oc_por_wd[py_wd]] = 1

    base_vals = [
        idmedico, especialidade, "all",
        1, 1, 1,
        1, 1,
        1, 1, 0,
        0, 1, 1,
        idade_min, idade_max, numero_rqe, descricao or None,
        d,                  # DataInicioExibicao = data do plantão
        data_fim_exib,      # DataFimExibicao = plantão + 7
        d,                  # DataPlantao = data do plantão
        bits["segunda"], bits["Terca"], bits["quarta"], bits["quinta"],
        bits["sexta"], bits["sabado"], bits["domingo"],
        ordem_chegada["Domingo"], ordem_chegada["segunda"], ordem_chegada["terca"],
        ordem_chegada["quarta"], ordem_chegada["quinta"], ordem_chegada["sexta"], ordem_chegada["sabado"],
        id_sala,
        hora_ini, hora_fim,
        1 if almoco else 0,
        almoco_ini if almoco else None,
        almoco_fim if almoco else None,
        valor_medico,
        vagas_min, vagas_max,
    ]

    placeholders = ",".join("?" * len(base_cols))
    cols_sql = ",".join(f"[{c}]" for c in base_cols)
    sql = f"""
    DECLARE @t TABLE (idEspecialidade INT);
    INSERT INTO cad_especialidade ({cols_sql})
    OUTPUT INSERTED.idEspecialidade INTO @t
    VALUES ({placeholders});
    SELECT idEspecialidade FROM @t;
    """

    try:
        with _conn_for_posto(posto) as con:
            id_usuario_op = _resolver_idusuario_no_posto(con, login_campinho)
            if not id_usuario_op:
                return jsonify({"error": ERR_SEM_VINCULO, "sem_vinculo_posto": True}), 403
            cur = con.cursor()
            cur.execute(sql, tuple(base_vals))
            while cur.description is None:
                if not cur.nextset():
                    break
            row = cur.fetchone()
            id_esp = int(row[0]) if row and row[0] is not None else None
            _audit(con, id_esp or int(idmedico), ID_TABELA_CAD_ESPECIALIDADE,
                   ID_COMANDO_INCLUSAO, id_usuario_op,
                   f"Inclusão Quadro Especialidade via RH&T (médico={idmedico}, plantão={d.isoformat()})")
            con.commit()
        return jsonify({"ok": True, "idEspecialidade": id_esp})
    except Exception as e:
        logger.exception("INSERT cad_especialidade falhou no posto %s", posto)
        return jsonify({"error": str(e)[:400]}), 500


@medico_novo_bp.get("/api/medico_novo/check_usuario")
def api_check_usuario():
    """Verifica se o login já existe no posto. Retorna {existe: bool, sugerido: str}."""
    email, postos, login_campinho = _check_admin()
    if not email:
        return jsonify({"error": "unauthorized"}), 401
    posto = request.args.get("posto", "")
    erro = _require_posto_in_acl(posto, postos)
    if erro:
        return jsonify({"error": erro}), 400
    nome = request.args.get("nome", "")
    usuario = request.args.get("usuario", "").strip().upper()
    sugerido = _gerar_login(nome, posto) if nome else None
    if not usuario:
        usuario = sugerido or ""
    if not usuario:
        return jsonify({"existe": False, "sugerido": ""})
    try:
        with _conn_for_posto(posto) as con:
            cur = con.cursor()
            cur.execute("SELECT TOP 1 1 FROM sis_usuario WHERE Usuario = ?", usuario)
            existe = cur.fetchone() is not None
        return jsonify({"existe": existe, "sugerido": sugerido, "usuario": usuario})
    except Exception as e:
        logger.exception("check_usuario falhou no posto %s", posto)
        return jsonify({"error": str(e)[:300]}), 500


@medico_novo_bp.post("/api/medico_novo/reset_senha")
def api_reset_senha():
    """Gera nova senha plain de 5 dígitos e atualiza sis_usuario.Senha do médico.
    Retorna { usuario, senha, idUsuario }.
    """
    email, postos, login_campinho = _check_admin()
    if not email:
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json(silent=True) or {}
    posto = (data.get("posto") or "").strip().upper()
    erro = _require_posto_in_acl(posto, postos)
    if erro:
        return jsonify({"error": erro}), 400
    idmedico = data.get("idmedico")
    if not idmedico:
        return jsonify({"error": "idmedico obrigatório"}), 400
    senha_plain = f"{random.randint(0, 99999):05d}"
    senha_cripto = _encrypt_camim_password(senha_plain)
    try:
        with _conn_for_posto(posto) as con:
            id_usuario_op = _resolver_idusuario_no_posto(con, login_campinho)
            if not id_usuario_op:
                return jsonify({"error": ERR_SEM_VINCULO, "sem_vinculo_posto": True}), 403
            cur = con.cursor()
            cur.execute(
                """SELECT TOP 1 idUsuario, Usuario, Nome FROM sis_usuario
                    WHERE idMedicoProntuario = ? AND Desativado = 0""",
                int(idmedico),
            )
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "médico não tem usuário ativo cadastrado"}), 404
            id_usuario, usuario, nome_usr = int(row[0]), row[1], row[2]
            cur.execute("UPDATE sis_usuario SET Senha = ? WHERE idUsuario = ?", senha_cripto, id_usuario)
            _audit(con, id_usuario, ID_TABELA_SIS_USUARIO, ID_COMANDO_EDICAO,
                   id_usuario_op, f"Reset de senha via RH&T (medico={idmedico}, login={usuario})")
            con.commit()
        return jsonify({
            "ok": True,
            "idUsuario": id_usuario,
            "usuario": usuario,
            "nome": nome_usr,
            "senha": senha_plain,
        })
    except Exception as e:
        logger.exception("reset_senha falhou no posto %s", posto)
        return jsonify({"error": str(e)[:400]}), 500


@medico_novo_bp.post("/api/medico_novo/usuario")
def api_insert_usuario():
    """Fase 3: INSERT em sis_usuario. Gera senha 5 dígitos plain-text e retorna."""
    email, postos, login_campinho = _check_admin()
    if not email:
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json(silent=True) or {}
    posto = (data.get("posto") or "").strip().upper()
    erro = _require_posto_in_acl(posto, postos)
    if erro:
        return jsonify({"error": erro}), 400

    idmedico = data.get("idmedico")
    nome = (data.get("nome") or "").strip()
    usuario = (data.get("usuario") or "").strip().upper()
    if not (idmedico and nome and usuario):
        return jsonify({"error": "idmedico, nome e usuario obrigatórios"}), 400
    # Garante o sufixo `.<POSTO>` mesmo se o admin editou o login
    suf_posto = f".{posto.upper()}"
    if not usuario.endswith(suf_posto):
        # Remove qualquer sufixo `.<X>` existente e adiciona o do posto correto
        usuario = re.sub(r"\.[A-Z0-9]$", "", usuario) + suf_posto

    senha_plain = f"{random.randint(0, 99999):05d}"
    senha_cripto = _encrypt_camim_password(senha_plain)

    try:
        with _conn_for_posto(posto) as con:
            id_usuario_op = _resolver_idusuario_no_posto(con, login_campinho)
            if not id_usuario_op:
                return jsonify({"error": ERR_SEM_VINCULO, "sem_vinculo_posto": True}), 403
            cur = con.cursor()
            # Recheck duplicidade dentro da mesma transação para evitar corrida
            cur.execute("SELECT TOP 1 idUsuario FROM sis_usuario WHERE Usuario = ?", usuario)
            if cur.fetchone():
                return jsonify({"error": "usuario_duplicado", "campo": "usuario"}), 409
            id_endereco = _fetch_id_endereco(con)
            # OBS: sis_usuario tem trigger TR_SIS_USUARIO_ImpedirUsuarioEmBranco.
            # OUTPUT INTO @t é a forma compatível com triggers para obter o IDENTITY.
            sql = """
            DECLARE @t TABLE (idUsuario INT);
            INSERT INTO sis_usuario
                (Usuario, Nome, Senha, idEndereco, idMedicoProntuario,
                 Desativado, Setor, idPerfil)
            OUTPUT INSERTED.idUsuario INTO @t
            VALUES (?, ?, ?, ?, ?, 0, 'MÉDICO', NULL);
            SELECT idUsuario FROM @t;
            """
            cur.execute(sql, (usuario, nome, senha_cripto, id_endereco, int(idmedico)))
            while cur.description is None:
                if not cur.nextset():
                    break
            row = cur.fetchone()
            if not row or row[0] is None:
                con.rollback()
                return jsonify({"error": "INSERT sis_usuario não retornou idUsuario"}), 500
            id_usuario = int(row[0])
            _audit(con, id_usuario, ID_TABELA_SIS_USUARIO, ID_COMANDO_INCLUSAO,
                   id_usuario_op, f"Inclusão de Usuário via RH&T (medico={idmedico}, login={usuario})")
            con.commit()
        return jsonify({
            "ok": True,
            "idUsuario": id_usuario,
            "usuario": usuario,
            "senha": senha_plain,
        })
    except Exception as e:
        logger.exception("INSERT sis_usuario falhou no posto %s", posto)
        return jsonify({"error": str(e)[:400]}), 500
