"""
wpp_cobranca_sql.py
Módulo compartilhado de acesso ao SQL Server para o sistema de Cobrança WhatsApp.

Usado por:
  wpp_cobranca_routes.py  — endpoints /api/opcoes e /api/preview
  send_whatsapp_cobranca.py — engine de envio (importa get_conn_posto, VIEW_NAME, build_where)
"""

import os
import logging

import pyodbc
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

log = logging.getLogger(__name__)

ODBC_DRIVER = os.getenv("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")
VIEW_NAME   = os.getenv("WAPP_VIEW",   "WEB_COB_DebitoEmAberto6Meses")

MODO_ATRASO = "atraso"
MODO_PRE_VENCIMENTO = "pre_vencimento"
MODO_CLIENTES = "clientes_admissao"

# Fonte para lembrete antes do vencimento, mantendo aliases compatíveis
# com WEB_COB_DebitoEmAberto6Meses.
SOURCE_PRE_VENCIMENTO = """
(
    SELECT
          c.idcliente as idcliente
        , c.dataadmissao as dataadmissao
        , c.matricula as matricula
        , e.codigo as codigoendereco
        , c.nome as nomecadastro
        , CASE
              WHEN c.tipoplano = 'j' THEN c.cnpj
              ELSE c.cpf
          END as cnpjcpf
        , CASE
              WHEN c.tipo = 'j' THEN c.razaosocial
              ELSE c.nome
          END as nomeexibicao
        , c.nomesocial as nomesocial
        , c.responsavel as responsavel
        , c.responsavelcpf as responsavelcpf
        , c.responsaveltelefonecelular as responsaveltelefonecelular
        , c.responsaveltelefonewhatsapp as responsaveltelefonewhatsapp
        , c.endereco as endereco
        , c.numero as numero
        , c.complemento as complemento
        , c.bairro as bairro
        , c.cidade as cidade
        , c.cep as cep
        , c.telefonecelular as telefonecelular
        , c.telefonewhatsapp as telefonewhatsapp
        , c.telefoneresidencial as telefoneresidencial
        , c.email as email
        , c.idadecomputada as idade
        , c.diacobranca as diacobranca
        , c.sexo as sexo
        , cc.nome as cobradornome
        , c.canceladoans as canceladoans
        , c.datacancelamentoans as datacancelamentoans
        , CASE
              WHEN p.clubebeneficio = 1 THEN 'clubebeneficio'
              WHEN p.clubebeneficiojoy = 1 THEN 'joy'
              WHEN p.planopremium = 1 THEN 'premium'
              ELSE 'camim padrao'
          END as planotipo
        , r2.idreceita as idreceita
        , r2.[Data Referencia] as datareferencia
        , r2.[Valor Devido] as valordevido
        , r2.[Valor Pago] as valorpago
        , r2.[Descrição] as descricao
        , r2.[Tipo] as tipo
        , r2.[Situação] as situacao
        , r2.[Registrado] as registrado
        , r2.[ClienteRecorrente] as clienterecorrente
        , r2.[Operadora] as operadora
        , r2.[TipoCobranca] as tipocobranca
        , DATEDIFF(year, c.dataadmissao, CAST(GETDATE() as date)) as anoscliente
        , DATEDIFF(month, c.dataadmissao, CAST(GETDATE() as date)) as mesescliente
        , DATEDIFF(
              day
            , MIN(r2.[Data Referencia]) OVER (PARTITION BY c.idcliente)
            , CAST(GETDATE() as date)
          ) as diasdebito
        , c.CodigoPessoal
        , r2.[Data De Vencimento] as datadevencimento
        , r2.[Cobrador] as Cobrador
        , r2.[Corretor] as Corretor
        , c.DataNascimento
    FROM cad_cliente c
    JOIN sis_empresa emp on c.idendereco = emp.idendereco
    JOIN cad_cobrador cc on cc.idcobrador = c.idcobrador
    JOIN cad_plano p on p.idplano = c.idplano
    JOIN cad_endereco e on e.idendereco = c.idendereco
    LEFT JOIN vw_fin_receita2 r2 on r2.idcliente = c.idcliente
    WHERE r2.[Situação] = 'aberto'
      AND r2.[Tipo] = 'Mensalidade'
      AND r2.[Data de Cancelamento] IS NULL
) src
"""

# Fonte para campanhas de boas-vindas/admissão de clientes
SOURCE_CLIENTES = """
(
    SELECT
          f5.idcliente
        , f5.Matricula                    AS matricula
        , f5.nome                         AS nomecadastro
        , CAST(f5.idEndereco AS VARCHAR(20)) AS codigoendereco
        , f5.Tipo                         AS titular_dependente
        , f5.idade                        AS idade
        , f5.DataAdmissao                 AS dataadmissao
        , f5.cobrador                     AS cobradornome
        , f5.corretor                     AS Corretor
        , f5.situação                     AS situacao
        , f5.[Dia cobrança]               AS diacobranca
        , f5.Nascimento                   AS DataNascimento
        , f5.Bairro                       AS bairro
        , f5.origem                       AS origem
        , f5.TelefoneWhatsApp             AS telefonewhatsapp
        , f5.canceladoans                 AS canceladoans
        , f5.tipo_FJ                      AS tipo_fj
        , f5.Responsavel                  AS responsavel
        , cc.responsaveltelefonewhatsApp  AS responsaveltelefonewhatsapp
        , vcc.SituaçãoClube               AS situacaoclube
        , ISNULL(p.ClubeBeneficio,    0)  AS clubebeneficio
        , ISNULL(p.ClubeBeneficioJoy, 0)  AS clubebeneficiojoy
        , ISNULL(p.PlanoPremium,      0)  AS planopremium
        -- Classificação do cliente conforme regras de negócio
        , CASE
              WHEN f5.idplano IS NULL AND f5.Matricula > 0             THEN 'edige'
              WHEN f5.Matricula = 0                                     THEN 'particular'
              WHEN f5.Matricula > 999999 AND f5.idplano IS NOT NULL    THEN 'clube'
              WHEN f5.Matricula BETWEEN 1 AND 999999
                   AND f5.idplano IS NOT NULL                          THEN 'camim'
              ELSE 'outro'
          END AS tipo_cliente
        -- Situação efetiva: clube usa situacaoclube, demais usam situação
        , CASE
              WHEN f5.Matricula > 999999 AND f5.idplano IS NOT NULL
                   THEN vcc.SituaçãoClube
              ELSE f5.situação
          END AS situacao_efetiva
        -- Tipo de plano
        , CASE
              WHEN ISNULL(p.ClubeBeneficio,    0) = 1 THEN 'clubebeneficio'
              WHEN ISNULL(p.ClubeBeneficioJoy, 0) = 1 THEN 'joy'
              WHEN ISNULL(p.PlanoPremium,      0) = 1 THEN 'premium'
              WHEN f5.idplano IS NOT NULL              THEN 'camim padrao'
              ELSE ''
          END AS planotipo
        -- Telefone efetivo: usa responsável como fallback
        , COALESCE(
              NULLIF(LTRIM(RTRIM(ISNULL(f5.TelefoneWhatsApp, ''))), ''),
              cc.responsaveltelefonewhatsApp
          )   AS telefone_efetivo
    FROM vw_Cad_PacienteView f5
    LEFT JOIN sis_empresa empresa ON empresa.idEndereco = f5.idEndereco
    JOIN  cad_cliente cc          ON cc.idcliente       = f5.idCliente
    JOIN  vw_cad_cliente vcc      ON vcc.idcliente      = f5.idCliente
    LEFT JOIN cad_plano p         ON p.idplano          = f5.idPlano
    WHERE f5.Desativado = 0
      AND f5.idEndereco = empresa.idEndereco
) src_cli
"""

# Mapeamento: nome do campo no form → coluna SQL na view
CAMPO_SQL = {
    "operadora": "operadora",
    "cobrador":  "cobradornome",
    "corretor":  "Corretor",
    "bairro":    "bairro",
    "rua":       "endereco",
}

# Mapeamento para modo clientes_admissao (usa SOURCE_CLIENTES)
CAMPO_SQL_CLIENTES = {
    "cobrador":        "cobradornome",
    "corretor":        "Corretor",
    "bairro":          "bairro",
    "origem":          "origem",
    "situacao_efetiva": "situacao_efetiva",
}


def _env(key, default=""):
    v = os.getenv(key, default)
    return v.strip() if isinstance(v, str) else v


def modo_envio(campanha: dict | None) -> str:
    m = str((campanha or {}).get("modo_envio") or MODO_ATRASO).strip().lower()
    return m if m in (MODO_ATRASO, MODO_PRE_VENCIMENTO, MODO_CLIENTES) else MODO_ATRASO


def source_sql(campanha: dict | None) -> str:
    m = modo_envio(campanha)
    if m == MODO_PRE_VENCIMENTO:
        return SOURCE_PRE_VENCIMENTO
    if m == MODO_CLIENTES:
        return SOURCE_CLIENTES
    return VIEW_NAME


def where_extras(campanha: dict | None) -> str:
    # Filtros legados específicos da view WEB_COB (não se aplicam a outros modos)
    if modo_envio(campanha) in (MODO_PRE_VENCIMENTO, MODO_CLIENTES):
        return ""
    return (
        " AND situacao <> 'Pré-Cadastro'"
        " AND descricao LIKE '%/20[0-9][0-9]'"
    )


def get_conn_posto(posto: str):
    """Abre conexão pyodbc para o posto. Retorna None se não configurado ou erro."""
    host = _env(f"DB_HOST_{posto}")
    base = _env(f"DB_BASE_{posto}")
    if not host or not base:
        return None
    user       = _env(f"DB_USER_{posto}")
    pwd        = _env(f"DB_PASSWORD_{posto}")
    port       = _env(f"DB_PORT_{posto}", "1433")
    encrypt    = _env("DB_ENCRYPT",    "yes")
    trust_cert = _env("DB_TRUST_CERT", "yes")
    timeout    = _env("DB_TIMEOUT",    "20")

    conn_str = (
        f"DRIVER={{{ODBC_DRIVER}}};"
        f"SERVER=tcp:{host},{port};"
        f"DATABASE={base};"
        f"Encrypt={encrypt};TrustServerCertificate={trust_cert};"
        f"Connection Timeout={timeout};"
    )
    conn_str += f"UID={user};PWD={pwd}" if user else "Trusted_Connection=yes"
    try:
        return pyodbc.connect(conn_str, timeout=int(timeout))
    except Exception as e:
        log.error("Erro ao conectar posto %s: %s", posto, e)
        return None


def _iso_to_br(val: str) -> str:
    """Converte YYYY-MM-DD para DD/MM/YYYY (formato esperado pelo SQL Server)."""
    if val and len(val) == 10 and val[4] == '-':
        return f"{val[8:10]}/{val[5:7]}/{val[0:4]}"
    return val


def _build_where_clientes(campanha: dict) -> tuple:
    """WHERE para modo clientes_admissao — filtra sobre SOURCE_CLIENTES."""
    filtros = [
        "telefone_efetivo IS NOT NULL",
        "telefone_efetivo <> ''",
    ]
    params = []

    if campanha.get("adm_data_ini"):
        filtros.append("dataadmissao >= ?")
        params.append(_iso_to_br(campanha["adm_data_ini"]))
    if campanha.get("adm_data_fim"):
        filtros.append("dataadmissao < ?")
        params.append(_iso_to_br(campanha["adm_data_fim"]))

    if campanha.get("tipo_cliente"):
        filtros.append("tipo_cliente = ?")
        params.append(campanha["tipo_cliente"])

    if campanha.get("titular_dependente"):
        filtros.append("titular_dependente = ?")
        params.append(campanha["titular_dependente"])

    if campanha.get("situacao_cliente"):
        filtros.append("situacao_efetiva = ?")
        params.append(campanha["situacao_cliente"])

    if campanha.get("tipo_fj"):
        filtros.append("tipo_fj = ?")
        params.append(campanha["tipo_fj"])

    if campanha.get("clube_beneficio"):
        filtros.append("clubebeneficio = 1")

    if campanha.get("clube_beneficio_joy"):
        filtros.append("clubebeneficiojoy = 1")

    if campanha.get("plano_premium"):
        filtros.append("planopremium = 1")

    if campanha.get("origem"):
        filtros.append("origem LIKE ?")
        params.append(f"%{campanha['origem']}%")

    if campanha.get("cobrador"):
        filtros.append("cobradornome LIKE ?")
        params.append(f"%{campanha['cobrador']}%")

    if campanha.get("corretor"):
        filtros.append("Corretor LIKE ?")
        params.append(f"%{campanha['corretor']}%")

    if campanha.get("bairro"):
        filtros.append("bairro LIKE ?")
        params.append(f"%{campanha['bairro']}%")

    if campanha.get("idade_min") is not None:
        filtros.append(f"idade >= {int(campanha['idade_min'])}")

    if campanha.get("idade_max") is not None:
        filtros.append(f"idade <= {int(campanha['idade_max'])}")

    return " AND ".join(filtros), params


def build_where(campanha: dict) -> tuple:
    """Monta cláusula WHERE para filtrar registros conforme as regras da campanha.
    Não inclui filtro de posto (cada posto tem seu próprio DB).
    Retorna (where_str, params_list).
    """
    if modo_envio(campanha) == MODO_CLIENTES:
        return _build_where_clientes(campanha)

    filtros = [
        "telefonewhatsapp IS NOT NULL",
        "telefonewhatsapp <> ''",
    ]
    params = []

    if modo_envio(campanha) == MODO_PRE_VENCIMENTO:
        dias_min = int(campanha.get("dias_ref_min") or 0)
        filtros.append(
            f"DATEDIFF(day, CAST(GETDATE() as date), CAST(datareferencia as date)) >= {dias_min}"
        )
        if campanha.get("dias_ref_max") is not None:
            filtros.append(
                "DATEDIFF(day, CAST(GETDATE() as date), CAST(datareferencia as date)) <= "
                f"{int(campanha['dias_ref_max'])}"
            )
    else:
        filtros.append(f"diasdebito >= {int(campanha.get('dias_atraso_min') or 1)}")
        if campanha.get("dias_atraso_max"):
            filtros.append(f"diasdebito <= {int(campanha['dias_atraso_max'])}")

    if not campanha.get("incluir_cancelados"):
        filtros.append("canceladoans = 0")

    if campanha.get("sem_email"):
        filtros.append("(email IS NULL OR email = '')")

    if campanha.get("sexo"):
        filtros.append("sexo = ?")
        params.append(campanha["sexo"])

    if campanha.get("idade_min") is not None:
        filtros.append(f"idade >= {int(campanha['idade_min'])}")

    if campanha.get("idade_max") is not None:
        filtros.append(f"idade <= {int(campanha['idade_max'])}")

    if campanha.get("nao_recorrente"):
        filtros.append("clienterecorrente = 'NÃO'")

    if campanha.get("operadora"):
        filtros.append("operadora = ?")
        params.append(campanha["operadora"])

    if campanha.get("cobrador"):
        filtros.append("cobradornome LIKE ?")
        params.append(f"%{campanha['cobrador']}%")

    if campanha.get("corretor"):
        filtros.append("Corretor LIKE ?")
        params.append(f"%{campanha['corretor']}%")

    if campanha.get("bairro"):
        filtros.append("bairro LIKE ?")
        params.append(f"%{campanha['bairro']}%")

    if campanha.get("rua"):
        filtros.append("endereco LIKE ?")
        params.append(f"%{campanha['rua']}%")

    return " AND ".join(filtros), params


def buscar_opcoes(postos: list, campo: str, modo: str = MODO_ATRASO) -> list:
    """Retorna lista ordenada de valores distintos não nulos do campo."""
    valores, _ = buscar_opcoes_debug(postos, campo, modo)
    return valores


def buscar_opcoes_debug(postos: list, campo: str, modo: str = MODO_ATRASO) -> tuple:
    """Retorna (lista_de_valores, lista_de_erros) para diagnóstico."""
    if modo == MODO_CLIENTES:
        col = CAMPO_SQL_CLIENTES.get(campo)
        if not col:
            return [], [f"campo inválido para modo clientes: {campo}"]
        src = SOURCE_CLIENTES
        extra = ""
    else:
        modo = modo if modo in (MODO_ATRASO, MODO_PRE_VENCIMENTO) else MODO_ATRASO
        col = CAMPO_SQL.get(campo)
        if not col:
            return [], [f"campo inválido: {campo}"]
        src = SOURCE_PRE_VENCIMENTO if modo == MODO_PRE_VENCIMENTO else VIEW_NAME
        extra = "" if modo == MODO_PRE_VENCIMENTO else (
            " AND situacao <> 'Pré-Cadastro'"
            " AND descricao LIKE '%/20[0-9][0-9]'"
        )
    valores = set()
    erros = []
    for posto in postos:
        conn = get_conn_posto(posto)
        if not conn:
            erros.append(f"posto {posto}: sem conexão (verifique DB_HOST_{posto} e DB_BASE_{posto} no .env)")
            continue
        try:
            cur = conn.cursor()
            cur.timeout = 25
            cur.execute(
                f"SELECT DISTINCT {col} FROM {src} "
                f"WHERE {col} IS NOT NULL AND LTRIM(RTRIM(CAST({col} AS NVARCHAR(500)))) <> '' "
                f"{extra} ORDER BY {col}"
            )
            for row in cur.fetchall():
                if row[0] is not None:
                    valores.add(str(row[0]).strip())
        except Exception as e:
            msg = f"posto {posto} campo {campo}: {e}"
            log.error("buscar_opcoes %s", msg)
            erros.append(msg)
        finally:
            conn.close()
    return sorted(valores), erros


def contar_preview(campanha: dict) -> dict:
    """Conta faturas e telefones únicos para os filtros da campanha.
    Retorna {total_faturas, total_telefones, por_posto}."""
    postos = campanha.get("postos") or []
    where, params = build_where(campanha)
    src = source_sql(campanha)
    extra = where_extras(campanha)
    total_faturas = 0
    total_tel = 0
    por_posto = {}

    for posto in postos:
        conn = get_conn_posto(posto)
        if not conn:
            por_posto[posto] = {"erro": "sem conexão"}
            continue
        try:
            cur = conn.cursor()
            cur.timeout = 25
            tel_col = "telefone_efetivo" if modo_envio(campanha) == MODO_CLIENTES else "telefonewhatsapp"
            cur.execute(
                f"SELECT COUNT(*) as f, COUNT(DISTINCT {tel_col}) as t "
                f"FROM {src} WHERE {where}{extra}",
                params,
            )
            row = cur.fetchone()
            f = row[0] or 0
            t = row[1] or 0
            por_posto[posto] = {"faturas": f, "telefones": t}
            total_faturas += f
            total_tel += t
        except Exception as e:
            por_posto[posto] = {"erro": str(e)[:120]}
            log.error("contar_preview posto=%s: %s", posto, e)
        finally:
            conn.close()

    return {
        "total_faturas":   total_faturas,
        "total_telefones": total_tel,
        "por_posto":       por_posto,
    }
