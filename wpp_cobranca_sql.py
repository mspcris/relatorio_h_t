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

# Mapeamento: nome do campo no form → coluna SQL na view
CAMPO_SQL = {
    "operadora": "operadora",
    "cobrador":  "cobradornome",
    "corretor":  "Corretor",
    "bairro":    "bairro",
    "rua":       "endereco",
}


def _env(key, default=""):
    v = os.getenv(key, default)
    return v.strip() if isinstance(v, str) else v


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


def build_where(campanha: dict) -> tuple:
    """Monta cláusula WHERE para filtrar registros da view conforme as regras da campanha.
    Não inclui filtro de posto (cada posto tem seu próprio DB).
    Retorna (where_str, params_list).
    """
    filtros = [
        "telefonewhatsapp IS NOT NULL",
        "telefonewhatsapp <> ''",
        f"diasdebito >= {int(campanha.get('dias_atraso_min') or 1)}",
    ]
    params = []

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


def buscar_opcoes(postos: list, campo: str) -> list:
    """Retorna lista ordenada de valores distintos não nulos do campo."""
    valores, _ = buscar_opcoes_debug(postos, campo)
    return valores


def buscar_opcoes_debug(postos: list, campo: str) -> tuple:
    """Retorna (lista_de_valores, lista_de_erros) para diagnóstico."""
    col = CAMPO_SQL.get(campo)
    if not col:
        return [], [f"campo inválido: {campo}"]
    valores = set()
    erros = []
    for posto in postos:
        conn = get_conn_posto(posto)
        if not conn:
            erros.append(f"posto {posto}: sem conexão (verifique DB_HOST_{posto} e DB_BASE_{posto} no .env)")
            continue
        try:
            cur = conn.cursor()
            cur.execute(
                f"SELECT DISTINCT {col} FROM {VIEW_NAME} "
                f"WHERE {col} IS NOT NULL AND LTRIM(RTRIM(CAST({col} AS NVARCHAR(500)))) <> '' "
                f"ORDER BY {col}"
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
            cur.execute(
                f"SELECT COUNT(*) as f, COUNT(DISTINCT telefonewhatsapp) as t "
                f"FROM {VIEW_NAME} WHERE {where}"
                f" AND situacao <> 'Pré-Cadastro'"
                f" AND descricao LIKE '%/20[0-9][0-9]'",
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
