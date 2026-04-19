"""
API row-level de fin_despesa — ChatGPT/Manus enxerga registro a registro.

Fonte: RDS AWS Postgres, DB relatorio_h_t, tabela fin_despesa (ETL
incremental por idDespesa a cada 2h — export_fin_despesa_pg.py).

Endpoints:
  GET /api/fin/despesas          — listagem paginada por cursor (id_despesa)
  GET /api/fin/despesas/resumo   — agregacao por dimensao (conta/plano/...)
  GET /api/fin/despesas/meta     — schema + valores distintos comuns

Auth: cookie de sessao OU X-Manus-Key (mesmo padrao de /api/receita_despesa/*).

Credenciais: PG_RDS_* no /opt/relatorio_h_t/.env (carregado pelo app.py).
"""
from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any

import psycopg2
import psycopg2.extras
from flask import Blueprint, jsonify, request

log = logging.getLogger(__name__)

fin_despesas_bp = Blueprint("fin_despesas_api", __name__)


# ============================================================
# AUTH — mesmo padrao de kpi_receita_despesa_api
# ============================================================

@fin_despesas_bp.before_request
def _exigir_auth():
    try:
        from auth_routes import require_auth_or_key
    except Exception as _e:  # pragma: no cover
        log.error("fin_despesas: auth_routes indisponivel (%s)", _e)
        return jsonify({"ok": False, "error": "auth indisponivel"}), 503
    principal, _ = require_auth_or_key()
    if not principal:
        return jsonify({
            "ok": False,
            "error": "nao autenticado",
            "hint": "envie cookie de sessao OU header X-Manus-Key",
        }), 401
    return None


# ============================================================
# DB
# ============================================================

POSTOS_VALIDOS = {"A", "B", "C", "D", "G", "I", "J", "M", "N", "P", "R", "X", "Y"}
GRUPOS = {
    "todos":    sorted(POSTOS_VALIDOS),
    "altamiro": ["A", "B", "G", "I", "N", "R", "X", "Y"],
    "couto":    ["C", "D", "J", "M", "P"],
}

# colunas seguras para ORDER BY / group_by / retorno
COLUNAS_RETORNO = [
    "posto", "id_despesa", "valor_devido", "valor_pago",
    "data_vencimento", "data_pagamento", "data_cancelamento",
    "descricao", "comentario", "tipo", "plano", "plano_principal",
    "corretor", "cobrador", "medico", "conta", "situacao", "talao",
    "valor_fatura", "cliente", "matricula", "funcionario", "fornecedor",
    "endereco", "usuario", "paciente", "ordem_pagamento", "contabilizado",
    "id_conta", "id_conta_tipo", "id_lancamento", "valor_rateio",
    "forma", "data_atendimento", "cargo",
]

GROUP_BY_ALIASES = {
    "tipo": "tipo",
    "plano": "plano",
    "plano_principal": "plano_principal",
    "conta": "conta",
    "fornecedor": "fornecedor",
    "corretor": "corretor",
    "medico": "medico",
    "forma": "forma",
    "posto": "posto",
    "mes": "to_char(data_pagamento, 'YYYY-MM')",
    "cliente": "cliente",
}


def _pg_conn():
    return psycopg2.connect(
        host=os.environ["PG_RDS_HOST"],
        port=int(os.environ.get("PG_RDS_PORT", "9432")),
        dbname=os.environ.get("PG_RDS_DB", "relatorio_h_t"),
        user=os.environ["PG_RDS_USER"],
        password=os.environ["PG_RDS_PASSWORD"],
        sslmode=os.environ.get("PG_RDS_SSLMODE", "require"),
        connect_timeout=10,
    )


# ============================================================
# PARSE FILTROS (compartilhado entre /despesas e /resumo)
# ============================================================

def _parse_postos(args) -> list[str]:
    grupo = (args.get("grupo") or "").strip().lower()
    if grupo in GRUPOS:
        return GRUPOS[grupo]

    raw = (args.get("postos") or args.get("posto") or "").strip().upper()
    if not raw:
        return []
    postos = [p.strip() for p in raw.split(",") if p.strip()]
    return [p for p in postos if p in POSTOS_VALIDOS]


def _parse_date(s: str | None):
    if not s:
        return None
    s = s.strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y-%m"):
        try:
            d = datetime.strptime(s, fmt)
            return d.date() if fmt != "%Y-%m" else d.date().replace(day=1)
        except ValueError:
            continue
    return None


def _parse_float(s: str | None):
    if not s:
        return None
    try:
        return float(s.replace(",", "."))
    except Exception:
        return None


def _build_where(args) -> tuple[str, list[Any]]:
    """Retorna (sql_where, params) - sempre comeca com '1=1'."""
    where: list[str] = ["1=1"]
    p: list[Any] = []

    postos = _parse_postos(args)
    if postos:
        where.append("posto = ANY(%s)")
        p.append(postos)

    data_ini = _parse_date(args.get("data_ini"))
    if data_ini:
        where.append("data_pagamento >= %s")
        p.append(data_ini)

    data_fim = _parse_date(args.get("data_fim"))
    if data_fim:
        where.append("data_pagamento < (%s::date + INTERVAL '1 day')")
        p.append(data_fim)

    # Filtros LIKE case-insensitive.
    LIKE_FIELDS = [
        "cliente", "tipo", "plano", "plano_principal",
        "fornecedor", "conta", "corretor", "medico",
        "forma", "descricao",
    ]
    for f in LIKE_FIELDS:
        v = (args.get(f) or "").strip()
        if v:
            where.append(f"{f} ILIKE %s")
            p.append(f"%{v}%")

    mn = _parse_float(args.get("min_valor"))
    if mn is not None:
        where.append("valor_pago >= %s")
        p.append(mn)

    mx = _parse_float(args.get("max_valor"))
    if mx is not None:
        where.append("valor_pago <= %s")
        p.append(mx)

    # Filtro por mes de pagamento (YYYY-MM).
    mes = (args.get("mes") or "").strip()
    if mes and len(mes) == 7 and mes[4] == "-":
        where.append("to_char(data_pagamento, 'YYYY-MM') = %s")
        p.append(mes)

    return " AND ".join(where), p


# ============================================================
# /api/fin/despesas  — listagem row-level com cursor
# ============================================================

@fin_despesas_bp.get("/api/fin/despesas")
def api_despesas_listar():
    args = request.args

    try:
        limit = max(1, min(500, int(args.get("limit", 100))))
    except Exception:
        limit = 100

    cursor_id = args.get("cursor")
    cursor_posto = (args.get("cursor_posto") or "").strip().upper()

    order = (args.get("order") or "data_pagamento_desc").lower()
    if order == "id_desc":
        order_clause = "ORDER BY posto, id_despesa DESC"
        cursor_cond = "(posto, id_despesa) < (%s, %s)" if cursor_id else None
    elif order == "id_asc":
        order_clause = "ORDER BY posto, id_despesa ASC"
        cursor_cond = "(posto, id_despesa) > (%s, %s)" if cursor_id else None
    elif order == "valor_desc":
        order_clause = "ORDER BY valor_pago DESC NULLS LAST, posto, id_despesa DESC"
        cursor_cond = None
    else:  # data_pagamento_desc (default)
        order_clause = "ORDER BY data_pagamento DESC NULLS LAST, posto, id_despesa DESC"
        cursor_cond = None

    where_sql, params = _build_where(args)
    if cursor_cond and cursor_id:
        where_sql += " AND " + cursor_cond
        params.append(cursor_posto or "Z")
        try:
            params.append(int(cursor_id))
        except Exception:
            params.append(0)

    cols_sql = ", ".join(COLUNAS_RETORNO)
    sql = f"""
        SELECT {cols_sql}
        FROM fin_despesa
        WHERE {where_sql}
        {order_clause}
        LIMIT %s
    """
    params.append(limit + 1)  # +1 para saber se tem proxima pagina

    try:
        with _pg_conn() as pg, pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as c:
            c.execute(sql, params)
            rows = c.fetchall()
    except Exception as e:
        log.exception("fin_despesas: erro query listagem")
        return jsonify({"ok": False, "error": f"db: {e}"}), 500

    has_more = len(rows) > limit
    if has_more:
        rows = rows[:limit]

    next_cursor = None
    if has_more and rows and order in ("id_desc", "id_asc"):
        last = rows[-1]
        next_cursor = {"id_despesa": last["id_despesa"], "posto": last["posto"]}

    # normaliza datas para ISO
    for r in rows:
        for k in ("data_pagamento", "data_vencimento", "data_cancelamento",
                  "data_atendimento"):
            v = r.get(k)
            if v is not None:
                r[k] = v.isoformat()

    return jsonify({
        "ok": True,
        "rows": rows,
        "count": len(rows),
        "has_more": has_more,
        "next_cursor": next_cursor,
        "order": order,
        "limit": limit,
    })


# ============================================================
# /api/fin/despesas/resumo  — agregacao por dimensao
# ============================================================

@fin_despesas_bp.get("/api/fin/despesas/resumo")
def api_despesas_resumo():
    args = request.args
    gb = (args.get("group_by") or "conta").strip().lower()
    if gb not in GROUP_BY_ALIASES:
        return jsonify({
            "ok": False,
            "error": f"group_by invalido: {gb}",
            "options": sorted(GROUP_BY_ALIASES.keys()),
        }), 400

    expr = GROUP_BY_ALIASES[gb]

    try:
        top = max(1, min(1000, int(args.get("top", 100))))
    except Exception:
        top = 100

    where_sql, params = _build_where(args)

    sql = f"""
        SELECT
            {expr}             AS chave,
            COUNT(*)           AS qtd,
            SUM(valor_pago)    AS soma_valor_pago,
            AVG(valor_pago)    AS media_valor_pago,
            MIN(valor_pago)    AS min_valor_pago,
            MAX(valor_pago)    AS max_valor_pago,
            MIN(data_pagamento) AS primeira_data,
            MAX(data_pagamento) AS ultima_data
        FROM fin_despesa
        WHERE {where_sql}
        GROUP BY {expr}
        ORDER BY soma_valor_pago DESC NULLS LAST
        LIMIT %s
    """
    params.append(top)

    try:
        with _pg_conn() as pg, pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as c:
            c.execute(sql, params)
            rows = c.fetchall()
    except Exception as e:
        log.exception("fin_despesas: erro query resumo")
        return jsonify({"ok": False, "error": f"db: {e}"}), 500

    for r in rows:
        # converte Decimal/datas para serializavel
        for k in ("soma_valor_pago", "media_valor_pago", "min_valor_pago", "max_valor_pago"):
            if r.get(k) is not None:
                r[k] = float(r[k])
        for k in ("primeira_data", "ultima_data"):
            v = r.get(k)
            if v is not None:
                r[k] = v.isoformat()

    return jsonify({
        "ok": True,
        "group_by": gb,
        "rows": rows,
        "count": len(rows),
    })


# ============================================================
# /api/fin/despesas/meta  — schema + hints para o agente
# ============================================================

@fin_despesas_bp.get("/api/fin/despesas/meta")
def api_despesas_meta():
    info = {
        "ok": True,
        "fonte": "RDS AWS Postgres — relatorio_h_t.fin_despesa (ETL a cada 2h)",
        "postos": sorted(POSTOS_VALIDOS),
        "grupos": GRUPOS,
        "filtros_aceitos": [
            "grupo (todos|altamiro|couto)",
            "postos (csv — A,B,C)",
            "data_ini, data_fim (YYYY-MM-DD ou DD/MM/YYYY)",
            "mes (YYYY-MM — atalho para data_pagamento naquele mes)",
            "cliente, tipo, plano, plano_principal, fornecedor, conta, corretor, medico, forma, descricao (LIKE ILIKE)",
            "min_valor, max_valor",
            "limit (1..500, default 100) — so em /despesas",
            "order (data_pagamento_desc|id_desc|id_asc|valor_desc) — so em /despesas",
            "cursor, cursor_posto (paginacao id_desc/id_asc) — so em /despesas",
            "top (1..1000, default 100) — so em /resumo",
            "group_by (conta|plano|plano_principal|tipo|fornecedor|corretor|medico|posto|mes|forma|cliente) — so em /resumo",
        ],
        "colunas_retorno_listagem": COLUNAS_RETORNO,
        "group_by_options": sorted(GROUP_BY_ALIASES.keys()),
        "exemplos": {
            "ultimos_100_altamiro":
                "/api/fin/despesas?grupo=altamiro&order=data_pagamento_desc&limit=100",
            "conta_que_aumentou_em_marco":
                "/api/fin/despesas/resumo?grupo=todos&mes=2026-03&group_by=conta&top=30",
            "plano_principal_por_posto":
                "/api/fin/despesas/resumo?postos=A&data_ini=2026-01-01&data_fim=2026-03-31&group_by=plano_principal",
            "despesas_de_um_fornecedor":
                "/api/fin/despesas?fornecedor=FORNECEDOR+XYZ&order=data_pagamento_desc&limit=50",
            "pagamentos_acima_de_10k":
                "/api/fin/despesas?min_valor=10000&order=valor_desc&limit=50",
        },
        "filtros_etl_fixos": [
            "[Valor pago] IS NOT NULL",
            "idContaTipo <> 11",
            "[Data de pagamento] >= 2020-01-01",
        ],
    }
    return jsonify(info)
