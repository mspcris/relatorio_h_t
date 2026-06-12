"""
API do KPI Indicadores Vinícius Gama — Indicador 1: Situação dos Clientes.

Fonte: RDS AWS Postgres, DB relatorio_h_t, tabelas kpi_vg_situacao_clientes e
kpi_vg_matriculas_anteriores (ETL diário export_vg_situacao_clientes.py —
full rebuild, dados isolados deste KPI).

Endpoints:
  GET /api/vg/situacao_clientes
        ?de=YYYY-MM-DD&ate=YYYY-MM-DD&postos=A,B (ou "todos")
        — linhas individuais filtradas pela ACL de postos do usuário
  GET /api/vg/situacao_clientes/<posto>/<id_cliente>/matriculas
        — detalhe das matrículas anteriores (modal do front)

Auth: cookie de sessão (decode_user), com filtro pela ACL de postos.
Credenciais: PG_RDS_* no /opt/relatorio_h_t/.env (carregado pelo app.py).
"""
from __future__ import annotations

import logging
import os
from datetime import date

import psycopg2
import psycopg2.extras
from flask import Blueprint, jsonify, request

log = logging.getLogger(__name__)

vg_indicadores_bp = Blueprint("vg_indicadores_api", __name__)

POSTOS_VALIDOS = {"A", "B", "C", "D", "G", "I", "J", "M", "N", "P", "R", "X", "Y"}


def _pg():
    return psycopg2.connect(
        host=os.environ["PG_RDS_HOST"],
        port=int(os.environ.get("PG_RDS_PORT", "9432")),
        dbname=os.environ.get("PG_RDS_DB", "relatorio_h_t"),
        user=os.environ["PG_RDS_USER"],
        password=os.environ["PG_RDS_PASSWORD"],
        sslmode=os.environ.get("PG_RDS_SSLMODE", "require"),
        connect_timeout=10,
    )


def _auth_postos():
    """Retorna (email, postos_autorizados) ou (None, None)."""
    from auth_routes import decode_user
    email, postos = decode_user()
    if not email:
        return None, None
    return email, [p for p in (postos or []) if p in POSTOS_VALIDOS]


def _parse_date(s):
    try:
        return date.fromisoformat((s or "").strip())
    except ValueError:
        return None


@vg_indicadores_bp.get("/api/vg/situacao_clientes")
def vg_listar_clientes():
    email, postos_acl = _auth_postos()
    if not email:
        return jsonify({"ok": False, "error": "não autenticado"}), 401
    if not postos_acl:
        return jsonify({"ok": False, "error": "usuário sem postos liberados"}), 403

    de = _parse_date(request.args.get("de"))
    ate = _parse_date(request.args.get("ate"))
    if not de or not ate:
        return jsonify({"ok": False, "error": "parâmetros de/ate (YYYY-MM-DD) são obrigatórios"}), 400

    postos_param = (request.args.get("postos") or "todos").strip().upper()
    if postos_param in ("", "TODOS"):
        postos = postos_acl
    else:
        pedidos = [p.strip() for p in postos_param.split(",") if p.strip()]
        postos = [p for p in pedidos if p in postos_acl]
        if not postos:
            return jsonify({"ok": False, "error": "nenhum posto solicitado está autorizado"}), 403

    sql = """
        SELECT posto, id_cliente, matricula, nome, cpf,
               to_char(data_admissao, 'YYYY-MM-DD') AS data_admissao,
               situacao, situacao_clube, idade, sexo, responsavel,
               telefone_whatsapp, telefone_celular,
               valor_devido::float8 AS valor_devido,
               valor_pago::float8  AS valor_pago,
               mensalidade::float8 AS mensalidade,
               receitas_qtd, dependentes_qtd,
               consultas_dia_adesao_qtd, usou_plano_dia_adesao,
               consultas_futuras_qtd, tem_consulta_futura,
               matriculas_anteriores_qtd, mat_ant_titular_qtd,
               mat_ant_dependente_qtd, mat_ant_responsavel_qtd
        FROM kpi_vg_situacao_clientes
        WHERE posto = ANY(%s)
          AND data_admissao BETWEEN %s AND %s
        ORDER BY data_admissao DESC, posto, nome
    """
    try:
        conn = _pg()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as c:
                c.execute(sql, (postos, de, ate))
                rows = c.fetchall()
                c.execute("SELECT to_char(MAX(atualizado_em), 'YYYY-MM-DD\"T\"HH24:MI:SS') FROM kpi_vg_situacao_clientes")
                gerado_em = c.fetchone()["to_char"]
        finally:
            conn.close()
    except Exception as e:
        log.exception("vg_listar_clientes: erro Postgres")
        return jsonify({"ok": False, "error": f"erro ao consultar dados: {e}"}), 500

    return jsonify({
        "ok": True,
        "total": len(rows),
        "gerado_em": gerado_em,
        "clientes": rows,
    })


@vg_indicadores_bp.get("/api/vg/situacao_clientes/<posto>/<int:id_cliente>/matriculas")
def vg_matriculas_anteriores(posto, id_cliente):
    email, postos_acl = _auth_postos()
    if not email:
        return jsonify({"ok": False, "error": "não autenticado"}), 401
    posto = (posto or "").strip().upper()
    if posto not in postos_acl:
        return jsonify({"ok": False, "error": "posto não autorizado"}), 403

    sql = """
        SELECT cpf, pessoa_nome, papel_na_nova,
               posto_anterior, id_cliente_anterior, id_dependente_anterior,
               matricula_anterior, papel_anterior, nome_anterior,
               to_char(data_admissao_anterior, 'YYYY-MM-DD') AS data_admissao_anterior,
               desativado_anterior
        FROM kpi_vg_matriculas_anteriores
        WHERE posto = %s AND id_cliente = %s
        ORDER BY pessoa_nome, papel_anterior, data_admissao_anterior
    """
    try:
        conn = _pg()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as c:
                c.execute(sql, (posto, id_cliente))
                rows = c.fetchall()
        finally:
            conn.close()
    except Exception as e:
        log.exception("vg_matriculas_anteriores: erro Postgres")
        return jsonify({"ok": False, "error": f"erro ao consultar dados: {e}"}), 500

    return jsonify({"ok": True, "total": len(rows), "matriculas": rows})
