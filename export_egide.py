#!/usr/bin/env python3
"""
export_egide.py — KPI Égide (marketplace de saúde + farmácia).

Gera:  json_consolidado/egide_kpi.json
Fonte: MySQL 8.4.8 em egide.cc0jc67g6tt1.sa-east-1.rds.amazonaws.com:29372
       (credenciais no .env: DB_HOST / DB_PORT / DB_USER / DB_PASSWORD / DB_NAME)

Responde as 5 perguntas dos diretores (séries mensais a partir de 2023-01):
  1. Clientes por período — base / novos / ativos por mês
  2. Consultas por período — consultas + exames, agendadas / pagas / canceladas
  3. Particular vs Convênio — volume e receita
  4. Arrecadação por contrato (particular)
  5. Arrecadação por convênio (empilhada por nome do convênio)

Regra de convênio: um atendimento é "convênio" quando o cliente tinha um
convênio ativo (customer_insurances.isCurrent=1) na data da consulta.

Valores monetários estão em centavos (int) — divididos por 100 ao serializar.
"""

from __future__ import annotations

import json
import os
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pymysql
import pymysql.cursors
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
JSON_DIR = BASE_DIR / "json_consolidado"
OUT_PATH = JSON_DIR / "egide_kpi.json"
JSON_DIR.mkdir(exist_ok=True)

load_dotenv(BASE_DIR / ".env")


def _env(egide_key: str, fallback_key: str) -> str:
    """Lê EGIDE_<KEY> primeiro; cai em <KEY> pra manter compat do ambiente local."""
    v = os.environ.get(egide_key) or os.environ.get(fallback_key)
    if not v:
        raise RuntimeError(f"variável {egide_key} (ou {fallback_key}) não definida no .env")
    return v


CONN_KW = dict(
    host=_env("EGIDE_DB_HOST",     "DB_HOST"),
    port=int(_env("EGIDE_DB_PORT", "DB_PORT")),
    user=_env("EGIDE_DB_USER",     "DB_USER"),
    password=_env("EGIDE_DB_PASSWORD", "DB_PASSWORD"),
    database=_env("EGIDE_DB_NAME", "DB_NAME"),
    charset="utf8mb4",
    connect_timeout=15,
    read_timeout=180,
    cursorclass=pymysql.cursors.DictCursor,
)

INICIO = os.getenv("EGIDE_INICIO_MES", "2023-01")

# Banco armazena datas em UTC; KPI e filtros são em horário de Brasília (BRT).
# Aplicamos CONVERT_TZ em toda coluna de data que aparece em DATE_FORMAT / WHERE.
# Uso: BRT("da.`date`") -> "CONVERT_TZ(da.`date`, '+00:00', '-03:00')"
def BRT(col: str) -> str:
    return f"CONVERT_TZ({col}, '+00:00', '-03:00')"


def _json_default(o: Any):
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    if isinstance(o, Decimal):
        return float(o)
    return str(o)


def _q(cur, sql: str, params=()):
    cur.execute(sql, params)
    return cur.fetchall()


def pergunta1_clientes(cur) -> dict:
    """Base total + novos por mês + ativos por mês."""
    base = _q(cur, """
        SELECT COUNT(*) AS total,
               SUM(CASE WHEN deleted_at IS NULL THEN 1 ELSE 0 END) AS ativos_cadastro
        FROM customers
    """)[0]

    novos = _q(cur, f"""
        SELECT DATE_FORMAT({BRT('created_at')},'%%Y-%%m') mes, COUNT(*) novos
        FROM customers
        WHERE {BRT('created_at')} >= %s
        GROUP BY mes ORDER BY mes
    """, (f"{INICIO}-01",))

    ativos_orders = _q(cur, f"""
        SELECT DATE_FORMAT({BRT('paymentDate')},'%%Y-%%m') mes,
               COUNT(DISTINCT customerId) ativos
        FROM orders
        WHERE paymentDate IS NOT NULL
          AND {BRT('paymentDate')} >= %s
        GROUP BY mes ORDER BY mes
    """, (f"{INICIO}-01",))

    ativos_appts = _q(cur, f"""
        SELECT DATE_FORMAT({BRT('`date`')},'%%Y-%%m') mes,
               COUNT(DISTINCT customerId) ativos
        FROM doctorappointments
        WHERE customerId IS NOT NULL
          AND (paymentDate IS NOT NULL OR confirmationDate IS NOT NULL)
          AND {BRT('`date`')} >= %s
        GROUP BY mes ORDER BY mes
    """, (f"{INICIO}-01",))

    ativos_qualquer = _q(cur, f"""
        SELECT mes, COUNT(DISTINCT customerId) ativos FROM (
            SELECT DATE_FORMAT({BRT('paymentDate')},'%%Y-%%m') mes, customerId
            FROM orders
            WHERE paymentDate IS NOT NULL AND {BRT('paymentDate')} >= %s
            UNION
            SELECT DATE_FORMAT({BRT('`date`')},'%%Y-%%m') mes, customerId
            FROM doctorappointments
            WHERE customerId IS NOT NULL
              AND (paymentDate IS NOT NULL OR confirmationDate IS NOT NULL)
              AND {BRT('`date`')} >= %s
        ) u
        GROUP BY mes ORDER BY mes
    """, (f"{INICIO}-01", f"{INICIO}-01"))

    # Base acumulada por mês (inclui pre-INICIO para bater com base_total)
    base_acumulada = _q(cur, f"""
        SELECT mes, novos,
               SUM(novos) OVER (ORDER BY mes) acumulada
        FROM (
            SELECT DATE_FORMAT({BRT('created_at')},'%%Y-%%m') mes, COUNT(*) novos
            FROM customers
            WHERE created_at IS NOT NULL
            GROUP BY mes
        ) t
        ORDER BY mes
    """)

    return {
        "base_total": int(base["total"] or 0),
        "base_ativos_cadastro": int(base["ativos_cadastro"] or 0),
        "novos_por_mes": novos,
        "ativos_orders_por_mes": ativos_orders,
        "ativos_consultas_por_mes": ativos_appts,
        "ativos_por_mes": ativos_qualquer,
        "base_acumulada_por_mes": base_acumulada,
    }


def pergunta2_consultas(cur) -> dict:
    """Consultas + exames por mês usando doctorappointments.date + drilldowns."""
    # Classificação consulta/exame alinhada ao drilldown (/api/egide/rows):
    #   consulta = schedule.clinicDoctorSpecialtyId OU (sem schedule AND da.specialtyId)
    #   exame    = schedule.clinicExamId/examgroupscheduleId OU (sem schedule AND da.examId)
    #   outros   = sem schedule E sem specialtyId E sem examId (não somam em consultas/exames)
    mensal = _q(cur, f"""
        SELECT DATE_FORMAT({BRT('da.`date`')},'%%Y-%%m') mes,
               COUNT(*) agendadas,
               SUM(CASE WHEN da.paymentDate     IS NOT NULL THEN 1 ELSE 0 END) pagas,
               SUM(CASE WHEN da.confirmationDate IS NOT NULL THEN 1 ELSE 0 END) confirmadas,
               SUM(CASE WHEN da.canceledAt      IS NOT NULL THEN 1 ELSE 0 END) canceladas,
               SUM(CASE WHEN s.clinicDoctorSpecialtyId IS NOT NULL
                             OR (s.id IS NULL AND da.specialtyId IS NOT NULL)
                        THEN 1 ELSE 0 END) consultas,
               SUM(CASE WHEN s.clinicExamId IS NOT NULL
                             OR s.examgroupscheduleId IS NOT NULL
                             OR (s.id IS NULL AND da.examId IS NOT NULL)
                        THEN 1 ELSE 0 END) exames
        FROM doctorappointments da
        LEFT JOIN schedules s ON s.id = da.scheduleId
        WHERE {BRT('da.`date`')} >= %s
        GROUP BY mes ORDER BY mes
    """, (f"{INICIO}-01",))

    totais = _q(cur, """
        SELECT
            COUNT(*) total,
            SUM(CASE WHEN da.paymentDate IS NOT NULL AND da.canceledAt IS NULL THEN 1 ELSE 0 END) pagas,
            SUM(CASE WHEN da.canceledAt  IS NOT NULL THEN 1 ELSE 0 END) canceladas,
            SUM(CASE WHEN s.clinicDoctorSpecialtyId IS NOT NULL
                          OR (s.id IS NULL AND da.specialtyId IS NOT NULL)
                     THEN 1 ELSE 0 END) consultas,
            SUM(CASE WHEN s.clinicExamId IS NOT NULL
                          OR s.examgroupscheduleId IS NOT NULL
                          OR (s.id IS NULL AND da.examId IS NOT NULL)
                     THEN 1 ELSE 0 END) exames
        FROM doctorappointments da
        LEFT JOIN schedules s ON s.id = da.scheduleId
    """)[0]

    # Drilldown por ESPECIALIDADE (agendadas não canceladas)
    por_especialidade_mes = _q(cur, f"""
        SELECT DATE_FORMAT({BRT('da.`date`')},'%%Y-%%m') mes,
               COALESCE(s.name, '(Exame — sem especialidade)') especialidade,
               COUNT(*) agendadas,
               SUM(CASE WHEN da.paymentDate IS NOT NULL THEN 1 ELSE 0 END) pagas,
               ROUND(SUM(CASE WHEN da.paymentDate IS NOT NULL THEN IFNULL(da.total,0) ELSE 0 END)/100.0, 2) receita_paga
        FROM doctorappointments da
        LEFT JOIN specialties s ON s.id = da.specialtyId
        WHERE {BRT('da.`date`')} >= %s AND da.canceledAt IS NULL
        GROUP BY mes, especialidade
        ORDER BY mes, agendadas DESC
    """, (f"{INICIO}-01",))

    # Total histórico por especialidade (ranking global)
    totais_especialidade = _q(cur, """
        SELECT COALESCE(s.name, '(Exame — sem especialidade)') especialidade,
               COUNT(*) agendadas,
               SUM(CASE WHEN da.paymentDate IS NOT NULL THEN 1 ELSE 0 END) pagas,
               ROUND(SUM(CASE WHEN da.paymentDate IS NOT NULL THEN IFNULL(da.total,0) ELSE 0 END)/100.0, 2) receita_paga,
               ROUND(AVG(CASE WHEN da.paymentDate IS NOT NULL THEN IFNULL(da.total,0) END)/100.0, 2) ticket_medio
        FROM doctorappointments da
        LEFT JOIN specialties s ON s.id = da.specialtyId
        WHERE da.canceledAt IS NULL
        GROUP BY especialidade
        ORDER BY agendadas DESC
    """)

    # Drilldown por CLÍNICA (via schedule → clinic_doctor_specialties/clinic_exams → clinics)
    por_clinica_mes = _q(cur, f"""
        SELECT DATE_FORMAT({BRT('da.`date`')},'%%Y-%%m') mes,
               COALESCE(c.name, '(sem clínica associada)') clinica,
               COALESCE(c.city, '—') cidade,
               COUNT(*) agendadas,
               SUM(CASE WHEN da.paymentDate IS NOT NULL THEN 1 ELSE 0 END) pagas,
               ROUND(SUM(CASE WHEN da.paymentDate IS NOT NULL THEN IFNULL(da.total,0) ELSE 0 END)/100.0, 2) receita_paga
        FROM doctorappointments da
        LEFT JOIN schedules sc ON sc.id = da.scheduleId
        LEFT JOIN clinic_doctor_specialties cds ON cds.id = sc.clinicDoctorSpecialtyId
        LEFT JOIN clinic_doctors cd ON cd.id = cds.clinicDoctorId
        LEFT JOIN clinic_exams ce ON ce.id = sc.clinicExamId
        LEFT JOIN clinics c ON c.id = COALESCE(cd.clinicId, ce.clinicId)
        WHERE {BRT('da.`date`')} >= %s AND da.canceledAt IS NULL
        GROUP BY mes, clinica, cidade
        ORDER BY mes, agendadas DESC
    """, (f"{INICIO}-01",))

    totais_clinica = _q(cur, """
        SELECT COALESCE(c.name, '(sem clínica associada)') clinica,
               COALESCE(c.city, '—') cidade,
               COALESCE(c.state, '—') estado,
               COUNT(*) agendadas,
               SUM(CASE WHEN da.paymentDate IS NOT NULL THEN 1 ELSE 0 END) pagas,
               ROUND(SUM(CASE WHEN da.paymentDate IS NOT NULL THEN IFNULL(da.total,0) ELSE 0 END)/100.0, 2) receita_paga,
               ROUND(AVG(CASE WHEN da.paymentDate IS NOT NULL THEN IFNULL(da.total,0) END)/100.0, 2) ticket_medio
        FROM doctorappointments da
        LEFT JOIN schedules sc ON sc.id = da.scheduleId
        LEFT JOIN clinic_doctor_specialties cds ON cds.id = sc.clinicDoctorSpecialtyId
        LEFT JOIN clinic_doctors cd ON cd.id = cds.clinicDoctorId
        LEFT JOIN clinic_exams ce ON ce.id = sc.clinicExamId
        LEFT JOIN clinics c ON c.id = COALESCE(cd.clinicId, ce.clinicId)
        WHERE da.canceledAt IS NULL
        GROUP BY clinica, cidade, estado
        ORDER BY agendadas DESC
    """)

    return {
        "mensal": mensal,
        "totais_historico": {k: int(v or 0) for k, v in totais.items()},
        "por_especialidade_mes": por_especialidade_mes,
        "totais_especialidade": totais_especialidade,
        "por_clinica_mes": por_clinica_mes,
        "totais_clinica": totais_clinica,
    }


def pergunta3_split(cur) -> dict:
    """Particular vs Convênio por mês: volume e receita, usando customer_insurances."""
    mensal = _q(cur, f"""
        SELECT
            DATE_FORMAT({BRT('da.`date`')},'%%Y-%%m') mes,
            SUM(CASE WHEN conv.cust IS NOT NULL THEN 1 ELSE 0 END) consultas_convenio,
            SUM(CASE WHEN conv.cust IS NULL     THEN 1 ELSE 0 END) consultas_particular,
            ROUND(SUM(CASE WHEN conv.cust IS NOT NULL THEN IFNULL(da.total,0) ELSE 0 END)/100.0, 2) receita_convenio,
            ROUND(SUM(CASE WHEN conv.cust IS NULL     THEN IFNULL(da.total,0) ELSE 0 END)/100.0, 2) receita_particular
        FROM doctorappointments da
        LEFT JOIN (
            SELECT DISTINCT ci.customerId cust, ci.created_at, ci.deleted_at
            FROM customer_insurances ci
            WHERE ci.isCurrent = 1
        ) conv
          ON conv.cust = da.customerId
         AND conv.created_at <= da.`date`
         AND (conv.deleted_at IS NULL OR conv.deleted_at > da.`date`)
        WHERE da.canceledAt IS NULL
          AND {BRT('da.`date`')} >= %s
        GROUP BY mes ORDER BY mes
    """, (f"{INICIO}-01",))

    # Totais históricos (para pizza)
    totais = _q(cur, """
        SELECT
            SUM(CASE WHEN conv.cust IS NOT NULL THEN 1 ELSE 0 END) consultas_convenio,
            SUM(CASE WHEN conv.cust IS NULL     THEN 1 ELSE 0 END) consultas_particular,
            ROUND(SUM(CASE WHEN conv.cust IS NOT NULL THEN IFNULL(da.total,0) ELSE 0 END)/100.0, 2) receita_convenio,
            ROUND(SUM(CASE WHEN conv.cust IS NULL     THEN IFNULL(da.total,0) ELSE 0 END)/100.0, 2) receita_particular
        FROM doctorappointments da
        LEFT JOIN (
            SELECT DISTINCT ci.customerId cust, ci.created_at, ci.deleted_at
            FROM customer_insurances ci
            WHERE ci.isCurrent = 1
        ) conv
          ON conv.cust = da.customerId
         AND conv.created_at <= da.`date`
         AND (conv.deleted_at IS NULL OR conv.deleted_at > da.`date`)
        WHERE da.canceledAt IS NULL
    """)[0]

    return {
        "mensal": mensal,
        "totais_historico": totais,
    }


def pergunta4_receita_particular(cur) -> dict:
    """Arrecadação por contrato (particular): Clínica + Farmácia.

    Regras:
      - 'Clínica' = orders.orderType='clinic' pagos + doctorappointments pagos
        sem orderId (fluxo antigo, onde a consulta foi paga direto na tabela
        doctorappointments sem criar um registro em orders).
      - 'Farmácia' = orders.orderType='pharmacy' pagos.
      - Demais orderTypes (ex: 'other') não aparecem nos dados hoje; se aparecerem,
        vão como 'Outros' para não perder receita.
    """
    orders = _q(cur, f"""
        SELECT DATE_FORMAT({BRT('paymentDate')},'%%Y-%%m') mes,
               orderType,
               COUNT(*) qtde,
               ROUND(SUM(IFNULL(total,0))/100.0, 2) receita
        FROM orders
        WHERE paymentDate IS NOT NULL
          AND cancelDate IS NULL
          AND {BRT('paymentDate')} >= %s
        GROUP BY mes, orderType ORDER BY mes, orderType
    """, (f"{INICIO}-01",))

    appt_sem_order = _q(cur, f"""
        SELECT DATE_FORMAT({BRT('paymentDate')},'%%Y-%%m') mes,
               COUNT(*) qtde,
               ROUND(SUM(IFNULL(total,0))/100.0, 2) receita
        FROM doctorappointments
        WHERE paymentDate IS NOT NULL
          AND canceledAt IS NULL
          AND orderId IS NULL
          AND total > 0
          AND {BRT('paymentDate')} >= %s
        GROUP BY mes ORDER BY mes
    """, (f"{INICIO}-01",))

    # Série unificada por canal lógico (3 categorias: clinica / farmacia / outros)
    # "clinica" = orders.clinic + appointments sem order. É tudo consulta/exame pra
    # quem usa o dashboard.
    por_canal_mes = {}
    def _acum(mes, canal, qtde, receita):
        key = (mes, canal)
        if key not in por_canal_mes:
            por_canal_mes[key] = {"mes": mes, "canal": canal, "qtde": 0, "receita": 0.0}
        por_canal_mes[key]["qtde"] += int(qtde or 0)
        por_canal_mes[key]["receita"] += float(receita or 0)

    for r in orders:
        ot = (r.get("orderType") or "").lower()
        if ot == "clinic":
            canal = "clinica"
        elif ot == "pharmacy":
            canal = "farmacia"
        else:
            canal = "outros"
        _acum(r["mes"], canal, r["qtde"], r["receita"])

    for r in appt_sem_order:
        _acum(r["mes"], "clinica", r["qtde"], r["receita"])

    serie_canal = sorted(por_canal_mes.values(), key=lambda x: (x["mes"], x["canal"]))
    for r in serie_canal:
        r["receita"] = round(r["receita"], 2)

    consolidado = _q(cur, f"""
        SELECT mes, ROUND(SUM(receita),2) receita, SUM(qtde) qtde FROM (
            SELECT DATE_FORMAT({BRT('paymentDate')},'%%Y-%%m') mes,
                   COUNT(*) qtde, SUM(IFNULL(total,0))/100.0 receita
            FROM orders
            WHERE paymentDate IS NOT NULL AND cancelDate IS NULL AND {BRT('paymentDate')} >= %s
            GROUP BY mes
            UNION ALL
            SELECT DATE_FORMAT({BRT('paymentDate')},'%%Y-%%m'),
                   COUNT(*), SUM(IFNULL(total,0))/100.0
            FROM doctorappointments
            WHERE paymentDate IS NOT NULL AND canceledAt IS NULL AND orderId IS NULL
              AND total > 0 AND {BRT('paymentDate')} >= %s
            GROUP BY 1
        ) x GROUP BY mes ORDER BY mes
    """, (f"{INICIO}-01", f"{INICIO}-01"))

    return {
        "orders_por_tipo": orders,
        "appointments_sem_order": appt_sem_order,
        "serie_canal": serie_canal,
        "consolidado_mensal": consolidado,
    }


def pergunta5_receita_convenio(cur) -> dict:
    """Arrecadação por convênio — empilhado por nome."""
    # LEFT JOIN + COALESCE expõe atendimentos sem convênio como 'PARTICULAR',
    # para que apareçam no filtro de convênios do frontend.
    mensal = _q(cur, f"""
        SELECT DATE_FORMAT({BRT('da.`date`')},'%%Y-%%m') mes,
               COALESCE(i.name, 'PARTICULAR') convenio,
               COUNT(DISTINCT da.id) consultas,
               ROUND(SUM(IFNULL(da.total,0))/100.0, 2) receita
        FROM doctorappointments da
        LEFT JOIN customer_insurances ci
          ON ci.customerId  = da.customerId
         AND ci.isCurrent   = 1
         AND ci.created_at <= da.`date`
         AND (ci.deleted_at IS NULL OR ci.deleted_at > da.`date`)
        LEFT JOIN insurances i ON i.id = ci.insuranceId
        WHERE da.canceledAt IS NULL
          AND {BRT('da.`date`')} >= %s
        GROUP BY mes, convenio
        ORDER BY mes, receita DESC
    """, (f"{INICIO}-01",))

    totais_conv = _q(cur, """
        SELECT COALESCE(i.name, 'PARTICULAR') convenio,
               COUNT(DISTINCT da.id) consultas,
               ROUND(SUM(IFNULL(da.total,0))/100.0, 2) receita,
               ROUND(AVG(IFNULL(da.total,0))/100.0, 2) ticket_medio
        FROM doctorappointments da
        LEFT JOIN customer_insurances ci
          ON ci.customerId  = da.customerId
         AND ci.isCurrent   = 1
         AND ci.created_at <= da.`date`
         AND (ci.deleted_at IS NULL OR ci.deleted_at > da.`date`)
        LEFT JOIN insurances i ON i.id = ci.insuranceId
        WHERE da.canceledAt IS NULL
        GROUP BY convenio
        ORDER BY receita DESC
    """)

    convenios_cadastrados = _q(cur, """
        SELECT id, name,
               (SELECT COUNT(*) FROM customer_insurances ci
                WHERE ci.insuranceId = i.id AND ci.isCurrent = 1) AS carteirinhas_ativas
        FROM insurances i
        WHERE i.deleted_at IS NULL
        ORDER BY name
    """)

    # Drilldown: por convênio x especialidade (histórico)
    por_convenio_especialidade = _q(cur, """
        SELECT COALESCE(i.name, 'PARTICULAR') convenio,
               COALESCE(s.name, '(Exame — sem especialidade)') especialidade,
               COUNT(DISTINCT da.id) consultas,
               ROUND(SUM(IFNULL(da.total,0))/100.0, 2) receita
        FROM doctorappointments da
        LEFT JOIN customer_insurances ci
          ON ci.customerId  = da.customerId
         AND ci.isCurrent   = 1
         AND ci.created_at <= da.`date`
         AND (ci.deleted_at IS NULL OR ci.deleted_at > da.`date`)
        LEFT JOIN insurances i ON i.id = ci.insuranceId
        LEFT JOIN specialties s ON s.id = da.specialtyId
        WHERE da.canceledAt IS NULL
        GROUP BY convenio, especialidade
        ORDER BY convenio, consultas DESC
    """)

    # Drilldown: por convênio x clínica (histórico)
    por_convenio_clinica = _q(cur, """
        SELECT COALESCE(i.name, 'PARTICULAR') convenio,
               COALESCE(c.name, '(sem clínica associada)') clinica,
               COALESCE(c.city, '—') cidade,
               COUNT(DISTINCT da.id) consultas,
               ROUND(SUM(IFNULL(da.total,0))/100.0, 2) receita
        FROM doctorappointments da
        LEFT JOIN customer_insurances ci
          ON ci.customerId  = da.customerId
         AND ci.isCurrent   = 1
         AND ci.created_at <= da.`date`
         AND (ci.deleted_at IS NULL OR ci.deleted_at > da.`date`)
        LEFT JOIN insurances i ON i.id = ci.insuranceId
        LEFT JOIN schedules sc ON sc.id = da.scheduleId
        LEFT JOIN clinic_doctor_specialties cds ON cds.id = sc.clinicDoctorSpecialtyId
        LEFT JOIN clinic_doctors cd ON cd.id = cds.clinicDoctorId
        LEFT JOIN clinic_exams ce ON ce.id = sc.clinicExamId
        LEFT JOIN clinics c ON c.id = COALESCE(cd.clinicId, ce.clinicId)
        WHERE da.canceledAt IS NULL
        GROUP BY convenio, clinica, cidade
        ORDER BY convenio, consultas DESC
    """)

    # Drilldown mensal: por convênio x especialidade (permite filtrar por período no front)
    por_convenio_especialidade_mes = _q(cur, f"""
        SELECT DATE_FORMAT({BRT('da.`date`')},'%%Y-%%m') mes,
               COALESCE(i.name, 'PARTICULAR') convenio,
               COALESCE(s.name, '(Exame — sem especialidade)') especialidade,
               COUNT(DISTINCT da.id) consultas,
               ROUND(SUM(IFNULL(da.total,0))/100.0, 2) receita
        FROM doctorappointments da
        LEFT JOIN customer_insurances ci
          ON ci.customerId  = da.customerId
         AND ci.isCurrent   = 1
         AND ci.created_at <= da.`date`
         AND (ci.deleted_at IS NULL OR ci.deleted_at > da.`date`)
        LEFT JOIN insurances i ON i.id = ci.insuranceId
        LEFT JOIN specialties s ON s.id = da.specialtyId
        WHERE da.canceledAt IS NULL
          AND {BRT('da.`date`')} >= %s
        GROUP BY mes, convenio, especialidade
        ORDER BY mes, convenio, consultas DESC
    """, (f"{INICIO}-01",))

    # Drilldown mensal: por convênio x clínica (permite filtrar por período no front)
    por_convenio_clinica_mes = _q(cur, f"""
        SELECT DATE_FORMAT({BRT('da.`date`')},'%%Y-%%m') mes,
               COALESCE(i.name, 'PARTICULAR') convenio,
               COALESCE(c.name, '(sem clínica associada)') clinica,
               COALESCE(c.city, '—') cidade,
               COUNT(DISTINCT da.id) consultas,
               ROUND(SUM(IFNULL(da.total,0))/100.0, 2) receita
        FROM doctorappointments da
        LEFT JOIN customer_insurances ci
          ON ci.customerId  = da.customerId
         AND ci.isCurrent   = 1
         AND ci.created_at <= da.`date`
         AND (ci.deleted_at IS NULL OR ci.deleted_at > da.`date`)
        LEFT JOIN insurances i ON i.id = ci.insuranceId
        LEFT JOIN schedules sc ON sc.id = da.scheduleId
        LEFT JOIN clinic_doctor_specialties cds ON cds.id = sc.clinicDoctorSpecialtyId
        LEFT JOIN clinic_doctors cd ON cd.id = cds.clinicDoctorId
        LEFT JOIN clinic_exams ce ON ce.id = sc.clinicExamId
        LEFT JOIN clinics c ON c.id = COALESCE(cd.clinicId, ce.clinicId)
        WHERE da.canceledAt IS NULL
          AND {BRT('da.`date`')} >= %s
        GROUP BY mes, convenio, clinica, cidade
        ORDER BY mes, convenio, consultas DESC
    """, (f"{INICIO}-01",))

    return {
        "mensal_por_convenio": mensal,
        "totais_por_convenio": totais_conv,
        "convenios_cadastrados": convenios_cadastrados,
        "por_convenio_especialidade": por_convenio_especialidade,
        "por_convenio_clinica": por_convenio_clinica,
        "por_convenio_especialidade_mes": por_convenio_especialidade_mes,
        "por_convenio_clinica_mes": por_convenio_clinica_mes,
    }


def visao_geral(cur) -> dict:
    """Pequenos números de topo (farmácia + clínica) para contexto."""
    farmacia = _q(cur, f"""
        SELECT DATE_FORMAT({BRT('paymentDate')},'%%Y-%%m') mes,
               COUNT(*) pedidos,
               COUNT(DISTINCT customerId) clientes,
               ROUND(SUM(IFNULL(total,0))/100.0, 2) receita
        FROM orders
        WHERE orderType = 'pharmacy'
          AND paymentDate IS NOT NULL
          AND cancelDate IS NULL
          AND {BRT('paymentDate')} >= %s
        GROUP BY mes ORDER BY mes
    """, (f"{INICIO}-01",))

    clinica = _q(cur, f"""
        SELECT DATE_FORMAT({BRT('paymentDate')},'%%Y-%%m') mes,
               COUNT(*) pedidos,
               COUNT(DISTINCT customerId) clientes,
               ROUND(SUM(IFNULL(total,0))/100.0, 2) receita
        FROM orders
        WHERE orderType = 'clinic'
          AND paymentDate IS NOT NULL
          AND cancelDate IS NULL
          AND {BRT('paymentDate')} >= %s
        GROUP BY mes ORDER BY mes
    """, (f"{INICIO}-01",))

    return {"farmacia_mensal": farmacia, "clinica_mensal": clinica}


def main():
    gerado_em = datetime.now().isoformat(timespec="seconds")
    print(f"[egide] conectando em {CONN_KW['host']}:{CONN_KW['port']}...")

    with pymysql.connect(**CONN_KW) as cn:
        with cn.cursor() as cur:
            print("[egide] P1 · clientes...")
            p1 = pergunta1_clientes(cur)
            print("[egide] P2 · consultas...")
            p2 = pergunta2_consultas(cur)
            print("[egide] P3 · particular vs convênio...")
            p3 = pergunta3_split(cur)
            print("[egide] P4 · receita particular...")
            p4 = pergunta4_receita_particular(cur)
            print("[egide] P5 · receita convênio...")
            p5 = pergunta5_receita_convenio(cur)
            print("[egide] visão geral (farmácia/clínica)...")
            geral = visao_geral(cur)

    payload = {
        "meta": {
            "gerado_em": gerado_em,
            "inicio_mes": INICIO,
            "fonte": "egide_production @ egide.cc0jc67g6tt1.sa-east-1.rds.amazonaws.com",
            "notas": [
                "Fuso horário: banco armazena em UTC; este KPI exibe e agrupa em BRT (UTC-3) via CONVERT_TZ.",
                "Valores monetários vêm de total (centavos) e foram convertidos para R$ (÷100).",
                "Definição de 'convênio': cliente com customer_insurances ATIVO na data da consulta.",
                "Pergunta 2 usa doctorappointments.date (data da consulta), não created_at.",
                "Pergunta 4: 'Clínica' = orders.clinic + appointments pagos sem orderId (fluxo antigo, onde a consulta foi paga direto na tabela doctorappointments, sem passar por orders). Tudo é consulta/exame.",
                "Pergunta 5 considera apenas appointments não cancelados com convênio ativo.",
            ],
        },
        "p1_clientes":            p1,
        "p2_consultas":           p2,
        "p3_particular_convenio": p3,
        "p4_receita_particular":  p4,
        "p5_receita_convenio":    p5,
        "visao_geral":            geral,
    }

    OUT_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default),
        encoding="utf-8",
    )
    tam_kb = OUT_PATH.stat().st_size / 1024
    print(f"[egide] ✔ {OUT_PATH} ({tam_kb:.1f} KB)")


if __name__ == "__main__":
    main()
