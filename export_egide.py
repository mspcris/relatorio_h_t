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

    # Série por DATA DE PAGAMENTO (casa com os cards "Consultas/Exames pagos" e "Arrecadação"):
    # agrupa por mês BRT do paymentDate (da.paymentDate OU o.paymentDate), sem cancelados.
    mensal_pagamento = _q(cur, f"""
        SELECT DATE_FORMAT({BRT('COALESCE(da.paymentDate, o.paymentDate)')},'%%Y-%%m') mes,
               COUNT(*) pagamentos,
               SUM(CASE WHEN s.clinicDoctorSpecialtyId IS NOT NULL
                             OR (s.id IS NULL AND da.specialtyId IS NOT NULL)
                        THEN 1 ELSE 0 END) consultas_pagas,
               SUM(CASE WHEN s.clinicExamId IS NOT NULL
                             OR s.examgroupscheduleId IS NOT NULL
                             OR (s.id IS NULL AND da.examId IS NOT NULL)
                        THEN 1 ELSE 0 END) exames_pagos,
               ROUND(SUM(COALESCE(NULLIF(da.total,0), o.total, 0))/100.0, 2) receita_paga
        FROM doctorappointments da
        LEFT JOIN schedules s ON s.id = da.scheduleId
        LEFT JOIN orders    o ON o.id = da.orderId
        WHERE da.canceledAt IS NULL
          AND (o.id IS NULL OR o.cancelDate IS NULL)
          AND COALESCE(da.paymentDate, o.paymentDate) IS NOT NULL
          AND {BRT('COALESCE(da.paymentDate, o.paymentDate)')} >= %s
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

    # Drilldown por ESPECIALIDADE — SÉRIE DE AGENDADAS.
    # Agrupa pelo mês de AGENDAMENTO (da.date). Só mede volume (agendadas /
    # agendadas_canc). A série de PAGAS/RECEITA é separada logo abaixo, porque
    # "o que foi agendado em Mar" e "o que foi pago em Mar" NÃO são o mesmo
    # conjunto: uma consulta marcada em Fev e paga em Mar conta como 1 em Fev
    # na coluna "agendadas" e 1 em Mar na coluna "pagas". Os dois mundos são
    # mergeados no front pela chave "especialidade" dentro do período filtrado.
    # Isso alinha a tabela com os cards do topo e o gráfico mensal_pagamento,
    # que também usam data de pagamento para "pagas".
    # IMPORTANTE: a tabela Top Especialidades mostra APENAS CONSULTAS. Exames
    # (clinicExamId / examgroupscheduleId / da.examId) têm card próprio e vão
    # para a tabela de Top Exames. O bucket "(Exame — sem especialidade)" que
    # aparecia aqui misturava critérios e confundia a leitura.
    IS_CONSULTA = (
        "(sc.clinicDoctorSpecialtyId IS NOT NULL"
        " OR (sc.id IS NULL AND da.specialtyId IS NOT NULL))"
    )
    IS_EXAME = (
        "(sc.clinicExamId IS NOT NULL"
        " OR sc.examgroupscheduleId IS NOT NULL"
        " OR (sc.id IS NULL AND da.examId IS NOT NULL))"
    )

    por_especialidade_agendadas_mes = _q(cur, f"""
        SELECT DATE_FORMAT({BRT('da.`date`')},'%%Y-%%m') mes,
               COALESCE(s.name, '(sem especialidade)') especialidade,
               SUM(CASE WHEN da.canceledAt IS NULL
                             AND (o.id IS NULL OR o.cancelDate IS NULL)
                        THEN 1 ELSE 0 END) agendadas,
               SUM(CASE WHEN da.canceledAt IS NOT NULL
                             OR o.cancelDate IS NOT NULL
                        THEN 1 ELSE 0 END) agendadas_canc
        FROM doctorappointments da
        LEFT JOIN schedules sc ON sc.id = da.scheduleId
        LEFT JOIN clinic_doctor_specialties cds ON cds.id = sc.clinicDoctorSpecialtyId
        LEFT JOIN specialties s ON s.id = COALESCE(da.specialtyId, cds.specialtyId)
        LEFT JOIN orders o ON o.id = da.orderId
        WHERE {BRT('da.`date`')} >= %s
          AND {IS_CONSULTA}
        GROUP BY mes, especialidade
        HAVING agendadas > 0 OR agendadas_canc > 0
        ORDER BY mes, agendadas DESC
    """, (f"{INICIO}-01",))

    # Drilldown por ESPECIALIDADE — SÉRIE DE PAGAS/RECEITA.
    # Agrupa pelo mês de PAGAMENTO (COALESCE da.paymentDate, o.paymentDate).
    # Mesma regra do card "Consultas pagas" e do gráfico mensal_pagamento.
    por_especialidade_pagas_mes = _q(cur, f"""
        SELECT DATE_FORMAT({BRT('COALESCE(da.paymentDate, o.paymentDate)')},'%%Y-%%m') mes,
               COALESCE(s.name, '(sem especialidade)') especialidade,
               SUM(CASE WHEN da.canceledAt IS NULL
                             AND (o.id IS NULL OR o.cancelDate IS NULL)
                        THEN 1 ELSE 0 END) pagas,
               ROUND(SUM(CASE WHEN da.canceledAt IS NULL
                                   AND (o.id IS NULL OR o.cancelDate IS NULL)
                              THEN COALESCE(NULLIF(da.total,0), o.total, 0) ELSE 0 END)/100.0, 2) receita_paga,
               SUM(CASE WHEN da.canceledAt IS NOT NULL
                             OR o.cancelDate IS NOT NULL
                        THEN 1 ELSE 0 END) pagas_canc,
               ROUND(SUM(CASE WHEN da.canceledAt IS NOT NULL
                                   OR o.cancelDate IS NOT NULL
                              THEN COALESCE(NULLIF(da.total,0), o.total, 0) ELSE 0 END)/100.0, 2) receita_paga_canc
        FROM doctorappointments da
        LEFT JOIN schedules sc ON sc.id = da.scheduleId
        LEFT JOIN clinic_doctor_specialties cds ON cds.id = sc.clinicDoctorSpecialtyId
        LEFT JOIN specialties s ON s.id = COALESCE(da.specialtyId, cds.specialtyId)
        LEFT JOIN orders o ON o.id = da.orderId
        WHERE COALESCE(da.paymentDate, o.paymentDate) IS NOT NULL
          AND {BRT('COALESCE(da.paymentDate, o.paymentDate)')} >= %s
          AND {IS_CONSULTA}
        GROUP BY mes, especialidade
        HAVING pagas > 0 OR pagas_canc > 0
        ORDER BY mes, pagas DESC
    """, (f"{INICIO}-01",))

    # Total histórico por especialidade (ranking global — só consultas).
    totais_especialidade = _q(cur, f"""
        SELECT COALESCE(s.name, '(sem especialidade)') especialidade,
               SUM(CASE WHEN da.canceledAt IS NULL
                             AND (o.id IS NULL OR o.cancelDate IS NULL)
                        THEN 1 ELSE 0 END) agendadas,
               SUM(CASE WHEN da.canceledAt IS NULL
                             AND (o.id IS NULL OR o.cancelDate IS NULL)
                             AND COALESCE(da.paymentDate, o.paymentDate) IS NOT NULL
                        THEN 1 ELSE 0 END) pagas,
               ROUND(SUM(CASE WHEN da.canceledAt IS NULL
                                   AND (o.id IS NULL OR o.cancelDate IS NULL)
                                   AND COALESCE(da.paymentDate, o.paymentDate) IS NOT NULL
                              THEN COALESCE(NULLIF(da.total,0), o.total, 0) ELSE 0 END)/100.0, 2) receita_paga,
               ROUND(AVG(CASE WHEN da.canceledAt IS NULL
                                   AND (o.id IS NULL OR o.cancelDate IS NULL)
                                   AND COALESCE(da.paymentDate, o.paymentDate) IS NOT NULL
                              THEN COALESCE(NULLIF(da.total,0), o.total, 0) END)/100.0, 2) ticket_medio,
               SUM(CASE WHEN da.canceledAt IS NOT NULL OR o.cancelDate IS NOT NULL
                        THEN 1 ELSE 0 END) agendadas_canc,
               SUM(CASE WHEN (da.canceledAt IS NOT NULL OR o.cancelDate IS NOT NULL)
                             AND COALESCE(da.paymentDate, o.paymentDate) IS NOT NULL
                        THEN 1 ELSE 0 END) pagas_canc,
               ROUND(SUM(CASE WHEN (da.canceledAt IS NOT NULL OR o.cancelDate IS NOT NULL)
                                   AND COALESCE(da.paymentDate, o.paymentDate) IS NOT NULL
                              THEN COALESCE(NULLIF(da.total,0), o.total, 0) ELSE 0 END)/100.0, 2) receita_paga_canc
        FROM doctorappointments da
        LEFT JOIN schedules sc ON sc.id = da.scheduleId
        LEFT JOIN clinic_doctor_specialties cds ON cds.id = sc.clinicDoctorSpecialtyId
        LEFT JOIN specialties s ON s.id = COALESCE(da.specialtyId, cds.specialtyId)
        LEFT JOIN orders o ON o.id = da.orderId
        WHERE {IS_CONSULTA}
        GROUP BY especialidade
        HAVING agendadas > 0 OR agendadas_canc > 0
        ORDER BY agendadas DESC
    """)

    # Drilldown por CLÍNICA — SÉRIE DE AGENDADAS (mês de agendamento).
    por_clinica_agendadas_mes = _q(cur, f"""
        SELECT DATE_FORMAT({BRT('da.`date`')},'%%Y-%%m') mes,
               COALESCE(c.name, '(sem clínica associada)') clinica,
               COALESCE(c.city, '—') cidade,
               SUM(CASE WHEN da.canceledAt IS NULL
                             AND (o.id IS NULL OR o.cancelDate IS NULL)
                        THEN 1 ELSE 0 END) agendadas,
               SUM(CASE WHEN da.canceledAt IS NOT NULL
                             OR o.cancelDate IS NOT NULL
                        THEN 1 ELSE 0 END) agendadas_canc
        FROM doctorappointments da
        LEFT JOIN schedules sc ON sc.id = da.scheduleId
        LEFT JOIN clinic_doctor_specialties cds ON cds.id = sc.clinicDoctorSpecialtyId
        LEFT JOIN clinic_doctors cd ON cd.id = cds.clinicDoctorId
        LEFT JOIN clinic_exams ce ON ce.id = sc.clinicExamId
        LEFT JOIN clinics c ON c.id = COALESCE(cd.clinicId, ce.clinicId)
        LEFT JOIN orders o ON o.id = da.orderId
        WHERE {BRT('da.`date`')} >= %s
        GROUP BY mes, clinica, cidade
        HAVING agendadas > 0 OR agendadas_canc > 0
        ORDER BY mes, agendadas DESC
    """, (f"{INICIO}-01",))

    # Drilldown por CLÍNICA — SÉRIE DE PAGAS/RECEITA (mês de pagamento).
    por_clinica_pagas_mes = _q(cur, f"""
        SELECT DATE_FORMAT({BRT('COALESCE(da.paymentDate, o.paymentDate)')},'%%Y-%%m') mes,
               COALESCE(c.name, '(sem clínica associada)') clinica,
               COALESCE(c.city, '—') cidade,
               SUM(CASE WHEN da.canceledAt IS NULL
                             AND (o.id IS NULL OR o.cancelDate IS NULL)
                        THEN 1 ELSE 0 END) pagas,
               ROUND(SUM(CASE WHEN da.canceledAt IS NULL
                                   AND (o.id IS NULL OR o.cancelDate IS NULL)
                              THEN COALESCE(NULLIF(da.total,0), o.total, 0) ELSE 0 END)/100.0, 2) receita_paga,
               SUM(CASE WHEN da.canceledAt IS NOT NULL
                             OR o.cancelDate IS NOT NULL
                        THEN 1 ELSE 0 END) pagas_canc,
               ROUND(SUM(CASE WHEN da.canceledAt IS NOT NULL
                                   OR o.cancelDate IS NOT NULL
                              THEN COALESCE(NULLIF(da.total,0), o.total, 0) ELSE 0 END)/100.0, 2) receita_paga_canc
        FROM doctorappointments da
        LEFT JOIN schedules sc ON sc.id = da.scheduleId
        LEFT JOIN clinic_doctor_specialties cds ON cds.id = sc.clinicDoctorSpecialtyId
        LEFT JOIN clinic_doctors cd ON cd.id = cds.clinicDoctorId
        LEFT JOIN clinic_exams ce ON ce.id = sc.clinicExamId
        LEFT JOIN clinics c ON c.id = COALESCE(cd.clinicId, ce.clinicId)
        LEFT JOIN orders o ON o.id = da.orderId
        WHERE COALESCE(da.paymentDate, o.paymentDate) IS NOT NULL
          AND {BRT('COALESCE(da.paymentDate, o.paymentDate)')} >= %s
        GROUP BY mes, clinica, cidade
        HAVING pagas > 0 OR pagas_canc > 0
        ORDER BY mes, pagas DESC
    """, (f"{INICIO}-01",))

    totais_clinica = _q(cur, """
        SELECT COALESCE(c.name, '(sem clínica associada)') clinica,
               COALESCE(c.city, '—') cidade,
               COALESCE(c.state, '—') estado,
               SUM(CASE WHEN da.canceledAt IS NULL
                             AND (o.id IS NULL OR o.cancelDate IS NULL)
                        THEN 1 ELSE 0 END) agendadas,
               SUM(CASE WHEN da.canceledAt IS NULL
                             AND (o.id IS NULL OR o.cancelDate IS NULL)
                             AND COALESCE(da.paymentDate, o.paymentDate) IS NOT NULL
                        THEN 1 ELSE 0 END) pagas,
               ROUND(SUM(CASE WHEN da.canceledAt IS NULL
                                   AND (o.id IS NULL OR o.cancelDate IS NULL)
                                   AND COALESCE(da.paymentDate, o.paymentDate) IS NOT NULL
                              THEN COALESCE(NULLIF(da.total,0), o.total, 0) ELSE 0 END)/100.0, 2) receita_paga,
               ROUND(AVG(CASE WHEN da.canceledAt IS NULL
                                   AND (o.id IS NULL OR o.cancelDate IS NULL)
                                   AND COALESCE(da.paymentDate, o.paymentDate) IS NOT NULL
                              THEN COALESCE(NULLIF(da.total,0), o.total, 0) END)/100.0, 2) ticket_medio,
               SUM(CASE WHEN da.canceledAt IS NOT NULL OR o.cancelDate IS NOT NULL
                        THEN 1 ELSE 0 END) agendadas_canc,
               SUM(CASE WHEN (da.canceledAt IS NOT NULL OR o.cancelDate IS NOT NULL)
                             AND COALESCE(da.paymentDate, o.paymentDate) IS NOT NULL
                        THEN 1 ELSE 0 END) pagas_canc,
               ROUND(SUM(CASE WHEN (da.canceledAt IS NOT NULL OR o.cancelDate IS NOT NULL)
                                   AND COALESCE(da.paymentDate, o.paymentDate) IS NOT NULL
                              THEN COALESCE(NULLIF(da.total,0), o.total, 0) ELSE 0 END)/100.0, 2) receita_paga_canc
        FROM doctorappointments da
        LEFT JOIN schedules sc ON sc.id = da.scheduleId
        LEFT JOIN clinic_doctor_specialties cds ON cds.id = sc.clinicDoctorSpecialtyId
        LEFT JOIN clinic_doctors cd ON cd.id = cds.clinicDoctorId
        LEFT JOIN clinic_exams ce ON ce.id = sc.clinicExamId
        LEFT JOIN clinics c ON c.id = COALESCE(cd.clinicId, ce.clinicId)
        LEFT JOIN orders o ON o.id = da.orderId
        GROUP BY clinica, cidade, estado
        HAVING agendadas > 0 OR agendadas_canc > 0
        ORDER BY agendadas DESC
    """)

    return {
        "mensal": mensal,
        "mensal_pagamento": mensal_pagamento,
        "totais_historico": {k: int(v or 0) for k, v in totais.items()},
        "por_especialidade_agendadas_mes": por_especialidade_agendadas_mes,
        "por_especialidade_pagas_mes": por_especialidade_pagas_mes,
        "totais_especialidade": totais_especialidade,
        "por_clinica_agendadas_mes": por_clinica_agendadas_mes,
        "por_clinica_pagas_mes": por_clinica_pagas_mes,
        "totais_clinica": totais_clinica,
    }


def pergunta3_split(cur) -> dict:
    """Particular vs Convênio por mês: volume e receita.

    Fonte de convênio (prioridade):
      1) partners.name via doctorappointments.partnerpeopleId → partnerpeople.partnerId
         (é o que o ERP da Égide mostra como "convênio": Camim, Liberty, Alamo…).
      2) insurances.name via customer_insurances (fallback, raro após Mar/2026).
      3) sem nenhum dos dois → PARTICULAR.

    Expõe tanto métricas sem cancelados (consultas_*/receita_*) quanto cancelados
    (consultas_*_canc/receita_*_canc) para o switch "Mostrar cancelados" do front.
    """
    # Dedupe ci3 para não duplicar appointments com 2+ seguros ativos.
    conv_join = (
        " LEFT JOIN partnerpeople pp ON pp.id = da.partnerpeopleId"
        " LEFT JOIN partners p_conv  ON p_conv.id = pp.partnerId"
        " LEFT JOIN customer_insurances ci3"
        "   ON ci3.customerId  = da.customerId"
        "  AND ci3.isCurrent   = 1"
        "  AND ci3.created_at <= da.`date`"
        "  AND (ci3.deleted_at IS NULL OR ci3.deleted_at > da.`date`)"
        "  AND ci3.id = ("
        "      SELECT MIN(ci3b.id) FROM customer_insurances ci3b"
        "      WHERE ci3b.customerId = da.customerId"
        "        AND ci3b.isCurrent = 1"
        "        AND ci3b.created_at <= da.`date`"
        "        AND (ci3b.deleted_at IS NULL OR ci3b.deleted_at > da.`date`)"
        "  )"
        " LEFT JOIN insurances i3 ON i3.id = ci3.insuranceId"
    )
    conv_expr = "(p_conv.id IS NOT NULL OR i3.id IS NOT NULL)"
    # Alinhado com P2/P5: cancelamento pode vir de da.canceledAt OU o.cancelDate.
    # Precisa de LEFT JOIN orders o3 — adicionamos abaixo nas queries que usam.
    not_canc = "(da.canceledAt IS NULL AND (o3.id IS NULL OR o3.cancelDate IS NULL))"
    is_canc  = "(da.canceledAt IS NOT NULL OR o3.cancelDate IS NOT NULL)"

    # mensal — agrupado por mês do AGENDAMENTO (da.date). Todos os appointments
    # do mês, pagos ou não, cancelados ou não. Receita = IFNULL(da.total) (é o
    # valor contratado, não o efetivamente arrecadado).
    mensal = _q(cur, f"""
        SELECT
            DATE_FORMAT({BRT('da.`date`')},'%%Y-%%m') mes,
            SUM(CASE WHEN {not_canc} AND  {conv_expr} THEN 1 ELSE 0 END) consultas_convenio,
            SUM(CASE WHEN {not_canc} AND NOT {conv_expr} THEN 1 ELSE 0 END) consultas_particular,
            ROUND(SUM(CASE WHEN {not_canc} AND  {conv_expr} THEN IFNULL(da.total,0) ELSE 0 END)/100.0, 2) receita_convenio,
            ROUND(SUM(CASE WHEN {not_canc} AND NOT {conv_expr} THEN IFNULL(da.total,0) ELSE 0 END)/100.0, 2) receita_particular,
            SUM(CASE WHEN {is_canc} AND  {conv_expr} THEN 1 ELSE 0 END) consultas_convenio_canc,
            SUM(CASE WHEN {is_canc} AND NOT {conv_expr} THEN 1 ELSE 0 END) consultas_particular_canc,
            ROUND(SUM(CASE WHEN {is_canc} AND  {conv_expr} THEN IFNULL(da.total,0) ELSE 0 END)/100.0, 2) receita_convenio_canc,
            ROUND(SUM(CASE WHEN {is_canc} AND NOT {conv_expr} THEN IFNULL(da.total,0) ELSE 0 END)/100.0, 2) receita_particular_canc
        FROM doctorappointments da
        LEFT JOIN orders o3 ON o3.id = da.orderId
        {conv_join}
        WHERE {BRT('da.`date`')} >= %s
        GROUP BY mes ORDER BY mes
    """, (f"{INICIO}-01",))

    # mensal_pagamento — agrupado por mês do PAGAMENTO. Só appointments com
    # paymentDate (da ou orders). Receita = IFNULL(da.total, o.total) (o que
    # efetivamente entrou no caixa no mês).
    pay_expr = "COALESCE(da.paymentDate, o3.paymentDate)"
    mensal_pagamento = _q(cur, f"""
        SELECT
            DATE_FORMAT({BRT(pay_expr)},'%%Y-%%m') mes,
            SUM(CASE WHEN {not_canc} AND  {conv_expr} THEN 1 ELSE 0 END) consultas_convenio,
            SUM(CASE WHEN {not_canc} AND NOT {conv_expr} THEN 1 ELSE 0 END) consultas_particular,
            ROUND(SUM(CASE WHEN {not_canc} AND  {conv_expr} THEN COALESCE(NULLIF(da.total,0), o3.total, 0) ELSE 0 END)/100.0, 2) receita_convenio,
            ROUND(SUM(CASE WHEN {not_canc} AND NOT {conv_expr} THEN COALESCE(NULLIF(da.total,0), o3.total, 0) ELSE 0 END)/100.0, 2) receita_particular,
            SUM(CASE WHEN {is_canc} AND  {conv_expr} THEN 1 ELSE 0 END) consultas_convenio_canc,
            SUM(CASE WHEN {is_canc} AND NOT {conv_expr} THEN 1 ELSE 0 END) consultas_particular_canc,
            ROUND(SUM(CASE WHEN {is_canc} AND  {conv_expr} THEN COALESCE(NULLIF(da.total,0), o3.total, 0) ELSE 0 END)/100.0, 2) receita_convenio_canc,
            ROUND(SUM(CASE WHEN {is_canc} AND NOT {conv_expr} THEN COALESCE(NULLIF(da.total,0), o3.total, 0) ELSE 0 END)/100.0, 2) receita_particular_canc
        FROM doctorappointments da
        LEFT JOIN orders o3 ON o3.id = da.orderId
        {conv_join}
        WHERE {pay_expr} IS NOT NULL
          AND {BRT(pay_expr)} >= %s
        GROUP BY mes ORDER BY mes
    """, (f"{INICIO}-01",))

    totais = _q(cur, f"""
        SELECT
            SUM(CASE WHEN {not_canc} AND  {conv_expr} THEN 1 ELSE 0 END) consultas_convenio,
            SUM(CASE WHEN {not_canc} AND NOT {conv_expr} THEN 1 ELSE 0 END) consultas_particular,
            ROUND(SUM(CASE WHEN {not_canc} AND  {conv_expr} THEN IFNULL(da.total,0) ELSE 0 END)/100.0, 2) receita_convenio,
            ROUND(SUM(CASE WHEN {not_canc} AND NOT {conv_expr} THEN IFNULL(da.total,0) ELSE 0 END)/100.0, 2) receita_particular,
            SUM(CASE WHEN {is_canc} AND  {conv_expr} THEN 1 ELSE 0 END) consultas_convenio_canc,
            SUM(CASE WHEN {is_canc} AND NOT {conv_expr} THEN 1 ELSE 0 END) consultas_particular_canc,
            ROUND(SUM(CASE WHEN {is_canc} AND  {conv_expr} THEN IFNULL(da.total,0) ELSE 0 END)/100.0, 2) receita_convenio_canc,
            ROUND(SUM(CASE WHEN {is_canc} AND NOT {conv_expr} THEN IFNULL(da.total,0) ELSE 0 END)/100.0, 2) receita_particular_canc
        FROM doctorappointments da
        LEFT JOIN orders o3 ON o3.id = da.orderId
        {conv_join}
    """)[0]

    return {
        "mensal": mensal,
        "mensal_pagamento": mensal_pagamento,
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

    # --- VERSÃO POR AGENDAMENTO: mês = da.date ou o.date (data que o cliente
    # marcou pra ser atendido). Receita aqui é VALOR CONTRATADO (não arrecadado).
    # Cancelados ficam FORA da métrica padrão.
    orders_agend = _q(cur, f"""
        SELECT DATE_FORMAT({BRT('o.`date`')},'%%Y-%%m') mes,
               o.orderType,
               COUNT(*) qtde,
               ROUND(SUM(IFNULL(o.total,0))/100.0, 2) receita
        FROM orders o
        WHERE o.cancelDate IS NULL
          AND o.`date` IS NOT NULL
          AND {BRT('o.`date`')} >= %s
        GROUP BY mes, o.orderType ORDER BY mes, o.orderType
    """, (f"{INICIO}-01",))

    appt_sem_order_agend = _q(cur, f"""
        SELECT DATE_FORMAT({BRT('`date`')},'%%Y-%%m') mes,
               COUNT(*) qtde,
               ROUND(SUM(IFNULL(total,0))/100.0, 2) receita
        FROM doctorappointments
        WHERE canceledAt IS NULL
          AND orderId IS NULL
          AND total > 0
          AND {BRT('`date`')} >= %s
        GROUP BY mes ORDER BY mes
    """, (f"{INICIO}-01",))

    # Série unificada por canal lógico (3 categorias: clinica / farmacia / outros)
    # "clinica" = orders.clinic + appointments sem order. É tudo consulta/exame pra
    # quem usa o dashboard.
    def _monta_serie_canal(rows_orders, rows_appt):
        por_canal_mes = {}
        def _acum(mes, canal, qtde, receita):
            key = (mes, canal)
            if key not in por_canal_mes:
                por_canal_mes[key] = {"mes": mes, "canal": canal, "qtde": 0, "receita": 0.0}
            por_canal_mes[key]["qtde"] += int(qtde or 0)
            por_canal_mes[key]["receita"] += float(receita or 0)
        for r in rows_orders:
            ot = (r.get("orderType") or "").lower()
            if ot == "clinic":
                canal = "clinica"
            elif ot == "pharmacy":
                canal = "farmacia"
            else:
                canal = "outros"
            _acum(r["mes"], canal, r["qtde"], r["receita"])
        for r in rows_appt:
            _acum(r["mes"], "clinica", r["qtde"], r["receita"])
        out = sorted(por_canal_mes.values(), key=lambda x: (x["mes"], x["canal"]))
        for r in out:
            r["receita"] = round(r["receita"], 2)
        return out

    serie_canal = _monta_serie_canal(orders, appt_sem_order)
    serie_canal_agendamento = _monta_serie_canal(orders_agend, appt_sem_order_agend)

    # "Total particular consolidado" — FILTRA convênio. Antes esse gráfico
    # somava tudo (inclusive convênio), por isso batia ~R$ 198k em Mar/2026
    # enquanto a tabela Totais por convênio mostrava PARTICULAR = R$ 80.708,95.
    # Agora soma:
    #   - orders.orderType='pharmacy' (farmácia não passa convênio);
    #   - orders.orderType='clinic' SE o appointment vinculado NÃO tem partner
    #     nem customer_insurance ativo (= realmente particular);
    #   - appointments sem order SE NÃO tem partner nem customer_insurance ativo.
    # Regra de convênio é a mesma do P3/P5: partnerpeople→partners OU
    # customer_insurances.isCurrent=1 com janela temporal.
    conv_join_p4 = (
        " LEFT JOIN partnerpeople pp ON pp.id = da.partnerpeopleId"
        " LEFT JOIN partners p_conv  ON p_conv.id = pp.partnerId"
        " LEFT JOIN customer_insurances ci4"
        "   ON ci4.customerId  = da.customerId"
        "  AND ci4.isCurrent   = 1"
        "  AND ci4.created_at <= da.`date`"
        "  AND (ci4.deleted_at IS NULL OR ci4.deleted_at > da.`date`)"
    )
    sem_conv = "(p_conv.id IS NULL AND ci4.id IS NULL)"

    consolidado_particular = _q(cur, f"""
        SELECT mes, ROUND(SUM(receita),2) receita, SUM(qtde) qtde FROM (
            SELECT DATE_FORMAT({BRT('o.paymentDate')},'%%Y-%%m') mes,
                   COUNT(*) qtde, SUM(IFNULL(o.total,0))/100.0 receita
            FROM orders o
            WHERE o.orderType = 'pharmacy'
              AND o.paymentDate IS NOT NULL AND o.cancelDate IS NULL
              AND {BRT('o.paymentDate')} >= %s
            GROUP BY mes
            UNION ALL
            SELECT DATE_FORMAT({BRT('o.paymentDate')},'%%Y-%%m') mes,
                   COUNT(*) qtde, SUM(IFNULL(o.total,0))/100.0 receita
            FROM orders o
            JOIN doctorappointments da ON da.orderId = o.id
            {conv_join_p4}
            WHERE o.orderType = 'clinic'
              AND o.paymentDate IS NOT NULL AND o.cancelDate IS NULL
              AND da.canceledAt IS NULL
              AND {sem_conv}
              AND {BRT('o.paymentDate')} >= %s
            GROUP BY mes
            UNION ALL
            SELECT DATE_FORMAT({BRT('da.paymentDate')},'%%Y-%%m') mes,
                   COUNT(*) qtde, SUM(IFNULL(da.total,0))/100.0 receita
            FROM doctorappointments da
            {conv_join_p4}
            WHERE da.paymentDate IS NOT NULL AND da.canceledAt IS NULL
              AND da.orderId IS NULL AND da.total > 0
              AND {sem_conv}
              AND {BRT('da.paymentDate')} >= %s
            GROUP BY mes
        ) x GROUP BY mes ORDER BY mes
    """, (f"{INICIO}-01", f"{INICIO}-01", f"{INICIO}-01"))

    # Versão AGENDAMENTO: por o.`date` / da.`date`. Ignora cancelados.
    consolidado_particular_agend = _q(cur, f"""
        SELECT mes, ROUND(SUM(receita),2) receita, SUM(qtde) qtde FROM (
            SELECT DATE_FORMAT({BRT('o.`date`')},'%%Y-%%m') mes,
                   COUNT(*) qtde, SUM(IFNULL(o.total,0))/100.0 receita
            FROM orders o
            WHERE o.orderType = 'pharmacy'
              AND o.`date` IS NOT NULL AND o.cancelDate IS NULL
              AND {BRT('o.`date`')} >= %s
            GROUP BY mes
            UNION ALL
            SELECT DATE_FORMAT({BRT('o.`date`')},'%%Y-%%m') mes,
                   COUNT(*) qtde, SUM(IFNULL(o.total,0))/100.0 receita
            FROM orders o
            JOIN doctorappointments da ON da.orderId = o.id
            {conv_join_p4}
            WHERE o.orderType = 'clinic'
              AND o.`date` IS NOT NULL AND o.cancelDate IS NULL
              AND da.canceledAt IS NULL
              AND {sem_conv}
              AND {BRT('o.`date`')} >= %s
            GROUP BY mes
            UNION ALL
            SELECT DATE_FORMAT({BRT('da.`date`')},'%%Y-%%m') mes,
                   COUNT(*) qtde, SUM(IFNULL(da.total,0))/100.0 receita
            FROM doctorappointments da
            {conv_join_p4}
            WHERE da.canceledAt IS NULL
              AND da.orderId IS NULL AND da.total > 0
              AND {sem_conv}
              AND {BRT('da.`date`')} >= %s
            GROUP BY mes
        ) x GROUP BY mes ORDER BY mes
    """, (f"{INICIO}-01", f"{INICIO}-01", f"{INICIO}-01"))

    return {
        "orders_por_tipo": orders,
        "appointments_sem_order": appt_sem_order,
        "serie_canal": serie_canal,
        "serie_canal_agendamento": serie_canal_agendamento,
        "consolidado_mensal": consolidado_particular,
        "consolidado_mensal_agendamento": consolidado_particular_agend,
    }


def pergunta5_receita_convenio(cur) -> dict:
    """Arrecadação por convênio — empilhado por nome.

    - Convênio vem do JOIN partnerpeople → partners (mesma fonte do ERP da Égide).
      Fallback: customer_insurances → insurances.
    - Base: paymentDate (para alinhar com o card "Arrecadação" do topo), não a
      data do agendamento. Cancelados ficam FORA da métrica padrão mas são
      expostos em colunas *_canc para o switch "Mostrar cancelados".
    """

    # Dedupe: cliente com 2+ seguros ativos na data duplicava o doctorappointment,
    # inflando contagens. Escolhemos o ci5 de menor id por (customerId, data).
    conv_join = (
        " LEFT JOIN partnerpeople pp ON pp.id = da.partnerpeopleId"
        " LEFT JOIN partners p_conv  ON p_conv.id = pp.partnerId"
        " LEFT JOIN customer_insurances ci5"
        "   ON ci5.customerId  = da.customerId"
        "  AND ci5.isCurrent   = 1"
        "  AND ci5.created_at <= da.`date`"
        "  AND (ci5.deleted_at IS NULL OR ci5.deleted_at > da.`date`)"
        "  AND ci5.id = ("
        "      SELECT MIN(ci5b.id) FROM customer_insurances ci5b"
        "      WHERE ci5b.customerId = da.customerId"
        "        AND ci5b.isCurrent = 1"
        "        AND ci5b.created_at <= da.`date`"
        "        AND (ci5b.deleted_at IS NULL OR ci5b.deleted_at > da.`date`)"
        "  )"
        " LEFT JOIN insurances i5 ON i5.id = ci5.insuranceId"
    )
    # COALESCE: partners > insurances > 'PARTICULAR'
    conv_name = "COALESCE(p_conv.name, i5.name, 'PARTICULAR')"
    paid = "(COALESCE(da.paymentDate, o5.paymentDate) IS NOT NULL)"
    not_canc = "(da.canceledAt IS NULL AND (o5.id IS NULL OR o5.cancelDate IS NULL))"
    is_canc  = "(da.canceledAt IS NOT NULL OR o5.cancelDate IS NOT NULL)"

    # Série mensal por mês de PAGAMENTO (alinhada com o card de topo).
    mensal = _q(cur, f"""
        SELECT DATE_FORMAT({BRT('COALESCE(da.paymentDate, o5.paymentDate)')},'%%Y-%%m') mes,
               {conv_name} convenio,
               SUM(CASE WHEN {paid} AND {not_canc} THEN 1 ELSE 0 END) consultas,
               ROUND(SUM(CASE WHEN {paid} AND {not_canc} THEN IFNULL(da.total,0) ELSE 0 END)/100.0, 2) receita,
               SUM(CASE WHEN {paid} AND {is_canc} THEN 1 ELSE 0 END) consultas_canc,
               ROUND(SUM(CASE WHEN {paid} AND {is_canc} THEN IFNULL(da.total,0) ELSE 0 END)/100.0, 2) receita_canc
        FROM doctorappointments da
        LEFT JOIN orders o5 ON o5.id = da.orderId
        {conv_join}
        WHERE COALESCE(da.paymentDate, o5.paymentDate) IS NOT NULL
          AND {BRT('COALESCE(da.paymentDate, o5.paymentDate)')} >= %s
        GROUP BY mes, convenio
        HAVING consultas > 0 OR consultas_canc > 0
        ORDER BY mes, receita DESC
    """, (f"{INICIO}-01",))

    # Série mensal por mês de AGENDAMENTO (da.date). TODOS os agendamentos do
    # mês (pagos ou não), com receita = valor contratado (IFNULL(da.total,0)).
    mensal_agendamento = _q(cur, f"""
        SELECT DATE_FORMAT({BRT('da.`date`')},'%%Y-%%m') mes,
               {conv_name} convenio,
               SUM(CASE WHEN {not_canc} THEN 1 ELSE 0 END) consultas,
               ROUND(SUM(CASE WHEN {not_canc} THEN IFNULL(da.total,0) ELSE 0 END)/100.0, 2) receita,
               SUM(CASE WHEN {is_canc} THEN 1 ELSE 0 END) consultas_canc,
               ROUND(SUM(CASE WHEN {is_canc} THEN IFNULL(da.total,0) ELSE 0 END)/100.0, 2) receita_canc
        FROM doctorappointments da
        LEFT JOIN orders o5 ON o5.id = da.orderId
        {conv_join}
        WHERE {BRT('da.`date`')} >= %s
        GROUP BY mes, convenio
        HAVING consultas > 0 OR consultas_canc > 0
        ORDER BY mes, receita DESC
    """, (f"{INICIO}-01",))

    # Totais históricos (todas as datas de pagamento).
    totais_conv = _q(cur, f"""
        SELECT {conv_name} convenio,
               SUM(CASE WHEN {paid} AND {not_canc} THEN 1 ELSE 0 END) consultas,
               ROUND(SUM(CASE WHEN {paid} AND {not_canc} THEN IFNULL(da.total,0) ELSE 0 END)/100.0, 2) receita,
               ROUND(AVG(CASE WHEN {paid} AND {not_canc} THEN IFNULL(da.total,0) END)/100.0, 2) ticket_medio,
               SUM(CASE WHEN {paid} AND {is_canc} THEN 1 ELSE 0 END) consultas_canc,
               ROUND(SUM(CASE WHEN {paid} AND {is_canc} THEN IFNULL(da.total,0) ELSE 0 END)/100.0, 2) receita_canc
        FROM doctorappointments da
        LEFT JOIN orders o5 ON o5.id = da.orderId
        {conv_join}
        WHERE COALESCE(da.paymentDate, o5.paymentDate) IS NOT NULL
        GROUP BY convenio
        HAVING consultas > 0 OR consultas_canc > 0
        ORDER BY receita DESC
    """)

    # Lista de convênios cadastrados (para montar filtro):
    # mistura nomes da tabela partners (fonte primária) + insurances (legado).
    # "carteirinhas_ativas" = pessoas com vínculo naquele convênio.
    convenios_cadastrados = _q(cur, """
        SELECT name, SUM(carteirinhas_ativas) carteirinhas_ativas
        FROM (
            SELECT p.name,
                   (SELECT COUNT(*) FROM partnerpeople pp
                    WHERE pp.partnerId = p.id AND pp.deleted_at IS NULL) AS carteirinhas_ativas
            FROM partners p
            WHERE p.deleted_at IS NULL
            UNION ALL
            SELECT i.name,
                   (SELECT COUNT(*) FROM customer_insurances ci
                    WHERE ci.insuranceId = i.id AND ci.isCurrent = 1) AS carteirinhas_ativas
            FROM insurances i
            WHERE i.deleted_at IS NULL
        ) x
        GROUP BY name
        ORDER BY name
    """)

    # IS_CONSULTA_P5: mesma regra do P2. A tabela Convênio × Especialidade
    # passa a mostrar APENAS CONSULTAS. Antes, o bucket "(Exame — sem
    # especialidade)" misturava exames reais com consultas sem specialtyId
    # cadastrado — no ERP da Égide o Camim ficou com 1.144 nesse bucket
    # enquanto o card de exames mostrava apenas 490 no mesmo mês.
    IS_CONSULTA_P5 = (
        "(sc5.clinicDoctorSpecialtyId IS NOT NULL"
        " OR (sc5.id IS NULL AND da.specialtyId IS NOT NULL))"
    )

    # Drilldown: por convênio x especialidade (histórico, base em pagamento).
    por_convenio_especialidade = _q(cur, f"""
        SELECT {conv_name} convenio,
               COALESCE(s.name, '(sem especialidade)') especialidade,
               SUM(CASE WHEN {paid} AND {not_canc} THEN 1 ELSE 0 END) consultas,
               ROUND(SUM(CASE WHEN {paid} AND {not_canc} THEN IFNULL(da.total,0) ELSE 0 END)/100.0, 2) receita,
               SUM(CASE WHEN {paid} AND {is_canc} THEN 1 ELSE 0 END) consultas_canc,
               ROUND(SUM(CASE WHEN {paid} AND {is_canc} THEN IFNULL(da.total,0) ELSE 0 END)/100.0, 2) receita_canc
        FROM doctorappointments da
        LEFT JOIN orders o5 ON o5.id = da.orderId
        {conv_join}
        LEFT JOIN schedules sc5 ON sc5.id = da.scheduleId
        LEFT JOIN clinic_doctor_specialties cds5 ON cds5.id = sc5.clinicDoctorSpecialtyId
        LEFT JOIN specialties s ON s.id = COALESCE(da.specialtyId, cds5.specialtyId)
        WHERE COALESCE(da.paymentDate, o5.paymentDate) IS NOT NULL
          AND {IS_CONSULTA_P5}
        GROUP BY convenio, especialidade
        HAVING consultas > 0 OR consultas_canc > 0
        ORDER BY convenio, consultas DESC
    """)

    por_convenio_clinica = _q(cur, f"""
        SELECT {conv_name} convenio,
               COALESCE(c.name, '(sem clínica associada)') clinica,
               COALESCE(c.city, '—') cidade,
               SUM(CASE WHEN {paid} AND {not_canc} THEN 1 ELSE 0 END) consultas,
               ROUND(SUM(CASE WHEN {paid} AND {not_canc} THEN IFNULL(da.total,0) ELSE 0 END)/100.0, 2) receita,
               SUM(CASE WHEN {paid} AND {is_canc} THEN 1 ELSE 0 END) consultas_canc,
               ROUND(SUM(CASE WHEN {paid} AND {is_canc} THEN IFNULL(da.total,0) ELSE 0 END)/100.0, 2) receita_canc
        FROM doctorappointments da
        LEFT JOIN orders o5 ON o5.id = da.orderId
        {conv_join}
        LEFT JOIN schedules sc ON sc.id = da.scheduleId
        LEFT JOIN clinic_doctor_specialties cds ON cds.id = sc.clinicDoctorSpecialtyId
        LEFT JOIN clinic_doctors cd ON cd.id = cds.clinicDoctorId
        LEFT JOIN clinic_exams ce ON ce.id = sc.clinicExamId
        LEFT JOIN clinics c ON c.id = COALESCE(cd.clinicId, ce.clinicId)
        WHERE COALESCE(da.paymentDate, o5.paymentDate) IS NOT NULL
        GROUP BY convenio, clinica, cidade
        HAVING consultas > 0 OR consultas_canc > 0
        ORDER BY convenio, consultas DESC
    """)

    por_convenio_especialidade_mes = _q(cur, f"""
        SELECT DATE_FORMAT({BRT('COALESCE(da.paymentDate, o5.paymentDate)')},'%%Y-%%m') mes,
               {conv_name} convenio,
               COALESCE(s.name, '(sem especialidade)') especialidade,
               SUM(CASE WHEN {paid} AND {not_canc} THEN 1 ELSE 0 END) consultas,
               ROUND(SUM(CASE WHEN {paid} AND {not_canc} THEN IFNULL(da.total,0) ELSE 0 END)/100.0, 2) receita,
               SUM(CASE WHEN {paid} AND {is_canc} THEN 1 ELSE 0 END) consultas_canc,
               ROUND(SUM(CASE WHEN {paid} AND {is_canc} THEN IFNULL(da.total,0) ELSE 0 END)/100.0, 2) receita_canc
        FROM doctorappointments da
        LEFT JOIN orders o5 ON o5.id = da.orderId
        {conv_join}
        LEFT JOIN schedules sc5 ON sc5.id = da.scheduleId
        LEFT JOIN clinic_doctor_specialties cds5 ON cds5.id = sc5.clinicDoctorSpecialtyId
        LEFT JOIN specialties s ON s.id = COALESCE(da.specialtyId, cds5.specialtyId)
        WHERE COALESCE(da.paymentDate, o5.paymentDate) IS NOT NULL
          AND {BRT('COALESCE(da.paymentDate, o5.paymentDate)')} >= %s
          AND {IS_CONSULTA_P5}
        GROUP BY mes, convenio, especialidade
        HAVING consultas > 0 OR consultas_canc > 0
        ORDER BY mes, convenio, consultas DESC
    """, (f"{INICIO}-01",))

    por_convenio_especialidade_mes_agend = _q(cur, f"""
        SELECT DATE_FORMAT({BRT('da.`date`')},'%%Y-%%m') mes,
               {conv_name} convenio,
               COALESCE(s.name, '(sem especialidade)') especialidade,
               SUM(CASE WHEN {not_canc} THEN 1 ELSE 0 END) consultas,
               ROUND(SUM(CASE WHEN {not_canc} THEN IFNULL(da.total,0) ELSE 0 END)/100.0, 2) receita,
               SUM(CASE WHEN {is_canc} THEN 1 ELSE 0 END) consultas_canc,
               ROUND(SUM(CASE WHEN {is_canc} THEN IFNULL(da.total,0) ELSE 0 END)/100.0, 2) receita_canc
        FROM doctorappointments da
        LEFT JOIN orders o5 ON o5.id = da.orderId
        {conv_join}
        LEFT JOIN schedules sc5 ON sc5.id = da.scheduleId
        LEFT JOIN clinic_doctor_specialties cds5 ON cds5.id = sc5.clinicDoctorSpecialtyId
        LEFT JOIN specialties s ON s.id = COALESCE(da.specialtyId, cds5.specialtyId)
        WHERE {BRT('da.`date`')} >= %s
          AND {IS_CONSULTA_P5}
        GROUP BY mes, convenio, especialidade
        HAVING consultas > 0 OR consultas_canc > 0
        ORDER BY mes, convenio, consultas DESC
    """, (f"{INICIO}-01",))

    por_convenio_clinica_mes = _q(cur, f"""
        SELECT DATE_FORMAT({BRT('COALESCE(da.paymentDate, o5.paymentDate)')},'%%Y-%%m') mes,
               {conv_name} convenio,
               COALESCE(c.name, '(sem clínica associada)') clinica,
               COALESCE(c.city, '—') cidade,
               SUM(CASE WHEN {paid} AND {not_canc} THEN 1 ELSE 0 END) consultas,
               ROUND(SUM(CASE WHEN {paid} AND {not_canc} THEN IFNULL(da.total,0) ELSE 0 END)/100.0, 2) receita,
               SUM(CASE WHEN {paid} AND {is_canc} THEN 1 ELSE 0 END) consultas_canc,
               ROUND(SUM(CASE WHEN {paid} AND {is_canc} THEN IFNULL(da.total,0) ELSE 0 END)/100.0, 2) receita_canc
        FROM doctorappointments da
        LEFT JOIN orders o5 ON o5.id = da.orderId
        {conv_join}
        LEFT JOIN schedules sc ON sc.id = da.scheduleId
        LEFT JOIN clinic_doctor_specialties cds ON cds.id = sc.clinicDoctorSpecialtyId
        LEFT JOIN clinic_doctors cd ON cd.id = cds.clinicDoctorId
        LEFT JOIN clinic_exams ce ON ce.id = sc.clinicExamId
        LEFT JOIN clinics c ON c.id = COALESCE(cd.clinicId, ce.clinicId)
        WHERE COALESCE(da.paymentDate, o5.paymentDate) IS NOT NULL
          AND {BRT('COALESCE(da.paymentDate, o5.paymentDate)')} >= %s
        GROUP BY mes, convenio, clinica, cidade
        HAVING consultas > 0 OR consultas_canc > 0
        ORDER BY mes, convenio, consultas DESC
    """, (f"{INICIO}-01",))

    por_convenio_clinica_mes_agend = _q(cur, f"""
        SELECT DATE_FORMAT({BRT('da.`date`')},'%%Y-%%m') mes,
               {conv_name} convenio,
               COALESCE(c.name, '(sem clínica associada)') clinica,
               COALESCE(c.city, '—') cidade,
               SUM(CASE WHEN {not_canc} THEN 1 ELSE 0 END) consultas,
               ROUND(SUM(CASE WHEN {not_canc} THEN IFNULL(da.total,0) ELSE 0 END)/100.0, 2) receita,
               SUM(CASE WHEN {is_canc} THEN 1 ELSE 0 END) consultas_canc,
               ROUND(SUM(CASE WHEN {is_canc} THEN IFNULL(da.total,0) ELSE 0 END)/100.0, 2) receita_canc
        FROM doctorappointments da
        LEFT JOIN orders o5 ON o5.id = da.orderId
        {conv_join}
        LEFT JOIN schedules sc ON sc.id = da.scheduleId
        LEFT JOIN clinic_doctor_specialties cds ON cds.id = sc.clinicDoctorSpecialtyId
        LEFT JOIN clinic_doctors cd ON cd.id = cds.clinicDoctorId
        LEFT JOIN clinic_exams ce ON ce.id = sc.clinicExamId
        LEFT JOIN clinics c ON c.id = COALESCE(cd.clinicId, ce.clinicId)
        WHERE {BRT('da.`date`')} >= %s
        GROUP BY mes, convenio, clinica, cidade
        HAVING consultas > 0 OR consultas_canc > 0
        ORDER BY mes, convenio, consultas DESC
    """, (f"{INICIO}-01",))

    return {
        "mensal_por_convenio": mensal,
        "mensal_por_convenio_agendamento": mensal_agendamento,
        "totais_por_convenio": totais_conv,
        "convenios_cadastrados": convenios_cadastrados,
        "por_convenio_especialidade": por_convenio_especialidade,
        "por_convenio_clinica": por_convenio_clinica,
        "por_convenio_especialidade_mes": por_convenio_especialidade_mes,
        "por_convenio_especialidade_mes_agendamento": por_convenio_especialidade_mes_agend,
        "por_convenio_clinica_mes": por_convenio_clinica_mes,
        "por_convenio_clinica_mes_agendamento": por_convenio_clinica_mes_agend,
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
