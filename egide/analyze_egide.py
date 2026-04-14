"""Análise profunda do banco Égide para KPIs.

Gera em egide/scan_output/analysis/:
  - 00_date_ranges.json         janela temporal de cada tabela transacional
  - 01_dictionaries.json        valores distintos de campos categóricos (status, type…)
  - 02_q1_clients.json          Pergunta 1: clientes por período
  - 03_q2_consultations.json    Pergunta 2: consultas/exames por período
  - 04_q3_particular_vs_conv.json Pergunta 3: particular vs convênio
  - 05_q4_revenue_contract.json Pergunta 4: arrecadação por contrato (particular)
  - 06_q5_revenue_insurance.json Pergunta 5: arrecadação por convênio
  - 10_pharmacy_kpis.json       KPIs de farmácia (stores, products, orders de farmácia)
  - 11_clinic_kpis.json         KPIs de clínicas/consultas/exames
  - 12_financial_kpis.json      KPIs financeiros (ticket médio, comissões, refunds)
  - 13_delivery_kpis.json       KPIs de delivery
  - 14_evaluation_kpis.json     KPIs de avaliações
  - 15_coupon_kpis.json         KPIs de cupons
  - 16_medical_kpis.json        KPIs de prontuário
  - 17_cohort_retention.json    cohorts simples de clientes
  - 18_geography.json           distribuição geográfica
  - 19_loyalty.json             recompra, LTV inicial
  - KPI_ANALYSIS_REPORT.md      relatório mestre legível
"""

from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pymysql
import pymysql.cursors
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
OUT = Path(__file__).resolve().parent / "scan_output" / "analysis"
OUT.mkdir(parents=True, exist_ok=True)

load_dotenv(ROOT / ".env")

CONN_KW = dict(
    host=os.environ["DB_HOST"],
    port=int(os.environ["DB_PORT"]),
    user=os.environ["DB_USER"],
    password=os.environ["DB_PASSWORD"],
    database=os.environ["DB_NAME"],
    charset="utf8mb4",
    connect_timeout=15,
    read_timeout=120,
    cursorclass=pymysql.cursors.DictCursor,
)


def json_default(o: Any):
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    if isinstance(o, timedelta):
        return str(o)
    if isinstance(o, Decimal):
        return float(o)
    if isinstance(o, (bytes, memoryview)):
        try:
            return bytes(o).decode("utf-8")
        except UnicodeDecodeError:
            return f"<bytes len={len(bytes(o))}>"
    return str(o)


def save(name: str, data: Any):
    (OUT / name).write_text(
        json.dumps(data, ensure_ascii=False, indent=2, default=json_default),
        encoding="utf-8",
    )
    print(f"  ✔ {name}")


def q(cur, sql: str, params: tuple | list = ()):
    cur.execute(sql, params)
    return cur.fetchall()


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------


def money(v) -> float:
    """Converte centavos → reais."""
    if v is None:
        return 0.0
    return float(v) / 100.0


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------


def main():
    conn = pymysql.connect(**CONN_KW)
    cur = conn.cursor()
    try:
        cur.execute("SET SESSION TRANSACTION READ ONLY")
    except Exception:
        pass

    print("== 0. Janelas temporais das tabelas transacionais ==")
    date_ranges = {}
    for tbl, col in [
        ("customers", "created_at"),
        ("orders", "created_at"),
        ("orders", "paymentDate"),
        ("doctorappointments", "date"),
        ("doctorappointments", "paymentDate"),
        ("doctorappointments", "canceledAt"),
        ("doctorappointmentstatuslogs", "created_at"),
        ("invoices", "created_at"),
        ("invoices", "paymentDate"),
        ("transfers", "created_at"),
        ("transfers", "transferDate"),
        ("evaluations", "created_at"),
        ("medical_records", "created_at"),
        ("externalprescriptions", "created_at"),
        ("schedules", "created_at"),
        ("customer_insurances", "created_at"),
        ("coupon_customers", "created_at"),
        ("webhooks", "created_at"),
    ]:
        r = q(
            cur,
            f"SELECT MIN(`{col}`) AS mn, MAX(`{col}`) AS mx, COUNT(*) AS total, "
            f"COUNT(`{col}`) AS non_null FROM `{tbl}`",
        )[0]
        date_ranges[f"{tbl}.{col}"] = r
    save("00_date_ranges.json", date_ranges)

    print("== 1. Dicionários de status/tipos ==")
    dicts = {}
    for name, sql in [
        ("orders.orderType", "SELECT orderType AS v, COUNT(*) AS c FROM orders GROUP BY orderType ORDER BY c DESC"),
        ("orders.by_paid", "SELECT CASE WHEN paymentDate IS NOT NULL THEN 'paid' WHEN cancelDate IS NOT NULL THEN 'canceled' ELSE 'pending' END AS v, COUNT(*) AS c FROM orders GROUP BY v"),
        ("orders.chargeType", "SELECT chargeType AS v, COUNT(*) AS c FROM orders GROUP BY chargeType ORDER BY c DESC"),
        ("invoices.type", "SELECT type AS v, COUNT(*) AS c FROM invoices GROUP BY type ORDER BY c DESC"),
        ("invoices.by_paid", "SELECT CASE WHEN paymentDate IS NOT NULL THEN 'paid' WHEN cancelDate IS NOT NULL THEN 'canceled' ELSE 'pending' END AS v, COUNT(*) AS c FROM invoices GROUP BY v"),
        ("invoices.chargeType", "SELECT chargeType AS v, COUNT(*) AS c FROM invoices GROUP BY chargeType ORDER BY c DESC"),
        ("doctorappointments.status", "SELECT status AS v, COUNT(*) AS c FROM doctorappointments GROUP BY status ORDER BY c DESC"),
        ("doctorappointments.scheduledBy", "SELECT scheduledBy AS v, COUNT(*) AS c FROM doctorappointments GROUP BY scheduledBy ORDER BY c DESC"),
        ("doctorappointmentstatuses", "SELECT id, name, tag FROM doctorappointmentstatuses ORDER BY id"),
        ("deliverystatuses", "SELECT id, name, tag, isFinish FROM deliverystatuses ORDER BY id"),
        ("specialties.type", "SELECT type AS v, COUNT(*) AS c FROM specialties GROUP BY type ORDER BY c DESC"),
        ("insurances", "SELECT id, name, isActive, isCamimEmployee FROM insurances ORDER BY id"),
        ("paymentgateways", "SELECT id, name, tag, isActive, isDefault FROM paymentgateways"),
        ("categories", "SELECT id, tag, name, isDentist, isActive FROM categories ORDER BY id"),
        ("producttypes", "SELECT id, name, commission FROM producttypes"),
        ("stores.by_category", "SELECT c.name AS category, COUNT(*) AS stores, SUM(s.isActive) AS active FROM stores s JOIN categories c ON c.id=s.categoryId GROUP BY c.name ORDER BY stores DESC"),
    ]:
        dicts[name] = q(cur, sql)
    save("01_dictionaries.json", dicts)

    # ============================================================ Pergunta 1
    print("== 2. Pergunta 1: Clientes por período ==")
    q1 = {}
    q1["total_historico"] = q(cur, "SELECT COUNT(*) AS total, COUNT(deleted_at) AS soft_deleted FROM customers")[0]
    q1["novos_por_mes"] = q(
        cur,
        """
        SELECT DATE_FORMAT(created_at,'%%Y-%%m') AS mes, COUNT(*) AS novos
        FROM customers
        GROUP BY mes ORDER BY mes
        """,
    )
    # ativo = com pelo menos 1 order PAGA ou 1 doctorappointment CONFIRMADA no período
    q1["ativos_por_mes_orders"] = q(
        cur,
        """
        SELECT DATE_FORMAT(o.paymentDate,'%%Y-%%m') AS mes,
               COUNT(DISTINCT o.customerId) AS ativos_orders
        FROM orders o
        WHERE o.paymentDate IS NOT NULL
        GROUP BY mes ORDER BY mes
        """,
    )
    q1["ativos_por_mes_consultas"] = q(
        cur,
        """
        SELECT DATE_FORMAT(da.`date`,'%%Y-%%m') AS mes,
               COUNT(DISTINCT da.customerId) AS ativos_consultas
        FROM doctorappointments da
        WHERE da.customerId IS NOT NULL
          AND (da.paymentDate IS NOT NULL OR da.confirmationDate IS NOT NULL)
        GROUP BY mes ORDER BY mes
        """,
    )
    q1["recorrentes_12m"] = q(
        cur,
        """
        SELECT DATE_FORMAT(o.paymentDate,'%%Y-%%m') AS mes,
               COUNT(DISTINCT o.customerId) AS recorrentes
        FROM orders o
        WHERE o.paymentDate IS NOT NULL
          AND EXISTS (
            SELECT 1 FROM orders o2
            WHERE o2.customerId = o.customerId
              AND o2.paymentDate IS NOT NULL
              AND o2.paymentDate < o.paymentDate
              AND o2.paymentDate >= DATE_SUB(o.paymentDate, INTERVAL 12 MONTH)
          )
        GROUP BY mes ORDER BY mes
        """,
    )
    save("02_q1_clients.json", q1)

    # ============================================================ Pergunta 2
    print("== 3. Pergunta 2: Consultas/exames por período ==")
    q2 = {}
    q2["total_historico"] = q(
        cur,
        "SELECT COUNT(*) AS total, COUNT(paymentDate) AS paid, COUNT(canceledAt) AS canceled FROM doctorappointments",
    )[0]
    q2["por_mes_criacao"] = q(
        cur,
        """
        SELECT DATE_FORMAT(created_at,'%%Y-%%m') AS mes,
               COUNT(*) AS agendadas,
               COUNT(paymentDate) AS pagas,
               COUNT(confirmationDate) AS confirmadas,
               COUNT(canceledAt) AS canceladas
        FROM doctorappointments
        GROUP BY mes ORDER BY mes
        """,
    )
    q2["por_mes_data_consulta"] = q(
        cur,
        """
        SELECT DATE_FORMAT(`date`,'%%Y-%%m') AS mes,
               COUNT(*) AS total,
               COUNT(paymentDate) AS pagas,
               COUNT(confirmationDate) AS confirmadas,
               COUNT(canceledAt) AS canceladas,
               SUM(CASE WHEN specialtyId IS NOT NULL THEN 1 ELSE 0 END) AS consultas,
               SUM(CASE WHEN specialtyId IS NULL THEN 1 ELSE 0 END) AS exames_ou_outros
        FROM doctorappointments
        GROUP BY mes ORDER BY mes
        """,
    )
    q2["por_status"] = q(
        cur,
        "SELECT status, COUNT(*) AS qtde FROM doctorappointments GROUP BY status ORDER BY qtde DESC",
    )
    q2["por_especialidade_top20"] = q(
        cur,
        """
        SELECT s.name AS especialidade, COUNT(*) AS total,
               COUNT(da.paymentDate) AS pagas,
               ROUND(AVG(da.total)/100.0,2) AS ticket_medio_reais
        FROM doctorappointments da
        LEFT JOIN specialties s ON s.id = da.specialtyId
        WHERE da.specialtyId IS NOT NULL
        GROUP BY s.name
        ORDER BY total DESC LIMIT 20
        """,
    )
    q2["por_clinica_top20"] = q(
        cur,
        """
        SELECT c.name AS clinica, COUNT(*) AS total,
               COUNT(da.paymentDate) AS pagas,
               ROUND(SUM(CASE WHEN da.paymentDate IS NOT NULL THEN da.total ELSE 0 END)/100.0,2) AS receita_reais
        FROM doctorappointments da
        JOIN schedules sch ON sch.id = da.scheduleId
        LEFT JOIN clinic_doctor_specialties cds ON cds.id = sch.clinicDoctorSpecialtyId
        LEFT JOIN clinic_doctors cd ON cd.id = cds.clinicDoctorId
        LEFT JOIN clinics c ON c.id = cd.clinicId
        GROUP BY c.name
        ORDER BY total DESC LIMIT 20
        """,
    )
    save("03_q2_consultations.json", q2)

    # ============================================================ Pergunta 3
    print("== 4. Pergunta 3: Particular vs Convênio ==")
    # Detecção: customer_insurances por appointment via customerId + appointment.customerdependentId?
    # Usamos doctorappointments.total > 0 → particular; total IS NULL or 0 → possivelmente convênio.
    # E validamos cruzando com customer_insurances ativos à data.
    q3 = {}
    q3["por_mes_payment_vs_null"] = q(
        cur,
        """
        SELECT DATE_FORMAT(da.`date`,'%%Y-%%m') AS mes,
               SUM(CASE WHEN IFNULL(da.total,0) > 0 THEN 1 ELSE 0 END) AS particular,
               SUM(CASE WHEN IFNULL(da.total,0) = 0 THEN 1 ELSE 0 END) AS convenio_ou_gratis,
               COUNT(*) AS total
        FROM doctorappointments da
        WHERE da.canceledAt IS NULL
        GROUP BY mes ORDER BY mes
        """,
    )
    # Via customer_insurances: appointments de clientes QUE TINHAM convênio ativo na data
    q3["com_convenio_ativo"] = q(
        cur,
        """
        SELECT DATE_FORMAT(da.`date`,'%%Y-%%m') AS mes,
               COUNT(DISTINCT da.id) AS com_convenio_ativo
        FROM doctorappointments da
        JOIN customer_insurances ci ON ci.customerId = da.customerId
             AND ci.isCurrent = 1
             AND ci.created_at <= da.`date`
             AND (ci.deleted_at IS NULL OR ci.deleted_at > da.`date`)
        WHERE da.canceledAt IS NULL
        GROUP BY mes ORDER BY mes
        """,
    )
    q3["por_insurance"] = q(
        cur,
        """
        SELECT i.name AS convenio, COUNT(DISTINCT da.id) AS consultas, COUNT(DISTINCT ci.customerId) AS clientes
        FROM doctorappointments da
        JOIN customer_insurances ci ON ci.customerId = da.customerId
             AND ci.isCurrent = 1
             AND ci.created_at <= da.`date`
             AND (ci.deleted_at IS NULL OR ci.deleted_at > da.`date`)
        JOIN insurances i ON i.id = ci.insuranceId
        WHERE da.canceledAt IS NULL
        GROUP BY i.name ORDER BY consultas DESC
        """,
    )
    # Se orders tem orderType que discrimine:
    q3["orders_por_tipo_mes"] = q(
        cur,
        """
        SELECT DATE_FORMAT(paymentDate,'%%Y-%%m') AS mes, orderType, COUNT(*) AS qtde,
               ROUND(SUM(total)/100.0,2) AS receita_reais
        FROM orders
        WHERE paymentDate IS NOT NULL
        GROUP BY mes, orderType ORDER BY mes, orderType
        """,
    )
    save("04_q3_particular_vs_conv.json", q3)

    # ============================================================ Pergunta 4
    print("== 5. Pergunta 4: Arrecadação por contrato (particular) ==")
    # 'contrato' = pagamento direto (particular) — appointments com total>0 + orders de farmácia/consulta pagas
    q4 = {}
    q4["por_mes_orders_pagas"] = q(
        cur,
        """
        SELECT DATE_FORMAT(paymentDate,'%%Y-%%m') AS mes,
               COUNT(*) AS orders_pagas,
               ROUND(SUM(total)/100.0,2) AS receita_total_reais,
               ROUND(SUM(subtotal)/100.0,2) AS subtotal_reais,
               ROUND(SUM(tax)/100.0,2) AS taxas_reais
        FROM orders
        WHERE paymentDate IS NOT NULL AND cancelDate IS NULL
        GROUP BY mes ORDER BY mes
        """,
    )
    q4["por_mes_appointments_pagas"] = q(
        cur,
        """
        SELECT DATE_FORMAT(paymentDate,'%%Y-%%m') AS mes,
               COUNT(*) AS appointments_pagos,
               ROUND(SUM(total)/100.0,2) AS receita_reais,
               ROUND(SUM(subtotal)/100.0,2) AS subtotal_reais,
               ROUND(SUM(discount)/100.0,2) AS descontos_reais
        FROM doctorappointments
        WHERE paymentDate IS NOT NULL AND canceledAt IS NULL AND total > 0
        GROUP BY mes ORDER BY mes
        """,
    )
    q4["por_mes_invoices_pagas"] = q(
        cur,
        """
        SELECT DATE_FORMAT(paymentDate,'%%Y-%%m') AS mes, type,
               COUNT(*) AS qtde, ROUND(SUM(total)/100.0,2) AS receita_reais
        FROM invoices
        WHERE paymentDate IS NOT NULL AND cancelDate IS NULL
        GROUP BY mes, type ORDER BY mes, type
        """,
    )
    q4["receita_consolidada_por_mes"] = q(
        cur,
        """
        SELECT mes, SUM(receita) AS receita_reais, SUM(qtde) AS qtde FROM (
          SELECT DATE_FORMAT(paymentDate,'%%Y-%%m') AS mes,
                 COUNT(*) AS qtde, SUM(total)/100.0 AS receita
          FROM orders
          WHERE paymentDate IS NOT NULL AND cancelDate IS NULL
          GROUP BY mes
          UNION ALL
          SELECT DATE_FORMAT(paymentDate,'%%Y-%%m') AS mes,
                 COUNT(*) AS qtde, SUM(total)/100.0 AS receita
          FROM doctorappointments
          WHERE paymentDate IS NOT NULL AND canceledAt IS NULL AND total > 0 AND orderId IS NULL
          GROUP BY mes
        ) x GROUP BY mes ORDER BY mes
        """,
    )
    save("05_q4_revenue_contract.json", q4)

    # ============================================================ Pergunta 5
    print("== 6. Pergunta 5: Arrecadação por convênio ==")
    q5 = {}
    q5["por_convenio_historico"] = q(
        cur,
        """
        SELECT i.name AS convenio,
               COUNT(DISTINCT da.id) AS consultas,
               ROUND(SUM(IFNULL(da.total,0))/100.0, 2) AS receita_reais,
               ROUND(AVG(IFNULL(da.total,0))/100.0, 2) AS ticket_medio_reais
        FROM doctorappointments da
        JOIN customer_insurances ci ON ci.customerId = da.customerId
             AND ci.isCurrent = 1
             AND ci.created_at <= da.`date`
             AND (ci.deleted_at IS NULL OR ci.deleted_at > da.`date`)
        JOIN insurances i ON i.id = ci.insuranceId
        WHERE da.canceledAt IS NULL
        GROUP BY i.name ORDER BY receita_reais DESC
        """,
    )
    q5["por_convenio_mes"] = q(
        cur,
        """
        SELECT DATE_FORMAT(da.`date`,'%%Y-%%m') AS mes, i.name AS convenio,
               COUNT(DISTINCT da.id) AS consultas,
               ROUND(SUM(IFNULL(da.total,0))/100.0, 2) AS receita_reais
        FROM doctorappointments da
        JOIN customer_insurances ci ON ci.customerId = da.customerId
             AND ci.isCurrent = 1
             AND ci.created_at <= da.`date`
             AND (ci.deleted_at IS NULL OR ci.deleted_at > da.`date`)
        JOIN insurances i ON i.id = ci.insuranceId
        WHERE da.canceledAt IS NULL
        GROUP BY mes, i.name ORDER BY mes, convenio
        """,
    )
    save("06_q5_revenue_insurance.json", q5)

    # ============================================================ Farmácia
    print("== 7. KPIs de Farmácia ==")
    pharma = {}
    pharma["farmacia_categoria_id"] = q(cur, "SELECT id, name, tag FROM categories WHERE tag IN ('drugstore','pharmacy','farmacia') OR name LIKE '%%arm%%'")
    pharma["top_stores_orders"] = q(
        cur,
        """
        SELECT s.id, s.name, s.city, s.state, s.isActive,
               COUNT(o.id) AS orders,
               COUNT(CASE WHEN o.paymentDate IS NOT NULL THEN 1 END) AS orders_pagas,
               ROUND(SUM(CASE WHEN o.paymentDate IS NOT NULL THEN o.total ELSE 0 END)/100.0,2) AS receita_reais,
               ROUND(AVG(CASE WHEN o.paymentDate IS NOT NULL THEN o.total END)/100.0,2) AS ticket_medio_reais
        FROM stores s
        LEFT JOIN orders o ON o.storeId = s.id
        GROUP BY s.id ORDER BY receita_reais DESC LIMIT 30
        """,
    )
    pharma["orders_por_mes"] = q(
        cur,
        """
        SELECT DATE_FORMAT(paymentDate,'%%Y-%%m') AS mes,
               COUNT(*) AS pedidos_pagos,
               ROUND(SUM(total)/100.0,2) AS receita_reais,
               ROUND(AVG(total)/100.0,2) AS ticket_medio_reais,
               COUNT(DISTINCT customerId) AS clientes_unicos,
               COUNT(DISTINCT storeId) AS lojas_ativas
        FROM orders
        WHERE paymentDate IS NOT NULL AND storeId IS NOT NULL
        GROUP BY mes ORDER BY mes
        """,
    )
    pharma["top_products_vendidos"] = q(
        cur,
        """
        SELECT p.id, p.title, p.activePrinciple, p.isControlled, p.producttypeId,
               COUNT(op.id) AS vendas,
               SUM(op.amount) AS unidades,
               ROUND(SUM(op.total)/100.0,2) AS receita_reais
        FROM order_products op
        JOIN orders o ON o.id = op.orderId AND o.paymentDate IS NOT NULL
        JOIN products p ON p.id = op.productId
        GROUP BY p.id ORDER BY receita_reais DESC LIMIT 30
        """,
    )
    pharma["products_ativos_por_storeapprovation"] = q(
        cur,
        """
        SELECT COUNT(DISTINCT storeId) AS lojas_com_produto,
               COUNT(DISTINCT productId) AS produtos_unicos,
               ROUND(AVG(price)/100.0,2) AS preco_medio_reais
        FROM store_products WHERE isActive = 1 AND inventoryAmount > 0
        """,
    )[0]
    pharma["produtos_controlados"] = q(
        cur,
        "SELECT COUNT(*) AS total, SUM(isControlled) AS controlados FROM products WHERE deleted_at IS NULL",
    )[0]
    pharma["orders_com_delivery"] = q(
        cur,
        """
        SELECT COUNT(*) AS total_pedidos,
               SUM(CASE WHEN deliverId IS NOT NULL THEN 1 ELSE 0 END) AS com_delivery,
               SUM(CASE WHEN deliverId IS NULL THEN 1 ELSE 0 END) AS sem_delivery,
               ROUND(AVG(CASE WHEN deliverId IS NOT NULL THEN deliverValue END),2) AS valor_medio_delivery
        FROM orders WHERE paymentDate IS NOT NULL AND storeId IS NOT NULL
        """,
    )[0]
    save("10_pharmacy_kpis.json", pharma)

    # ============================================================ Clínicas
    print("== 8. KPIs de Clínicas/Médicos ==")
    clinic = {}
    clinic["totais"] = q(
        cur,
        """
        SELECT
          (SELECT COUNT(*) FROM clinics WHERE deleted_at IS NULL) AS clinicas_total,
          (SELECT COUNT(*) FROM clinics WHERE isActive=1 AND deleted_at IS NULL) AS clinicas_ativas,
          (SELECT COUNT(*) FROM doctors WHERE deleted_at IS NULL) AS medicos_total,
          (SELECT COUNT(*) FROM doctors WHERE isActive=1 AND deleted_at IS NULL) AS medicos_ativos,
          (SELECT COUNT(*) FROM specialties WHERE deleted_at IS NULL) AS especialidades,
          (SELECT COUNT(*) FROM exams WHERE deleted_at IS NULL) AS exames_catalogo,
          (SELECT COUNT(*) FROM clinic_doctors WHERE deleted_at IS NULL) AS vinculos_medico_clinica
        """,
    )[0]
    clinic["top_clinicas_receita"] = q(
        cur,
        """
        SELECT c.id, c.name,
               COUNT(DISTINCT da.id) AS consultas_total,
               COUNT(DISTINCT CASE WHEN da.paymentDate IS NOT NULL THEN da.id END) AS pagas,
               ROUND(SUM(CASE WHEN da.paymentDate IS NOT NULL THEN da.total ELSE 0 END)/100.0,2) AS receita_reais
        FROM clinics c
        LEFT JOIN clinic_doctors cd ON cd.clinicId = c.id
        LEFT JOIN clinic_doctor_specialties cds ON cds.clinicDoctorId = cd.id
        LEFT JOIN schedules sch ON sch.clinicDoctorSpecialtyId = cds.id
        LEFT JOIN doctorappointments da ON da.scheduleId = sch.id
        GROUP BY c.id ORDER BY receita_reais DESC LIMIT 30
        """,
    )
    clinic["top_especialidades"] = q(
        cur,
        """
        SELECT s.name, s.type,
               COUNT(DISTINCT da.id) AS consultas,
               COUNT(DISTINCT da.customerId) AS clientes_unicos,
               ROUND(SUM(IFNULL(da.total,0))/100.0,2) AS receita_reais
        FROM specialties s
        LEFT JOIN doctorappointments da ON da.specialtyId = s.id
        GROUP BY s.id, s.type ORDER BY consultas DESC LIMIT 30
        """,
    )
    clinic["top_doctors_consultas"] = q(
        cur,
        """
        SELECT d.id, d.name, COUNT(DISTINCT da.id) AS consultas,
               ROUND(AVG(da.evaluationDoctor),2) AS nota_media,
               ROUND(SUM(IFNULL(da.total,0))/100.0,2) AS receita_reais
        FROM doctors d
        JOIN clinic_doctors cd ON cd.doctorId = d.id
        JOIN clinic_doctor_specialties cds ON cds.clinicDoctorId = cd.id
        JOIN schedules sch ON sch.clinicDoctorSpecialtyId = cds.id
        JOIN doctorappointments da ON da.scheduleId = sch.id
        GROUP BY d.id ORDER BY consultas DESC LIMIT 30
        """,
    )
    clinic["taxa_ocupacao_mes"] = q(
        cur,
        """
        SELECT DATE_FORMAT(da.`date`,'%%Y-%%m') AS mes,
               COUNT(*) AS appointments,
               COUNT(da.paymentDate) AS pagas,
               COUNT(da.canceledAt) AS canceladas,
               ROUND(100.0*COUNT(da.canceledAt)/COUNT(*),2) AS pct_canceladas,
               ROUND(100.0*COUNT(da.paymentDate)/COUNT(*),2) AS pct_convertidas
        FROM doctorappointments da
        WHERE da.`date` >= '2023-01-01'
        GROUP BY mes ORDER BY mes
        """,
    )
    clinic["exames_top20"] = q(
        cur,
        """
        SELECT e.id, e.name, e.tussCode,
               COUNT(DISTINCT ce.clinicId) AS clinicas_ofertantes,
               ROUND(AVG(ce.price)/100.0,2) AS preco_medio_reais,
               ROUND(AVG(ce.priceWithDiscount)/100.0,2) AS preco_com_desc_reais
        FROM exams e
        LEFT JOIN clinic_exams ce ON ce.examId = e.id AND ce.isActive=1
        WHERE e.isActive = 1
        GROUP BY e.id ORDER BY clinicas_ofertantes DESC LIMIT 20
        """,
    )
    save("11_clinic_kpis.json", clinic)

    # ============================================================ Financeiro
    print("== 9. KPIs Financeiros ==")
    fin = {}
    fin["receita_por_fonte_historico"] = q(
        cur,
        """
        SELECT 'orders' AS fonte, COUNT(*) AS qtde, ROUND(SUM(total)/100.0,2) AS receita_reais
        FROM orders WHERE paymentDate IS NOT NULL AND cancelDate IS NULL
        UNION ALL
        SELECT 'doctorappointments', COUNT(*), ROUND(SUM(total)/100.0,2)
        FROM doctorappointments WHERE paymentDate IS NOT NULL AND canceledAt IS NULL AND orderId IS NULL
        UNION ALL
        SELECT 'invoices', COUNT(*), ROUND(SUM(total)/100.0,2)
        FROM invoices WHERE paymentDate IS NOT NULL AND cancelDate IS NULL
        """,
    )
    fin["comissoes_por_origem"] = q(
        cur,
        """
        SELECT origin, type, COUNT(*) AS qtde, ROUND(SUM(value)/100.0,2) AS total_reais
        FROM ordercommissions GROUP BY origin, type ORDER BY total_reais DESC
        """,
    )
    fin["comissoes_por_mes"] = q(
        cur,
        """
        SELECT DATE_FORMAT(created_at,'%%Y-%%m') AS mes, origin,
               COUNT(*) AS qtde, ROUND(SUM(value)/100.0,2) AS total_reais
        FROM ordercommissions
        GROUP BY mes, origin ORDER BY mes, origin
        """,
    )
    fin["transfers_por_mes"] = q(
        cur,
        """
        SELECT DATE_FORMAT(IFNULL(transferDate,created_at),'%%Y-%%m') AS mes,
               SUM(CASE WHEN storeId IS NOT NULL THEN 1 ELSE 0 END) AS para_lojas,
               SUM(CASE WHEN deliverId IS NOT NULL THEN 1 ELSE 0 END) AS para_delivers,
               SUM(CASE WHEN clinicId IS NOT NULL THEN 1 ELSE 0 END) AS para_clinicas,
               ROUND(SUM(value)/100.0,2) AS total_reais,
               ROUND(SUM(withdrawTaxValue)/100.0,2) AS taxa_reais
        FROM transfers GROUP BY mes ORDER BY mes
        """,
    )
    fin["refunds"] = q(
        cur,
        """
        SELECT 'orders' AS fonte, COUNT(*) AS qtde,
               ROUND(SUM(total)/100.0,2) AS total_reembolsado_reais
        FROM orders WHERE refundDate IS NOT NULL
        UNION ALL
        SELECT 'doctorappointments', COUNT(*), ROUND(SUM(total)/100.0,2)
        FROM doctorappointments WHERE refundDate IS NOT NULL
        """,
    )
    fin["chargetype_dist"] = q(
        cur,
        """
        SELECT chargeType, COUNT(*) AS qtde, ROUND(SUM(total)/100.0,2) AS receita_reais
        FROM orders WHERE paymentDate IS NOT NULL
        GROUP BY chargeType ORDER BY qtde DESC
        """,
    )
    fin["gateway_usage"] = q(
        cur,
        """
        SELECT pg.name AS gateway, COUNT(pm.id) AS metodos_salvos
        FROM paymentgateways pg
        LEFT JOIN paymentmethods pm ON pm.paymentgatewayId = pg.id
        GROUP BY pg.name
        """,
    )
    save("12_financial_kpis.json", fin)

    # ============================================================ Delivery
    print("== 10. KPIs de Delivery ==")
    deliv = {}
    deliv["delivers_totais"] = q(
        cur,
        """
        SELECT COUNT(*) AS total,
               SUM(isActive) AS ativos,
               SUM(CASE WHEN status='approved' THEN 1 ELSE 0 END) AS aprovados,
               SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) AS pendentes,
               ROUND(AVG(rateStars),2) AS avaliacao_media
        FROM delivers WHERE deleted_at IS NULL
        """,
    )[0]
    deliv["por_mes"] = q(
        cur,
        """
        SELECT DATE_FORMAT(o.paymentDate,'%%Y-%%m') AS mes,
               COUNT(*) AS pedidos_com_delivery,
               COUNT(DISTINCT o.deliverId) AS delivers_ativos,
               ROUND(AVG(o.deliverValue),2) AS valor_medio_delivery
        FROM orders o
        WHERE o.paymentDate IS NOT NULL AND o.deliverId IS NOT NULL
        GROUP BY mes ORDER BY mes
        """,
    )
    deliv["top_delivers"] = q(
        cur,
        """
        SELECT d.id, d.name, d.city, d.rateStars,
               COUNT(o.id) AS entregas,
               ROUND(SUM(o.deliverValue),2) AS ganhos_delivery_reais
        FROM delivers d
        LEFT JOIN orders o ON o.deliverId = d.id AND o.paymentDate IS NOT NULL
        GROUP BY d.id ORDER BY entregas DESC LIMIT 20
        """,
    )
    save("13_delivery_kpis.json", deliv)

    # ============================================================ Avaliações
    print("== 11. KPIs de Avaliações ==")
    eva = {}
    eva["distribuicao_notas"] = q(
        cur,
        """
        SELECT who, evaluation, COUNT(*) AS qtde
        FROM evaluations GROUP BY who, evaluation ORDER BY who, evaluation
        """,
    )
    eva["media_por_tipo"] = q(
        cur,
        "SELECT who, ROUND(AVG(evaluation),3) AS media, COUNT(*) AS n FROM evaluations GROUP BY who",
    )
    eva["top_avaliados_doctors"] = q(
        cur,
        """
        SELECT d.name, ROUND(AVG(e.evaluation),2) AS media, COUNT(*) AS n_avaliacoes
        FROM evaluations e JOIN doctors d ON d.id = e.whoId
        WHERE e.who='doctor'
        GROUP BY d.id HAVING n_avaliacoes >= 5
        ORDER BY media DESC, n_avaliacoes DESC LIMIT 20
        """,
    )
    eva["nps_style_consulta"] = q(
        cur,
        """
        SELECT DATE_FORMAT(`date`,'%%Y-%%m') AS mes,
               ROUND(AVG(evaluationDoctor),2) AS nota_medico,
               ROUND(AVG(evaluationClinic),2) AS nota_clinica,
               COUNT(evaluationDoctor) AS n_avaliacoes
        FROM doctorappointments
        WHERE evaluationDoctor IS NOT NULL
        GROUP BY mes ORDER BY mes
        """,
    )
    save("14_evaluation_kpis.json", eva)

    # ============================================================ Cupons
    print("== 12. KPIs de Cupons ==")
    cup = {}
    cup["totais"] = q(
        cur,
        """
        SELECT COUNT(*) AS total_cupons, SUM(isActive) AS ativos,
               SUM(amount) AS capacidade_total, SUM(amountUsed) AS total_usados
        FROM coupons WHERE deleted_at IS NULL
        """,
    )[0]
    cup["top_cupons"] = q(
        cur,
        """
        SELECT c.id, c.name, c.code, c.value, c.isPercent, c.percentValue,
               c.amount, c.amountUsed,
               ROUND(100.0*c.amountUsed/NULLIF(c.amount,0),2) AS pct_uso
        FROM coupons c
        WHERE c.deleted_at IS NULL
        ORDER BY c.amountUsed DESC LIMIT 20
        """,
    )
    cup["cupons_por_mes"] = q(
        cur,
        """
        SELECT DATE_FORMAT(created_at,'%%Y-%%m') AS mes, COUNT(*) AS utilizacoes
        FROM coupon_customers GROUP BY mes ORDER BY mes
        """,
    )
    save("15_coupon_kpis.json", cup)

    # ============================================================ Prontuário
    print("== 13. KPIs de Prontuário ==")
    med = {}
    med["prontuarios_criados_mes"] = q(
        cur,
        """
        SELECT DATE_FORMAT(created_at,'%%Y-%%m') AS mes,
               COUNT(*) AS prontuarios,
               COUNT(finishedAt) AS finalizados
        FROM medical_records
        GROUP BY mes ORDER BY mes
        """,
    )
    med["cid_top20"] = q(
        cur,
        """
        SELECT code, description, COUNT(*) AS qtde
        FROM medical_record_cids
        GROUP BY code, description ORDER BY qtde DESC LIMIT 20
        """,
    )
    med["prescricoes"] = q(
        cur,
        """
        SELECT 'interna' AS tipo, COUNT(*) AS total FROM medical_record_prescriptions
        UNION ALL
        SELECT 'externa', COUNT(*) FROM externalprescriptions
        """,
    )
    med["prescricoes_externas_mes"] = q(
        cur,
        """
        SELECT DATE_FORMAT(created_at,'%%Y-%%m') AS mes, COUNT(*) AS qtde
        FROM externalprescriptions GROUP BY mes ORDER BY mes
        """,
    )
    save("16_medical_kpis.json", med)

    # ============================================================ Cohort
    print("== 14. Cohort de clientes (retenção) ==")
    cohort = {}
    cohort["primeira_compra_vs_mes"] = q(
        cur,
        """
        WITH firstp AS (
          SELECT customerId, DATE_FORMAT(MIN(paymentDate),'%%Y-%%m') AS cohort
          FROM orders WHERE paymentDate IS NOT NULL
          GROUP BY customerId
        )
        SELECT f.cohort AS cohort_mes,
               DATE_FORMAT(o.paymentDate,'%%Y-%%m') AS mes,
               COUNT(DISTINCT o.customerId) AS clientes_ativos
        FROM orders o JOIN firstp f ON f.customerId = o.customerId
        WHERE o.paymentDate IS NOT NULL
        GROUP BY f.cohort, mes ORDER BY f.cohort, mes
        """,
    )
    cohort["ltv_top20"] = q(
        cur,
        """
        SELECT c.id, c.name,
               c.totalSpent/100.0 AS total_gasto_reais,
               c.qtdeSpent AS qtde_compras,
               c.totalConsults/100.0 AS total_consultas_reais,
               c.totalExams/100.0 AS total_exames_reais
        FROM customers c
        WHERE c.deleted_at IS NULL
        ORDER BY c.totalSpent DESC LIMIT 20
        """,
    )
    cohort["distribuicao_ltv"] = q(
        cur,
        """
        SELECT bucket, COUNT(*) AS clientes FROM (
          SELECT CASE
            WHEN totalSpent = 0 THEN '00. zero'
            WHEN totalSpent < 5000 THEN '01. <R$50'
            WHEN totalSpent < 20000 THEN '02. R$50-200'
            WHEN totalSpent < 50000 THEN '03. R$200-500'
            WHEN totalSpent < 100000 THEN '04. R$500-1k'
            WHEN totalSpent < 500000 THEN '05. R$1k-5k'
            ELSE '06. >R$5k'
          END AS bucket
          FROM customers WHERE deleted_at IS NULL
        ) x GROUP BY bucket ORDER BY bucket
        """,
    )
    save("17_cohort_retention.json", cohort)

    # ============================================================ Geografia
    print("== 15. Geografia ==")
    geo = {}
    geo["clientes_por_estado"] = q(
        cur,
        """
        SELECT ca.state, COUNT(DISTINCT c.id) AS clientes
        FROM customers c
        LEFT JOIN customeraddresses ca ON ca.customerId = c.id AND ca.current = 1
        WHERE c.deleted_at IS NULL
        GROUP BY ca.state ORDER BY clientes DESC
        """,
    )
    geo["clientes_por_cidade_top20"] = q(
        cur,
        """
        SELECT ca.city, ca.state, COUNT(DISTINCT c.id) AS clientes
        FROM customers c
        JOIN customeraddresses ca ON ca.customerId = c.id AND ca.current = 1
        WHERE c.deleted_at IS NULL AND ca.city IS NOT NULL
        GROUP BY ca.city, ca.state ORDER BY clientes DESC LIMIT 20
        """,
    )
    geo["clinicas_por_estado"] = q(
        cur,
        """
        SELECT state, COUNT(*) AS clinicas, SUM(isActive) AS ativas
        FROM clinics WHERE deleted_at IS NULL
        GROUP BY state ORDER BY clinicas DESC
        """,
    )
    geo["stores_por_estado"] = q(
        cur,
        """
        SELECT state, COUNT(*) AS stores, SUM(isActive) AS ativas
        FROM stores WHERE deleted_at IS NULL
        GROUP BY state ORDER BY stores DESC
        """,
    )
    save("18_geography.json", geo)

    # ============================================================ Loyalty
    print("== 16. Fidelização / recompra ==")
    loy = {}
    loy["compras_por_cliente"] = q(
        cur,
        """
        SELECT bucket, COUNT(*) AS clientes FROM (
          SELECT CASE
            WHEN n = 1 THEN '1'
            WHEN n = 2 THEN '2'
            WHEN n BETWEEN 3 AND 5 THEN '3-5'
            WHEN n BETWEEN 6 AND 10 THEN '6-10'
            ELSE '11+'
          END AS bucket
          FROM (
            SELECT customerId, COUNT(*) AS n
            FROM orders WHERE paymentDate IS NOT NULL
            GROUP BY customerId
          ) x
        ) y GROUP BY bucket ORDER BY bucket
        """,
    )
    loy["days_between_orders"] = q(
        cur,
        """
        SELECT ROUND(AVG(diff),1) AS media_dias,
               ROUND(AVG(diff),1) AS mediana_estimativa_dias,
               MIN(diff) AS menor, MAX(diff) AS maior
        FROM (
          SELECT customerId, DATEDIFF(paymentDate,
                    LAG(paymentDate) OVER (PARTITION BY customerId ORDER BY paymentDate)) AS diff
          FROM orders WHERE paymentDate IS NOT NULL
        ) x WHERE diff IS NOT NULL
        """,
    )
    loy["tempo_ate_2a_compra"] = q(
        cur,
        """
        SELECT bucket, COUNT(*) AS clientes FROM (
          SELECT CASE
            WHEN diff IS NULL THEN 'so_1'
            WHEN diff <= 7 THEN '01. ate_1sem'
            WHEN diff <= 30 THEN '02. ate_1mes'
            WHEN diff <= 90 THEN '03. ate_3m'
            WHEN diff <= 180 THEN '04. ate_6m'
            WHEN diff <= 365 THEN '05. ate_1ano'
            ELSE '06. mais_1ano'
          END AS bucket
          FROM (
            SELECT customerId,
                   DATEDIFF(MIN(CASE WHEN rn=2 THEN paymentDate END),
                            MIN(CASE WHEN rn=1 THEN paymentDate END)) AS diff
            FROM (
              SELECT customerId, paymentDate,
                     ROW_NUMBER() OVER (PARTITION BY customerId ORDER BY paymentDate) AS rn
              FROM orders WHERE paymentDate IS NOT NULL
            ) r
            GROUP BY customerId
          ) x
        ) y GROUP BY bucket ORDER BY bucket
        """,
    )
    save("19_loyalty.json", loy)

    # ---------------- Relatório mestre ----------------
    print("== Gerando KPI_ANALYSIS_REPORT.md ==")
    build_markdown_report()
    cur.close()
    conn.close()
    print("\n✔ Análise concluída.")


def build_markdown_report():
    def load(fn):
        return json.loads((OUT / fn).read_text())

    def fmt_money(v):
        if v is None:
            return "—"
        return f"R$ {float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

    def fmt_int(v):
        if v is None:
            return "—"
        return f"{int(v):,}".replace(",", ".")

    info = json.loads((OUT.parent / "00_server_info.json").read_text())
    lines: list[str] = []
    L = lines.append

    L("# Análise de KPIs — Banco Égide")
    L("")
    L(f"Data de coleta: `{info['now']}` · MySQL `{info['version']}`")
    L("")
    L("> Documento gerado automaticamente por `egide/analyze_egide.py`.  ")
    L("> Dados detalhados em JSON no mesmo diretório.")
    L("")

    # 5 perguntas
    L("## ⭐ Respostas às 5 perguntas dos diretores")
    L("")

    q1 = load("02_q1_clients.json")
    L("### 1) Clientes por período")
    total = q1["total_historico"]
    L(f"- **Base total de clientes:** {fmt_int(total['total'])} (soft-deleted: {fmt_int(total['soft_deleted'])})")
    L("")
    L("**Novos clientes por mês** (últimos 12 meses registrados):")
    L("")
    L("| Mês | Novos | Ativos (orders) | Ativos (consultas) |")
    L("|---|---:|---:|---:|")
    novos = {r["mes"]: r["novos"] for r in q1["novos_por_mes"]}
    ao = {r["mes"]: r["ativos_orders"] for r in q1["ativos_por_mes_orders"]}
    ac = {r["mes"]: r["ativos_consultas"] for r in q1["ativos_por_mes_consultas"]}
    meses = sorted(set(novos) | set(ao) | set(ac))[-18:]
    for m in meses:
        L(f"| {m} | {fmt_int(novos.get(m,0))} | {fmt_int(ao.get(m,0))} | {fmt_int(ac.get(m,0))} |")
    L("")

    q2 = load("03_q2_consultations.json")
    L("### 2) Consultas / exames por período")
    t = q2["total_historico"]
    L(f"- **Total histórico:** {fmt_int(t['total'])} appointments · pagos: {fmt_int(t['paid'])} · cancelados: {fmt_int(t['canceled'])}")
    L("")
    L("**Appointments por mês (data da consulta):**")
    L("")
    L("| Mês | Total | Pagos | Confirmados | Cancelados | Consultas | Exames/outros |")
    L("|---|---:|---:|---:|---:|---:|---:|")
    for r in q2["por_mes_data_consulta"][-18:]:
        L(
            f"| {r['mes']} | {fmt_int(r['total'])} | {fmt_int(r['pagas'])} | "
            f"{fmt_int(r['confirmadas'])} | {fmt_int(r['canceladas'])} | "
            f"{fmt_int(r['consultas'])} | {fmt_int(r['exames_ou_outros'])} |"
        )
    L("")

    q3 = load("04_q3_particular_vs_conv.json")
    L("### 3) Particular vs Convênio")
    L("")
    L("**Heurística 1** — `total>0` = particular · `total=0` = convênio/gratuito:")
    L("")
    L("| Mês | Particular | Convênio/gratuito | Total |")
    L("|---|---:|---:|---:|")
    for r in q3["por_mes_payment_vs_null"][-18:]:
        L(f"| {r['mes']} | {fmt_int(r['particular'])} | {fmt_int(r['convenio_ou_gratis'])} | {fmt_int(r['total'])} |")
    L("")
    L("**Heurística 2** — appointments de clientes com convênio ativo na data:")
    L("")
    L("| Mês | Com convênio ativo |")
    L("|---|---:|")
    for r in q3["com_convenio_ativo"][-18:]:
        L(f"| {r['mes']} | {fmt_int(r['com_convenio_ativo'])} |")
    L("")
    L("**Distribuição por convênio (histórico):**")
    L("")
    L("| Convênio | Consultas | Clientes |")
    L("|---|---:|---:|")
    for r in q3["por_insurance"]:
        L(f"| {r['convenio']} | {fmt_int(r['consultas'])} | {fmt_int(r['clientes'])} |")
    L("")

    q4 = load("05_q4_revenue_contract.json")
    L("### 4) Arrecadação por contrato (particular)")
    L("")
    L("**Orders pagas (farmácia + consultas via order):**")
    L("")
    L("| Mês | Orders pagas | Receita | Subtotal | Taxas |")
    L("|---|---:|---:|---:|---:|")
    for r in q4["por_mes_orders_pagas"][-18:]:
        L(
            f"| {r['mes']} | {fmt_int(r['orders_pagas'])} | "
            f"{fmt_money(r['receita_total_reais'])} | "
            f"{fmt_money(r['subtotal_reais'])} | {fmt_money(r['taxas_reais'])} |"
        )
    L("")
    L("**Appointments pagas diretamente (particular, sem order):**")
    L("")
    L("| Mês | Appointments | Receita | Subtotal | Descontos |")
    L("|---|---:|---:|---:|---:|")
    for r in q4["por_mes_appointments_pagas"][-18:]:
        L(
            f"| {r['mes']} | {fmt_int(r['appointments_pagos'])} | "
            f"{fmt_money(r['receita_reais'])} | {fmt_money(r['subtotal_reais'])} | "
            f"{fmt_money(r['descontos_reais'])} |"
        )
    L("")

    q5 = load("06_q5_revenue_insurance.json")
    L("### 5) Arrecadação por convênio")
    L("")
    L("**Histórico por convênio:**")
    L("")
    L("| Convênio | Consultas | Receita | Ticket médio |")
    L("|---|---:|---:|---:|")
    for r in q5["por_convenio_historico"]:
        L(
            f"| {r['convenio']} | {fmt_int(r['consultas'])} | "
            f"{fmt_money(r['receita_reais'])} | {fmt_money(r['ticket_medio_reais'])} |"
        )
    L("")

    # KPIs por domínio
    L("## 📊 KPIs adicionais (todos os domínios)")
    L("")

    pharma = load("10_pharmacy_kpis.json")
    L("### Farmácia")
    L("")
    p1 = pharma["products_ativos_por_storeapprovation"]
    L(f"- Lojas com produto ativo em estoque: **{fmt_int(p1['lojas_com_produto'])}**")
    L(f"- Produtos únicos ativos: **{fmt_int(p1['produtos_unicos'])}** · preço médio {fmt_money(p1['preco_medio_reais'])}")
    pc = pharma["produtos_controlados"]
    L(f"- Catálogo total: **{fmt_int(pc['total'])}** · controlados: **{fmt_int(pc['controlados'])}**")
    od = pharma["orders_com_delivery"]
    L(f"- Pedidos com delivery: **{fmt_int(od['com_delivery'])}** / {fmt_int(od['total_pedidos'])}  (valor médio {fmt_money(od['valor_medio_delivery'])})")
    L("")
    L("**Pedidos de farmácia por mês:**")
    L("")
    L("| Mês | Pedidos | Receita | Ticket médio | Clientes únicos | Lojas |")
    L("|---|---:|---:|---:|---:|---:|")
    for r in pharma["orders_por_mes"][-18:]:
        L(
            f"| {r['mes']} | {fmt_int(r['pedidos_pagos'])} | {fmt_money(r['receita_reais'])} | "
            f"{fmt_money(r['ticket_medio_reais'])} | {fmt_int(r['clientes_unicos'])} | "
            f"{fmt_int(r['lojas_ativas'])} |"
        )
    L("")
    L("**Top 10 lojas por receita (farmácias):**")
    L("")
    L("| Loja | Cidade | Ativa | Orders | Pagos | Receita | Ticket médio |")
    L("|---|---|:-:|---:|---:|---:|---:|")
    for r in pharma["top_stores_orders"][:10]:
        L(
            f"| {r['name']} | {r.get('city') or '—'}/{r.get('state') or '—'} | "
            f"{'✓' if r['isActive'] else '✗'} | {fmt_int(r['orders'])} | {fmt_int(r['orders_pagas'])} | "
            f"{fmt_money(r['receita_reais'])} | {fmt_money(r['ticket_medio_reais'])} |"
        )
    L("")
    L("**Top 10 produtos mais vendidos:**")
    L("")
    L("| Produto | Unidades | Receita |")
    L("|---|---:|---:|")
    for r in pharma["top_products_vendidos"][:10]:
        L(f"| {r['title']} | {fmt_int(r['unidades'])} | {fmt_money(r['receita_reais'])} |")
    L("")

    clinic = load("11_clinic_kpis.json")
    t = clinic["totais"]
    L("### Clínicas / médicos")
    L(f"- Clínicas: **{fmt_int(t['clinicas_ativas'])}/{fmt_int(t['clinicas_total'])}** ativas")
    L(f"- Médicos: **{fmt_int(t['medicos_ativos'])}/{fmt_int(t['medicos_total'])}** ativos")
    L(f"- Especialidades: **{fmt_int(t['especialidades'])}** · Exames catálogo: **{fmt_int(t['exames_catalogo'])}**")
    L(f"- Vínculos médico↔clínica: **{fmt_int(t['vinculos_medico_clinica'])}**")
    L("")
    L("**Ocupação/conversão mensal (appointments):**")
    L("")
    L("| Mês | Appointments | Pagas | Canceladas | % conv | % canc |")
    L("|---|---:|---:|---:|---:|---:|")
    for r in clinic["taxa_ocupacao_mes"][-18:]:
        L(
            f"| {r['mes']} | {fmt_int(r['appointments'])} | {fmt_int(r['pagas'])} | "
            f"{fmt_int(r['canceladas'])} | {r['pct_convertidas']}% | {r['pct_canceladas']}% |"
        )
    L("")
    L("**Top 10 especialidades por volume:**")
    L("")
    L("| Especialidade | Tipo | Consultas | Clientes | Receita |")
    L("|---|---|---:|---:|---:|")
    for r in clinic["top_especialidades"][:10]:
        L(
            f"| {r['name']} | {r['type']} | {fmt_int(r['consultas'])} | "
            f"{fmt_int(r['clientes_unicos'])} | {fmt_money(r['receita_reais'])} |"
        )
    L("")

    fin = load("12_financial_kpis.json")
    L("### Financeiro")
    L("")
    L("**Receita por fonte (histórico):**")
    L("")
    L("| Fonte | Qtde | Receita |")
    L("|---|---:|---:|")
    for r in fin["receita_por_fonte_historico"]:
        L(f"| {r['fonte']} | {fmt_int(r['qtde'])} | {fmt_money(r['receita_reais'])} |")
    L("")
    L("**Comissões por origem:**")
    L("")
    L("| Origem | Tipo | Qtde | Total |")
    L("|---|---|---:|---:|")
    for r in fin["comissoes_por_origem"]:
        L(f"| {r['origin']} | {r['type']} | {fmt_int(r['qtde'])} | {fmt_money(r['total_reais'])} |")
    L("")
    L("**Refunds (reembolsos):**")
    L("")
    L("| Fonte | Qtde | Total reembolsado |")
    L("|---|---:|---:|")
    for r in fin["refunds"]:
        L(f"| {r['fonte']} | {fmt_int(r['qtde'])} | {fmt_money(r['total_reembolsado_reais'])} |")
    L("")

    deliv = load("13_delivery_kpis.json")
    d = deliv["delivers_totais"]
    L("### Delivery")
    L(f"- Delivers cadastrados: **{fmt_int(d['total'])}** · ativos: {fmt_int(d['ativos'])} · aprovados: {fmt_int(d['aprovados'])} · pendentes: {fmt_int(d['pendentes'])}")
    L(f"- Avaliação média: **{d['avaliacao_media']}**")
    L("")

    eva = load("14_evaluation_kpis.json")
    L("### Avaliações")
    L("")
    L("**Média por tipo de avaliação:**")
    L("")
    L("| Tipo | Média | N |")
    L("|---|---:|---:|")
    for r in eva["media_por_tipo"]:
        L(f"| {r['who']} | {r['media']} | {fmt_int(r['n'])} |")
    L("")

    cup = load("15_coupon_kpis.json")
    t = cup["totais"]
    L("### Cupons")
    L(f"- Cupons cadastrados: **{fmt_int(t['total_cupons'])}** · ativos: {fmt_int(t['ativos'])}")
    L(f"- Capacidade total: {fmt_int(t['capacidade_total'])} · usados: **{fmt_int(t['total_usados'])}**")
    L("")

    med = load("16_medical_kpis.json")
    L("### Prontuário eletrônico")
    L("")
    L("| Tipo de prescrição | Total |")
    L("|---|---:|")
    for r in med["prescricoes"]:
        L(f"| {r['tipo']} | {fmt_int(r['total'])} |")
    L("")
    L("**Top 10 CIDs registrados:**")
    L("")
    L("| CID | Descrição | Qtde |")
    L("|---|---|---:|")
    for r in med["cid_top20"][:10]:
        L(f"| {r['code']} | {r['description']} | {fmt_int(r['qtde'])} |")
    L("")

    loy = load("19_loyalty.json")
    L("### Fidelização / recompra")
    L("")
    L("**Distribuição de compras por cliente (farmácia):**")
    L("")
    L("| Bucket | Clientes |")
    L("|---|---:|")
    for r in loy["compras_por_cliente"]:
        L(f"| {r['bucket']} | {fmt_int(r['clientes'])} |")
    L("")
    L("**Tempo até a 2ª compra:**")
    L("")
    L("| Bucket | Clientes |")
    L("|---|---:|")
    for r in loy["tempo_ate_2a_compra"]:
        L(f"| {r['bucket']} | {fmt_int(r['clientes'])} |")
    L("")

    geo = load("18_geography.json")
    L("### Geografia")
    L("")
    L("**Clientes por estado (top 10):**")
    L("")
    L("| UF | Clientes |")
    L("|---|---:|")
    for r in geo["clientes_por_estado"][:10]:
        L(f"| {r['state'] or '—'} | {fmt_int(r['clientes'])} |")
    L("")

    (OUT / "KPI_ANALYSIS_REPORT.md").write_text("\n".join(lines), encoding="utf-8")
    print(f"  → {(OUT / 'KPI_ANALYSIS_REPORT.md').relative_to(ROOT)}")


if __name__ == "__main__":
    main()
