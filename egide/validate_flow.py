import os, pymysql
from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[1] / ".env")
conn = pymysql.connect(host=os.environ["DB_HOST"], port=int(os.environ["DB_PORT"]),
    user=os.environ["DB_USER"], password=os.environ["DB_PASSWORD"],
    database=os.environ["DB_NAME"], charset="utf8mb4", cursorclass=pymysql.cursors.DictCursor)
cur = conn.cursor()

print("=== Validar fluxo order <-> doctorappointment ===\n")
cur.execute("SELECT COUNT(*) c, COUNT(orderId) with_order, COUNT(*)-COUNT(orderId) without_order FROM doctorappointments")
r = cur.fetchone()
print(f"doctorappointments: total={r['c']:,}  com orderId={r['with_order']:,}  sem orderId={r['without_order']:,}")

cur.execute("""SELECT orderType, COUNT(*) qtd FROM orders o
  WHERE EXISTS(SELECT 1 FROM doctorappointments da WHERE da.orderId=o.id)
  GROUP BY orderType""")
print("\nOrders com ao menos 1 appointment linkado, por orderType:")
for r in cur.fetchall(): print(f"  {r['orderType']}: {r['qtd']:,}")

cur.execute("""SELECT orderType, COUNT(*) qtd FROM orders o
  WHERE NOT EXISTS(SELECT 1 FROM doctorappointments da WHERE da.orderId=o.id)
  GROUP BY orderType""")
print("\nOrders SEM appointments linkando, por orderType:")
for r in cur.fetchall(): print(f"  {r['orderType']}: {r['qtd']:,}")

print("\n=== Orders pagas x existe customer_insurance ativo na data ===")
cur.execute("""
SELECT o.orderType,
       CASE WHEN EXISTS(
         SELECT 1 FROM customer_insurances ci WHERE ci.customerId=o.customerId
           AND ci.isCurrent=1 AND ci.created_at<=o.paymentDate
           AND (ci.deleted_at IS NULL OR ci.deleted_at > o.paymentDate)
       ) THEN 'tinha_convenio' ELSE 'sem_convenio' END AS flag,
       COUNT(*) qtd, ROUND(SUM(total)/100.0,2) receita
FROM orders o WHERE paymentDate IS NOT NULL AND cancelDate IS NULL
GROUP BY o.orderType, flag ORDER BY o.orderType, flag
""")
for r in cur.fetchall():
    print(f"  {r['orderType']:<15} {r['flag']:<20} qtd={r['qtd']:>7,}  R$ {float(r['receita']):>12,.2f}")

print("\n=== Comparacao farmacia x clinica (ultimos 12m) ===")
cur.execute("""
SELECT DATE_FORMAT(paymentDate,'%%Y-%%m') mes,
       SUM(CASE WHEN orderType='pharmacy' THEN 1 ELSE 0 END) qtd_farmacia,
       ROUND(SUM(CASE WHEN orderType='pharmacy' THEN total ELSE 0 END)/100.0,2) rec_farmacia,
       SUM(CASE WHEN orderType='clinic' THEN 1 ELSE 0 END) qtd_clinica,
       ROUND(SUM(CASE WHEN orderType='clinic' THEN total ELSE 0 END)/100.0,2) rec_clinica
FROM orders WHERE paymentDate IS NOT NULL AND cancelDate IS NULL
  AND paymentDate >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH)
GROUP BY mes ORDER BY mes
""")
print(f"{'mes':<10}{'farm_qtd':>10}{'farm_R$':>14}{'clin_qtd':>10}{'clin_R$':>14}")
for r in cur.fetchall():
    print(f"  {r['mes']:<8} {r['qtd_farmacia']:>8,} R$ {float(r['rec_farmacia']):>10,.2f} {r['qtd_clinica']:>8,} R$ {float(r['rec_clinica']):>10,.2f}")

conn.close()
