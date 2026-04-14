# Catálogo de KPIs — Projeto Égide

> Fonte: varredura completa do banco `egide_production` (MySQL 8.4.8 em RDS AWS sa-east-1), coletada em 2026-04-14.
> Artefatos: [scan_output/SCHEMA_REPORT.md](scan_output/SCHEMA_REPORT.md) · [scan_output/analysis/KPI_ANALYSIS_REPORT.md](scan_output/analysis/KPI_ANALYSIS_REPORT.md) · dumps JSON no mesmo diretório.

---

## 1. Modelo de dados — entendimento

A aplicação Égide é um **marketplace de saúde + farmácia** unificado. A tabela central é **`orders`** (117k registros), que suporta 4 tipos via `orderType`:

| `orderType`   | Qtde   | Descrição                                     |
|---------------|-------:|-----------------------------------------------|
| `clinic`      | 94.282 | Consulta ou exame em clínica credenciada      |
| `pharmacy`    | 14.707 | Compra de medicamentos (produto de farmácia)  |
| `other`       |  7.895 | Outras transações (telemedicina/diversos)     |
| `appointment` |    123 | Legado (migrado para `clinic`)                |

**Fluxo de consultas/exames (médico)**: `doctorappointments` ← liga via `orderId` → `orders` (orderType='clinic'). 89% dos 107.618 appointments têm `orderId`. Os 11.830 sem `orderId` são majoritariamente convênios ou cortesias.

**Fluxo de farmácia**: `orders` (orderType='pharmacy') → `order_products` → `products` / `store_products`. Comissões em `ordercommissions`, repasses em `transfers`.

**Janela temporal dos dados**:

| Tabela                 | Primeira data        | Última data          |
|------------------------|----------------------|----------------------|
| `customers`            | 2021-06-09           | 2026-04-14 (live)    |
| `orders`               | 2021-06-14           | 2026-04-14 (live)    |
| `orders.paymentDate`   | 2021-07-26           | 2026-04-14           |
| `doctorappointments`   | 2022-05-02           | 2026-05-28 (futuro)  |
| `invoices`             | 2022-09-27           | 2026-04-14           |
| `transfers`            | 2021-11-10           | 2026-04-14           |
| `evaluations`          | 2024-07-04           | 2026-04-14           |
| `externalprescriptions`| 2025-02-14           | 2026-04-14           |

Tabelas vazias notáveis: `plans`, `customer_plans`, `customer_plancharges` (assinatura não implementada); `transactions`, `invoiceitems`, `creditcards`, `storerepasses` (fluxos alternativos não usados).

---

## 2. ⭐ As 5 perguntas dos diretores

### Pergunta 1 — Clientes por período

**Base total**: 56.874 clientes (6.595 soft-deleted = 11,6%).

**Definições operacionais**:
- **Novo cliente**: `customers.created_at` no período.
- **Cliente ativo no mês**: existe ao menos 1 `orders` com `paymentDate` no mês (= comprou ou pagou consulta) OU 1 `doctorappointments` com `confirmationDate` ou `paymentDate` no mês.
- **Cliente recorrente**: comprou >= 2 vezes nos últimos 12 meses.

**Fórmulas SQL canônicas** ([02_q1_clients.json](scan_output/analysis/02_q1_clients.json)):

```sql
-- Novos por mês
SELECT DATE_FORMAT(created_at,'%Y-%m') mes, COUNT(*) novos
FROM customers GROUP BY mes;

-- Ativos por mês (orders)
SELECT DATE_FORMAT(paymentDate,'%Y-%m') mes, COUNT(DISTINCT customerId) ativos
FROM orders WHERE paymentDate IS NOT NULL GROUP BY mes;

-- Ativos por mês (consultas)
SELECT DATE_FORMAT(`date`,'%Y-%m') mes, COUNT(DISTINCT customerId) ativos
FROM doctorappointments
WHERE customerId IS NOT NULL
  AND (paymentDate IS NOT NULL OR confirmationDate IS NOT NULL)
GROUP BY mes;

-- Recorrentes nos últimos 12 meses (referencial)
SELECT DATE_FORMAT(o.paymentDate,'%Y-%m') mes, COUNT(DISTINCT o.customerId) recorrentes
FROM orders o
WHERE o.paymentDate IS NOT NULL
  AND EXISTS (SELECT 1 FROM orders o2
              WHERE o2.customerId = o.customerId
                AND o2.paymentDate BETWEEN DATE_SUB(o.paymentDate, INTERVAL 12 MONTH) AND o.paymentDate - INTERVAL 1 DAY)
GROUP BY mes;
```

**Últimos 12 meses (novos clientes)**: média ≈ 1.583/mês · pico 1.976 em 2025-10 · 2026-04 parcial 529.

---

### Pergunta 2 — Consultas/exames por período

**Total histórico**: 107.618 appointments — 21.703 pagos, 47.220 cancelados, resto em abertos/sem-mostrar.

**Classificação**:
- **Consulta**: `doctorappointments.specialtyId IS NOT NULL` (28.851 histórico)
- **Exame**: `specialtyId IS NULL` (inclui `bundleId` para exames agrupados — 75.788 + 2.979 bundles)

**Estados oficiais** em `doctorappointmentstatuses` (8 tags):
`missingConfirmation`, `missingInsuranceConfirmation`, `confirmed`, `denied`, `cancelled`, `attended`, `patientArrived`, `mustReschedule`. O histórico completo de mudanças está em `doctorappointmentstatuslogs`.

**Recomendação**: usar **`doctorappointments.date`** (data da consulta) para relatórios mensais, não `created_at` (data do agendamento). A diferença pode ser de vários meses em exames pré-agendados.

**SQL canônico**:

```sql
SELECT DATE_FORMAT(`date`,'%Y-%m') mes,
       COUNT(*) agendadas,
       COUNT(paymentDate) pagas,
       COUNT(confirmationDate) confirmadas,
       COUNT(canceledAt) canceladas,
       SUM(CASE WHEN specialtyId IS NOT NULL THEN 1 ELSE 0 END) consultas,
       SUM(CASE WHEN specialtyId IS NULL THEN 1 ELSE 0 END) exames_bundles
FROM doctorappointments
GROUP BY mes;
```

**Insight**: taxa de conversão (pagas/total) cresceu fortemente — passou de ~20% em meados de 2025 para ~75% em março/abril 2026. Sugere mudança de processo/política (pagamento antecipado obrigatório? cobrança automática?). **Conferir com os diretores**.

---

### Pergunta 3 — Particular vs Convênio

Esta é a pergunta mais delicada — o banco não tem um campo explícito "é convênio". A CAMIM usa **três sinais combinados**:

1. **`customer_insurances`** — cadastro de convênio do cliente (1.101 ativos · `isCurrent=1`)
2. **`orders.orderType`** + **`doctorappointments.total`** — convênios costumam ter `total=0` ou não ter `order` vinculado
3. Há 14 convênios em `insurances` (Bradesco, Amil, Unimed, SulAmérica, Cassi, Fiosaude, Porto, Prevent Senior, Assim, Petrobras, Golden Cross, Colaboradores OC, Égide Promocional, etc.)

**Regra recomendada** (validada):

```sql
-- "É convênio" se o cliente tinha convênio ATIVO na data da consulta
SELECT da.id AS appointment_id,
       CASE WHEN EXISTS(
         SELECT 1 FROM customer_insurances ci
         WHERE ci.customerId = da.customerId
           AND ci.isCurrent = 1
           AND ci.created_at <= da.`date`
           AND (ci.deleted_at IS NULL OR ci.deleted_at > da.`date`)
       ) THEN 'convenio' ELSE 'particular' END AS tipo
FROM doctorappointments da
WHERE da.canceledAt IS NULL;
```

**Resultado histórico** (consultas com convênio ativo): **1.861 consultas de 9 convênios** para **R$ 254.305 de arrecadação**. Principais:

| Convênio           | Consultas | Ticket médio |
|--------------------|----------:|-------------:|
| Colaboradores OC   | 1.124     | R$ 113,55    |
| Unimed             | 387       | R$ 65,26     |
| Égide Promocional  | 184       | R$ 63,03     |
| SulAmérica Saúde   | 97        | R$ 72,65     |
| Bradesco Saúde     | 44        | R$ 88,25     |
| Outros             | 25        | variados     |

**Particular** é o complemento: consultas cujo cliente não tinha convênio ativo na data OU cujo `orders.paymentDate` foi efetivado via gateway (pagou com cartão/PIX). A receita particular é **~R$ 5,9M contra R$ 254k de convênio** — convênio representa **~4%** da receita de saúde (e 0% da farmácia).

---

### Pergunta 4 — Arrecadação por contrato (particular)

**Definição**: "contrato" = venda direta, cliente paga via gateway próprio (Iugu ou Pagarme).

**Fontes**:
- `orders` com `paymentDate IS NOT NULL AND cancelDate IS NULL` → farmácia + clínica via marketplace
- `doctorappointments` com `paymentDate IS NOT NULL AND orderId IS NULL` → pagamentos diretos sem order (fluxo legado/manual)

**SQL unificado**:

```sql
SELECT mes, SUM(receita_reais) receita, SUM(qtde) qtde FROM (
  SELECT DATE_FORMAT(paymentDate,'%Y-%m') mes, COUNT(*) qtde, SUM(total)/100.0 receita_reais
  FROM orders WHERE paymentDate IS NOT NULL AND cancelDate IS NULL GROUP BY mes
  UNION ALL
  SELECT DATE_FORMAT(paymentDate,'%Y-%m'), COUNT(*), SUM(total)/100.0
  FROM doctorappointments
  WHERE paymentDate IS NOT NULL AND canceledAt IS NULL AND orderId IS NULL AND total > 0
  GROUP BY mes
) x GROUP BY mes;
```

**Últimos 12 meses (orders pagas)** — média ≈ R$ 170k/mês, pico R$ 210k em 2025-10.

---

### Pergunta 5 — Arrecadação por convênio

**Regra**: mesma da pergunta 3 + soma de `doctorappointments.total`.

```sql
SELECT DATE_FORMAT(da.`date`,'%Y-%m') mes, i.name convenio,
       COUNT(DISTINCT da.id) consultas,
       ROUND(SUM(IFNULL(da.total,0))/100.0, 2) receita_reais
FROM doctorappointments da
JOIN customer_insurances ci ON ci.customerId = da.customerId
     AND ci.isCurrent = 1
     AND ci.created_at <= da.`date`
     AND (ci.deleted_at IS NULL OR ci.deleted_at > da.`date`)
JOIN insurances i ON i.id = ci.insuranceId
WHERE da.canceledAt IS NULL
GROUP BY mes, i.name;
```

Ver [06_q5_revenue_insurance.json](scan_output/analysis/06_q5_revenue_insurance.json) para detalhe mensal.

---

## 3. 📊 Catálogo completo de KPIs por domínio

Todos os JSONs de origem estão em [egide/scan_output/analysis/](scan_output/analysis/).

### 3.1 Farmácia (`orders.orderType='pharmacy'`) — [10_pharmacy_kpis.json](scan_output/analysis/10_pharmacy_kpis.json)

| KPI | Definição / Fórmula |
|---|---|
| **Pedidos no mês** | `COUNT(*) FROM orders WHERE orderType='pharmacy' AND paymentDate IS NOT NULL` agrupado por mês |
| **Receita bruta** | `SUM(total)/100` nas mesmas condições |
| **Ticket médio** | receita / pedidos |
| **Clientes únicos** | `COUNT(DISTINCT customerId)` |
| **Lojas ativas com venda** | `COUNT(DISTINCT storeId)` |
| **Top lojas por receita** | agrupar por `storeId` |
| **Top produtos vendidos** | `SUM(order_products.total)` por `productId` |
| **% pedidos com delivery** | `deliverId IS NOT NULL` / total |
| **Valor médio de delivery** | `AVG(deliverValue)` em orders com delivery |
| **Catálogo ativo** | `COUNT(*) FROM products WHERE deleted_at IS NULL AND isActive=1` |
| **% produtos controlados** | `SUM(isControlled)/COUNT(*) FROM products` |
| **Estoque agregado** | `SUM(inventoryAmount) FROM store_products WHERE isActive=1` |
| **Preço médio catálogo** | `AVG(store_products.price)/100` |
| **Rotatividade de produto** | `vendas_mes / estoque_medio` por produto |
| **Lojas por categoria** | `categories` (Medicamentos, Higiene, Beleza, Conveniência, Suplementos, etc) |
| **Comissão gerada** | `SUM(ordercommissions.value) WHERE storeId IS NOT NULL` |
| **Repasses financeiros** | `transfers WHERE storeId IS NOT NULL` |

### 3.2 Clínicas / Médicos — [11_clinic_kpis.json](scan_output/analysis/11_clinic_kpis.json)

| KPI | Definição / Fórmula |
|---|---|
| **Clínicas ativas** | `COUNT(*) FROM clinics WHERE isActive=1 AND deleted_at IS NULL` (hoje: 320 cadastradas) |
| **Médicos ativos** | `COUNT(*) FROM doctors WHERE isActive=1 AND deleted_at IS NULL` (1.354 cadastrados) |
| **Especialidades ofertadas** | 85 (82 presenciais · 2 online) |
| **Vínculos médico↔clínica** | 1.714 em `clinic_doctors` |
| **Exames no catálogo** | `exams` (37.252 registros — catálogo TUSS completo) |
| **Consultas agendadas/mês** | `doctorappointments` com `date` no mês, `specialtyId NOT NULL` |
| **Exames agendados/mês** | `doctorappointments` com `specialtyId IS NULL` |
| **Taxa de confirmação** | `confirmationDate IS NOT NULL` / total |
| **Taxa de comparecimento** | appointments com status `attended` / confirmadas |
| **Taxa de cancelamento** | `canceledAt IS NOT NULL` / total |
| **Motivos de cancelamento** | `canceledBy` + `canceledReason` + `canceledAutomaticAnswerId` |
| **Top especialidades** | grupo por `specialtyId` |
| **Top médicos por consultas** | grupo por `doctors.id` via `schedules` |
| **Nota média do médico** | `AVG(evaluationDoctor)` em `doctorappointments` |
| **Nota média da clínica** | `AVG(evaluationClinic)` em `doctorappointments` |
| **Ocupação da agenda** | `appointments_no_mes / slots_disponiveis` (cruzar `schedules` × `examgroupschedules` × `workingtimes`) |
| **No-show** | agendado + cliente não apareceu (precisa inferir de `status` na log) |
| **Tempo médio entre agendamento e consulta** | `DATEDIFF(date, created_at)` |
| **Dependentes atendidos** | `customerdependentId NOT NULL` |
| **Exames oferecidos por clínica** | `clinic_exams` (6.240) |
| **Consultas em dependentes** | % appointments com `customerdependentId NOT NULL` |
| **Clínicas com telemedicina** | via `specialties.type='online'` |

### 3.3 Financeiro — [12_financial_kpis.json](scan_output/analysis/12_financial_kpis.json)

| KPI | Definição / Fórmula |
|---|---|
| **Receita total** | orders + doctorappointments sem order + invoices (ver pergunta 4) |
| **Receita por gateway** | cruzar `orders.chargeType` com `paymentgateways` |
| **Ticket médio global** | receita / pedidos pagos |
| **% pagos / % pendentes / % cancelados** | `orders.paymentDate/cancelDate` |
| **Comissões por origem** | `ordercommissions.origin` (egide, store, clinic, deliver) |
| **Taxa de refund** | `refundDate IS NOT NULL` / total pagos |
| **Valor reembolsado** | `SUM(total) WHERE refundDate IS NOT NULL` |
| **Transfers para lojas** | `transfers WHERE storeId IS NOT NULL` |
| **Transfers para clínicas** | `transfers WHERE clinicId IS NOT NULL` |
| **Transfers para delivers** | `transfers WHERE deliverId IS NOT NULL` |
| **Taxa retida pelo Égide** | `SUM(withdrawTaxValue)/SUM(value) FROM transfers` |
| **Split ratio** | `orders.splitData` (JSON com % por destinatário) |
| **Métodos de pagamento ativos** | `paymentmethods WHERE deactive_at IS NULL` (~14k) |
| **Formas de pagamento usadas** | `orders.chargeType` distinct (CREDIT, PIX, BOLETO, etc) |
| **Número de parcelas** | `configinstallments` + `orders.installments` (se existir) |
| **DRE mensal (simplificado)** | receita − comissões repassadas − taxas gateway |

### 3.4 Convênios — [04_q3_particular_vs_conv.json](scan_output/analysis/04_q3_particular_vs_conv.json)

| KPI | Definição / Fórmula |
|---|---|
| **Convênios ativos** | 14 em `insurances WHERE isActive=1` (Bradesco, Amil, Unimed, SulAmérica, Cassi, Fiosaude, Porto, Prevent Senior, Assim, Petrobras, Golden Cross, Colaboradores OC, Égide Promocional, funcionário CAMIM) |
| **Tipos de plano** | 84 em `insurancetypes` |
| **Carteirinhas cadastradas** | 1.101 em `customer_insurances WHERE isCurrent=1` |
| **Clientes com convênio** | `COUNT(DISTINCT customerId) FROM customer_insurances WHERE isCurrent=1` |
| **Consultas por convênio/mês** | ver pergunta 5 |
| **Ticket médio por convênio** | receita / consultas |
| **Taxa de denied** | `doctorappointmentstatuslogs` com tag `denied` / `missingInsuranceConfirmation` |
| **Especialidades cobertas por convênio** | `insurance_specialties` (9 vínculos hoje) |
| **Exames cobertos** | `insurance_exams` (vazia — não há cadastro) |
| **Tempo de autorização** | diferença entre `missingInsuranceConfirmation` → `confirmed` em `statuslogs` |
| **% consultas por convênio vs particular** | ver pergunta 3 |

### 3.5 Delivery — [13_delivery_kpis.json](scan_output/analysis/13_delivery_kpis.json)

| KPI | Definição / Fórmula |
|---|---|
| **Delivers cadastrados / ativos** | `delivers WHERE deleted_at IS NULL` |
| **Delivers aprovados** | `delivers.status='approved'` |
| **Pedidos com delivery/mês** | `orders WHERE deliverId IS NOT NULL AND paymentDate IS NOT NULL` |
| **Ganho médio por delivery** | `AVG(deliverValue)` em orders |
| **Top delivers por entregas** | grupo por `delivers.id` |
| **Avaliação média delivers** | `AVG(delivers.rateStars)` |
| **Status de entrega** | distribuição via `deliverystatuses` (19 status diferentes) |
| **Tempo médio de entrega** | `deliveredAt − paymentDate` (via `order_statuses` logs) |
| **Distância média** | `deliverdistances` |
| **Tipos de veículo** | `delivervehicles` |
| **Ocorrências em entregas** | `deliverissues` |

### 3.6 Avaliações — [14_evaluation_kpis.json](scan_output/analysis/14_evaluation_kpis.json)

| KPI | Definição / Fórmula |
|---|---|
| **Avaliações dadas/mês** | `evaluations` agrupadas por `created_at` |
| **Nota média geral** | `AVG(evaluation) FROM evaluations` |
| **Nota média por `who`** | tipo (doctor, clinic, store, deliver) |
| **Avaliações aprovadas/reprovadas** | `approved_at` vs `reproved_at` |
| **NPS médico (mensal)** | `AVG(evaluationDoctor)` em `doctorappointments` |
| **NPS clínica (mensal)** | `AVG(evaluationClinic)` em `doctorappointments` |
| **Top 20 médicos por nota** | `evaluations WHERE who='doctor'` com `n>=5` |
| **Taxa de resposta** | % consultas com avaliação preenchida |

### 3.7 Cupons & Campanhas — [15_coupon_kpis.json](scan_output/analysis/15_coupon_kpis.json)

| KPI | Definição / Fórmula |
|---|---|
| **Cupons cadastrados / ativos** | `coupons WHERE deleted_at IS NULL` (650) |
| **Utilização total** | `SUM(amountUsed) FROM coupons` |
| **% de uso por cupom** | `amountUsed/amount` |
| **Desconto acumulado** | cruzar com `coupon_customers.orderId` → `orders.discount`... |
| **Cupons % vs R$ fixo** | `isPercent=1` |
| **Cupons por categoria** | `allowDrugstores`, `allowAllClinics`, `allowAllExams`, `allowAllSpecialties` |
| **Primeira compra** | `onlyFirstAppointment` flag |
| **Utilização mensal** | `coupon_customers` (5.724 usos) |
| **Cupons por cliente** | `coupon_customers` GROUP BY `customerId` |

### 3.8 Prontuário eletrônico — [16_medical_kpis.json](scan_output/analysis/16_medical_kpis.json)

| KPI | Definição / Fórmula |
|---|---|
| **Prontuários criados/mês** | `medical_records` GROUP BY `created_at` (1.323 histórico) |
| **% prontuários finalizados** | `finishedAt IS NOT NULL` |
| **Top CIDs diagnosticados** | `medical_record_cids` GROUP BY `code` |
| **Prescrições internas** | `medical_record_prescriptions` (26 — feature pouco usada) |
| **Prescrições externas** | `externalprescriptions` (11.131 — principal canal) |
| **Prescrições por mês** | `externalprescriptions` GROUP BY `created_at` |
| **Prescrições com PDF assinado** | `signed_pdf_url IS NOT NULL` |
| **Imagens/laudos registrados** | `medical_record_images`, `medical_record_documents` |
| **Pedidos de exames em receita** | `medical_record_prescription_exams` |

### 3.9 Cohort & Retenção — [17_cohort_retention.json](scan_output/analysis/17_cohort_retention.json)

| KPI | Definição / Fórmula |
|---|---|
| **Cohort por mês de 1ª compra** | `firstp` = mês da primeira compra, expandir ao longo do tempo |
| **Retenção M+1, M+3, M+6, M+12** | % clientes ativos nos meses seguintes ao cohort |
| **LTV (gasto total)** | `customers.totalSpent` (pré-agregado) |
| **LTV médio** | `AVG(totalSpent)` |
| **Distribuição de LTV** | buckets (zero, <50, 50-200, 200-500, 500-1k, 1k-5k, 5k+) |
| **Top 20 clientes por LTV** | ORDER BY `totalSpent` DESC |
| **Frequência de compra** | `qtdeSpent` (pré-agregado) |

### 3.10 Fidelização / recompra — [19_loyalty.json](scan_output/analysis/19_loyalty.json)

| KPI | Definição / Fórmula |
|---|---|
| **% clientes com ≥2 compras** | `orders` agrupado por customerId com count≥2 |
| **Distribuição de compras/cliente** | buckets (1, 2, 3-5, 6-10, 11+) |
| **Dias entre compras (média)** | `DATEDIFF(paymentDate, LAG(paymentDate))` |
| **Tempo até 2ª compra** | buckets (1sem, 1m, 3m, 6m, 1ano, >1ano) |
| **Churn mensal** | clientes ativos no mês anterior que não apareceram |
| **Clientes recuperados** | voltaram após 90+ dias inativos |

### 3.11 Geografia — [18_geography.json](scan_output/analysis/18_geography.json)

| KPI | Definição / Fórmula |
|---|---|
| **Clientes por estado** | `customeraddresses WHERE current=1` |
| **Clientes por cidade** | idem |
| **Clínicas por estado** | `clinics.state` |
| **Lojas por estado** | `stores.state` |
| **Cobertura geográfica** | UFs/cidades distintas com ≥1 clínica ativa |
| **Raio de atendimento médio** | `clinics.radiusMin/Medium/Max`, `stores.maxRadius` |
| **Heatmap de pedidos** | `customeraddresses.latitude/longitude` |

### 3.12 Integrações & monitoramento

| KPI | Definição / Fórmula |
|---|---|
| **Webhooks recebidos/dia** | `webhooks` (57k — a partir de 2025-02) |
| **% webhooks processados** | `processedAt IS NOT NULL` |
| **Webhooks por gateway** | `gateway` (iugu, pagarme) |
| **Events em atraso** | `processedAt IS NULL AND created_at < NOW() - INTERVAL 1 HOUR` |
| **Logs de integração** | `companyintegrationlogs` (25 registros — baixo uso hoje) |
| **Push notifications enviadas** | `pushnotifications` (47.398) |
| **Notificações automáticas** | `automaticnotifications` |
| **Respostas automáticas** | `automatic_answers` (WhatsApp/chatbot?) |

### 3.13 CRM & Parceiros

| KPI | Definição / Fórmula |
|---|---|
| **Leads no CRM** | `crms` + `crmchats` |
| **Parceiros** | `partners`, `partnerpeople` (895.380 pessoas — possível CRM massivo) |
| **Parceiros por clínica** | `partner_clinics` |
| **Clientes de parceiros** | `customer_partners` (28.045) |
| **Invitations** | `invitations` |

---

## 4. Pontos que precisam de confirmação com os diretores

1. **Taxa de conversão**: saltou de 20% → 75% em um ano. O que mudou? Cobrança antecipada?
2. **`orders.orderType='other'` (7.895 orders)**: o que é? Telemedicina avulsa? Kits?
3. **Convênio via `orders`**: 771 orders clínicas de clientes com convênio ativo foram pagas (R$ 54k). São co-pagamentos ou particular de cliente que ALEATORIAMENTE tem convênio mas não usou?
4. **Plans vazios**: não há plano de assinatura em operação? `plans`, `customer_plans`, `customer_plancharges` todas com 0 linhas.
5. **Appointments 2026-05-28**: há consultas agendadas no futuro — qual o horizonte máximo de agendamento?
6. **`product_storeapprovations` (175M linhas, 13 GB)**: é só log de auditoria de aprovação de preço? Vale analisar?
7. **Evaluation só a partir de 2024-07**: antes não havia sistema de avaliação?
8. **`externalprescriptions` só a partir de 2025-02**: quando foi lançada a prescrição eletrônica externa?

---

## 5. Recomendação de entregáveis (fases)

### Fase 1 — MVP Directorial (5 perguntas)
1. `export_egide.py` → gera `json_consolidado/egide_kpi.json` com as 5 perguntas (séries mensais 2023-01 até hoje).
2. `kpi_egide.html` com 5 cards de destaque + gráficos mensais:
   - Card 1: base total / novos no mês / ativos no mês
   - Card 2: consultas + exames separados (pagos)
   - Card 3: pizza particular × convênio (receita + volume)
   - Card 4: série de receita particular
   - Card 5: série de receita convênio empilhada por nome
3. Filtros obrigatórios: **multiseleção de convênios**, **intervalo de datas** ([feedback_filtros_padrao.md](../../.claude/projects/-home-cristiano--rea-de-Trabalho-github-projetos-relatorio-h-t/memory/feedback_filtros_padrao.md)).

### Fase 2 — Farmácia detalhada
Cards: top lojas · top produtos · comissão · delivery performance · estoque crítico.

### Fase 3 — Clínicas detalhado
Cards: top médicos · top especialidades · ocupação · taxa de confirmação · NPS · motivos de cancelamento.

### Fase 4 — Financeiro consolidado
DRE simplificada · receita × comissão × repasse · split gateway · refund rate.

### Fase 5 — Cohort & Fidelização
Retenção M+N · LTV · segmentação RFM (recência, frequência, valor) · churn mensal.

### Fase 6 — Operacional
Webhooks saúde · notificações · prescrições · avaliações pendentes.

---

## 6. Padrão de implementação recomendado

Seguindo a arquitetura dos demais KPIs do `relatorio_h_t`:

```
egide/
  scan_egide.py              (já existe)    — varredura de schema
  analyze_egide.py           (já existe)    — análise profunda (roda hoje)
  scan_output/               (já existe)    — JSONs da varredura
  scan_output/analysis/      (já existe)    — JSONs das análises + KPI_ANALYSIS_REPORT.md
  KPI_CATALOG.md             (este arquivo)

relatorio_h_t/
  export_egide_q1_clientes.py    (a criar)  — gera JSON diário
  export_egide_q2_consultas.py   (a criar)
  export_egide_q3_split.py       (a criar)
  export_egide_q4_receita_part.py (a criar)
  export_egide_q5_receita_conv.py (a criar)
  kpi_egide.html             (a criar)      — dashboard consolidado

json_consolidado/
  egide_kpi.json             (a gerar)      — dados do dashboard

app.py                                       — registrar em _TEMPLATE_TO_PAGINA: "kpi_egide.html": "kpi_egide"
auth_routes.py                               — registrar em PAGINAS_DISPONIVEIS: {"key":"kpi_egide","label":"KPI Égide"}
```

Deploy: GitHub Actions → `/opt/relatorio_h_t/` + `/opt/camim-auth/templates/` + `/var/www/` (conforme [CLAUDE.md](../CLAUDE.md)).
