# Manus Integration Guide — KPI Registry & Discovery System

**Document Version:** 1.0
**Last Updated:** 2026-03-25
**Status:** Production Ready

## Overview

The CAMIM KPI system provides a production-grade integration interface for external AI systems (like Manus) to discover, query, and analyze all available KPIs. This document describes the architecture, API endpoints, and integration patterns.

---

## Core Concepts

### What is a KPI?

Each KPI in CAMIM is a complete analytical dashboard with:
- **Metadata:** Title, description, category, keywords
- **Data:** JSON files with historical and current metrics
- **Filters:** Customizable parameters (date ranges, postos, groups)
- **Metrics:** Specific business indicators (retention %, revenue, count, etc.)

### KPI Discovery Model

Instead of hardcoding KPI URLs, external systems discover KPIs at runtime by:
1. Querying the KPI manifest for available KPIs
2. Reading metadata for each KPI (filters, metrics, examples)
3. Constructing appropriate URLs with query parameters
4. Accessing data via the KPI page or JSON APIs

This decouples external systems from internal changes to URLs, data structure, or naming.

---

## API Endpoints

All endpoints return JSON. No authentication required for discovery endpoints (public catalog).

### 1. Get Complete KPI Manifest

**Endpoint:** `GET /api/kpis/manifest`

**Purpose:** Retrieve the complete catalog of all available KPIs with full metadata.

**Response Example:**

```json
{
  "version": "1.0.0",
  "last_updated": "2026-03-25",
  "base_url": "https://teste-ia.camim.com.br",
  "kpis": [
    {
      "id": "fidelizacao",
      "title": "Fidelização de Clientes",
      "description": "Visualize a retenção de clientes por período de admissão...",
      "url": "/kpi_fidelizacao_cliente",
      "route": "kpi_fidelizacao_cliente.html",
      "category": "Clientes",
      "icon": "fa-handshake",
      "keywords": ["fidelizacao", "retencao", "clientes", "admissao"],
      "filters": {
        "month": {
          "type": "month",
          "required": true,
          "format": "YYYY-MM",
          "default_offset": -13
        },
        "postos": {
          "type": "multi-select",
          "options": ["A", "B", "C", ...],
          "groups": {
            "altamiro": ["A", "B", "G", ...],
            "couto": ["C", "D", "J", ...]
          }
        }
      },
      "metrics": [
        {
          "name": "total_admissoes",
          "label": "Total de Admissões",
          "type": "integer"
        },
        {
          "name": "retencao_1m",
          "label": "Retenção 1 mês",
          "type": "percentage"
        }
      ],
      "api_endpoints": {
        "data": "/json_consolidado/fidelizacao_cliente.json",
        "metadata": "/api/kpis/metadata/fidelizacao"
      },
      "examples": {
        "basic": "/kpi_fidelizacao_cliente?month=2025-02",
        "with_postos": "/kpi_fidelizacao_cliente?month=2025-02&postos=A,B,C",
        "group": "/kpi_fidelizacao_cliente?month=2025-02&postos=altamiro"
      },
      "manus_prompt_template": "Mostre a fidelização de clientes para {month}..."
    },
    // ... more KPIs
  ]
}
```

---

### 2. Get Individual KPI Metadata

**Endpoint:** `GET /api/kpis/metadata/<kpi_id>`

**Purpose:** Retrieve detailed metadata for a single KPI.

**Example Request:**

```
GET /api/kpis/metadata/vendas
```

**Response:**

```json
{
  "ok": true,
  "kpi": {
    "id": "vendas",
    "title": "KPI Vendas",
    "description": "Dashboard de vendas com análise...",
    // ... full metadata as in manifest
  }
}
```

**Error Response (KPI not found):**

```json
{
  "ok": false,
  "error": "KPI 'invalid_id' não encontrado"
}
```

---

### 3. Get KPIs by Category

**Endpoint:** `GET /api/kpis/category/<category>`

**Purpose:** Retrieve all KPIs in a specific category.

**Available Categories:**
- `Clientes` — Customer-related KPIs
- `Financeiro` — Financial/revenue KPIs
- `Operacional` — Operational metrics
- `Dashboard` — Informational dashboards
- `Econômico` — Economic indicators

**Example Request:**

```
GET /api/kpis/category/Financeiro
```

**Response:**

```json
{
  "ok": true,
  "category": "Financeiro",
  "kpis": [
    { "id": "vendas", ... },
    { "id": "mensalidades", ... },
    { "id": "medicos", ... }
  ]
}
```

---

### 4. Search KPIs by Keywords

**Endpoint:** `GET /api/kpis/search?q=<search_term>`

**Purpose:** Search KPIs by title, description, or keywords.

**Example Request:**

```
GET /api/kpis/search?q=retenção
GET /api/kpis/search?q=fatura
```

**Response:**

```json
{
  "ok": true,
  "query": "retenção",
  "results": [
    {
      "id": "fidelizacao",
      "title": "Fidelização de Clientes",
      // ... matching KPI metadata
    }
  ]
}
```

---

## How to Integrate (Manus Example)

### Step 1: Discover Available KPIs

```python
import requests

# Get the KPI manifest
manifest = requests.get("https://teste-ia.camim.com.br/api/kpis/manifest").json()

# List all available KPIs
for kpi in manifest["kpis"]:
    print(f"{kpi['id']}: {kpi['title']}")
    print(f"  Category: {kpi['category']}")
    print(f"  Description: {kpi['description']}")
```

### Step 2: Understand KPI Filters & Metrics

```python
# Get metadata for fidelizacao KPI
fidelizacao = requests.get(
    "https://teste-ia.camim.com.br/api/kpis/metadata/fidelizacao"
).json()["kpi"]

# Inspect filters
filters = fidelizacao["filters"]
print(f"Required filters: {[f for f, v in filters.items() if v.get('required')]}")

# Inspect metrics
metrics = fidelizacao["metrics"]
print(f"Available metrics: {[m['name'] for m in metrics]}")

# Inspect examples
examples = fidelizacao["examples"]
print(f"Example query: {examples['with_postos']}")
```

### Step 3: Construct Appropriate Query

```python
# Build query using template
template = fidelizacao["manus_prompt_template"]
query = template.format(
    month="2025-02",
    postos_desc="postos A, B e C"
)

# Generate URL using examples
url = fidelizacao["examples"]["with_postos"]
# Result: /kpi_fidelizacao_cliente?month=2025-02&postos=A,B,C

# Or use data API directly
data_url = fidelizacao["api_endpoints"]["data"]
# Result: /json_consolidado/fidelizacao_cliente.json
```

### Step 4: Natural Language Processing

When user requests "Show me customer retention for February 2025 in Altamiro group":

1. Parse user intent → KPI type: `fidelizacao`
2. Extract parameters → month: `2025-02`, postos: `altamiro`
3. Query metadata → get groups definition
4. Replace variables in Manus prompt template
5. Execute LLM analysis with data
6. Render visualization or summary

---

## KPI Metadata Structure

### Top-Level Fields

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Unique identifier (used in URLs) |
| `title` | string | Human-readable name |
| `description` | string | Detailed explanation |
| `url` | string | Primary URL path |
| `route` | string | HTML template filename |
| `category` | string | Grouping category |
| `icon` | string | FontAwesome icon class |
| `keywords` | array | Search keywords |

### Filters Object

Each filter defines how users can customize the KPI:

```json
"filters": {
  "month": {
    "type": "month|range|multi-select|...",
    "required": true|false,
    "description": "...",
    "format": "YYYY-MM (for months)",
    "default_offset": -13,  // Optional: default 13 months back
    "options": ["A", "B", ...],  // For multi-select
    "groups": {  // Optional: named groups
      "altamiro": ["A", "B", ...],
      "couto": ["C", "D", ...]
    }
  }
}
```

### Metrics Array

Each metric describes a specific business indicator:

```json
"metrics": [
  {
    "name": "total_admissoes",
    "label": "Total de Admissões",
    "type": "integer|percentage|currency|...",
    "description": "Detailed explanation"
  }
]
```

### API Endpoints

```json
"api_endpoints": {
  "data": "/json_consolidado/fidelizacao_cliente.json",
  "metadata": "/api/kpis/metadata/fidelizacao"
}
```

---

## Query Examples

### Example 1: Fidelização (Single Month, Multi-Posto)

**User Request:** "Show me retention for March 2025 in Altamiro and Couto groups"

**Processing:**
```
KPI ID: fidelizacao
Month: 2025-03
Postos: altamiro,couto (expands to A,B,G,I,N,R,X,Y,C,D,J,M,P)

URL: /kpi_fidelizacao_cliente?month=2025-03&postos=altamiro,couto
```

### Example 2: Vendas (Date Range)

**User Request:** "Compare sales from January to March 2025 in posto A"

**Processing:**
```
KPI ID: vendas
Month From: 2025-01
Month To: 2025-03
Postos: A

URL: /kpi_vendas?month_from=2025-01&month_to=2025-03&postos=A
```

### Example 3: Search & Recommend

**User Request:** "What KPIs show customer metrics?"

**Processing:**
```
1. Search: /api/kpis/search?q=customer
   Returns: fidelizacao, clientes, consulttas, ...

2. Filter by category: /api/kpis/category/Clientes
   Returns: fidelizacao, clientes

3. Manus recommends: "I found 2 customer KPIs:
   - Fidelização: Shows retention by admission period
   - Clientes: Shows overall customer base growth"
```

---

## HTML Metadata Tags (Semantic Discovery)

Each KPI page includes semantic metadata tags in the `<head>` for machine-readable discovery:

```html
<meta name="kpi-id" content="fidelizacao" />
<meta name="kpi-title" content="Fidelização de Clientes" />
<meta name="kpi-description" content="Visualize a retenção de clientes..." />
<meta name="kpi-category" content="Clientes" />
<meta name="kpi-keywords" content="fidelizacao, retencao, clientes, admissao" />
<meta name="kpi-api-manifest" content="/api/kpis/manifest" />
<meta name="kpi-api-metadata" content="/api/kpis/metadata/fidelizacao" />
```

These tags enable:
- Web scraping to discover KPIs from HTML
- OpenGraph-style metadata for rich previews
- Fallback discovery if API is unavailable

---

## Current KPIs Catalog

### Fully Documented (Complete Metadata)

| ID | Title | Category | Filters | Metrics |
|---|---|---|---|---|
| `fidelizacao` | Fidelização de Clientes | Clientes | month, postos | 5 (1m/3m/6m/12m retention) |
| `vendas` | KPI Vendas | Financeiro | month_range, postos | quantity, revenue, ticket |
| `mensalidades` | KPI Mensalidades | Financeiro | month_range, postos | quantity, revenue, ticket |
| `clientes` | KPI Clientes | Clientes | postos | total, novos, growth% |
| `prescricoes` | KPI Prescrições | Operacional | postos | total, vencidas, risco |
| `consultas` | KPI Consultas | Operacional | postos | agendadas, realizadas, canceladas |

### Partially Documented (Basic Metadata)

| ID | Title | Category | Status |
|---|---|---|---|
| `alimentacao` | KPI Alimentação | Operacional | Basic metadata only |
| `governo` | Indicadores Macro | Econômico | Basic metadata only |
| `liberty` | CAMIM LIBERTY | Operacional | Basic metadata only |
| `medicos` | KPI Médicos | Financeiro | Basic metadata only |
| `vagas` | KPI Vagas | Operacional | Basic metadata only |
| `home` | KPI Home | Dashboard | Basic metadata only |

---

## Error Handling

### Common Error Scenarios

**Invalid KPI ID:**
```json
{
  "ok": false,
  "error": "KPI 'xyz' não encontrado"
}
// HTTP 404
```

**Missing Required Parameter:**
```json
{
  "ok": false,
  "error": "Parâmetro 'q' é obrigatório"
}
// HTTP 400
```

**Empty Search Results:**
```json
{
  "ok": true,
  "query": "nonexistent",
  "results": []
}
```

---

## Best Practices for Integration

### 1. Cache the Manifest

The manifest changes infrequently (only when new KPIs are added). Cache it locally:

```python
import json
from datetime import datetime, timedelta

cache_file = "kpi_manifest_cache.json"
cache_ttl = timedelta(hours=24)

# Load from cache if fresh
if os.path.exists(cache_file):
    stat = os.stat(cache_file)
    age = datetime.now() - datetime.fromtimestamp(stat.st_mtime)
    if age < cache_ttl:
        with open(cache_file) as f:
            manifest = json.load(f)
        goto fetch_data  # Skip API call

# Fetch fresh manifest
manifest = requests.get("https://teste-ia.camim.com.br/api/kpis/manifest").json()

# Cache it
with open(cache_file, "w") as f:
    json.dump(manifest, f)
```

### 2. Validate User Input

Always validate filter values against the manifest:

```python
# User requested postos: ["A", "X", "INVALID"]
manifest = get_manifest()
kpi = next(k for k in manifest["kpis"] if k["id"] == "fidelizacao")
valid_postos = set(kpi["filters"]["postos"]["options"])

user_postos = ["A", "X", "INVALID"]
valid = [p for p in user_postos if p in valid_postos]
invalid = [p for p in user_postos if p not in valid_postos]

if invalid:
    return f"Postos inválidos: {invalid}. Use: {valid_postos}"
```

### 3. Use Examples from Manifest

Each KPI includes example URLs. Use them as templates:

```python
kpi = manifest["kpis"][0]
example_url = kpi["examples"]["with_postos"]
# Result: "/kpi_fidelizacao_cliente?month=2025-02&postos=A,B,C"

# Substitute your parameters
url = example_url.replace("2025-02", target_month).replace("A,B,C", target_postos)
```

### 4. Handle Manus Prompt Templates

Use the template to generate consistent prompts:

```python
template = kpi["manus_prompt_template"]
# "Mostre a fidelização de clientes para {month}..."

prompt = template.format(
    month=user_month,
    postos_desc=describe_postos(user_postos)
)
```

---

## Future Enhancements

**Planned for Next Versions:**

1. **Real-time Data Endpoints** — `/api/kpis/{id}/data` for JSON responses
2. **Authentication** — Optional auth for sensitive/private KPIs
3. **Webhook Subscriptions** — Notify external systems when KPI data updates
4. **Custom Metrics** — Allow defining new metrics per user/role
5. **KPI Relationships** — Link related KPIs for comprehensive analysis

---

## Support & Questions

For integration questions or issues:

1. Check this documentation first
2. Review endpoint examples above
3. Inspect the manifest structure for your specific KPI
4. Verify filter values and data types

**API Status:** All endpoints tested and production-ready.

---

## Changelog

### Version 1.0 (2026-03-25)

- Initial release
- 6 fully documented KPIs
- 6 partially documented KPIs
- 4 discovery/search endpoints
- Semantic HTML metadata tags
- This integration guide
