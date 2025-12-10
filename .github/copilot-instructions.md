# AI Coding Agent Instructions for relatorio_h_t

## Project Overview

**relatorio_h_t** is a **multi-source reporting & KPI aggregation system** integrating Harvest (time tracking), Trello (task management), and SQL databases (Liberty health plan data, financial data).

### Architecture: Data → Extract → Consolidate → LLM Enrich → HTML Reports

1. **Data Sources**: SQL databases (11 postos/branches), Harvest API, Trello API
2. **ETL Pipeline**: Extract to CSV → SQL queries → JSON consolidation (with KPI calculations)
3. **LLM Layer**: Groq API for text summarization, analysis, formatting
4. **Output**: Interactive HTML dashboards with AdminLTE + Chart.js

---

## Critical Modules & Data Flow

### 1. **Core ETL Exports** (Data→CSV→JSON)
- **`export_governanca.py`** (1301 lines): Main ETL orchestrator
  - Executes SQL queries in `/sql/` directory for each **posto** (A, N, X, Y, B, R, P, C, D, G, I, M, J)
  - Saves CSV to `/dados/` and JSON to `/json_consolidado/`
  - **Key function**: `sanitize_nan()` — converts numpy.nan/NaT to None for JSON serialization
  - **Pattern**: Matches CSV columns to categories (mensalidade, medico, alimentacao, prescricao, liberty) via regex in `KEY_PATTERNS` dict
  - **KPI Calculation**: Adds geometric mean, mix ratios, performance metrics to JSON footer
  - Uses `SQLAlchemy>=2,<3` with `pyodbc` — connection string built from env: `ODBC Driver 17 for SQL Server`

- **`export_harvest.py`**: Fetches last 7 days of Harvest time entries via REST API → CSV in `/export_harvest/`
  - Pagination support (2000 rows/page), uses Bearer token from `HARVEST_TOKEN` env
  - Sets file mtime to reflect actual harvest date (not import date)

- **`export_trello.py`**: Exports Trello board cards to CSV in timestamped dir `/export_trello/export_YYYYMMDD_HHMMSS/`
  - Requires `TRELLO_KEY` and `TRELLO_TOKEN` env vars
  - Multiple CSV outputs: cards, lists, labels, members

### 2. **HTML Report Generation**
- **`build_relatorio_html.py`** (460 lines): Merges Harvest + Trello CSVs into interactive HTML
  - Finds **latest Trello export dir** (by mtime) containing `cards.csv`
  - Finds **latest Harvest CSV** by file modification time
  - Generates `/relatorio/relatorio_YYYYMMDD_HHMMSS.html` + `trello_harvest.html` (always overwritten)
  - Uses embedded CSS/JS (no external dependencies for deployed version)
  - **Table rendering**: `df_to_html_table()` with client-side filtering via JavaScript

### 3. **LLM Integration** (Centralized)
- **`llm_client.py`**: **Single point of contact for Groq API**
  - `LLMConfig`: Temperature=0.1, max_tokens=4096, model from `GROQ_MODEL` env
  - `gerar_texto()`: Accepts prompt + optional system_prompt, returns cleaned string
  - **Always use this client** instead of calling Groq directly — ensures consistency across project

- **`summarizer_universal.py`**: Reusable text summarization (PT-BR, 3 sentences max)
  - `resumir_texto_conciso()`: Takes texto + optional instrucoes_extras

- **`formatter_html_universal.py`**: Generates formatted HTML sections for dashboard
  - Input: produto (e.g., "mensalidades"), texto_completo, metadata
  - Output: Bootstrap/AdminLTE-compliant HTML sections (no `<html>`, `<body>`)
  - Uses FontAwesome icons (`<i class="fas fa-*"></i>`)

- **`sanitizer_texto_universal.py`**: Cleans LLM output before display/reuse
  - Removes markdown code blocks, ASCII tables, heading symbols
  - Normalizes whitespace and line breaks
  - **Always sanitize before HTML injection or piping to next LLM call**

### 4. **Multi-Agent Orchestration** (Portuguese-first, pipeline pattern)
- **`orquestrador.py`**: Multi-agent workflow for deep analysis
  - Flow: PT-BR input → Translate to EN → Multiple specialist agents → Writer consolidates → Translate back to PT-BR → Format
  - Agents load prompts from `/prompt/` directory
  - `class Agente`: Base agent with generic `executar()` method
  - `class AgenteWriter`: Consolidates multi-agent outputs
  - `class FormatterFinal`: Wraps response with "ANÁLISE EXECUTIVA" header/footer
  - **Pattern**: Always respond in agent's designated language, let orchestrator handle translation

- **`ia_router.py`**: Flask Blueprint for `/ia/analisar` endpoint
  - Receives JSON payload from frontend, calls `processar_requisicao_ia()`
  - Returns standardized JSON: `{ "content_mode": "json", "data": { "html": "...", "resumo_curto": "...", "full_text": "..." } }`

### 5. **Authentication & Session Management**
- **`app.py`** (Flask): HTTP-based auth layer
  - Uses htpasswd file at `/etc/nginx/.htpasswd`
  - ACL mapping in `/etc/nginx/postos_acl.json` (email → allowed postos list)
  - TimestampSigner-based session tokens (8h TTL)
  - Endpoint `/auth` used by Nginx for subrequest auth

- **`admin_htpasswd.py`**: User management for htpasswd file
  - FastAPI router for CRUD operations on users
  - Password hashing via `passlib.apache.HtpasswdFile`

- **`api_crud.py`**: FastAPI microservice for user admin
  - Stores users in JSON file at `/var/lib/users_api/users.json`
  - Thread-safe reads/writes with atomic tmp file swaps

---

## Key Patterns & Conventions

### Data Flow Convention
1. **Extract → Normalize**: CSV with `pd.read_csv()` (try utf-8, fallback latin-1)
2. **Sanitize NaNs**: Call `sanitize_nan()` recursively before JSON dumps
3. **Consolidate**: Load all posto-specific JSONs, merge into global JSON in `/json_consolidado/`
4. **Enrich**: Add KPIs, averages, performance metrics
5. **Serve**: JSON exposed via `/public/` for frontend consumption

### Environment Variables Required
```bash
# Database
DATABASE_URL=mssql+pyodbc://...  # or build from ODBC params
ODBC_DRIVER="ODBC Driver 17 for SQL Server"

# Harvest
HARVEST_TOKEN=<bearer_token>
HARVEST_ACCOUNT_ID=<account_id>
USER_AGENT=CamimReports/1.0

# Trello
TRELLO_KEY=<key>
TRELLO_TOKEN=<token>

# LLM
GROQ_API_KEY=<api_key>
GROQ_MODEL=openai/gpt-oss-120b  # or alternate model

# Auth
SESSION_SECRET=<32-byte-hex>  # generated if not set
HTPASS_PATH=/etc/nginx/.htpasswd
ACL_PATH=/etc/nginx/postos_acl.json
```

### Column Matching for CSV Data
- Uses regex matching in `KEY_PATTERNS` dict in `export_governanca.py`
- Category detection (mensalidade, medico, alimentacao, prescricao, liberty)
- **Priority columns** in `PREFER_NOMES` dict — looks for "ValorPago" before "valor"
- Pattern: `r"(mensal|mensalid|receita|fin_?receita)"` for mensalidade

### File Naming Conventions
- **Exports**: `<type>_YYYYMMDD_HHMMSS.<ext>` for timestamped uniqueness (never overwrites)
- **Reports**: `/relatorio/relatorio_YYYYMMDD_HHMMSS.html` + `trello_harvest.html` (always overwrites)
- **CSV data**: `/dados/A_2020-01_alimentacao.csv` (posto_YYYY-MM_category.csv)
- **JSON**: `/json_consolidado/A_2020-01.json` per posto+month

### Frontend Integration Points
- REST endpoint: `POST /ia/analisar` with payload `{ query, page_ctx }`
- Response format: `{ content_mode: "json"|"free_text", data: {...} }`
- Chat interface: Frontend likely uses `chat.js` with button clicks on KPI pages

---

## Development Workflows

### Adding a New Data Source
1. Create `export_<source>.py` following `export_harvest.py` pattern
2. Use dotenv for credentials
3. Save to dated subdirectory in `/export_<source>/` or CSV in appropriate folder
4. Update `build_relatorio_html.py` to incorporate if HTML aggregation needed

### Adding a New LLM Agent
1. Create prompt file in `/prompt/agente_<name>.txt`
2. Instantiate `Agente(nome="...", prompt_path="prompt/agente_<name>.txt", llm_call=llm_call_fn)`
3. Add to agent dict in `orquestrador.py`
4. Ensure response is in agent's designated language (orchestrator handles translation)

### Debugging SQL Issues
1. Check `/sql/` directory for query templates
2. Build connection string from env or use `build_conns_from_env()` utility
3. Common patterns: `SELECT ... WHERE data_inicio >= ? AND data_fim < ?` with parametrized dates
4. Use `pd.read_sql()` to test queries directly

### Testing LLM Calls Locally
- Set `GROQ_API_KEY` and `GROQ_MODEL` env vars
- Use `llm_client.LLMClient().gerar_texto(prompt)` directly
- Check `analyze_groq.py` for example usage patterns

---

## Common Pitfalls to Avoid

1. **NaN Serialization**: JSON doesn't support `nan`, `NaT`, or `inf`. Always call `sanitize_nan()` before `json.dumps()`
2. **Encoding Issues**: CSV files may be latin-1 (legacy). Use try/except pattern: utf-8 first, fallback latin-1
3. **File Mtimes**: Use `_set_mtime()` to preserve original data timestamps rather than import times
4. **Groq Model Name**: Uses `openai/gpt-oss-120b` format, not standard OpenAI names
5. **Posto Iteration**: Always iterate `POSTOS = list("ANXYBRPCDGIMJ")` (13 branches) for completeness
6. **HTML Injection**: Never directly inject unsanitized LLM output into HTML. Call `sanitizar_texto_bruto()` first
7. **DataFrame Column Selection**: Use `PREFER_NOMES` prioritization, don't assume column order

---

## File Structure Quick Reference

```
/sql/                    # SQL query templates (11 .sql files for different data types)
/dados/                  # CSV exports from SQL (organized by posto_YYYY-MM_category)
/json_consolidado/       # Consolidated JSON per posto+month with KPI footer
/export_harvest/         # Timestamped Harvest CSV exports
/export_trello/          # Timestamped Trello board exports (each dir has cards.csv, lists.csv, etc.)
/relatorio/              # Final timestamped HTML reports
/css/, /js/, /fonts/     # Frontend assets
/templates/              # Placeholder (actual templates in `/opt/camim-auth/templates` on production)
/prompt/                 # LLM agent prompt files (agente_*.txt)
```

---

## Questions for Feedback

1. **SQL Query Generation**: Should new SQL files be added to `/sql/` or embedded in Python?
2. **JSON Schema Version**: Is the consolidation JSON format documented elsewhere? Any breaking changes expected?
3. **Frontend Frameworks**: Is AdminLTE still the primary dashboard framework, or moving to something else?
4. **LLM Model Versioning**: How are model upgrades (e.g., Groq releasing new models) handled — env var only?
5. **Deployment**: Is Nginx-based auth still the auth layer, or migrating to separate identity provider?

