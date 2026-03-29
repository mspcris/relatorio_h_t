# CLAUDE.md — Guia de Contexto do Projeto relatorio_h_t

> Lido automaticamente pelo Claude Code em cada sessão.
> Mantido atualizado conforme o projeto evolui.

---

## Visão geral

Sistema de dashboards KPI da CAMIM (rede de clínicas médicas).
Dados vêm de um banco SQL Server via pyodbc, são transformados em JSON por scripts Python e consumidos por páginas HTML/JS estáticas.

Domínio público: **teste-ia.camim.com.br**

---

## Serviços na VM

| Serviço systemd | Diretório | Porta | Domínio | Função |
|---|---|---|---|---|
| `camim-auth.service` | `/opt/camim-auth/` | 8020 | `teste-ia.camim.com.br` | Flask: autenticação, admin de usuários, KPIs, proxy de IA OpenAI |
| `wpp-campanhas.service` | `/opt/relatorio_h_t/wpp-campanhas/` | 8023 | `camila1.ia.camim.com.br` | Flask: plataforma WhatsApp Campanhas (auth via IDCAMIM OIDC) |
| `ia-groq.service` | `/opt/ia-groq/` | — | — | Flask: análise IA com Groq |
| nginx | `/var/www/` | 80/443 | — | Serve arquivos estáticos + proxy reverso para os Flask apps |

### Relação entre camim-auth e wpp-campanhas

Ambos usam os mesmos módulos Python (`wpp_cobranca_routes.py`, `wpp_cobranca_db.py`), mas de caminhos diferentes:

- **camim-auth** (porta 8020): importa de `/opt/camim-auth/`, templates em `/opt/camim-auth/templates/`
- **wpp-campanhas** (porta 8023): importa de `/opt/relatorio_h_t/` (via `sys.path`), templates em `/opt/wpp-campanhas/templates/` (prioridade) e `/opt/camim-auth/templates/` (fallback via `ChoiceLoader`)

> **Deploy de arquivos WPP:** ao atualizar `wpp_cobranca_routes.py` ou templates HTML do WPP, copiar para **ambos** os locais e reiniciar **ambos** os serviços:
> ```bash
> scp arquivo root@VM:/opt/camim-auth/ && scp arquivo root@VM:/opt/relatorio_h_t/
> scp template root@VM:/opt/camim-auth/templates/ && scp template root@VM:/opt/wpp-campanhas/templates/
> ssh root@VM 'systemctl restart camim-auth wpp-campanhas'
> ```

---

## Fluxo de deploy completo

```
git push → main
    ↓
GitHub Actions (.github/workflows/deploy.yml)
    ↓ rsync via SSH
/opt/camim-auth/          ← app.py, auth_db.py, auth_routes.py,
                             ia_router_openai.py, llm_client_openai.py,
                             migrate_usuarios.py
/opt/camim-auth/templates/ ← todos os *.html do repo
/opt/ia-groq/              ← analyze_groq.py + módulos IA + prompts
/opt/relatorio_h_t/        ← *.py (ETL/KPI), sql/, requirements.txt, postos_acl.json
/var/www/                  ← js/, css/, fonts/, images/, postos_acl.json
    ↓
Cron na VM (a cada ~5-10 min)
    ↓ copia de /opt/ para /var/www/
/var/www/                  ← arquivos estáticos finais servidos pelo nginx
    ↓
nginx serve ao usuário
```

> **Importante:** `sync_www.sh` na VM é a versão manual do cron (faz a mesma cópia /opt → /var/www).
> Rodar `sync_www.sh` NÃO substitui o deploy do Actions — o Actions precisa rodar antes para
> atualizar `/opt/`. Só depois `sync_www.sh` (ou o cron) leva para `/var/www/`.

---

## Arquivos .sh na VM — PENDENTE DE REVISÃO

**Tarefa:** Quando abrir o Claude Code diretamente na VM:

1. Listar todos os `.sh` nos diretórios do projeto:
   ```bash
   find /opt/camim-auth /opt/relatorio_h_t /opt/ia-groq /var/www -name "*.sh" 2>/dev/null
   # e também no home do usuário / raiz do projeto clonado
   ```
2. Ler cada um e verificar se há:
   - Credenciais hardcoded (senhas, tokens, chaves)
   - IPs ou hosts internos que não devem ser públicos
   - Lógica que revele infraestrutura sensível
3. Se seguros → copiar para o repo git e commitar
4. Se tiver algo sensível → parametrizar com variáveis de ambiente antes de commitar

Scripts conhecidos (a confirmar na VM):
- `sync_www.sh` — copia arquivos de /opt para /var/www (versão manual do cron)
- Outros desconhecidos — verificar

---

## Cron na VM — PENDENTE DE VERIFICAÇÃO

**Tarefa:** Verificar o crontab completo quando na VM:
```bash
crontab -l
sudo crontab -l
cat /etc/cron.d/* 2>/dev/null
```

Esperado: entrada que roda a cada 5-10 minutos copiando arquivos de `/opt/` para `/var/www/`.
Documentar aqui o comando exato após verificar.

---

## Autenticação

- **DB:** SQLite em `/opt/relatorio_h_t/camim_auth.db` (env: `AUTH_DB_PATH`)
- **Sessão:** cookie `appsess` assinado com `itsdangerous.TimestampSigner` (TTL 8h)
- **Admin:** campo `is_admin` no model `User`; rota `/admin` requer `is_admin=True`
- **Reset de senha:** token UUID com expiração de 1h; e-mail enviado via Gmail SMTP SSL porta 465

## Variáveis de ambiente (`.env` em `/opt/relatorio_h_t/`)

```
AUTH_DB_PATH, SECRET_KEY, SESS_NAME
EMAIL_HOST, EMAIL_PORT, EMAIL_HOST_USER, EMAIL_HOST_PASSWORD, DEFAULT_FROM_EMAIL
APP_BASE_URL
GROQ_API_KEY, OPENAI_API_KEY
```

---

## Estrutura dos módulos Python principais

```
/opt/camim-auth/
  app.py               ← Flask app factory, rotas KPI, serve templates
  auth_db.py           ← SQLAlchemy models: User, UserPosto; init_db()
  auth_routes.py       ← Blueprint auth_bp: login, logout, reset, /admin API
  ia_router_openai.py  ← Rota IA via OpenAI
  llm_client_openai.py ← Cliente OpenAI

/opt/relatorio_h_t/
  *.py (ETL)           ← scripts de geração de JSON por KPI
  sql/                 ← queries .sql
  .venv/               ← virtualenv com dependências

/opt/ia-groq/
  analyze_groq.py      ← análise IA com Groq
  orquestrador.py, ia_router.py, llm_client.py, ...
  prompt/              ← arquivos .txt de prompt
```

---

## Regras de desenvolvimento

- Cada KPI é independente — nunca compartilha cálculos entre páginas
- HTML templates sem Jinja2 são servidos como estáticos via nginx (via cron /opt → /var/www)
- HTML templates com `{% %}` devem ser servidos pelo Flask (ficam em `/opt/camim-auth/templates/`)
- Atualmente `login.html` é estático (sem Jinja2) — ok servir via /var/www
- `nova_senha.html` e `reset_senha.html` usam `{% if erro %}` — servidos pelo Flask
- `.gitignore` deve excluir `.env`, `*.db`, `__pycache__`, `.venv`

---

## Controle de acesso por página

Usuários com `all_pages=True` (padrão para usuários existentes) acessam tudo.
Usuários com `all_pages=False` (padrão para novos usuários) só acessam páginas liberadas pelo admin.

### Como adicionar uma nova página ao controle de acesso

Ao criar uma nova página HTML, siga estes 3 passos obrigatórios:

**1. `app.py` — registrar em `_TEMPLATE_TO_PAGINA`**

```python
# Para template interno (arquivo .html servido pelo Flask):
"nome_do_template.html": "page_key",

# Para link externo (aparece no sidebar de mais_servicos.html):
"https://exemplo.camim.com.br/": "page_key",
```

**2. `auth_routes.py` — adicionar em `PAGINAS_DISPONIVEIS`**

```python
{"key": "page_key", "label": "Nome Legível para o Admin"},
```

**3. Comportamento automático**

- Usuários com `all_pages=True`: acessam normalmente (sem mudança)
- Usuários com `all_pages=False`: **não veem** a nova página no sidebar e recebem 403 se tentarem acessá-la diretamente — até o admin liberar explicitamente
- O admin libera via modal de edição do usuário em `/admin`

### Como funciona o filtro de sidebar

`render_protected_page()` e `any_html()` em `app.py` injetam um `<script>` antes de `</body>` que:
1. Lê a lista de `page_key` liberados para o usuário
2. Para cada `<a class="nav-link">` no sidebar, extrai o `href`
3. Consulta `_TEMPLATE_TO_PAGINA[href]` para obter a `page_key`
4. Se a `page_key` não está na lista do usuário → esconde o `<li>` pai

Isso funciona sem modificar cada template individualmente.
