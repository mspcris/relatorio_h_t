# CLAUDE.md — Guia de Contexto do Projeto relatorio_h_t

> Lido automaticamente pelo Claude Code em cada sessão.
> Mantido atualizado conforme o projeto evolui.

---

## Visão geral

Sistema de dashboards KPI da CAMIM (rede de clínicas médicas).
Dados vêm de um banco SQL Server via pyodbc, são transformados em JSON por scripts Python e consumidos por páginas HTML/JS estáticas.

Domínio público (canônico): **kpi.camim.com.br**
Domínio antigo: `teste-ia.camim.com.br` continua respondendo, mas faz **301 redirect** para `kpi.camim.com.br` (migração 2026-05-01)

---

## Serviços na VM

| Serviço systemd | Diretório | Porta | Domínio | Função |
|---|---|---|---|---|
| `camim-auth.service` | `/opt/camim-auth/` | 8020 | `kpi.camim.com.br` (e `teste-ia.camim.com.br` via 301) | Flask: autenticação, admin de usuários, KPIs, proxy de IA OpenAI |
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

## REGRA CRÍTICA — Formato de datas no SQL Server

O SQL Server da CAMIM tem `SET DATEFORMAT dmy` em todas as views.

- **Views:** datas SEMPRE em DD/MM/YYYY → `date.strftime("%d/%m/%Y")`
- **Tabelas:** pode usar formato ISO (YYYY-MM-DD)
- **NUNCA** usar `str(date)` como parâmetro SQL — gera `'2026-03-01'` (ISO) que o SQL Server interpreta errado com SET DATEFORMAT dmy
- Em março/2026, esse bug retornou 165 clientes em vez de 6.632 em Anchieta. Ficou meses sem ser detectado.
- Todos os scripts `export_*.py` devem usar `strftime("%d/%m/%Y")` ao passar datas para queries em views

---

## Dashboard Pré-Agendamento — regra de confirmação 5-2 dias

Adicionado em **2026-04-26**. Mede o impacto da regra de confirmação obrigatória.

**Linha do tempo:**
- Pré-2026-01: sem regra (baseline)
- 2026-01-01 → 2026-04-30: regra ativa, push enviado, mas SEM cancelamento
- 2026-05-01+: cancelamento automático de quem não confirma (previsto)

**Regras de negócio chave:**
- Janela de confirmação rígida: 5 a 2 dias antes da consulta
- 1 push por pré-agendamento (campo `DataHoraNotificacaoPreAgendamento`)
- Confirmação registrada em `DataConfirmacaoAgendamentoConsulta` (app OU central)
- Cliente que marca consulta com `<5 dias` de antecedência é isento (não dá tempo de janela)
- Movimento esperado: clientes desistem na janela → vagas liberadas → outros pegam em <5d (rotatividade saudável, não fuga da regra)

**Categorização do campo `Atendido`:**
- `Médico Faltou` → EXCLUI da análise (não conta)
- `Atendido` / `Aguardando` → compareceu
- `Faltou` / `Ausente` → falta
- `Não Atendido` → falta SE >1h após `DataConsulta + HoraPrevistaConsulta`; senão pendente
- Outros → fallback "falta"

**REGRA — o foco desta página é saber QUEM FALTOU** (decidido 2026-07-21)

Não é um prontuário. Status que descrevem pendência administrativa não interessam
aqui e não devem ser reintroduzidos:

- `PENDÊNCIA DE GUIA`, `PENDÊNCIA DE PAGAMENTO`, `PENDÊNCIA RECEPÇÃO` — **não
  calcular**. Todos os três viram `Faltou`/`Ausente`/`Não atendido`, que é onde
  já caíam na categorização do dashboard.
- `PENDÊNCIA RECEPÇÃO` especificamente = cliente que marcou e ficou inadimplente.
  O Cristiano trata isso como **exceção no balcão**, não por dashboard. Não pedir
  para reativar, não sugerir card/gráfico para isso.
- `Médico faltou` **continua fora** da análise. No CASE do SQL a ordem é
  `Atendido` → `Médico faltou` → `Aguardando`, e cada posição tem motivo:
  - ganha de `Aguardando` — na view original `Aguardando` vinha primeiro, então
    um lançamento com falta do médico era contado como *compareceu*;
  - **perde** para `Atendido` — `StatusAtendimento=1` é fato registrado (médico
    substituto, falta lançada errado). Excluir isso jogaria fora atendimento
    real. Decidido em 2026-07-21 sobre 1 caso medido em 11.012 linhas.

Isso vale para `sql/preagendamento.sql` e para a página `preagendamento.html`.
**Não vale** para `kpi_consultas_status.html` / `export_consultas_mensal_json.py`,
que são um KPI independente, ainda leem a view e mantêm o balde `pend_recepcao`
de propósito (cards e série de gráfico próprios).

**Coluna `origem_cancelamento`** (adicionada 2026-07-21, sugestão do Léo Carneiro).
No JSON vai como `oc`, código de 1 letra para não inchar 50-80MB:
- `R` — robô de pré-agendamento cancelou (não confirmou na janela 5-2d)
- `A` — o próprio cliente cancelou pelo site/app
- `O` — outro cancelamento
- `""` — não foi cancelado

Quem foi cancelado pelo robô **não é falta** — foi cancelado, não deixou de
comparecer. É a mesma população que a página `/cancelados_robo` lista ao vivo.

**Canal de marcação:**
- `MarcadoViaWeb=1` → WEB (App Camim ou Égide)
- `MarcadoViaAgendaUnificada=1` → ASU (central de atendimento)
- `CtrlF6=1` ou todos zero/null → F6 (lançamento direto pelo posto)

**Arquitetura:**
- ETL: `export_preagendamento.py` + `export_preagendamento.sh` + `sql/preagendamento.sql`
- Cron: 1x/dia às 02:30 — regenera JSON inteiro do zero (não é incremental)
- Janela coletada: 12 meses passado + 90 dias futuro
- Saída: `json_consolidado/preagendamento.json` (~50-80MB; sob gzip nginx ~10-15MB)
- Frontend: `preagendamento.html` carrega 1 vez e processa em JS (Chart.js)
- Inclui ambas populações (desistência 0 e 1) — frontend filtra
- **A query NÃO usa mais `vw_Cad_LancamentoProntuarioComDesistencia`** (desde 2026-07-21) —
  lê as tabelas base. Medido em abr-jun/2026: posto B 8,1s→3,9s, posto G 10,6s→5,1s.
  Repetindo a medição o ganho oscilou entre 2,1x e 3,4x conforme a carga do
  servidor — conte com ~2x no pior caso.
  O que a view custava e a query nova não paga:
  a UDF escalar `dbo.PossuiGuia()` chamada linha a linha (PENDÊNCIA DE GUIA),
  o join em `Fin_Receita` (PENDÊNCIA DE PAGAMENTO) e o join na view aninhada
  `vw_Cad_ClienteDiasInadimplenciaANSview` (PENDÊNCIA RECEPÇÃO).
  Validado com dry-run: conjunto de `idLancamento` idêntico, e `paciente`,
  `medico`, `especialidade`, `dif_dias`, `desistencia`, `matricula` batendo
  100% com a view. Só 10 linhas em ~7.000 mudaram de status, sem mexer em
  nenhum balde do dashboard.
- **Ao mexer nessa query, replicar os filtros do WHERE da view**: `Codigo > 0`,
  `DataEstorno IS NULL` e `(ExibenoProntuarioF3 = 1 OR PermitirAgendamentoF6eCTRLF6 = 1)`.
  Sem eles entram estornos e serviços que não aparecem no F3 (foi o que fez a
  primeira versão devolver 3 linhas a mais por posto).
- **Nada de literal de hora com dois-pontos no .sql** — o `text()` do SQLAlchemy
  lê `'23:59'` como bind param. Use `DATEPART(hour/minute, ...)`.

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


## Regra de design — Preservar URL após login (`?next=`)

Sempre que um usuário deslogado tentar acessar uma URL protegida, capturar o
caminho completo (path + query string) e propagar via `?next=<url>` por todo o
fluxo de autenticação, de modo que ao final ele caia na URL original — nunca em
uma home/dashboard genérico.

**Onde está implementado:**
- [`nginx/teste-ia.conf`](nginx/teste-ia.conf) — `error_page 401 = @loginredir;`
  + named location `@loginredir { return 302 /login?next=$request_uri; }`.
  Toda location protegida por `auth_request /auth` herda esse comportamento.
- [`login.html`](login.html) — JS no final do arquivo lê `?next=` e propaga
  para o botão IDCamim e para o form local (hidden input).
- [`auth_routes.py`](auth_routes.py) — `/session/login` (POST), `/auth/idcamim`
  e `/auth/idcamim/callback` honram `next`. O state OAuth (`_oauth_states`)
  agora carrega `{"status": ..., "next": ...}`.
- `_safe_next()` valida (`/`-prefix, sem `//`, sem `/\\`) — anti open-redirect.

A `state` OAuth permanece sendo apenas anti-CSRF — o `next` é guardado
junto no dict server-side (não dentro da string `state`).


## REGRA CRÍTICA — Mudanças que podem disparar custos reais (Meta/WhatsApp/SMS/Email/SQL)

Cada mensagem WhatsApp via Meta custa **~R$ 0,35** (depende do dólar). Cada
SMS, e-mail transacional, request a API paga, INSERT em base de produção,
chamada Cielo etc. tem custo real ou efeito colateral irreversível.

**Antes de fazer commit/deploy de qualquer alteração que toque código que
roda em cron, daemon, scheduler, ETL, ou que processa lotes de
clientes/pacientes/faturas, OBRIGATÓRIO:**

1. **Mapear TODOS os consumidores** dos dados que você está alterando.
   Ex.: alterou tabela `campanhas` → grep por `listar_campanhas`,
   `get_campanha`, `campanhas WHERE`, em `*.py`/`*.sh`/cron jobs.
2. **Identificar guards/kill-switches** existentes (filtros `WHERE postos != []`,
   `WHERE ativa=1`, `IF modo NOT IN (...)` etc.) e checar se sua mudança
   anula algum deles sem perceber.
3. **Dry-run obrigatório** antes do deploy real, mesmo que pareça "simples":
   - cron de envio em massa: rodar com `--dry-run` ou flag equivalente
   - INSERT em SQL Server CAMIM: rodar a SELECT equivalente primeiro
   - chamada Cielo/Meta: usar sandbox quando existir
4. **Filtrar pelo modo no consumidor, não confiar só na ausência de dados**.
   Se uma feature é "disparada via API" (não via cron), o cron precisa ter
   `if modo == 'api_direta': continue` explícito — não basta deixar
   `postos=[]` como kill switch implícito.
5. **Em deploys que mexem em comportamento de envios em massa**: revisar
   manualmente o crontab da VM (`crontab -l`, `/etc/cron.d/*`) pra ter
   ciência de QUANDO o cron vai rodar pela próxima vez e o que ele faz.

### Incidente de referência: 2026-05-06

**O que aconteceu:** 473 envios WhatsApp errados pra clientes em atraso do
posto A (Anchieta), custo aproximado R$ 165,55. Mensagem chegou com TODOS
os campos do template vazios:

> "Olá . Informamos que o(a) Dr(a). não poderá comparecer no dia .
> A agenda foi fechada pelo(a) ., foi o motivo registrado."

**Causa raiz em 3 camadas:**

1. A campanha 29 era a "Falta de Médico" original, com `modo='falta_medico'`,
   `template='aviso_de_fechamento_de_agenda'` e **`postos=[]`**. O cron de
   cobrança ignorava ela na prática porque sem postos não tinha o que
   processar — `postos=[]` virou kill-switch implícito.
2. Pra implementar o roteamento por grupo de posto (Altamiro 2455 vs Couto
   3529), eu adicionei `postos=[A,B,G,I,N,R,X,Y]` na campanha 29. Isso
   **REMOVEU o kill-switch implícito** sem que eu percebesse.
3. O cron `send_whatsapp_cobranca.main()` iterava todas as campanhas com
   `ativa=1` e processava qualquer modo. **Faltava** o guard
   `if modo == 'falta_medico': continue`.

Resultado: o cron pegou a campanha 29 com posto A preenchido, processou-a
como cobrança normal, expandiu o template `aviso_de_fechamento_de_agenda`
(que espera `{paciente}{medico}{data_consulta}{local}{resp_fechamento}{motivo}`)
com os parâmetros da fatura (`{nome}{ref}{valor}{venc}`) — placeholders
não bateram, ficaram vazios — e enviou. A Meta aceitou e cobrou as 473
mensagens.

**Por que não foi pego no QA:** o teste end-to-end disparou pelo
`/medico_falta` (caminho API direto), que funcionou perfeito. Não testei
o cron — não me ocorreu que MUDAR `postos` da campanha 29 ia mudar o
comportamento do cron.

**Não há reparo possível**: a Meta é integração oficial paga, não dá pra
"desfazer" nem mandar mensagem corretiva (cada nova mensagem custa de
novo, e clientes que não responderam não podem receber template fora da
janela de 24h sem nova cobrança).

**Primeiro hotfix** (commit `23dce87`): filtro explícito
`if modo == 'falta_medico': continue` no cron. **NÃO RESOLVEU SOZINHO**
porque a função `wpp_cobranca_sql.modo_envio()` tem uma whitelist:

```python
return m if m in (MODO_ATRASO, MODO_PRE_VENCIMENTO, MODO_CLIENTES,
                   MODO_CLIENTE_NOVO) else MODO_ATRASO
```

`'falta_medico'` não estava lá → silenciosamente convertido pra `'atraso'`
→ filtro nunca disparava. Custo final SUBIU de R$ 165 (estimativa) pra
**R$ 421,40 reais (1.204 envios)**. Cron continuou processando entre
02:27 e 02:56 mesmo após o "fix".

**Hotfix REAL** (commit `a2bb607`): adicionou `MODO_FALTA_MEDICO` à
whitelist + filtro de defesa em profundidade que compara TANTO o valor
bruto do dict QUANTO o valor normalizado.

**Lições permanentes:**

1. **Filtros implícitos são bombas**: alterar dados que outras partes do
   sistema usam COMO KILL-SWITCH IMPLÍCITO (`postos=[]`, `ativa=0`,
   `dias_atraso=0`, `status=null`, etc.) é EQUIVALENTE a alterar lógica
   de negócio. Rastreie TODOS os consumidores antes.

2. **Whitelists silenciosas escondem bugs**: funções que devolvem um
   default quando o input não bate em uma lista (`return X if X in
   (...) else DEFAULT`) escondem dados inválidos. Quando você adicionar
   um novo modo/tipo/status, OBRIGATÓRIO grep pela whitelist em TODO o
   código pra incluir o novo valor. Idealmente, log um warning quando o
   default é usado.

3. **Defesa em profundidade**: o filtro do consumidor (cron) NÃO PODE
   depender só de uma camada (a função normalizadora). Compare o valor
   bruto E normalizado.

4. **Dry-run não é opcional**: depois de QUALQUER mudança em código de
   cron/scheduler, rodar `--dry-run` e verificar o LOG mostra o
   comportamento esperado. Foi o dry-run que revelou que o primeiro
   hotfix não funcionou.

5. **Sync de arquivos compartilhados em múltiplos paths**: o `wpp-campanhas`
   service importa módulos do `/opt/relatorio_h_t` via `sys.path.insert`,
   mas se houver cópia local em `/opt/wpp-campanhas`, o Python pode
   preferir essa cópia. O `deploy.yml` precisa sincronizar os arquivos
   compartilhados (`wpp_cobranca_*`, `send_whatsapp_cobranca`) PARA OS
   DOIS PATHS pra evitar versões divergentes silenciosas.
