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
  Validado com dry-run (abr-jun, postos B e G, ~20 mil linhas): conjunto de
  linhas idêntico, e `paciente`, `medico`, `especialidade`, `dif_dias`,
  `desistencia`, `matricula` batendo 100% com a view. Zero mudança nos baldes
  compareceu/falta/pendente/medico_faltou.
- **Ao mexer nessa query, replicar os filtros do WHERE da view**: `Codigo > 0`,
  `DataEstorno IS NULL` e `(ExibenoProntuarioF3 = 1 OR PermitirAgendamentoF6eCTRLF6 = 1)`.
  Sem eles entram estornos e serviços que não aparecem no F3 (foi o que fez a
  primeira versão devolver 3 linhas a mais por posto).
- **Nada de literal de hora com dois-pontos no .sql** — o `text()` do SQLAlchemy
  lê `'23:59'` como bind param. Use `DATEPART(hour/minute, ...)`.

**Ao comparar duas versões desta query (ou de qualquer coisa sobre lançamentos):
a chave da linha é `idLancamentoServico`, NUNCA `idLancamento`.**
Um lançamento tem N serviços. Chavear um dict por `idLancamento` colapsa os
irmãos e a última linha de cada resultado ganha — como a ordem de retorno muda
entre duas queries diferentes, aparecem "divergências" que são só o dict
sobrescrevendo. Foi assim que 2 linhas `Faltou → Atendido` apareceram do nada
num dry-run e sumiram ao trocar a chave. Se um diff acusar poucas linhas
estranhas, suspeitar da chave antes de suspeitar da query.

**Pendências conhecidas (2026-07-21):**
- `/cancelados_robo` ([cancelados_robo_routes.py](cancelados_robo_routes.py)) ainda
  usa `vw_Cad_LancamentoProntuarioComDesistencia` (~1,8s/posto no balcão). Cabe o
  mesmo tratamento, mas ela precisa de `Paciente`, `TelefoneResidencial`,
  `NomeMedico` e `idadePaciente` — remapear para as tabelas base dá mais trabalho
  do que o ETL e não foi validado por dry-run.
- A coluna `origem_cancelamento` deu **exatamente 72 "Pré agendamento não
  confirmado" em B e 72 em G** em julho/2026. Número idêntico em dois postos
  cheira a teto/limite do robô, não a coincidência. Conferir contra a lista ao
  vivo do `/cancelados_robo` antes de usar esse número em qualquer relatório.
- Primeira execução do cron com a query nova: **02:30 de 2026-07-22**. Conferir
  tempo total em `logs/export_preagendamento_*.log` (o benchmark foi só 2 postos
  / 3 meses) e o tamanho do `preagendamento.json` com o campo `oc` novo.

---

## Custos de TI (ex-"Custos com IA") — adicionado 2026-08-02

Consolida **todos** os custos de tecnologia por centro de custo. O painel
`Custos com IA` não sumiu: virou o centro de custo **IA** dentro deste módulo.

**Arquitetura:**

| Arquivo | Papel |
|---|---|
| `custos_ti_db.py` | Models SQLAlchemy no **Postgres RDS** (tabelas `ti_*`) |
| `custos_ti.py` | Regras de negócio, agregações por período, câmbio, ponte com o `custos_ia` |
| `custos_ti_meta.py` | Importação de custos da Meta (texto colado + Graph API) |
| `custos_ti_routes.py` | Blueprint `/api/custos-ti/*` |
| `custos_ti.html` | Home — consolidação gráfica do período (default = mês atual) |
| `custos_ti_centro.html` | Página de um centro (contas + lançamentos + import) |
| `custos_ti_cadastros.html` | Centros de custo, formas de pagamento, cotação |
| `_custos_ti_sidebar.html` / `_custos_ti_head.html` | Menu e CSS/JS compartilhados |
| `migrate_custos_ti.py` | Cria as tabelas + semeia os centros (idempotente) |

**Por que Postgres e não JSON como o `custos_ia`:** o `custos_ia` guarda
snapshots congelados por mês (bom para o que a Costs API devolveu). Aqui o dado
é relacional e editado à mão (centro ↔ conta ↔ lançamento ↔ forma de pagamento),
com dedupe por ID de transação. Isso é tabela.

**Tabelas:** `ti_centro_custo`, `ti_forma_pagamento`, `ti_conta`,
`ti_lancamento`, `ti_cotacao`. Só este projeto escreve (mesmo RDS do
`public.servicos`).

**Cadastrar um centro cria a página e o item de menu.** Não há lista fixa de
centros em HTML nenhum — o sidebar destas telas é Jinja, montado a partir de
`ti_centro_custo` (`app.py::_ti_centros()`, cache de 30s). A rota
`/custos_ti/<key>` é uma só; o conteúdo vem do banco.

**Controle de acesso:** page_key `custos_ti`, PROPOSITALMENTE fora de
`public.servicos` — mesmo truque do `acesso_avancado` e do `custos_ia`: só
`all_pages=True` entra e nenhum admin consegue liberar avulso. **Não** rodar
`seed_servicos.py` para ele.

**Moeda — os DOIS valores são congelados.** Cada lançamento guarda `valor` +
`moeda` (original da fatura) e **`valor_brl` E `valor_usd`** calculados no
momento do registro, mais a `cotacao` usada. A tela tem um switch US$ / R$
(padrão **dólar**) que só escolhe qual campo mostrar — **não reconverte nada** e
não faz request novo. Consequência: a despesa em dólar exibe o dólar EXATO da
fatura, sem passar por cotação alguma; só o valor da moeda oposta é convertido.
A tabela de lançamentos marca o convertido com `≈`.

Cotação por competência em `ti_cotacao`, com carry-forward do mês anterior e
fallback `CUSTOS_TI_USD_BRL` (default 5,40). **`cotacoes_detalhe()` devolve a
PROCEDÊNCIA de cada mês** — própria / herdada de outro mês / padrão do sistema —
e a tela avisa em amarelo quando o número em real saiu de um fallback. Sem isso
o usuário não tem como saber se o valor convertido vale alguma coisa.

Lição de 2026-08-02: eu semeei uma cotação de teste (5,42) direto na produção e
os meses anteriores caíram no fallback de 5,40. Nenhum dos dois era a cotação
real daquelas datas, e a tela não dizia isso. **Não semear cotação inventada** —
deixar o mês sem cotação é melhor, porque aí a tela avisa.

**Centro `fonte='ia'`:** o total dele NÃO vem de `ti_lancamento` — vem dos
snapshots do `custos_ia` (OpenAI + Groq + assinaturas), convertidos pela cotação
do mês. Lançamento manual no mesmo centro **soma por cima**, então não relançar
à mão o que já está no painel de IA (contaria duas vezes).

### Meta / WhatsApp — o que tem API e o que não tem

- **Cobrança real no cartão** (Business Manager → Cobrança e pagamentos →
  Atividade de pagamento): **NÃO tem API pública**. `/{business-id}/business_invoices`
  só existe para conta em linha de crédito/faturamento mensal, não para cartão.
  → entra por **texto colado** (`parse_payment_activity`), mesmo molde do import
  por texto da Groq. Parsing local, zero chamada a LLM, zero custo.
- **Custo estimado das mensagens**: **tem API**. Precisa de `META_WABA_ID` +
  `META_ACCESS_TOKEN` (System User com `whatsapp_business_management`, e a WABA
  atribuída a ele em Ativos) no `.env` do `/opt/relatorio_h_t/` + restart do
  camim-auth. Entra como lançamento **previsto**, origem `meta_api` — não se
  mistura com a cobrança real (`meta_texto`), que é o que fecha o mês.
  - `pricing_analytics` — **o certo hoje**, alinhado ao modelo POR MENSAGEM
    (vigente desde jul/2025). `metric_types=[COST,VOLUME]`,
    `dimensions=[PRICING_CATEGORY,TIER]`.
  - `conversation_analytics` — modelo antigo, por conversa. Fica de reserva:
    `fetch_custo_mensagens()` tenta o novo e cai no antigo se vier zero, e
    registra em `fonte` qual respondeu.
  - **A Meta não devolve `cost` para WABA que usa linha de crédito de Solution
    Partner.** A conta da CAMIM é paga direto no cartão (Visa ···· 6852, moeda
    USD), então vem — mas se um dia migrar para BSP, o custo some da API.
  - **O WABA ID não é o número que a tela de Cobrança mostra ao lado de "camim"**
    (`1855125718445969` → "Object does not exist"). O certo é o `asset_id=` da URL
    do billing_hub: **`25859435250382411`**. Descobrir por
    `/{business-id}/owned_whatsapp_business_accounts` exige `business_management`,
    que o token de produção não tem — bata direto em `GET /{candidato}?fields=id,name,currency`.
  - `testar_credencial()` lê `id,name,currency` da WABA e traduz os códigos de
    erro (190 = token inválido; 10/200/803 = falta permissão ou a WABA não foi
    atribuída ao System User). É o botão "Testar conexão" da tela — separa
    "credencial errada" de "não houve gasto no mês".

**Dedupe:** `UNIQUE(origem, external_id)` em `ti_lancamento`. O `external_id` é o
ID da transação da Meta, então colar o mesmo extrato de novo não duplica nada —
pode colar o histórico inteiro toda vez.

**Cartão desconhecido no extrato** é cadastrado automaticamente em
`ti_forma_pagamento` (bandeira + 4 últimos dígitos, que são `UNIQUE`), e a
importação reporta o que criou.

### Página com modal PRECISA carregar o Bootstrap

O `adminlte.min.js` do AdminLTE 3 **não embute o Bootstrap**. Sem
`bootstrap.bundle@4.6.2`, `$.fn.modal` não existe: o clique estoura TypeError
dentro do handler, o modal nunca abre e **nada aparece em log de servidor** —
o erro fica só no console do browser. Em 2026-08-02 isso deixou a seção inteira
inutilizável (Lançar despesa, Nova conta, Importar Meta, Novo centro, Nova
forma) sem nenhum sinal em lugar nenhum.

Já está no `_custos_ti_head.html`, junto do guard `tiModal()`, que avisa na tela
se o Bootstrap não vier em vez de morrer mudo. **Toda abertura de modal desta
seção deve passar por `tiModal()`, nunca por `$(...).modal()` direto.**

Quando o sintoma for "cliquei e não aconteceu nada": olhar
`/var/log/nginx/*access*.log` na VM. Se o request não está lá, o problema é
JavaScript — não adianta ler o Python.

### Cores dos centros de custo

`custos_ti.PALETA` (Python) e `TI_PALETA` (JS, em `_custos_ti_head.html`) são a
MESMA lista de 8 hex, validada para daltonismo (`dataviz/scripts/validate_palette.js`,
todos os checks PASS em superfície clara). A cor identifica a **entidade**, não a
posição no ranking — filtrar não pode repintar os sobreviventes. Slot novo é
atribuído na ordem; a partir do 9º centro o gráfico agrupa em "Outros" em vez de
inventar cor. **Ao mexer na paleta, rodar o validador antes.**

### Exclusões preservam histórico

Forma de pagamento e conta com lançamento ligado são **desativadas**, não
apagadas (senão o histórico perde em que cartão foi pago). Centro com lançamento
recusa a exclusão com mensagem. O centro `ia` é fixo.

### Migration

```bash
# local (dry-run, não grava nada)
python migrate_custos_ti.py --dry-run
# na VM
cd /opt/relatorio_h_t && .venv/bin/python migrate_custos_ti.py
```

---

## Médico · Custo Efetivo Nominal — regras de negócio (2026-08-02)

Página nova (`medico_custo.html`), ETL dedicado `export_medico_custo.py` +
`sql/medico_custo_efetivo.sql`. **Não** mexe no `export_custo_medico_ctrlq.py`,
que alimenta outro botão e tem outro contrato.

Fonte: `cad_medico` + `cad_especialidade` nos 13 postos. É CADASTRO, não
movimento — a página é a foto do contratado hoje, sem histórico.

### Modalidade de atendimento — três bits que convivem

`<Dia>OrdemChegada`, `<Dia>Internet`, `<Dia>Telefone` em `cad_especialidade`:

- **OC** = ordem de chegada / livre demanda. **Clínico geral é quase todo OC —
  só ~1% tem agenda.**
- **WWW** = agendamento pela internet · **TEL** = pela central.
- **Os três convivem no mesmo dia**: o caso típico é *"10 números agendados e o
  restante por ordem de chegada"*.

Daí sai `modalidade` ∈ {`ordem_chegada`, `agendado`, `misto`}.

**Custo por consulta só é calculado em `agendado` puro.** Em OC e em MISTO o
denominador não existe (a demanda é aberta), então a métrica fica **vazia** com
o selo da modalidade. Decidido pelo Cristiano: no misto as 10 vagas cobririam
só parte da demanda e o número enganaria. Não inventar denominador.

Ausência de vagas cadastradas **não** é dado faltando — é livre demanda.

### Hora de almoço — o imbróglio

**Padrão: a CAMIM não paga a hora de almoço.** Logo o valor/hora sai sobre a
jornada TRABALHADA: `R$ 1.100` numa janela de 12h com 1h de almoço → 11h →
**R$ 100/hora**. (Exemplo dado pelo próprio Cristiano.)

Mas a realidade é misturada e por isso a página mostra as DUAS visões, com
botão para alternar:

- médicos **antigos**: a hora de almoço era paga, faz parte do pagamento;
- médicos **novos**: paga-se só a hora trabalhada, **exceto plantão**;
- quem faz **menos de 10h** não tem almoço na jornada.

O ETL grava `valor_hora` (líquido, padrão), `valor_hora_bruto` (se o almoço
fosse pago) e `delta_almoco_hora`.

### Schema NÃO é igual entre os 13 postos

`cad_especialidade.valorconsultaclube` **não existe em Nova Iguaçu** — nem todo
posto atende clube. A query inteira falhava só naquele posto com "Nome de
coluna inválido".

Solução: placeholder `{{OPC:alias.coluna:tipo}}` no `.sql`, que o ETL resolve
por posto via `COL_LENGTH()` — vira a coluna real onde existe e `NULL` onde não
existe. **Ao acrescentar coluna que possa não existir em todo posto, usar o
placeholder** em vez de removê-la de todos.

### Outras decisões

- **Consolidação por médico** (CPF/CRM) com abertura por posto: o mesmo médico
  atende em vários. A pergunta principal é "quanto o Dr. X custa para a rede".
- **Quinzenal** (`AgendaQuinzenal=1`) entra pela METADE na projeção mensal.
- Projeção mensal usa **4,345 semanas/mês** (365,25/7/12). Usar 4 subestima ~8%.
- `MedicoRecebePorComissao=1` → o valor fixo **não** é o custo real; sinalizar.
- Turno que vira a madrugada (fim < início) soma 24h, senão dá negativo.

### ARMADILHA — dois-pontos no .sql vale para COMENTÁRIO também

O `text()` do SQLAlchemy varre a string inteira. Um `08:00` **dentro de um
comentário** já estoura com *"A value is required for bind parameter '00'"*.
Foi exatamente o comentário que avisava sobre a regra que quebrou a query na
primeira execução. Escreva `8h30`, nunca `8` dois-pontos `30`.

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
