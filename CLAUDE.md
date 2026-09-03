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

**`/cancelados_robo` — período + rede inteira (2026-08-31, pedido do Petterson/CG
via Cristiano):**
- A API aceita `?ini=&fim=` (teto de 92 dias — consulta ao vivo é operacional;
  histórico longo é papel do dashboard de pré-agendamento). `?data=` continua
  aceito como atalho de 1 dia.
- **Leitura é da rede inteira para qualquer usuário logado**: postos derivados
  de `DB_HOST_*` no `.env` (nunca lista fixa — filial nova com robô aparece
  sozinha), e a página saiu do gate de page_key (fora de `_TEMPLATE_TO_PAGINA`,
  filtrada do modal do admin via `PAGINAS_SEM_GATE` em `auth_routes.py`; a
  linha em `public.servicos` FICA, porque é dela que sai o card de Mais
  Serviços).
- **Marcar "tratado" continua exigindo o posto no ACL** — ver é de todos,
  tratar é da recepção do posto. O front desabilita o checkbox fora do ACL
  (`postos_acl` vem no payload da API).
- Auto-refresh de 2 min só roda quando o período inclui hoje/futuro.
- Período de mais de um dia agrupa a grid por dia de consulta (linha
  separadora com dia da semana + data + contagem), pedido do Petterson.

**Pendências conhecidas (2026-07-21):**
- `/cancelados_robo` ([cancelados_robo_routes.py](cancelados_robo_routes.py)) ainda
  usa `vw_Cad_LancamentoProntuarioComDesistencia` (~1,8s/posto no balcão). Cabe o
  mesmo tratamento, mas ela precisa de `Paciente`, `TelefoneResidencial`,
  `NomeMedico` e `idadePaciente` — remapear para as tabelas base dá mais trabalho
  do que o ETL e não foi validado por dry-run. Com o filtro de período
  (2026-08-31) a view passou a ser consultada em ranges de até 92 dias — se o
  tempo por posto crescer demais, essa migração sobe de prioridade.
- A coluna `origem_cancelamento` deu **exatamente 72 "Pré agendamento não
  confirmado" em B e 72 em G** em julho/2026. Número idêntico em dois postos
  cheira a teto/limite do robô, não a coincidência. Conferir contra a lista ao
  vivo do `/cancelados_robo` antes de usar esse número em qualquer relatório.
- Primeira execução do cron com a query nova: **02:30 de 2026-07-22**. Conferir
  tempo total em `logs/export_preagendamento_*.log` (o benchmark foi só 2 postos
  / 3 meses) e o tamanho do `preagendamento.json` com o campo `oc` novo.

---

## KPI Médicos (Qualidade) — aba "Sem contrato PJ" + justificativa mensal (2026-08-17)

Em `ctrlq_relatorio.html`, os cards **Com contrato PJ** e **Sem contrato PJ**
abrem o mesmo modal (`#pjModal`) com duas abas. A aba *Sem contrato PJ* é
onde o Cristiano cobra do gerente do posto uma **justificativa por mês** de
por que o médico ainda não tem PJ. Mostra a última justificativa, tem
"Anteriores (N)" para o histórico e caixa para justificar na hora.

| Arquivo | Papel |
|---|---|
| `ctrlq_pj_routes.py` | `GET/POST /api/ctrlq/pj/justificativas`, `POST .../<id>/desativar` |
| `sql_ctrlq_relatorio/sql_ctrlq_relatorio.sql` | passou a exportar `idmedico` + `idespecialidade` |
| `ctrlq_relatorio.html` | modal, índice local, formulário |

- **O dado mora no CAMIM**, tabela `Cad_EspecialidadeJustificativaPJ` (Janderson,
  existe nos 13 postos): `idEspecialidade`, `DataHora`, `idUsuario`,
  `Justificativa varchar(250)`, `Desativado`. Não é tabela nossa — o app do
  CAMIM também escreve nela (a de teste veio de `VICTORIA.A`).
- **Chave física é `idEspecialidade`, mas a pergunta é por MÉDICO.** Leitura
  agrupa por `idMedico` (join em `Cad_Especialidade`); escrita usa o
  `idespecialidade` da linha dedupada do KPI, ou a especialidade ativa mais
  antiga do médico se o JSON for antigo. JSON sem `idmedico` casa por CRM.
- **"Situação no mês"** = existe justificativa com `DataHora` dentro da
  competência escolhida no modal. **Default = MÊS DE HOJE**, não o mês do
  card: a cobrança é do mês corrente e justificativa nova entra sempre com a
  data de hoje. Abrir em jul-26 fez a primeira justificativa real (17/08)
  parecer "não contada" — o Cristiano estranhou na hora. Competência passada
  ganha selo amarelo dizendo em que mês a nova vai cair.
- **Auditoria:** a tabela não está em `Sis_HistoricoTabela`; grava-se em
  `idTabela=53` (Cad_Especialidade) com `id=idEspecialidade`, comando 1
  (incluir) / 3 (desativar). `idUsuario` vem do `login_campinho` resolvido na
  `sis_usuario` do posto (mesmo padrão do `medico_novo_routes`). Sem vínculo,
  vê mas não escreve, e a tela diz por quê.
- Desativar (soft delete) só pelo autor ou admin; nunca DELETE.
- Se hoje o médico já é PJ no cadastro (KPI é foto de ontem), o POST devolve
  409 — não há o que justificar.
- Posto fora do ar aparece como "sem leitura", **não** como pendente (mesma
  regra do medico_custo: falha de posto não pode parecer resultado). GET lê os
  13 postos em paralelo; posto com timeout custa ~10s à chamada.

---

## Notas x RPS — relatório "NF emitidas × meta" no celular (2026-08-31)

Pedido do Cristiano: imagem que caiba na tela do celular com as notas emitidas
por posto/empresa (grupo **Clínicas**, mês corrente) colorida contra a meta de
NF do CNPJ, enviada por **zap e e-mail** para ele e para o **Vinicius Gomes**
nos dias **25, 28, 30 e 31** (31 só quando existe — o cron resolve), e com um
**link em toda mensagem** que, ao ser tocado, responde "⏳ Espere, atualizando
os dados…" e depois manda mensagem NOVA com imagem nova e link novo.

| Arquivo | Papel |
|---|---|
| `relatorio_nf.py` | Dados (lê `json_notas_rps/*_notas_rps_<ym>.json`), imagem (Pillow), token, envio (Evolution `sendMedia` + SMTP), fila, CLI |
| `relatorio_nf_routes.py` | `/relatorio_nf/<token>` (pública), `POST /relatorio_nf/<token>/ir`, `POST /api/relatorio_nf/enviar`, `GET /api/relatorio_nf/info` |
| `kpi_notas_rps.html` | Notas emitidas ANTES de RPS pendentes; coluna `% Meta` colorida; card "📲 Relatório no celular" no rodapé da aba Resumo |
| `cron/relatorio_ht` | `30 9 25,28,30,31 * *` (envio) e `* * * * *` (fila) |
| `nginx/teste-ia.conf` | `location ^~ /relatorio_nf/` sem `auth_request` (mesmo modelo do `/ciencia/`) |

**Régua de cores** (`cor_meta()` no Python e `metaCor()` no HTML — **mudar nos
dois**), definida pelo Cristiano em 2026-08-31: gradiente contínuo — até 50 %
vermelho · 50→100 % do amarelo escuro ao verde intenso · 100→150 % do verde de
volta ao amarelo · ≥150 % vermelho intenso (estouro). Sem meta cadastrada →
cinza, fora da contagem. `faixa()`/chips contam por zona (vermelho/amarelo/
verde/estouro).

**% da meta = NF CONTABILIZADAS (emitidas − canceladas) ÷ objetivo.** Decisão
minha em 2026-08-31 (o pedido dizia só "valor da meta"): nota cancelada não
conta para o teto do CNPJ. Se o Cristiano quiser sobre EMITIDAS, é um campo em
`carregar_dados()` e em `metaBadge()`. Caso real onde a escolha muda a cor:
Anchieta em ago/26 — emitidas 100,8 % (verde) vs contabilizadas 99,99 % (amarelo).

**Por que FILA (spool) e não envio direto pelo Flask.** O camim-auth roda como
`www-data` e não escreve em `json_notas_rps` (root:deploy); o ETL precisa de
root. Então o Flask só grava um `pedido_*.json` em
`/opt/camim-auth/relatorio_nf_spool/` (o www-data está no grupo `deploy`, que
tem escrita em `/opt/camim-auth`) e manda o zap "Espere…" na hora; o cron de
root (`--spool --run`, 1×/min) roda `export_notas_rps.sh` esperando o lock do
job horário (`flock -w 600`), gera a imagem e envia. ETL medido: ~40 s para os
13 postos. Fila vazia termina em < 1 s.

**Anti-loop do link público — três camadas, não tirar nenhuma:**
1. `GET /relatorio_nf/<token>` **não tem efeito colateral**; o disparo é um
   `POST /ir` que a página faz por JS ao abrir. Scanner de link de e-mail
   (Safe Links) e prefetch de preview fazem GET sem JS → nada acontece.
2. `linkPreview: False` em todo envio pela Evolution — senão o próprio robô
   faria GET na URL que acabou de mandar.
3. `pedir_envio()`: recusa se já há pedido pendente para o destinatário,
   cooldown de 90 s após o último envio por link e teto de 20 reenvios/dia.
   Não custa dinheiro (Evolution é texto livre), mas toque duplo ou scanner
   não pode virar rajada.

**Kill-switches explícitos:** sem `--run` o script é dry-run (o `--run` fica
escrito NA LINHA do cron, padrão do `import_email_custos_ti`);
`RELATORIO_NF_ENVIO=0` no `.env` desliga o envio mesmo com `--run` e a página
mostra o aviso.

**Destinatários** em `RELATORIO_NF_DESTINATARIOS` (`.env`, formato
`id:Nome:email:telefone;...`), padrão no código: Cristiano
(`5521994317573`, mesmo número do `monitor_loop_chat`) e Vinicius Gomes
(`viniciusgomes@camim.com.br`, **sem telefone** — não existe em `users`, no
CRM `gestores` nem no `alarmes.db`; até cadastrar, ele recebe só e-mail e o log
diz "pulado: sem telefone").

**Token** = `URLSafeTimedSerializer(RELATORIO_NF_SECRET, salt='relatorio-nf-v1')` com
`{d: id, n: nonce}`; validade 90 dias (`RELATORIO_NF_TOKEN_DIAS`). O nonce faz
cada envio ter link diferente; link velho continua abrindo (e entrega o dado de
HOJE) até expirar. **`RELATORIO_NF_SECRET` mora no `.env` de
`/opt/relatorio_h_t/` (manual, fora do git)** — não existe `SECRET_KEY` em
.env nenhum da VM; cron e Flask leem o mesmo arquivo, e é isso que faz o link
gerado pelo cron abrir no Flask. Trocar o valor invalida todos os links já
enviados. O venv de `/opt/relatorio_h_t` precisou de `pip install itsdangerous`
(agora no `requirements.txt`).

`python relatorio_nf.py --preview /tmp/nf.png` gera só a imagem (útil para
conferir layout); `--enviar --para cristiano` sem `--run` mostra o que faria.

---

## KPI Receita × Despesa — drill-down até a despesa (como no APP Gestão) — 2026-09-03

Clique no mês em qualquer tabela do explorador (Nível 1 plano principal,
Nível 2 plano, Nível 3 tipo) abre um painel lateral com os registros
individuais: ID, data da prestação, data de pagamento, valor pago, descrição,
comentário, fornecedor, usuário. Mesmo formato dos cards do APP Gestão.

| Peça | Onde |
|---|---|
| Fonte | RDS `fin_despesa` (`export_fin_despesa_pg.py`, cron 2 h) via `/api/fin/despesas` |
| Filtros novos na API | `mes_base=auto`, `<campo>_eq` (exato), `retirada=0`, `cancelada=0`, `totais=1` |
| Página | `abrirRegistros()` / `renderRegistros()` em `kpi_receita_despesa.html`, drawer `#regDrawer` |

**Por que a carga mudou (o Cristiano autorizou "mudar o select e fazer carga"):**
- O gráfico agrupa o mês por **`DataPagamentoAuto`** (data em que o pagamento
  foi LANÇADO — `sql_full/*.sql`), não por `DataPagamento`. A view
  `vw_Fin_Despesa` não expõe essa coluna; ~1,6 % das linhas caem em mês
  diferente (G 135/8.250, A 297/29.568 em 12 m). O ETL passou a fazer JOIN em
  `Fin_Despesa` e gravar `data_pagamento_auto`; `--backfill-auto` preencheu o
  histórico (rodado em 2026-09-03). O drill-down pede `mes_base=auto`, senão
  não fecha com o total do mês.
- A carga era só `INSERT … DO NOTHING`: despesa editada ou cancelada depois
  nunca chegava ao RDS. Agora, além do incremental, há **refresh com UPSERT
  dos últimos 45 dias** por `DataPagamentoAuto` (`FIN_DESPESA_REFRESH_DIAS`).
  ~1.000 linhas por posto por execução, 5 s.
- `--dry-run` só conta. `ensure_schema()` é idempotente (coluna + índice).

**Conferência na tela:** o painel mostra "Total no gráfico" × "Soma dos
registros" (soma do filtro inteiro no RDS, não só da página carregada) e
avisa em amarelo quando difere — em geral é defasagem de até 2 h da carga.
"Última atualização" do app não existe no banco (é a hora da consulta do
app); o card mostra a hora da carga no RDS.

Filtro exato com os rótulos que a página usa para vazio ("Sem plano", "Sem
classificação") vira `IS NULL OR ''` na API (`ROTULOS_VAZIO`).

---

## Desbloqueio CTRL-Q (`/ctrlq_desbloqueio`) — gatilho, "antes" e prazo do ERP (2026-09-03)

Regra de negócio que faz o registro aparecer (**não é trigger no banco, é o
ERP**): ao mudar custo semanal ou tempo semanal de uma agenda, o ERP grava
`cad_especialidade.DataFimExibicao = agora + 8 dias EXATOS` e anexa
" - Custo/Tempo Semanal Anterior de X para Y" em `ObservacaoDesbloqueio` (log
cumulativo, do mais antigo ao mais novo). A `vw_Sis_Historico` registra a
mesma edição com "DataFimExibicao de (vazio) para dd/mm/aaaa hh:mm:ss" mais os
campos que mudaram. **Esse é o gatilho.** O Cristiano vai fazer o ERP disparar
também por mudança de quantidade de pacientes por hora (valor por consulta
sobe) — o parse é genérico, rótulo novo aparece sem mexer no código.

O que estava errado até 2026-09-03 (caso Dr. Milton, R, id 2586):
- "Como era antes" era o snapshot de `Cad_EspecialidadeHistorico` anterior à
  **DataFimExibicao** — que está no futuro; logo era a foto de ontem, já com
  todas as mudanças, e a comparação saía sem diferença.
- A auditoria olhava 10 dias antes da data fim; com prorrogação (gatilho
  24/07, data fim 05/09) o gatilho ficava fora e caía em "últimos 5".
- Linhas de histórico só com hora, sem dia.

Como ficou (`ctrlq_desbloqueio.py` + `sql_ctrlq_desbloqueio_aud.sql`):
- Auditoria dos últimos 365 dias; `detectar_gatilho()` acha a última linha
  "DataFimExibicao de (vazio) para X" e a **prorrogação** (linha posterior
  que trocou X por Y — botão Prorrogar Agenda ou ERP). O card mostra o ciclo
  desde o gatilho (+3 linhas anteriores apagadas, contexto).
- Agenda **criada** já com data fim não gera linha com DataFimExibicao →
  gatilho **estimado** = data fim − 8 dias (`DIAS_DATAFIM_ERP`). Se o ERP
  mudar o prazo, mudar a constante.
- "Antes" = foto de `Cad_EspecialidadeHistorico` (diária, 23:59:59) da
  **véspera do gatilho**, por `idEspecialidade`. Sem foto anterior (agenda
  criada no ciclo) → `reconstruir_antes()` desfaz as mudanças do ciclo a partir
  do estado atual, usando a própria auditoria. `hist_fonte` diz qual caminho
  foi usado e a página escreve isso ao lado do número.
- Bloco amarelo "Por que este registro está aqui": gatilho (data, usuário,
  campos alterados em chips), motivo gravado pelo ERP, prorrogação, e custo /
  tempo semanal / R$·h antes → agora.

---

## Farmácia · Saídas e Consumo por posto (`/farmacia_saidas`) — 2026-09-03

Pergunta do Cristiano: *"quanto saiu da farmácia de Realengo nos últimos 2-3
meses, quanto eu tenho em estoque, quanto mando agora"*. **As saídas são o
foco; estoque é complemento.** Um posto por vez (sem Altamiro/Couto — decisão
dele: saída se olha posto a posto; agrupar só faria sentido em entradas).

| Arquivo | Papel |
|---|---|
| `export_farmacia.py` + `export_farmacia.sh` | ETL diário 03:20, 13 postos em paralelo (~6 s/posto), só SELECT |
| `sql/farmacia_saidas.sql` | Est_Saida/Est_SaidaItem por produto × mês × destino × setor |
| `sql/farmacia_entradas.sql` | Est_Entrada/Est_EntradaItem por produto × mês × fornecedor |
| `sql/farmacia_consumo.sql` | Cad_LancamentoServico classe MEDICAMENTO* (lançado ao paciente) |
| `sql/farmacia_estoque.sql` | Cad_Produto + Cad_ProdutoLote — foto do estoque de hoje |
| `farmacia_saidas.html` | 4 abas: Saídas · Consumo real · Entradas · Estoque |
| `json_consolidado/farmacia_<P>.json` + `farmacia_index.json` | 245-485 KB por posto |

**Três leituras diferentes de "consumo" — não misturar:**
- **Saída da farmácia** (`Est_Saida`) = o que a farmácia ENTREGOU aos setores
  (enfermagem, laboratório, recepção…) ou MANDOU (outro posto, descarte). É o
  número que o Cristiano usa para decidir envio.
- **Consumo real** (`Cad_LancamentoServico`, classe `MEDICAMENTO*`) = remédio
  LANÇADO AO PACIENTE. Só medicamento tem isso; material (gaze, luva) não é
  lançado. Liga com o produto por `Cad_Produto.idServico` (156 de 355 produtos
  em Anchieta). Serviço compartilhado por vários produtos (SONDA FOLEY 14…24 →
  um idServico) é marcado `×N` na tela.
- **QuantidadeEnfermaria** de `Cad_Produto` NÃO é estoque: a enfermaria não é
  controlada, o campo só acumula envios (1,6 milhão de gazes, saldos
  negativos). Cristiano confirmou em 2026-09-03. Nunca usar como saldo.

**Fatos medidos que definem o desenho:**
- **`idProduto` é diferente em cada posto** (id 25 = Benzetacil em C,
  Diazepam em R; só 15 de 211 nomes iguais entre C e R). Todo cruzamento é
  dentro do posto. A coluna "Estoque no remetente" casa por **nome
  normalizado** e mostra quantos casaram — "sem par" não é zero.
- **Só A, C, R, G, I usam o módulo de saída** de verdade. N, X, Y, B, D têm
  poucas; **P, M, J têm zero em 12 meses**. A página avisa "sem saída
  registrada — não é consumo zero" e manda para a aba Consumo real.
- **Hub de abastecimento:** Anchieta supre o grupo Altamiro (R recebe 100 %
  de A), Campinho supre o Couto (J, D, M) e Anchieta recebe de Campinho. O ETL
  deduz o remetente pelas entradas (`remetente_sugerido`).
- **Entrada em unidades = `Quantidade × CaixaCom`** (Quantidade é nº de
  caixas, quase sempre 1). Mesma conta do `QuantidadeTotal` da
  `vw_Est_Entradaitem`. Somar só Quantidade daria "1 tubo" onde entraram 1.200.
- **Saída com `Gravado NULL` (Numero 0) é rascunho**, ~5 % do volume. Fica
  fora, como na `vw_Est_Saida`, e a página diz quanto ficou de fora.
- **A classe `MEDICAMENTO 90` NÃO EXISTE** em posto nenhum (o select do
  Cristiano citava 120 e 90). Existem `MEDICAMENTO 120`, `MEDICAMENTO 30`,
  `MEDICAMENTO`, `MEDICAMENTO EXTERNO`. A classe vai como coluna e a página
  filtra (padrão: todas menos EXTERNO, que é remédio trazido pelo paciente).
- `Cad_ProdutoGrupo` só está preenchido em Anchieta (19) e Campo Grande (1);
  nos outros o grupo é NULL e o filtro de grupo some.
- Departamentos do próprio posto têm código **numérico** em `cad_endereco`
  (TOMOGRAFIA '13', RAIOX '14', LIMPEZA '15', ENDOSCOPIA '16'…): saída para
  eles é **interna**, com o nome do departamento como setor.

**Sugestão de envio** = média mensal de saída (contada = interna; a
transferência para outro posto entra só com o checkbox) × cobertura desejada
(input, padrão 1 mês) − estoque de hoje, nunca negativa. Mês corrente entra
com peso inteiro e a página avisa; o botão "Últimos 3 meses" usa só meses
fechados.

**ARMADILHA repetida duas vezes no mesmo dia:** `:ini` escrito no COMENTÁRIO
do .sql virou bind param ("1 parameter markers, but 2 supplied") e, ao
documentar isso, `":nome"` no comentário novo virou outro. `text()` varre o
arquivo inteiro. Nenhum `:palavra` fora do bind real — nem em comentário.

Posto que falha no ETL mantém o JSON anterior e sai marcado no índice; a
página mostra "dado de <data>" em vermelho. Falha não vira consumo zero.

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
| `custos_ti.html` | Home — consolidação gráfica do período (default = **mês anterior**) |
| `custos_ti_centro.html` | Página de um centro (contas + lançamentos + import) |
| `custos_ti_cadastros.html` | Centros de custo, formas de pagamento, cotação |
| `_custos_ti_sidebar.html` / `_custos_ti_head.html` | Menu e CSS/JS compartilhados |
| `migrate_custos_ti.py` | Cria as tabelas + semeia os centros (idempotente) |

**Período de abertura — Visão geral abre no mês ANTERIOR, centro no ATUAL**
(2026-08-06). Não é inconsistência, é o uso de cada tela: a Visão geral é
leitura e o mês corrente pela metade faz o custo de TI parecer menor do que é;
a página do centro é onde se LANÇA despesa, e abrir no mês fechado esconderia a
conta lançada hoje — parece que não gravou. Quem escolhe é `tiPeriodoInit(fn,
{padrao:'anterior'})`; sem a opção, mês atual. **Não uniformizar as duas.**
O fallback de mês vazio (`tiFallbackMes`) continua valendo e a frase dele cita o
mês que abriu, não "mês atual".

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

### Contas fixas por e-mail — cron 3x/dia (2026-08-06)

`import_email_custos_ti.py` + `import_email_custos_ti.sh`. Horário definido, de
manhã, de tarde e de noite:

```cron
0 8,14,20 * * * /bin/bash /opt/relatorio_h_t/import_email_custos_ti.sh --run >> /var/log/relatorio_h_t/import_email_custos_ti.log 2>&1
```

**INSTALADO em 2026-08-06**, depois de fechar as duas pendências que impediam
(tela da fila e leitura do PDF). Linha exata no crontab do root da vps154.
Validada rodando o comando idêntico ao do cron, com `env -i`.

Env no `.env` do `/opt/relatorio_h_t/`: `IMAP_HOST`, `IMAP_PORT`, `IMAP_USER`,
`IMAP_PASSWORD` (Gmail exige APP PASSWORD, não a senha da conta) e
`CONTAS_REMETENTES`.

**Credencial: já resolvida na VM, não configurar nada (verificado 2026-08-06.)**
Não existe `IMAP_*` no `.env` da vps154 e não precisa existir — `_cfg()` cai no
`ALARM_EMAIL_USER`/`ALARM_EMAIL_PASSWORD`, que é `auditoria@camim.com.br` com
app password de 16 caracteres já usada pelos alarmes. Login IMAP testado OK.
`CONTAS_REMETENTES` está preenchido com `lcarneiro@arquitetodigital.com.br` e
`cristiano@camim.com.br`.

> Ao conferir isso de novo, **grep pelo valor, não pelo nome da variável**.
> `grep -oE "^CONTAS_REMETENTES="` imprime só o prefixo casado e a variável
> parece vazia — foi assim que eu concluí "kill-switch ligado" com ela cheia.

**`auditoria@` é caixa COMPARTILHADA, não é caixa de contas.** O CRM manda cópia
de tudo para ela. Medido em 2026-08-06: 399 mensagens, 361 não-lidas, das quais
**351 são cópia do CRM** e só 10 são fatura. A lista de remetentes não é
firula — é ela que segura 351 e-mails por execução. E o filtro roda **antes** de
qualquer `\Seen` (`run()`, linha 293), então e-mail do CRM não é tocado.
Confirmado no dry-run: `ignorados: 351`.

Por que 3x e não 1x como os outros ETLs: os outros leem o SQL Server e a foto do
dia serve. Aqui a conta chega ao longo do expediente e o gasto só aparece no
painel depois que o robô passa. 08h pega o que chegou de madrugada, 14h a manhã,
20h o resto do dia.

Rodar várias vezes é seguro **por construção**: busca só `UNSEEN`, dedupe por
`Message-ID` contra `ti_lancamento` E `ti_email_auditoria`, e só marca `\Seen`
**depois** do commit. Execução que morre no meio deixa o e-mail não-lido e a
próxima pega. Tem `flock` contra sobreposição.

- **`--run` fica escrito na linha do cron, não dentro do `.sh`.** Sem argumento o
  script roda `--probe` (só leitura). Quem abre o crontab tem que enxergar qual
  linha escreve em produção.
- **`CONTAS_REMETENTES` vazio é kill-switch e sai com erro** — de propósito. Por
  cron isso vira falha no log em vez de lançar e-mail que não é conta. Ao mexer
  nisso, lembrar do incidente de 2026-05-06: kill-switch implícito que some sem
  ninguém perceber é o padrão do estrago.
- **ARMADILHA — o deploy copia `.sh` sem bit de execução.** O rsync do
  `deploy.yml` para `/opt/relatorio_h_t` usa `--no-perms`, então script NOVO
  chega 644 e o cron falharia calado. Por isso a linha chama `/bin/bash <script>`
  em vez do caminho direto. Vale para todo `.sh` novo deste diretório.
- **Quem ler a caixa `auditoria@` na mão tira o e-mail do robô.** Lido é lido: o
  `UNSEEN` não acha mais e aquela conta nunca entra. Ninguém deve trabalhar essa
  caixa manualmente — o que o parser não entender já cai sozinho na fila de
  auditoria da tela.
- **O robô pergunta ao Gmail por remetente** (`UNSEEN FROM fulano`), em vez de
  baixar os 361 não-lidos e descartar 351 no Python. Isso é desempenho, **não é
  a trava**: a conferência de remetente continua no laço do `run()` — defesa em
  profundidade, porque `FROM` no IMAP casa por substring de cabeçalho.

### A conta chega em PDF ESCANEADO — quem lê é OCR (2026-08-06)

Medido nas 10 faturas de julho: **7 não têm texto nenhum dentro** (TecnoSpeed ×2,
PayGo, MongoDB, Google API, Google Workspace, Contabo). São imagem. Só KingHost
×2 e AWS têm texto extraível. O corpo do e-mail nunca traz o valor — diz o nome
do arquivo e a competência.

`contas_pdf.py` tenta `pdftotext` e cai no OCR (`pdftoppm` 300dpi + `tesseract
por+eng`) quando o texto vem abaixo de 80 chars. Pacotes de SISTEMA instalados na
vps154: `poppler-utils`, `tesseract-ocr`, `tesseract-ocr-por`. No venv: `pypdf`.

**REGRA — valor lido de PDF NUNCA vira lançamento sozinho.** Todo PDF para na
fila com a sugestão preenchida e o documento anexado; só vira despesa quando
alguém confirma em `/custos_ti_auditoria`. Não é excesso de zelo, é medição: na
fatura do MongoDB o OCR leu `Amount Due $23.` (comeu os centavos), leu o ano
como `2926`, e o número que o robô escolheu (US$ 25,46) veio de uma tabela de
pagamentos, não do total. Valor errado entrando calado no painel é pior que
valor faltando.

Junto com o valor vai o **trecho** do PDF de onde ele saiu — mesma regra do
`explicacao()` do medico_custo: número na tela diz de onde veio. Foi o trecho
que denunciou o MongoDB. A tela ainda avisa quando (a) a leitura foi por OCR e
(b) o rótulo tinha **mais de dois números ao lado** (serviço, desconto, ISS...),
sinal de que a escolha do robô é frágil.

Âncoras do total, em ordem de prioridade, em `contas_pdf.ANCORAS`: `VALOR TOTAL
DO SERVIÇO`, `VALOR TOTAL DA NFS-e`, `Total a Pagar`, `Amount Due`... **Ao
acrescentar fornecedor novo, acrescentar a âncora dele lá** em vez de afrouxar o
`\btotal\b`, que casa com qualquer linha de somatório da nota.

### Tela da fila — `/custos_ti_auditoria`

Endereço próprio, **não** `/custos_ti/auditoria`: aquela rota é a dos centros de
custo e cadastrar um centro com a key `auditoria` engoliria a página. Mesmo
motivo do `/custos_ti_cadastros`.

O modal de confirmação mostra **o PDF ao lado do formulário** — a conferência é
olhando a nota, sem trocar de janela. O valor gravado é o do formulário, não o
sugerido, e passa pelo mesmo `salvar_lancamento()` do resto do módulo para que
cotação, `valor_brl` e `valor_usd` congelem igual a um lançamento manual.

O PDF vai **inteiro** para o Postgres (`ti_email_auditoria.anexo_bytes`): as 10
faturas de julho deram 3,0 MB, ~36 MB/ano. `to_dict()` NÃO devolve os bytes — o
documento é servido sob demanda em `/api/custos-ti/auditoria/<id>/pdf`, senão a
listagem carregaria megabytes por nada.

**Descartar não apaga.** Item descartado sai da fila mas guarda e-mail e PDF —
descartar é dizer "não é custo", não é apagar a prova de que a conta chegou.
Tem `reabrir` para o descarte por engano.

O selo do menu (`tiBadgeAuditoria`) vive no `_custos_ti_sidebar.html`, então
aparece em TODAS as telas de Custos de TI — é ele que impede a conta de morrer
numa tabela que ninguém abre, que era o buraco que travava o cron.

### Antiduplicidade — a mesma conta não pode entrar duas vezes (2026-08-06)

O `UNIQUE(origem, external_id)` só protege contra o **mesmo e-mail** chegando
duas vezes. Ele não vê a mesma **fatura** por caminho diferente: reenviada pelo
fornecedor, encaminhada por duas pessoas, ou já lançada à mão antes de o robô
passar. Message-ID diferente, despesa em dobro no painel.

Medido em 2026-08-06: a **Google Workspace de julho já estava lançada (#29,
R$ 1.442,05)** e o item continuava pendente na fila. Confirmar na tela criava a
segunda. A chave de negócio é `conta_id + competência`.

**Sem `conta_id` não existe antiduplicidade.** O modal da fila não tinha campo
de conta — toda fatura de e-mail virava despesa solta, sem ligação com o
cadastro do serviço. Por isso o campo Conta existe agora, e é ele que sustenta
o resto.

`custos_ti.lancamentos_semelhantes()` procura, na mesma competência, por: mesma
conta · mesmo fornecedor · **a descrição citando o nome da conta**. O terceiro
é rede de segurança para a despesa digitada à mão, que quase nunca tem conta
nem fornecedor preenchidos — só "Contabo julho" na descrição.

**Não é bloqueio, é recusa com a lista na mão.** TecnoSpeed e KingHost mandaram
DUAS faturas no mesmo mês em julho/2026, ambas legítimas. Barrar
automaticamente comeria conta de verdade. A rota devolve **409 + a lista**, e a
tela oferece os dois caminhos: *completar a despesa que já existe* ou *é outra
fatura do mês, lançar assim mesmo*. A trava está no SERVIDOR — a tela é só o
aviso, senão um clique duplo ou uma aba velha fura tudo.

**Completar não sobrescreve.** `complementar_lancamento()` preenche só o que
está VAZIO (conta, fornecedor, cartão, data) e **nunca** troca o valor sozinho:
número que alguém já conferiu só muda com marcação explícita na tela, mostrando
o de-para. A `cotacao` original fica preservada mesmo quando o valor muda — ela
é o que torna o `valor_brl` auditável; recongelar no câmbio de hoje reescreve
história.

Completar gruda o PDF na despesa que já existia (`item.lancamento_id`), e o
clipe na tabela de lançamentos do centro serve o documento. Sem isso a nota
morre na fila e ninguém acha a prova quando perguntam de onde saiu o valor.

**`_ja_visto()` do robô procura o Message-ID em QUALQUER origem.** Quando a
auditoria gruda o e-mail numa despesa lançada à mão, o identificador fica numa
linha `origem='manual'`. Filtrar por `origem='email'` ali faria o robô trazer a
mesma conta de volta na execução seguinte.

**A trava vale para o robô também, não só para a tela** (lição de 2026-05-06:
guard no consumidor). E-mail redondo com despesa parecida no mês **para na
fila** em vez de lançar sozinho de madrugada.

#### Como o e-mail é casado com a conta cadastrada

`custos_ti.reconhecer_conta()`. Três armadilhas medidas em faturas reais:

- **Só assunto + nome do anexo.** Procurar no texto lido do PDF pôs a "Fatura
  Google API" na conta "Google Workspace" — o nome da outra conta aparecia
  dentro da nota. Assunto e nome do arquivo são escritos por quem encaminha e
  identificam a fatura; o miolo cita outros produtos do fornecedor.
- **Fornecedor com mais de uma conta NÃO escolhe conta nenhuma.** Uma fatura da
  Contabo cobre as **17 VPS** cadastradas (todas com `fornecedor='Contabo'`).
  Pendurar em uma delas jogaria a conta inteira na máquina errada. Devolve
  `conta=None` + `fornecedor='Contabo'` — o fornecedor sozinho já sustenta a
  busca por repetição. Nome sempre ganha de fornecedor; nome mais longo ganha.
- **Fronteira de palavra, não `in`.** "AWS" casaria dentro de "laws".

**O centro do padrão não pode ganhar do centro da conta.** `_centro_id()` tem
`usar_padrao=False` para distinguir "o e-mail escolheu infra" de "ninguém
escolheu nada". A ordem é: centro escrito no e-mail > centro da conta
reconhecida > padrão. Sem isso a Workspace (centro 5) ia para o centro 3.

**A competência vem do ASSUNTO** (`_competencia_do_assunto`, espelho do
`competenciaDoAssunto` da tela): estes e-mails não trazem vencimento no corpo,
o mês está em "Julho/2026". Sem isso a competência saía vazia e **a busca por
repetição não buscava nada**.

Contas ainda NÃO cadastradas em 2026-08-06 (a proteção nelas depende só do
fornecedor digitado): AWS, KingHost, MongoDB, TecnoSpeed, Google API.

### Fatura AGREGADA — uma nota que cobre várias contas (2026-08-06)

A fatura da Contabo é **uma só e paga as 17 VPS**, cada uma cadastrada como sua
própria conta e lançada individualmente. Quando essa nota vira despesa, o painel
soma o mesmo dinheiro duas vezes — uma pelo detalhe, outra pelo total.

Medido em julho/2026: as 17 VPS somam **R$ 1.387,98** e a fatura que chegou por
e-mail veio **R$ 1.387,98**. Ao centavo. A despesa #636 foi removida e a nota
virou `anexado` (item 9 da fila), com o PDF preservado.

**Por que a antiduplicidade não pegou:** `lancamentos_semelhantes()` compara o
`fornecedor` **escrito na despesa**, e as despesas das VPS têm esse campo vazio
— o "Contabo" mora no cadastro da **conta**. A busca varria e não achava nada.
Quem enxerga por esse caminho agora é `custos_ti.detalhamento_fornecedor()`.

**Só dispara para fornecedor com MAIS DE UMA conta e com a fatura SEM conta
definida.** As duas condições importam:
- uma conta só → a duplicidade normal (mesma conta, mesma competência) já
  resolve, e avisar de novo seria ruído sobre todo lançamento legítimo;
- conta escolhida/reconhecida → é a fatura daquele contrato específico, não a
  nota agregada. Lançar a fatura de UMA VPS com as outras 16 já lançadas é
  normal e não pode ser barrado.
- Medido em 2026-08-06: **Contabo é o único fornecedor multi-conta**. A trava
  nasce disparando exatamente onde deve.

**Quarto estado da fila: `anexado`.** A nota agregada não pode virar despesa
(dobra o mês) nem ser descartada (`descartado` diz *"não é custo"*, e é custo —
só está detalhado em outro lugar). `anexado` guarda o e-mail e o PDF sem criar
lançamento nenhum. Tem `reabrir` para o engano.

`rateio_fornecedor` + `rateio_competencia` gravam **o que** aquela nota cobre.
Sem os dois a nota fica órfã: presa num item de fila que ninguém mais abre, e
quem olhar a despesa da VPS não acha o documento — que é exatamente quando
alguém pergunta "de onde saiu esse valor?". É por eles que
`anexos_por_lancamento()` põe o clipe nas 17 despesas (validado: 17 de 17).
Nota própria do lançamento **ganha** da nota do fornecedor. Sair de `anexado`
(descartar/reabrir) limpa os dois campos, senão o clipe aponta para nota
renegada.

**A trava vale para o robô também** (mesma lição de 2026-05-06: guard no
consumidor). No `import_email_custos_ti.py` a fatura agregada **para na fila**
em vez de lançar sozinha às 08h/14h/20h. Se ficasse só na tela, o mês seguinte
duplicaria de madrugada e ninguém veria.

**Não é bloqueio.** A rota devolve 409 com o detalhamento na mão e o botão
*"Já está detalhada — só guardar a nota"*. O caminho de forçar continua ali:
duas faturas legítimas do mesmo fornecedor no mesmo mês já aconteceram
(TecnoSpeed e KingHost em julho/2026).

**Rateio automático NÃO foi feito** (dividir a nota entre as 17 contas). Fica
para quando existir o caso de mês em que as VPS ainda não foram lançadas —
precisa de regra para o centavo que sobra e para VPS não cadastrada.

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

### Piso do plantão remunerado — R$ 200

*"Menos de R$ 200 é estranho demais. 200 já é muito estranho, mas passa."*
(Cristiano, 2026-08-02). Linha abaixo disso **não é jornada de médico** e sai
das estatísticas — continua no JSON, num grupo próprio, com o MOTIVO:

| motivo | linhas | o que é |
|---|---|---|
| `exame` | 113 | MAPA, Raio-X, USG, Holter... o "médico" é o aparelho ou a sala |
| `comissao` | 46 | fono, psicologia, psicopedagogia, terapia ABA, fisioterapia — **recebem SÓ por comissão**, nunca por plantão |
| `abaixo_do_piso` | 49 | o resto: pediatria e clínica geral com R$ 1 a R$ 120 — **estes valem conferir**, incluindo um "TESTE - PROFISSIONAL" em Anchieta |

`ESPECIALIDADES_POR_COMISSAO` e `ESPECIALIDADES_EXAME` no ETL existem só para
dar o motivo certo na tela — o corte quem faz é o piso.

### ARMADILHA — `or todas` que anulava o próprio filtro

`analisar()` tinha `ls = [l for l in todas if _vale(l)] or todas`. O `or` era
para não gerar referência vazia, mas fazia o oposto do pretendido quando
NENHUMA linha da especialidade passava: voltava a usar todas e publicava
mediana de R$ 0,30 com spread de 16.230% para FONOAUDIOLOGIA. Especialidade sem
plantão remunerado agora fica com referência **nula** e o selo
`sem_plantao_remunerado`. São 28.

Depois do conserto os spreads viraram achado de negócio: NEUROLOGIA varia
**219% entre 10 postos**, CARDIOLOGIA 151% entre 12, GERIATRIA 108%.

### Todo selo tem que dizer POR QUÊ (2026-08-02)

O `+46%` do selo é **sobre a mediana de R$/hora da especialidade na REDE** —
não sobre o próprio médico, não sobre o posto, não sobre a média. Cristiano
perguntou "46% de quê?" olhando a tela, e a resposta só existia num `title` de
hover. Sinal que precisa de hover para ser entendido não serve para decidir.

Ao clicar o médico, o bloco `explicacao()` (função em `medico_custo.html`) abre
com a conta inteira de cada agenda que disparou o sinal: hora contratada ÷
mediana da especialidade − 1, o limite (mediana × 1,30), de quantas agendas e
postos saiu essa mediana, e a diferença em reais por hora. Para `conferir`, lista
qual agenda e qual motivo. **Ao criar sinal novo nesta página, criar junto o
bloco que explica a conta** — o selo sozinho é acusação sem prova.

Dois avisos que o bloco dá e não podem sumir:
- mediana apoiada em **menos de 3 postos** → comparação frágil, dito na tela;
- com o botão "pagar almoço" ligado, o sinal continua sendo calculado sobre a
  hora **líquida** — sem esse aviso o R$/hora da tabela não bate com o do bloco.

Para isso o ETL passou a publicar `linhas_base` em `referencias` (agendas que
sustentam a mediana) e `pct_acima_mediana_vaga` na linha. A tela degrada sozinha
se o JSON ainda for o antigo.

### NUNCA escrever nome de posto à mão

Os nomes dos 13 postos estão em **`cad_endereco`** (`Codigo` → `Descricao`,
mais `Bairro`, `Cidade`, `Desativado`, `AtendimentoAtivoPosto`). A tabela lista
a rede INTEIRA em qualquer base, então uma leitura basta.

Em 2026-08-02 eu escrevi um dicionário letra→bairro deduzindo e **errei 9 dos
13**. Os que confundem:

| Código | Nome real | O que NÃO é |
|---|---|---|
| `C` | Campinho (bairro Oswaldo Cruz) | Centro |
| `D` | Del Castilho | Duque de Caxias |
| `N` | Nilópolis | Nova Iguaçu |
| `I` | Nova Iguaçu (bairro da Luz) | Irajá |
| `G` | Campo Grande | Guadalupe |
| `P` | Rio das Pedras | Padre Miguel |
| `X` / `Y` | X Campo Grande / Y Campo Grande | Caxias / Campo Grande |
| `J` | Jacarepaguá (Tanque) | — |

`alarmes_db.POSTOS_NOMES` já tinha o mapa certo. Ao precisar do nome, ler de
`cad_endereco` (preferível, porque acompanha mudança de cadastro) ou reusar o
`alarmes_db`. Nunca inventar.

### A mediana é dos POSTOS SELECIONADOS (2026-08-02)

Filtrou 3 postos, a mediana é a **desses 3** e a comparação acontece só entre
eles. Comparar Anchieta com a mediana da rede quando se está olhando só Anchieta
responde outra pergunta.

Por isso o cálculo **saiu do ETL e foi para a tela** (`calcularReferencias()` em
`medico_custo.html`). Sem filtro o resultado é idêntico ao do Python — medido:
31 especialidades, 0 divergência de mediana, as mesmas 50 linhas marcadas.
`referencias` continua no JSON como referência de rede.

A base da mediana é filtrada **só por posto**, de propósito:
- especialidade — filtrar não muda a mediana daquela especialidade;
- busca — buscar "Angelica" faria a mediana ser a dela, e todo mundo ficaria
  "na média" por construção;
- sinal — é o que se quer descobrir, não pode ser entrada do próprio cálculo.
  (Por isso `plantoes(ignorarSinal)` e os cards contando sobre a base.)

**O escopo tem que aparecer no texto.** `escopoRef()` devolve "na rede" / "nos 3
postos A, B, P" / "no posto B", e toda frase que cita mediana usa isso. Número
certo com legenda errada é pior que número errado. Foi o que aconteceu com
`ROT_ALERTA`, que era objeto literal no topo do arquivo: avaliado uma vez no
load, dizia "na rede" para sempre. Virou função.

Exemplo real de por que isso muda decisão — ANGELA MARIA MAGALHAES, GINECOLOGIA
em A, B e P:

| Base | Mediana | Limite | Agendas dela fora da curva |
|---|---|---|---|
| Rede (12 postos) | R$ 132,88 | R$ 172,74 | **4 de 4** (+30 a +32%) |
| Só A, B, P | R$ 136,00 | R$ 176,80 | nenhuma |

`cadastro_suspeito` **não** entra nessa lógica: problema de cadastro é da linha,
não depende de com quem ela está sendo comparada. Continua vindo do ETL.

### Histórico diário — ver o cadastro de um dia passado

**Não existe página de comparação** (decisão do Cristiano, 2026-08-02). O que
existe é um seletor de data no topo da própria página: escolhe o dia e a tela
inteira passa a mostrar como estava. A página **abre sempre em hoje**.

Guarda MUDANÇA, não foto. Chave da agenda **medida** (776 chaves para 776
linhas): `posto + id_medico + id_especialidade + dia_semana`. O **horário está
fora da chave de propósito** — agenda que sai das 7h para as 8h é a mesma agenda
alterada, não uma removida e outra criada.

| Arquivo | Papel |
|---|---|
| `medico_custo_hist_db.py` | Models `mc_execucao`, `mc_agenda_versao`, `mc_mudanca` no RDS |
| `medico_custo_hist.py` | `registrar()` (chamado pelo ETL) e `linhas_em(data)` |
| `medico_custo_routes.py` | `/api/medico_custo/datas` e `/snapshot?data=` |
| `migrate_medico_custo_hist.py` | Cria as tabelas (`--dry-run` disponível) |

**REGRA DE OURO — posto que falhou no ETL não remove agenda nenhuma.** Se um
posto der timeout, fechar as agendas dele grava "45 agendas removidas,
−R$ 180 mil/mês". Economia falsa é pior que dado faltando: tem a cara exata do
resultado que se está procurando. Só postos com `erro=None` são processados; os
outros ficam congelados. Testado (`removidas == 0` com o posto em erro).

**Hash só sobre `CAMPOS_CADASTRO`.** Nada de derivado (`valor_hora`,
`custo_mensal`): mexer numa constante do ETL marcaria as 776 agendas como
alteradas no mesmo dia e o histórico viraria lixo.

**É `detectado_em`, não `alterado_em`** — o cadastro muda às 14h e o ETL só vê
às 02:50 do dia seguinte. Não inventar precisão que o dado não tem.

Dia reconstruído é remontado com a régua de HOJE (piso, fator) pelo mesmo
`montar_payload()` do ETL — é isso que torna as datas comparáveis. A régua
daquele dia fica em `mc_execucao.parametros`.

`export_medico_custo.py` aceita `--dry-run` (não grava JSON nem histórico, só
imprime o que faria) e `--sem-historico`. Falha do RDS **nunca** derruba o ETL:
o JSON já foi gravado e perde-se um dia de histórico, não a página.

### Realizado do card "Nominal × realizado" — de onde sai

Do **mesmo** `json_consolidado/consolidado_mensal_por_posto.json` que alimenta o
KPI Custo Médico: soma do campo `medico` por posto, chave `"AAAA-MM"`. Julho/2026
= R$ 2.219.216,89, que bate com o card daquela página.

Era um `2219217` digitado no HTML com a palavra "julho" na frase — em setembro
a tela ainda diria "julho" e o percentual estaria errado sem avisar. Agora a
página escolhe sozinha o **último mês fechado** (nunca o corrente, que tem
poucos dias lançados) e nomeia o mês que usou.

O realizado só tem quebra **por posto e mês**. Então o card se desliga sozinho
(mostra `—` e diz por quê) quando há filtro de **especialidade** ou **busca** —
nesses casos o nominal encolhe e o realizado não, e a diferença seria inventada.
Com filtro de posto ele soma só os postos que entraram no nominal.

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

## WPP — Controle e auditoria dos disparos (`/wpp/previsao`) — 2026-08-10

Responde "quantas mensagens saem hoje, para quem, quando, por quê — e por que
NÃO", linha a linha, em linguagem leiga. Nasceu do incômodo do Cristiano de
olhar a lista de campanhas e não ter como afirmar se 0 envio era regra de
negócio ou ferramenta quebrada.

| Arquivo | Papel |
|---|---|
| `wpp_previsao.py` | Engine da análise (thread de fundo, SOMENTE-LEITURA) |
| `wpp_cobranca_routes.py` | Rotas `/wpp/previsao` + `/wpp/api/previsao/{start,status,resultado}` |
| `wpp_previsao.html` | Central de controle (data, resumo, linhas com motivo) |
| `wpp_campanhas.html` | Botões "Calcular previsão de hoje" / "Ver em detalhes" |

**Três regimes por data:**
- **Passado** = auditoria das tabelas `envios`/`nao_enviados` (não toca SQL
  Server). Bloqueios de cadência (intervalo global, telefone repetido na
  rodada) **não são gravados** por decisão de projeto (`_MOTIVOS_NAO_CONTABILIZAR`
  no `wpp_cobranca_db`), então a auditoria de dias passados não os mostra — a
  tela avisa isso.
- **Hoje** = simulação fiel do cron cruzada com o realizado do dia (envios do
  dia aparecem como "enviada às HH:MM"; erro_api do dia entra como "erro").
- **Futuro** = simulação com a régua deslocada (`_campanha_ajustada`): atraso
  desloca limites para baixo, pré-vencimento para cima, cliente_novo desloca a
  janela de 7d. Marcada como ESTIMATIVA na tela.

**Fidelidade por REUSO, não por cópia:** o engine importa do próprio
`send_whatsapp_cobranca` as funções puras (`buscar_faturas`, `limpar_telefone`,
`montar_params_template`, bodies de template) e repete a ordem da rodada
(sort por `hora_fim`) e as duas políticas de dedupe (`ignorar_intervalo` vs
intervalo global). Se o cron mudar, a previsão muda junto. **Nenhum arquivo do
cron foi alterado** — e deve continuar assim: qualquer regra nova no cron
precisa aparecer aqui, de preferência importando a função em vez de duplicar.

**SEGURANÇA — o módulo nunca envia nada.** Só SELECTs (SQL Server) e leitura
do SQLite. Jamais chamar `enviar*`/`registrar_*` de dentro dele.

- Roda em **thread de fundo** com progresso (mesmo padrão do cache-refresh):
  varre os 13 postos e pode levar minutos; request síncrono derrubaria o
  worker único do gunicorn (nginx corta em 60s).
- Import **lazy** nas rotas: problema no módulo não derruba o serviço, só a tela.
- Posto sem conexão/erro de query vira ALERTA visível (`erros_postos`) — nunca
  silêncio, senão "0 previstas" mente (mesma lição do medico_custo: falha de
  posto não pode parecer economia).
- Teto de 4.000 linhas de detalhe por campanha no JSON; contagens sempre
  completas e a truncagem é avisada na tela (nada de teto silencioso).
- Todo motivo tem código técnico + frase leiga (`MOTIVOS_LEGENDA`) — padrão
  "todo selo diz por quê" do medico_custo.
- Custo Meta estimado = previstas × preço da CATEGORIA real do template
  (UTILITY/MARKETING, lida da API `/templates`) × dólar ao vivo (awesomeapi,
  mesma fonte do wpp_dashboard). Fallback de cotação avisado na tela; preços
  ajustáveis por env (`WPP_PRECO_MKT_USD`, `WPP_PRECO_UTIL_USD`, `WPP_USD_BRL`).
  Só para campanhas com `enviar_meta=1`; a conta inteira aparece ao lado do valor.
- `wpp_dashboard.json` ficou 70 dias congelado (31/05→10/08/2026) porque o
  export morria com fatia de período vazia (`KeyError has_atraso`) — desde
  jun/2026 só falta_medico envia via Meta e o recorte de cobrança do mês ficou
  vazio. Ao mexer no `export_wpp_dashboard.py`, testar sempre o caso "período
  sem cobrança" e o df 100% vazio.
- Rodada do robô = `*/15 min` (`sync_wpp.sh` no cron) — é daí que sai o "que
  horas vai". Se o agendamento mudar, atualizar `RODADA_MIN`.

### Robô-fiscal TEMPORÁRIO — `monitor_previsao_wpp.py` (2026-08-10 → 2026-08-17)

**TEMPORÁRIO por pedido do Cristiano: roda NO MÁXIMO 1 semana.** Criado após o
incidente do fd leak (Py3.14 não fecha conexão sqlite no GC; o cron de
cobrança morreu no meio da rodada por DIAS sem ninguém perceber).

- Cron: `/etc/cron.d/monitor-previsao-wpp` — hora em hora 08h–20h, usuário
  www-data (para os arquivos de estado da previsão continuarem graváveis pela
  página). Log: `/var/log/relatorio_h_t/monitor_previsao_wpp.log`.
- A cada hora: refaz a análise de hoje (mantém `/wpp/previsao` fresca),
  compara enviadas com a hora anterior e manda e-mail para o Cristiano com o
  quadro por campanha + link da página. Previstas paradas / traceback no log
  do cron / erro de API / posto fora ⇒ assunto vira `[ALERTA]` com tail do
  log do cron no corpo.
- **Expira sozinho** (`EXPIRA_EM = 2026-08-17`): depois disso o script loga
  "EXPIRADO" e sai. Limpeza definitiva: `rm /etc/cron.d/monitor-previsao-wpp`
  e apagar os dois arquivos `monitor_previsao_wpp.*` do repo.

---

## Outros Monitores (`/outros_monitores.html`) — 2026-08-10

Página para monitores de serviços específicos. Primeiro: **Leads criados**.

| Arquivo | Papel |
|---|---|
| `export_monitor_leads.py` + `.sh` | ETL horário (cron `:10`, `/etc/cron.d/monitor-leads`) — conta leads no MySQL `camim_leads_production` e manda e-mail aos inscritos |
| `outros_monitores_routes.py` | Blueprint `/api/monitores/*` (dados + inscrição de notificação) |
| `outros_monitores.html` | Página (tiles, gráficos hora/dia, fontes, switch de notificação) |

- Sem page_key de propósito — mesmo precedente do `monitorarrobos.html`: todo
  usuário logado acessa (não passa por seed_servicos).
- **O MySQL de leads está em UTC** e o servidor local em America/Sao_Paulo. O
  ETL calcula o offset AO VIVO (NOW() do MySQL vs relógio local) — não
  hardcodar −3.
- Inscrição de notificação: tabela `monitor_notificacoes` no `camim_auth.db`
  (email + monitor_key), criada pelo blueprint, consumida pelo ETL. E-mails só
  entre 07h–22h, via credencial `ALARM_EMAIL_*`.
- `leads` não tem índice em `created_at`, mas o volume é baixo (~30/dia) e as
  queries custam ~0,5s. Se o volume crescer, criar o índice antes de apertar a
  frequência.

## Central de Notificação de Problemas + Ciência (2026-08-10)

Nasceu do incidente das campanhas 59 dias mudas: o painel Monitorar Robôs
mostrava "Horrível" o tempo todo e **zero alarmes estavam configurados** — o
sinal existia, ninguém era avisado. Agora o alarme notifica E cobra ciência.

| Peça | Onde |
|---|---|
| Registro de ciência (1 token por destinatário×canal) | tabela `ciencia` no `alarmes.db` (`alarmes_db.py`) |
| Link de ciência anexado a CADA zap/e-mail de alarme | `disparar_alarmes.disparar()` (pré-registra o disparo p/ amarrar tokens) |
| Página pública de confirmação | `/ciencia/<token>` (`ciencia_bp` em `alarmes_routes.py`) — **sem login de propósito**: gestor clica do WhatsApp; o token uuid é a credencial; nginx expõe `location ^~ /ciencia/` sem auth_request |
| Central (registro de envio + ciência) | `central_problemas.html` + `GET /alarmes/api/central` — menu "Central de Problemas" |
| Serviço novo `wpp_campanha` | `status_wpp_campanha()` em `disparar_alarmes.py`: PIOR campanha ativa do posto. Detecta o padrão do incidente (campanha 1 enviando, resto mudo — o `wpp` posto-level não pega) |
| Seed dos alarmes de Horrível | `seed_alarmes_horrivel.py` (idempotente, `--dry-run`): 1 alarme `wpp_campanha`/horrível por posto COM gerente, diário 08:30, e-mail+zap, diretor Cristiano associado |

- O zap dos alarmes sai pela **Evolution API** (texto livre, custo zero) — não
  é o canal Meta pago das campanhas.
- `alarmes_db.get_conn` ganhou o mesmo `_AutoCloseConn` do wpp (fd leak Py3.14)
  — os helpers rodam no worker de vida longa do camim-auth.
- Segunda confirmação de ciência NÃO sobrescreve a primeira (fica o registro
  original de quem/quando/IP).
- `disparar_alarmes` roda a cada minuto pelo cron e dispara 1×/dia no minuto
  exato de `hora_disparo` — o reenvio diário até resolver é isso.

## camim-auth — worker único agora tem THREADS (2026-08-10)

`ExecStart` ganhou `--workers 1 --worker-class gthread --threads 8`. Motivo:
um request longo (`/api/medico_falta/enviar_wpp` mandando dezenas de WhatsApps
síncronos) segurava o worker único e até o `auth_request` do nginx morria →
**qualquer página protegida devolvia 500** enquanto durasse o envio (visto em
2026-08-10 11:26, WORKER TIMEOUT no journal). Threads no MESMO processo
resolvem sem quebrar o `_oauth_states` em memória do login IDCamim —
**não subir `--workers` >1 por causa disso**.

---

## REGRA — Menu lateral: SÓ existe UM, o canônico (2026-08-10)

`js/menu.js` é a ÚNICA fonte de verdade do menu do kpi.camim.com.br: bloco-base
(Buscar, Monitor de Avisos, Monitor de Robôs, Outros Monitores, Central de
Problemas, Painel Antigo, KPI's, Mais Serviços) + seções Indicadores e
WhatsApp + Admin/Sair — idêntico em toda página. O script SUBSTITUI o
`ul.nav-sidebar` em runtime; o HTML local é só fallback sem-JS.

- **Página nova**: incluir `<script src="/js/menu.js"></script>` antes de
  `</body>` e NÃO caprichar no sidebar hardcoded — ele será substituído.
- **Item novo de menu**: editar a lista em `js/menu.js`, NUNCA o HTML de
  páginas individuais (foi assim que viraram 26 menus diferentes).
- `menu_enforce.js` vira no-op quando o canônico está presente.
- **Exceções deliberadas** (menu próprio): família `wpp_*` (produto
  camila1/IDCamim) e `custos_ti*` (sidebar Jinja dinâmico do banco).

## Gestores de posto — fonte única é o CRM (2026-08-10)

`sync_gerentes.py` espelha diariamente o cadastro de gestores do CRM
(Postgres do /opt/crm, `gestores`+`postos`, id_endereco↔letra) para
`gerente_posto` do alarmes.db. A fonte antiga (sis_empresa) tinha wa.me da
clínica/fixo/vazio. **Não editar gerente no /alarmes** — editar em
https://crm.camim.com.br/admin (atalho no /admin do KPI). Posto com vários
gestores ativos: o de MENOR id ganha o alerta; para trocar o titular,
desativar o outro no CRM. Falha de conexão preserva o espelho anterior.

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
