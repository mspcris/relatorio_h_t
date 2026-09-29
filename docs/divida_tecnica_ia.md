# Dívida técnica — IA pela OpenRouter

Criado em 2026-09-29, na migração de todas as chamadas de IA do relatorio_h_t para a
OpenRouter (`openrouter.py`). Decisão do Cristiano: **tudo no provedor mais barato**
(`provider.sort = price`) agora, e tratar **um a um** os casos que mandam dado
sensível.

## O risco, em uma frase

A OpenRouter não roda o modelo: ela repassa o pedido a um de vários provedores
(no teste de 29/09 o GPT-OSS-120B foi para "Mancer 2" e depois para "CoreWeave").
Cada provedor tem sua regra sobre **guardar ou treinar com o que recebe**. No
"mais barato", quem recebe o dado muda a cada chamada e não é escolhido por nós.

## Como resolver, por chamada (quando for tratar)

A OpenRouter aceita, no mesmo `extra_body.provider` que hoje só tem `sort`:

| Opção | O que faz | Custo |
|---|---|---|
| `"data_collection": "deny"` | só provedores que **não guardam nem treinam** com o pedido | pode subir um pouco |
| `"zdr": true` | só provedores com **retenção zero** (nada fica gravado) | sobe mais; menos provedores |
| `"order": [...]` + `"allow_fallbacks": false` | fixa provedores conhecidos (ex.: Groq, Cerebras, OpenAI) | preço do escolhido |

Para aplicar só em um caso, passar um `extra_body` próprio naquela chamada em vez de
`openrouter.EXTRA_BODY`. Também existe a opção global na conta
(openrouter.ai/settings/privacy), que vale para todas as chaves — aí deixa de ser
"tratar um a um".

## Chamadas que mandam dado sensível

| # | Onde | Quem aciona | Modelo | O que vai para a IA | Sensibilidade |
|---|---|---|---|---|---|
| 1 | `auth_routes.py` → `POST /api/ia/chat` (chat "IA CAMIM" nas páginas) | usuário logado, em 13 páginas | GPT-OSS-120B (padrão) ou GPT-4.1 | **números financeiros e operacionais da CAMIM** por posto (receita, despesa, rateio, vendas, metas, clientes/vidas, NF×RPS, governo, Liberty, growth), montados por `ia_context_builder.py` ou pela própria página (até 100 mil caracteres); **pergunta livre do usuário + histórico da conversa**; texto de contexto do KPI escrito pelo gestor | **ALTA** — dado financeiro da empresa; e a pergunta é texto livre: se alguém digitar nome/CPF de paciente, vai junto |
| 2 | `auditoria_llm_relatorio.py` (cron 06:00) | robô, 1×/dia | GPT-5-mini (`OPENAI_AUDITORIA_MODEL`) | itens de `auditoria_financeira.json`: despesas anômalas com **fornecedor, descrição, valor, posto** | **ALTA** — fornecedor pode ser pessoa física (médico PJ, prestador) |
| 3 | `ia_router_openai.py` → `POST /api/ia/pergunta` (rota legada, `js/ia_client.js`) | página que ainda use o cliente antigo | GPT-4.1 | contexto arbitrário enviado pela página + pergunta | **MÉDIA/ALTA** — depende da página; rota legada, candidata a remoção |
| 4 | `app.py` → `_embed_query` (busca semântica da home) | usuário logado | text-embedding-3-small | **só o texto digitado na busca** | BAIXA — mas é texto livre |

A página `qualidade_agenda.html` manda nome de posto e contagens (sem nome de médico
ou paciente — conferido em 29/09). `agenda_dia.html` e `indicadores_vg.html` mandam
**só a pergunta**, sem dado da tela.

## Chamadas sem dado sensível (podem ficar no mais barato)

| Onde | O que vai |
|---|---|
| `build_search_index.py` (manual) | texto visível das páginas HTML do KPI, sem dados (só layout/rótulos) |
| `custos_ia.py` → `extract_groq_from_image` | print da tela de custos da Groq |

## Código morto ou parado (não migrar mais do que o necessário)

- `analyze_groq.py` + `orquestrador.py` + `ia_router.py`/`ia_orquestrador.py`: o
  serviço `ia-groq` está **inactive** na VM. Migrado por consistência; se continuar
  parado, apagar.
- `analyze_groq_bac.py`: backup, ainda chama a Groq direto. Apagar.
- `openai_client.py`: ninguém importa. Apagar.
- `llm_client_anthropic.py`: **continua direto na Anthropic** (fora do pedido de
  29/09, que era Groq + OpenAI). É a opção "anthropic" do chat.

## Centro de custo — ainda falta

A página Custos com IA (`custos_ia.py`) lê OpenAI (Costs API) e Groq (print). Com a
migração, o gasto real passa a estar na **OpenRouter**, que tem API:
`GET /api/v1/key` devolve o uso da chave (por projeto, já que cada projeto tem a sua);
com uma *provisioning key*, `GET /api/v1/keys` lista todas as chaves e o uso de cada
uma — é isso que dá o custo por projeto no centro IA do Custos de TI. OpenAI e Groq
vão zerar a partir de 29/09 (menos o que ainda sair por Anthropic).

## Queda para o provedor antigo

Sem `OPENROUTER_API_KEY` no ambiente, cada cliente volta sozinho para Groq/OpenAI
direto (`openrouter.ativo()`). As chaves `GROQ_API_KEY`/`OPENAI_API_KEY` continuam no
`.env` por isso — revogar só depois de algumas semanas estável.
