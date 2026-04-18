# Integração Manus — KPI Receita x Despesa

> **Resumo em 3 linhas**
> 1. Manus descobre o KPI em `GET /api/kpis/manifest`.
> 2. Lê o contrato completo e as regras de filtro em `GET /api/receita_despesa/contexto`.
> 3. Responde perguntas chamando `GET /api/receita_despesa/pergunta_assistida?q=...` ou `GET /api/receita_despesa/analise_completa`.

Base URL: **https://teste-ia.camim.com.br**

**Autenticação (obrigatória a partir de 2026-04-18):** todo request deve enviar o header
`X-Manus-Key: <chave>` com a chave de serviço fornecida pelo CAMIM (variável
`MANUS_SERVICE_KEY` no ambiente). Sem esse header, nginx devolve **401** em
`/api/*` e em `/json_consolidado/*`. A chave só deve trafegar via HTTPS.

```bash
curl -H "X-Manus-Key: $MANUS_SERVICE_KEY" \
  https://teste-ia.camim.com.br/api/kpis/manifest
```

---

## Regra de ouro

Antes de responder qualquer pergunta financeira, Manus deve confirmar:

1. **Escopo de posto**: "Você quer analisar **todos os postos**, o grupo **Altamiro**, o grupo **Couto**, ou um **posto específico** (A, B, C, D, G, I, J, M, N, P, R, X, Y)?"
2. **Escopo de período**: "Qual **mês** ou **intervalo de meses**?" (se ainda não ficou claro)

Os grupos mapeiam para:

| Grupo    | Postos                          |
|----------|---------------------------------|
| todos    | A, B, C, D, G, I, J, M, N, P, R, X, Y |
| altamiro | A, B, G, I, N, R, X, Y          |
| couto    | C, D, J, M, P                   |

---

## Endpoints

Todos retornam JSON. Parâmetros comuns: `grupo` (todos/altamiro/couto), `postos` (CSV de códigos, sobrescreve grupo), `mes` (YYYY-MM), `mes_ini` / `mes_fim`.

### `/api/receita_despesa/contexto`
Contrato completo: dimensões, postos, grupos, regra de retirada, meses disponíveis, perguntas obrigatórias. **Manus chama isso primeiro** para entender o KPI.

### `/api/receita_despesa/resumo`
Totais consolidados: `receita_total`, `despesa_total`, `saldo`, `margem`, mais variação MoM e YoY.
Ex.: `/api/receita_despesa/resumo?grupo=todos&mes=2026-03`

### `/api/receita_despesa/serie`
Série mensal (receita + despesa + saldo) para plotar evolução.
Ex.: `/api/receita_despesa/serie?grupo=altamiro&mes_ini=2025-04&mes_fim=2026-03`

### `/api/receita_despesa/crescimento`
Responde "estou crescendo ou encolhendo?" — tendência linear (slope) + variação ponta-a-ponta em janela configurável (`janela=6` por padrão).
Ex.: `/api/receita_despesa/crescimento?postos=A&janela=12`

### `/api/receita_despesa/ranking_postos`
Lista postos ordenados pela variação de receita OU despesa vs. mês anterior OU ano anterior. Resposta direta para "qual posto aumentou o custo em março?".
Ex.: `/api/receita_despesa/ranking_postos?metrica=despesa&base=mes_anterior&mes=2026-03`

### `/api/receita_despesa/composicao`
Top-N linhas que compõem receita ou despesa em uma dimensão.
- Receita: `dimensao=tipo | forma | servico`
- Despesa: `dimensao=plano_principal | plano | tipo`
Ex.: `/api/receita_despesa/composicao?tipo=despesa&dimensao=plano_principal&grupo=todos&mes=2026-03&top=15`

### `/api/receita_despesa/drilldown_variacao`
Explode a variação mês-a-mês por plano/tipo — responde "qual tipo de conta subiu neste posto?".
Ex.: `/api/receita_despesa/drilldown_variacao?tipo=despesa&postos=A&mes=2026-03&base=mes_anterior`

### `/api/receita_despesa/posto_detalhe`
Pacote completo de um posto em um mês: totais, variações, top dimensões.
Ex.: `/api/receita_despesa/posto_detalhe?posto=A&mes=2026-03`

### `/api/receita_despesa/alertas`
Anomalias por posto: linhas onde receita ou despesa ficou acima de `média + 1σ` em janela de 6 meses, ou variação YoY > limiar. Resposta direta para "onde está o risco?".
Ex.: `/api/receita_despesa/alertas?grupo=todos&mes=2026-03&janela=6`

### `/api/receita_despesa/analise_completa`
**Pacote executivo em uma chamada**: resumo + ranking MoM + ranking YoY + composição top-10 receita + composição top-10 despesa + alertas. Use quando o usuário pede análise geral.
Ex.: `/api/receita_despesa/analise_completa?grupo=altamiro&mes=2026-03`

### `/api/receita_despesa/pergunta_assistida`
Roteador heurístico que interpreta a pergunta em linguagem natural (`q=`) e chama o endpoint mais adequado. Use como "porta da frente" quando não souber qual endpoint chamar.
Ex.: `/api/receita_despesa/pergunta_assistida?q=qual+posto+subiu+o+custo+em+marco&grupo=todos&mes=2026-03`

---

## Fluxo recomendado para Manus

```
1. GET /api/kpis/manifest                      → descobre que existe o KPI receita_despesa
2. GET /api/receita_despesa/contexto           → obtém contrato + perguntas obrigatórias
3. Manus pergunta ao usuário: grupo? período?
4. GET /api/receita_despesa/pergunta_assistida (pergunta livre)
   OU
   GET /api/receita_despesa/analise_completa   (análise executiva)
   OU
   Endpoint específico conforme o contrato
5. Manus sintetiza resposta em linguagem natural para o usuário
```

---

## Exemplos de perguntas respondíveis

| Pergunta do diretor | Endpoint que responde |
|---|---|
| Qual posto aumentou o custo em março? | `/ranking_postos?metrica=despesa&base=mes_anterior&mes=2026-03` |
| Qual tipo de conta subiu no posto A? | `/drilldown_variacao?tipo=despesa&postos=A&mes=2026-03` |
| Estou crescendo ou encolhendo? | `/crescimento?grupo=todos&janela=12` |
| O posto A está encolhendo? | `/crescimento?postos=A&janela=12` |
| Altamiro cresceu mais que Couto? | Duas chamadas a `/crescimento?grupo=altamiro` e `/crescimento?grupo=couto` |
| Comparando março/2026 com março/2025, o que piorou? | `/ranking_postos?base=ano_anterior&mes=2026-03` e `/composicao?...` |
| Onde a margem está caindo? | `/analise_completa` e inspeciona `alertas` + `posto_detalhe` |

---

## Regra de retirada

As linhas cujo PlanoPrincipal, plano ou tipo contêm `RETIRADA` ou `CAMPINHO` (qualquer capitalização) são **excluídas automaticamente** — mesma regra da página `kpi_receita_despesa.html`. Para trazê-las de volta, use `&incluir_retirada=true`.

---

## Dados de origem

- 6 JSONs consolidados em `/var/www/json_consolidado/fin_*.json` (protegidos por `X-Manus-Key` ou cookie de sessão)
- Gerados por `export_receita_despesa.py` (ETL do SQL Server de 13 postos)
- Atualizados diariamente pela madrugada
- Arquivos:
  - `fin_receita_tipo.json`
  - `fin_receita_forma.json`
  - `fin_receita_lancamento.json`
  - `fin_despesa_planodeprincipal.json`
  - `fin_despesa_plano.json`
  - `fin_despesa_tipo.json`

---

## Como apontar o Manus para este KPI

Configure o Manus para consultar, em ordem:

1. `https://teste-ia.camim.com.br/api/kpis/manifest` — descoberta
2. `https://teste-ia.camim.com.br/api/receita_despesa/contexto` — contrato
3. `https://teste-ia.camim.com.br/api/receita_despesa/pergunta_assistida?q={pergunta}` — roteador

Todas as chamadas devem incluir `X-Manus-Key: <chave>`. Os endpoints aceitam apenas `GET`.
