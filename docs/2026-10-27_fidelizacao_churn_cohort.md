# KPI Fidelização — churn dos gestores, cohort e curva de sobrevivência (2026-10-27)

Página: `kpi_fidelizacao_cliente.html` (servida pelo Flask, de
`/opt/camim-auth/templates/` — a cópia em `/var/www` não é usada).

## 1. Churn = perda líquida de pagantes (decisão do Cristiano)

Caso que originou: o gestor Vinícius (Realengo) não conseguia bater a base de
fevereiro e maio com a planilha dele. A conta que os gestores fazem é:

```
perdidos do mês = vendas do mês − (pagantes do mês − pagantes do mês anterior)
churn %         = perdidos do mês ÷ pagantes do mês anterior
```

- **Pagantes** = mensalidades PAGAS no mês = `mensalidade_qtd` de
  `json_consolidado/consolidado_mensal_por_posto.json` (o mesmo número do KPI Metas).
- **Vendas** = regra única de venda de plano =
  `json_vendas/vendas_mensal.json` → `por_posto[P].qtd_vendas`.
- O Cristiano sabe que a conta tem defeitos (conta mensalidades, não pessoas;
  oscila com atraso de pagamento; o perdido pode sair negativo) e quer assim:
  *"Eu sei que está errado, mas é assim que verificamos churn. Basta explicar
  na tela o que está sendo calculado e como."*
- Conferido: jul/26 Realengo dá 16 perdidos contra 13 da planilha dele — a
  diferença está nas vendas (268 pela regra única × 265 na planilha).

### Como a página ficou

1. **Nota curta no topo** ("Como medimos churn"): não olhamos cancelamentos; o
   churn é baseado em mensalidades pagas e nas vendas, como os gestores fazem.
2. **Card "Churn dos postos"** (`renderChurnGestor()`): últimos 12 meses, com a
   conta escrita linha a linha; `renderChurnGestorPostos()` abre por posto. O
   filtro do topo só destaca a linha e escolhe o mês da tabela por posto — a
   tabela sempre mostra os 12 meses (o `selMonth` padrão é 12 meses atrás e
   abriria a tabela em 2025).
3. **Cohort** como leitura de apoio, com bloco explicativo ANTES dos números.
4. **Tabela de cancelamentos do ERP no fim**, renomeada: *"Pedidos de
   cancelamento registrados no ERP — só referência, não é o nosso churn"*,
   coluna **"% cancelado"**. Nunca chamar cancelamento de churn em tela nova.
5. Textos visíveis da página acentuados (só texto; atributos como `data-mes`
   não podem ser tocados).

## 2. Cohort — consulta própria em vez da `PC_Fin_Fidelizacao`

A procedure do ERP dava números impossíveis (retenção de 100 % e acima). Três
defeitos medidos:

| Defeito da procedure | Efeito | Na consulta nova |
|---|---|---|
| Só admitidos nos últimos 2 anos | curva de 24/36 meses sem turma com idade | janela de `FIDELIZACAO_ANOS` (padrão 5) anos, desde 1º de janeiro |
| `count(*)` de linhas de receita | cliente com duas mensalidades do mês contava 2 | `COUNT(DISTINCT idCliente)` |
| Mês seguinte contava qualquer cliente com aquela data de admissão | retenção passava de 100 % | só conta quem é da turma (`#coorte`) |

- SQL: `sql_fidelizacao/cohort.sql`. Turma = cliente ativo, matrícula > 0, não
  PJ, admitido no mês, com a mensalidade do mês da admissão paga. "Ficou no mês
  X" = a mensalidade de referência X foi paga, em qualquer data.
- `export_fidelizacao.py` → `run_extraction_cohort()`: uma consulta por posto,
  `ini` em DD/MM/YYYY; descarta referência maior que o mês corrente; troca as
  linhas do posto (DELETE + insert) e, se o posto falhar, **mantém as antigas**.
- `--procedure` volta ao caminho antigo (só para comparar).
- `fidelizacao.db` ganhou índice único `ux_fidelizacao_chave`; `init_db()`
  remove repetidos mantendo o `MAX(id)` (havia gravação duplicada).
- Resultado em produção (27/10): turmas de 2021-01 a 2026-10, retenção máxima
  da rede 85,2 %. Os 64 casos de 100 % que sobraram são turmas pequenas de um
  posto só (ex.: 2 admitidos, 2 pagando).

### Curva de sobrevivência

- Botões **12m (padrão), 24m, 36m** (`survivorHorizonte`).
- Ao escolher 24/36, se a turma selecionada não tem idade suficiente, a página
  volta para `hoje − (N+1)` meses (`definirMes()`), que muda também o filtro do
  topo.
- Campo de mês (`#survivorMes`) no próprio gráfico, sincronizado com o topo.
- Meses de referência iguais ou posteriores ao mês corrente ficam fora
  (eram eles que davam 100 % no fim da curva). `#survivorAviso` diz quando a
  turma ainda não tem idade para o horizonte.
- Tabela do cohort inteira, sem rolagem interna nem cabeçalho fixo.

## 3. Growth — base recalculada

`export_growth.py` só recalculava o mês corrente, e meses antigos ficavam com a
foto de quando foram gravados (degrau falso em R: 4.929 → 4.193). Agora:

- recalcula os últimos `GROWTH_RECALC_MESES` (padrão 7) a cada execução;
- `--recalcular-tudo` refaz desde 2024-01 (rodado em 27/10, terminou 16:38).
- Depois: R de out/25 a out/26 sem degrau (ex.: mar/26 3.492, abr/26 3.581).

## 4. Operação

| Robô | Cron (root) | Chamada |
|---|---|---|
| Fidelização | 02:30 | `run_export_fidelizacao.sh` → `python3 export_fidelizacao.py` (caminho novo, sem argumento) |
| Growth | 03:00 | `export_growth.sh` → `python3 export_growth.py` |

Backups feitos antes da recarga de 27/10:
`/root/fidelizacao.db.bak-20261027` e `/root/growth_dashboard.json.bak-20261027`.

## 5. Pendências conhecidas

- Turmas abr–out/2024 com retenção de 1–16 % parecem falha de carga antiga; não
  investigado (não pedido).
