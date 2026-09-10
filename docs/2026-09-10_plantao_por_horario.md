# Plantão por horário, custo real e pagamento em dobro

**Data:** 10 de setembro de 2026
**Origem:** o Cristiano não conseguia entender a tela `/ctrlq_desbloqueio`
("eu vejo, leio, e nunca consigo entender claramente o que aconteceu").
**Commit da entrega:** `4f6414e` — deploy concluído, ETL rodado nos 13 postos.
**Página de análise:** https://claude.ai/code/artifact/778687a0-92b5-4237-bbfd-99f62bf4f779

---

## 1. O problema de leitura

A tela ancorava tudo no cadastro da agenda (`idEspecialidade`) e comparava a
agenda **com ela mesma**. Num plantão de ordem de chegada isso nunca responde a
pergunta certa: o turno é fixo e os médicos passam por ele. Quando um sai e
outro entra, a agenda nova não tem "antes" nenhum, e a tela mostrava diferença
de dois centavos do ERP (`R$ 2.490,02 → R$ 2.490,00`) enquanto escondia a troca
de médico.

**O "antes" verdadeiro é quem cobria aquele horário.**

---

## 2. O caso do Dr. Marcelo, relido

Realengo, clínica geral, cadastro 2655, aberto em 10/09/2026.

| Turno | Antes | Depois |
|---|---|---|
| Sexta 07–19 | só Dra. Alessandra Becker — R$ 1.186 | Alessandra + Marcelo — R$ 2.431 |
| Sábado 07–19 | Ilona + Luísa Portugal — R$ 2.375 | Ilona + Luigi + Marcelo — R$ 3.620 |

**Mas não houve aumento de custo.** Ele já fazia sexta, sábado e domingo como
plantão avulso **desde 3 de abril**: 61 plantões e **R$ 73.355** pagos até
13/09. O que mudou em 10/09 foi o **contrato**, não a escala. O dinheiro já
saía; só não aparecia na tela, porque a tela mostra a agenda fixa e ignora o
avulso.

O que continua verdadeiro: com a agenda nova ele faz sábado 07–19 e sábado
19–07, ou seja **24 horas seguidas**, e **48 horas na semana** no posto.

---

## 3. O custo real é 30 % maior do que qualquer tela mostrava

Clínica geral em Realengo, 19 semanas medidas (mai–set/2026):

| | Por semana |
|---|---|
| Agenda fixa (o que a tela via) | R$ 21.972 |
| Plantão avulso (`Temporario = 1`) | R$ 9.424 |
| **Custo real** | **R$ 31.396** |

O avulso **nunca ficou abaixo de 20 %** do total. Sexta, sábado e domingo
tiveram avulso em **19 dos 19 dias** do período — é a escala normal do fim de
semana, montada fora do cadastro fixo. Quarta é o oposto: 4 de 20 dias.

**Por isso o avulso não sai da conta principal.** A aba separada para estudo
vai existir, mas tirar o avulso da visão principal é exatamente o que fazia a
tela mentir.

Na rede, plantão de clínica geral soma **R$ 128.367/semana**, ou
**R$ 557.755/mês** — e isso só conta a agenda fixa.

---

## 4. Não dá para separar "cobriu falta" de "entrou a mais"

O pedido era distinguir o avulso que cobre uma falta (não muda o custo) do que
entra por cima (aumenta). O registro de falta não sustenta essa conta:

| | |
|---|---|
| Linhas de FERIADO em `Cad_MedicoFalta` | 30.956 (semeadas até 2099) |
| Faltas reais (clínica geral + pediatria, Realengo) | 468 |
| Faltas registradas em 2026 | **3** (todas de pediatria) |
| Plantões avulsos no mesmo período | 233 |

Detalhe de schema: a falta real tem **`DataFalta` NULL** e vive em `DataHora` /
`DatahoraFim`. Só o feriado preenche `DataFalta`.

**Consequência de operação:** hoje o sistema não sabe se um plantonista faltou.
Enquanto a recepção não registrar, o painel mostra que o dia custou mais e
mostra quem estava a mais, mas **não afirma o motivo**.

A régua passa a ser outra, e não depende de ninguém preencher nada: **o custo do
dia contra o padrão daquele dia da semana**.

---

## 5. Pagamento em dobro — comprovado no financeiro

O Cristiano corrigiu o caminho duas vezes, e as duas mudaram o resultado:
*"não é algo que posso deduzir, já que tenho o dado no banco"* e
*"posso validar se o médico atendeu alguém naquele dia"*.

**As duas chaves:**
- `Fin_Despesa.idEspecialidade` liga a despesa ao cadastro do plantão.
  `DataVencimento` = dia do plantão.
- `cad_prontuario.idEspecialidade` diz por qual cadastro cada paciente foi
  atendido. É a contraprova.

### Resultado: 10 plantões, R$ 10.750,97 em 12 meses

| Médico | Posto | Casos | Valor |
|---|---|---|---|
| Fernanda Banaszeski | Campo Grande | 6 | R$ 6.780,00 |
| Marcelo Lopes Cardoso | Realengo | 2 | R$ 2.315,99 |
| Isabela Cardoso Borges | Realengo | 1 | R$ 1.129,99 |
| Pietro Francesco Oliveto | Realengo | 1 | R$ 524,99 |

### O caso de Campo Grande continua ativo

A Dra. Fernanda tem **dois cadastros fixos ativos**, `1039` e `1405`, nenhum
desativado. Comparadas as **215 colunas**, diferem em 20 — e **todas as 20 são
de outros dias**:

| Dia | Cadastro 1039 | Cadastro 1405 |
|---|---|---|
| Quarta | 19:00–07:00 · R$ 1.130 | — |
| Quinta | 19:00–07:00 · R$ 1.130 | — |
| **Sexta** | **07:00–19:00 · R$ 1.130** | **07:00–19:00 · R$ 1.130** |
| Sábado | 19:00–07:00 · R$ 1.170 | — |

Na sexta são idênticos: mesmo horário, mesmo valor, mesma sala, mesma
modalidade. O 1405 nasceu em 10/12/2025, quando o 1039 já cobria a sexta.

Em 21/08/2026 os atendimentos dos dois se **intercalam minuto a minuto** (05:24
no 1039, 07:34 no 1405, 10:56 no 1039…): uma médica, um plantão das 5h24 às
18h47, pacientes divididos entre duas agendas idênticas na tela da recepção.

O financeiro de Campo Grande **já vinha cancelando a segunda despesa à mão** —
em 07/08 e 14/08 a conta do 1405 foi cancelada. Em 21/08 passou e as duas foram
pagas. **Ação que resolve: desativar o cadastro 1405.**

### A objeção do plantão emendado foi testada e não se aplica

O Cristiano levantou que ela emenda plantões (quinta 19h–07h termina na sexta
às 7h). O cadastro 1039 gera **uma despesa por dia da própria agenda**: 23 na
quarta, 26 na quinta, 30 na sexta, 26 no sábado. O plantão de quinta à noite
vence na quinta. Na sexta o 1039 já cobra a sexta, e o 1405 cobra outra vez.

### Correções que o Cristiano provocou nos meus números

| Versão | Valor | O que estava errado |
|---|---|---|
| 1ª | R$ 27.812 | somei `Valor` no lugar de `ValorPago` |
| 2ª | R$ 13.642 | contei as despesas de R$ 0,01 |
| **Final** | **R$ 10.751** | 3 casos eram despesa em dia diferente do plantão |

**As despesas de R$ 0,01 são prática deliberada:** quando o médico tem duas
especialidades, lança-se o valor integral numa e R$ 0,01 na outra, porque o ERP
obriga a pôr valor. São 332 só em Jacarepaguá. **Nunca contar.**

### Critério final do alerta

Duas ou mais despesas **não canceladas**, **pagas**, de **R$ 200 para cima**, do
mesmo médico, no mesmo dia, na **mesma hora de início** e na **mesma
especialidade**, com o cadastro **vigente naquela data**. Depois, contagem de
atendimentos em `cad_prontuario` por cadastro.

Testes que excluem falso positivo: `Parcela`/`Parcelas` (2 em 16.777) e
`idDespesaDestino`/`DataAgrupamento` (4). Nenhum explica os casos.

### Ressalvas antes de cobrar alguém

- `idusuarioInclusao` vem **vazio em todos os postos**. Só se sabe quem
  **liberou** o pagamento (`idUsuarioPagamentoAuto`), que pode ter recebido
  ordem de terceiro.
- **Campinho, Madureira e Rio das Pedras ficam fora**: `idEspecialidade` é 100 %
  NULL na despesa deles. Não é zero, é buraco de dado.
- A causa provável em Campo Grande é cadastro duplicado que ninguém apagou.

---

## 6. Alertas de jornada

**Acima de 24 h/semana no mesmo posto** — 15 casos na rede (salas e aparelhos
cadastrados como médico excluídos pelo piso de R$ 200):

| Médico | Posto | Horas |
|---|---|---|
| Luanna Cristina dos Santos Bessa | Realengo | 60 |
| Fernanda Santana Banaszeski | Campo Grande | 60 |
| Janaina Bellerophonte Alevato | Anchieta | 48 |
| Thayse Gonçalves Fernandes | Campo Grande | 48 |
| Marcelo Lopes Cardoso | Realengo | 48 |

**Plantões encostados entre postos** (menos de 2 h entre sair de um e entrar em
outro) — 7 casos. Ilona sai de Campo Grande domingo 07:00 e entra em Realengo
segunda 07:00. **Luis Gustavo Maiolini soma 95 h em três postos.** Zero
sobreposição no relógio: o problema é deslocamento e descanso.

---

## 7. O que foi entregue na tela (commit `4f6414e`)

Cada registro passou a mostrar um **resumo em português com % de certeza**, e
dois botões: *Ver cobertura do turno* e *Ver auditoria* (o conteúdo antigo,
preservado inteiro, em modal).

**A % de certeza é conta, não palpite.** Começa em 100 e cada buraco de
evidência desconta um valor fixo, **listado na própria tela**:

| Desconto | Motivo |
|---|---|
| −30 | sem auditoria no posto |
| −30 | cadastro sem dia ativo |
| −20 | gatilho estimado, não lido da auditoria |
| −15 | estado anterior reconstruído, não fotografado |
| −15 | algum dia sem horário no cadastro |
| −10 | sem foto anterior em `Cad_EspecialidadeHistorico` |

Medido em produção: Realengo e Campinho deram 65 %, os dois de Del Castilho
deram 100 % (gatilho lido e foto existindo). A régua discrimina.

**Registro com data fim vencida aparece por padrão** e os botões funcionam nele.
Enquanto o ERP criar registro assim, ele continua ativo, valendo e custando.

### Arquivos

| Arquivo | Papel |
|---|---|
| `sql_ctrlq_desbloqueio/sql_ctrlq_desbloqueio_cobertura.sql` | todas as agendas ativas da mesma especialidade no posto |
| `ctrlq_desbloqueio_cobertura.py` | turno, carga do médico, resumo e certeza |
| `ctrlq_desbloqueio.py` | `fetch_cobertura` / `merge_cobertura` |
| `ctrlq_desbloqueio.html` | `renderResumo`, `blocoCobertura`, `blocoAuditoriaCompleto`, modais |

- O SQL principal passou a trazer `{Dia}OrdemChegada/Internet/Telefone` — sem
  isso `plantao` vinha sempre falso.
- **Mesmo turno** = começa até 1 h de diferença **ou** um horário cobre mais da
  metade do outro. 07–19 e 08–19 são o mesmo plantão de dia; 07–19 e 19–07 não.
- Falha na cobertura **não derruba o registro**: fica sem a leitura nova e o
  resumo desconta certeza dizendo por quê.

---

## 8. Decisões do Cristiano

1. **Quem entra na leitura por horário:** clínica geral e pediatria em ordem de
   chegada, **sem corte de horas** (caiu o mínimo de 11 h que eu propus).
   Agenda marcada fica na leitura por cadastro.
2. **Limite de jornada:** 24 h/semana no mesmo posto, e o mesmo alerta para quem
   soma demais na rede.
3. **Intervalo mínimo entre postos:** 2 horas.
4. **Plantão avulso:** aba separada para estudo, mas **não sai da conta
   principal**.
5. **Histórico:** para sempre, sem guardar em lugar novo — a foto diária já está
   no banco de cada posto desde dezembro de 2022.
6. **Cadastro duplo (mesmo médico, dia e horário):** **nunca é legítimo**.
7. **Caso Marcelo 15/05** (dois avulsos, mesma especialidade, valores
   diferentes): vai para lista de **conferir caso a caso**.
8. **Despesa em dia diferente do plantão:** vira **alerta separado**.

### Sobre usar IA

Não para a conta. Cobertura, custo do turno e alertas são aritmética sobre
cadastro, e um erro raro aí passa despercebido justamente no número que decide
contrato. IA cabe no texto livre do ERP (`ObservacaoDesbloqueio`,
`vw_Sis_Historico`) e num botão "não entendi este caso", sob demanda, uma
chamada por clique — nunca no cron de 30 min × 13 postos.

---

## 9. Descobertas de banco que valem para outros relatórios

- **`Cad_EspecialidadeHistorico` é máquina do tempo:** foto **diária** de
  **todas** as agendas ativas, não só das alteradas. Em Realengo, 94.998 linhas
  desde 29/12/2022. Tem `idMedico`, `Especialidade`, `Desativado`, `Temporario`,
  `DataPlantao`.
- **`Temporario = 1` é plantão avulso de um dia**, com `DataPlantao` preenchida.
  Depois do plantão vira `Desativado = 1` — para série histórica é preciso
  incluir desativadas.
- **Literal de data comparado com coluna `datetime` exige DD/MM/YYYY**
  (`SET DATEFORMAT dmy`); com coluna `date`, ISO funciona. `DataHora >=
  '2026-08-01'` estoura "valor fora do intervalo"; `DataFalta >= '2026-04-01'`
  passa.
- **`Fin_Despesa` não tem schema igual entre postos:** `idLancamentoServico`,
  `idSalarioPagamento` e `ValorMedicoCtrlQ` não existem em Realengo e derrubam a
  query em 8 dos 13 postos.
- **Dado sujo de horário:** a mesma coluna devolve `07:00`, `07:00:00`, vazio e
  `'  '` (dois espaços). 14 de 116 linhas de agenda em Realengo sem horário.
- **Postos C e M dão timeout** com 13 conexões paralelas; com 7 workers e 3
  tentativas passam. **Posto que falha não pode virar zero.**
- **`r.sem` no pandas devolve o método `Series.sem`** (desvio padrão), não a
  coluna. Gerou "Invalid Date" na tela sem erro nenhum.

---

## 10. Pendente

1. **Alerta de pagamento em dobro** sobre `Fin_Despesa`, com as três contagens:
   dois cadastros para o mesmo turno, um cadastro gerando várias contas, e
   despesa em dia diferente do plantão. É o único item que já custou dinheiro.
2. **Aba de estudo dos plantões avulsos.**
3. **Custo do dia** contra o padrão do dia da semana, com clique abrindo quem
   estava no turno.
4. **Os dois alertas de jornada como filtro** na lista.
5. **Desativar o cadastro 1405** da Dra. Fernanda em Campo Grande — ação de
   operação, não de código.
6. **Conferir com o financeiro** os R$ 60 mil em aberto levantados antes da
   depuração final (a lista precisa ser refeita com o critério de hoje).
