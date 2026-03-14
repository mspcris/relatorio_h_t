MANUAL_DE_RACIOCINIO_PARA_IA_DO_SISTEMA_RELATORIO_HT

OBJETIVO DESTE DOCUMENTO

Este documento ensina como um agente de Inteligência Artificial deve raciocinar sobre os dados do sistema relatorio_h_t.

O objetivo não é apenas entender os dados, mas entender como analisá-los corretamente.

A IA deve conseguir responder perguntas como:

por que a despesa aumentou

por que a receita caiu

qual tipo de conta causou determinada variação

qual indicador está fora do padrão

A IA deve sempre buscar explicar as causas, e não apenas repetir números.

REGRA FUNDAMENTAL DO SISTEMA

Cada página KPI é independente.

Isso significa:

nenhum KPI depende de cálculos feitos por outro KPI

KPIs podem consultar as mesmas tabelas, mas não compartilham lógica

cada página possui sua própria regra de negócio

A IA nunca deve assumir que dados de um KPI podem ser usados para explicar outro KPI.

PRINCIPAIS DOMÍNIOS DE DADOS DO SISTEMA

O sistema possui os seguintes domínios de dados:

Financeiro
Consultas médicas
Mensalidades
Vendas
Clientes
Prescrições
Metas comerciais
Notas fiscais
Especialidades médicas (CTRLQ)
Índices econômicos

Cada domínio possui suas próprias regras de interpretação.

DOMÍNIO FINANCEIRO

Este domínio contém as informações financeiras da empresa.

Ele é representado principalmente pelo KPI:

KPI RECEITAS X DESPESAS

Este é o único painel que representa a realidade contábil completa da empresa.

ESTRUTURA DAS CONTAS FINANCEIRAS

As contas são organizadas em três níveis:

PLANO PRINCIPAL
PLANO DE CONTAS
TIPO DE CONTA

Exemplo:

Despesas Operacionais
→ Recursos Humanos
→ Salário Funcionário

Porém, para análise de variação financeira, o sistema utiliza:

TIPO DE CONTA

Portanto, quando a IA responder perguntas como:

"por que a despesa aumentou?"

ela deve sempre comparar TIPO DE CONTA entre períodos.

COMO ANALISAR VARIAÇÕES FINANCEIRAS

Para responder perguntas sobre aumento ou queda de despesas ou receitas, a IA deve seguir o seguinte método.

Passo 1
identificar os períodos comparados

Passo 2
agrupar os dados por tipo de conta

Passo 3
calcular a variação entre os períodos

Passo 4
ordenar os resultados pelo maior impacto financeiro

Passo 5
identificar os principais responsáveis pela mudança

A resposta deve sempre explicar:

quais tipos aumentaram
quais tipos diminuíram
quais tipos surgiram
quais tipos desapareceram

DOMÍNIO DE MENSALIDADES

Mensalidades são a principal fonte de receita da empresa.

Os KPIs relacionados a mensalidades analisam:

valor arrecadado
evolução mensal
crescimento ou queda da base de clientes

Muitos KPIs utilizam mensalidades como base de comparação para medir eficiência de custos.

KPI CUSTO ALIMENTAÇÃO

Este KPI mede quanto a empresa gasta com alimentação de funcionários.

A análise compara:

despesa com alimentação
versus
receita de mensalidades

Isso permite avaliar se o custo está dentro de um nível saudável em relação à principal receita.

KPI CUSTO MÉDICO

Este KPI mede o custo com médicos.

A lógica é semelhante ao KPI anterior.

A comparação é feita entre:

custo médico
versus
receita de mensalidades

Este KPI não soma nem utiliza dados do KPI Custo Alimentação.

DOMÍNIO DE VENDAS

A empresa vende planos de saúde.

Cada venda gera novos clientes.

Cada cliente gera mensalidades.

O KPI de vendas analisa:

quantidade de contratos vendidos
evolução das vendas
desempenho comercial

DOMÍNIO DE CLIENTES

Este domínio analisa a base de clientes.

Os dados são classificados em três grupos.

VIDAS
todos que se consultam na empresa

BENEFICIÁRIOS
clientes que possuem plano

BENEFICIÁRIOS PJ
clientes vinculados a contratos empresariais

DOMÍNIO DE PRESCRIÇÕES

Este domínio monitora o uso de prescrições médicas digitais.

Permite analisar:

quais médicos adotaram a tecnologia
quais médicos ainda utilizam métodos antigos
nível de adesão à prescrição digital

DOMÍNIO DE METAS

O KPI de metas permite:

definir metas comerciais
acompanhar metas em tempo real
comparar desempenho atual com anos anteriores

DOMÍNIO DE NOTAS FISCAIS

O KPI Notas RPS analisa a emissão de notas fiscais.

Ele permite acompanhar:

RPS emitidos
RPS convertidos em notas fiscais
pendências fiscais

DOMÍNIO DE ESPECIALIDADES MÉDICAS (CTRLQ)

CTRLQ significa Quadro de Especialidades.

Este domínio não possui relação com dados financeiros.

Ele analisa dados estruturais do corpo médico.

KPI CTRLQ

O painel CTRLQ apresenta um retrato da estrutura médica da empresa.

Os dados são analisados sempre no último dia de cada mês.

O objetivo é registrar como está a estrutura médica naquele momento.

INFORMAÇÕES ANALISADAS NO CTRLQ

O painel registra dados como:

lista de médicos ativos
especialidades médicas
médicos com RQE
médicos PJ
estrutura das agendas médicas

Esse painel funciona como um snapshot mensal da estrutura médica.

IMPORTANTE SOBRE CTRLQ

O CTRLQ não deve ser usado para análises financeiras.

Ele pertence exclusivamente ao domínio de estrutura médica e agendas.

DOMÍNIO DE CONSULTAS

O KPI consultas analisa o comportamento das consultas médicas.

Permite avaliar:

faltas de pacientes
faltas de médicos
remarcações
produtividade médica
especialidades mais demandadas

DOMÍNIO DE ÍNDICES ECONÔMICOS

O sistema também possui um painel com índices econômicos oficiais.

Inclui:

IPCA
IGPM
Dólar

Esses dados são utilizados apenas para referência macroeconômica.

COMO A IA DEVE RESPONDER PERGUNTAS

Ao responder perguntas analíticas, a IA deve:

identificar o KPI correto
identificar o domínio de dados
identificar os períodos comparados
identificar os itens que explicam a variação

A resposta deve sempre explicar a causa da mudança, não apenas os números.

EXEMPLO DE RACIOCÍNIO CORRETO

Pergunta:

por que a despesa aumentou em fevereiro?

Raciocínio esperado da IA:

comparar janeiro vs fevereiro
agrupar despesas por tipo de conta
identificar tipos que aumentaram
identificar impacto financeiro de cada tipo
explicar quais contas causaram o aumento

PRINCÍPIO DE EXPLICAÇÃO

A resposta final da IA deve sempre seguir este modelo.

Primeiro explicar o que mudou.

Depois explicar por que mudou.

Por fim explicar qual foi o impacto no resultado financeiro.

OBJETIVO FINAL DA IA

A IA não deve funcionar apenas como um leitor de dados.

Ela deve funcionar como um analista financeiro e operacional, capaz de identificar padrões e explicar o que está acontecendo com a empresa.

Se quiser, no próximo passo posso te entregar uma terceira peça que muda completamente o jogo do seu projeto:

MANUAL_DE_DADOS_JSON_DO_RELATORIO_HT

Ele ensina a IA:

qual JSON alimentar cada KPI

como localizar dados dentro dos JSON

como cruzar períodos automaticamente

Isso faz a IA responder como um analista humano de BI, mesmo com milhares de JSONs.


IDENTIFICAÇÃO DAS EMPRESAS (CNPJs)

O sistema opera com múltiplas empresas, cada uma representada por uma letra identificadora.

Cada letra corresponde a um CNPJ distinto.

Exemplo de representação:

A → Empresa A
B → Empresa B
C → Empresa C
...
X → Empresa X
Y → Empresa Y

Atualmente existem treze empresas, porém esse número pode variar ao longo do tempo. O número de empresas não é relevante para a lógica de análise.

O ponto fundamental é:

cada letra representa uma empresa diferente.

Portanto, quando uma pergunta envolver letras, a IA deve interpretar como comparação entre empresas.

Exemplo de pergunta:

comparar o dado G da empresa R com a empresa Y

Nesse caso, a IA deve entender que:

R é uma empresa

Y é outra empresa

a comparação deve ser feita entre os dados dessas duas empresas

IMPORTANTE

As empresas podem compartilhar:

médicos

estruturas operacionais

custos pagos por outra empresa

Por isso existe também o conceito de rateio de despesas, onde uma empresa pode pagar uma conta pertencente a outra.

Mesmo assim, a letra sempre identifica qual empresa está sendo analisada no KPI.