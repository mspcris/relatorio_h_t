MANUAL TÉCNICO – ARQUITETURA DO SISTEMA RELATORIO_H_T

VISÃO GERAL

O sistema relatorio_h_t é uma plataforma de análise de dados operacionais e financeiros da empresa. Ele consolida dados provenientes do banco de dados em arquivos JSON, que são consumidos por dashboards HTML e processados em JavaScript no frontend.

A arquitetura do sistema foi projetada para gerar dados consolidados e independentes para cada KPI, evitando dependências entre páginas.

Fluxo geral da arquitetura:

Banco de dados
↓
Consultas SQL
↓
Scripts Python de exportação (ETL)
↓
Arquivos JSON consolidados
↓
Dashboards HTML
↓
Processamento em JavaScript
↓
Análise visual ou por Inteligência Artificial

PRINCÍPIO ARQUITETURAL MAIS IMPORTANTE

Cada KPI é independente.

Isso significa:

cada página possui sua própria consulta de dados

um KPI nunca depende de cálculos realizados por outro KPI

diferentes KPIs podem usar as mesmas tabelas do banco, mas possuem lógica própria

Portanto:

Cada página deve ser interpretada como um universo analítico isolado.

ARQUITETURA DE DADOS

O sistema funciona como um data warehouse leve baseado em JSON.

Os dados passam por três camadas:

Camada 1 – Banco de dados
Contém as tabelas operacionais da empresa.

Camada 2 – ETL Python
Scripts executam consultas SQL e transformam dados em JSON.

Camada 3 – Camada de consumo
Dashboards HTML + JavaScript consomem os JSONs e geram análises.

ESTRUTURA DO PROJETO

Estrutura principal do diretório:

relatorio_h_t
│
├── app.py
├── api_crud.py
├── ia_router.py
│
├── sql/
├── sql_full/
├── sql_consultas_mensal/
├── sql_metas/
├── sql_ctrlq_relatorio/
│
├── json_consultas_mensal/
├── json_metas/
├── json_notas_rps/
├── json_ctrlq_relatorio/
├── json_consolidado/
├── json_vendas/
│
├── js/
├── css/
├── images/
│
├── export_.py
└── kpi_.html

BACKEND – SCRIPTS PYTHON

Os scripts Python possuem três responsabilidades principais:

Extração de dados do banco
Transformação dos dados
Geração de JSON consolidado

SCRIPTS DE EXPORTAÇÃO

Scripts com padrão export_*.py executam consultas SQL e salvam os resultados em JSON.

Exemplos:

export_consultas_mensal_json.py
export_receita_despesa.py
export_notas_rps.py
export_fin_full.py
export_vendas.py
export_metas.py
export_ctrlq_relatorio.py

Fluxo interno típico:

SQL → pandas → transformação → JSON

CONSULTAS SQL

Os arquivos SQL ficam organizados por domínio de dados.

Principais pastas:

sql_consultas_mensal/
sql_metas/
sql_ctrlq_relatorio/
sql_notas_rps/
sql_financeiro/
sql_rateio/

Cada arquivo SQL representa uma consulta especializada para um KPI específico.

GERAÇÃO DE JSON

Os scripts Python salvam os dados nas seguintes pastas:

json_consultas_mensal
json_metas
json_notas_rps
json_ctrlq_relatorio
json_consolidado
json_vendas
json_rateio
json_cadastro

Cada JSON representa dados já consolidados para análise.

Os dashboards nunca acessam diretamente o banco de dados.

CONCEITO DE EMPRESAS (CNPJs)

O sistema trabalha com múltiplas empresas.

Cada empresa é representada por uma letra identificadora.

Exemplo:

A → empresa A
B → empresa B
C → empresa C
X → empresa X
Y → empresa Y

Cada letra corresponde a um CNPJ distinto.

Isso permite análises como:

comparar empresa G com empresa R
comparar empresa R com empresa Y

Esse conceito é fundamental para interpretar corretamente os dados.

DOMÍNIOS DE DADOS DO SISTEMA

O sistema possui os seguintes domínios principais:

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

Cada domínio alimenta um ou mais KPIs.

DOMÍNIO FINANCEIRO

O domínio financeiro contém os dados contábeis da empresa.

O principal painel é:

KPI RECEITAS X DESPESAS

Esse KPI apresenta:

receita total
despesa total
resultado
margem

As contas financeiras possuem três níveis:

PLANO PRINCIPAL
PLANO DE CONTAS
TIPO DE CONTA

Para análise de variação financeira, o agrupamento principal é:

TIPO DE CONTA

RATEIO ENTRE EMPRESAS

Existe um sistema interno de rateio.

Uma empresa pode pagar uma conta que pertence a outra empresa.

Por isso existe o KPI:

KPI RECEITA X DESPESA COM RATEIO

Esse KPI redistribui as despesas para mostrar a produtividade financeira real de cada empresa.

DOMÍNIO DE MENSALIDADES

Mensalidades representam a principal fonte de receita da empresa.

Vários KPIs utilizam mensalidades como base de comparação.

Esse domínio permite analisar:

receita recorrente
inadimplência
crescimento de clientes
projeções financeiras

KPI CUSTO ALIMENTAÇÃO

Este KPI mede o custo de alimentação dos funcionários.

A análise compara:

despesa com alimentação
versus
receita de mensalidades

KPI CUSTO MÉDICO

Este KPI mede o custo com médicos.

A análise compara:

custo médico
versus
receita de mensalidades

DOMÍNIO DE VENDAS

A empresa vende planos de saúde.

Cada venda gera novos clientes.

Cada cliente gera novas mensalidades.

O KPI de vendas analisa:

contratos vendidos
crescimento da base
desempenho comercial

DOMÍNIO DE CLIENTES

O sistema classifica clientes em três categorias:

VIDAS
todos que se consultam na empresa

BENEFICIÁRIOS
clientes com plano ativo

BENEFICIÁRIOS PJ
clientes vinculados a contratos empresariais

DOMÍNIO DE PRESCRIÇÕES

Este domínio analisa prescrições médicas digitais.

Permite monitorar:

adoção pelos médicos
uso da tecnologia
taxa de adesão

DOMÍNIO DE METAS

O KPI Metas permite:

cadastrar metas
acompanhar metas
comparar metas com resultados reais
comparar desempenho com anos anteriores

DOMÍNIO DE CONSULTAS

Esse domínio analisa o comportamento das consultas médicas.

Indicadores analisados:

faltas de pacientes
faltas de médicos
remarcações
produtividade médica
especialidades mais demandadas

DOMÍNIO CTRLQ – QUADRO DE ESPECIALIDADES

CTRLQ significa Quadro de Especialidades.

Esse domínio não possui qualquer relação com dados financeiros.

KPI CTRLQ

Este painel apresenta um retrato da estrutura médica da empresa.

Ele registra informações sobre médicos no último dia de cada mês.

O objetivo é criar um snapshot mensal da estrutura médica.

INFORMAÇÕES ANALISADAS NO CTRLQ

O painel registra:

lista de médicos
especialidades
médicos PJ
médicos com RQE
estrutura das agendas médicas

Esse painel é utilizado para análise organizacional da estrutura médica.

DOMÍNIO DE NOTAS FISCAIS

O KPI Notas RPS monitora documentos fiscais.

Permite acompanhar:

RPS emitidos
RPS convertidos em notas fiscais
pendências fiscais

FRONTEND – DASHBOARDS

Os dashboards são páginas HTML que consomem dados JSON.

Exemplos:

kpi_receita_despesa.html
kpi_consultas_status.html
kpi_vendas.html
kpi_notas_rps.html
kpi_home.html

PROCESSAMENTO NO FRONTEND

Os dashboards utilizam JavaScript para:

buscar JSON
filtrar dados
agrupar resultados
ordenar resultados
calcular percentuais
gerar gráficos

Exemplo de fluxo:

fetch("json_consolidado/receita_despesa.json")
→ processamento JS
→ renderização na tela

ARQUITETURA PARA IA

A IA não precisa entender cada JSON individual.

Ela precisa entender:

domínio de dados
estrutura dos JSON
períodos de análise
regras de agrupamento

ESCALA DE DADOS

O sistema possui aproximadamente:

1400+ arquivos JSON

Porém esses arquivos pertencem a poucos domínios de dados.

Isso simplifica a interpretação por IA.

REGRA DE ANÁLISE TEMPORAL

Sempre que houver comparação entre períodos, a IA deve:

comparar períodos
agrupar dados
identificar variação
identificar responsáveis pela variação

CONCLUSÃO

O sistema relatorio_h_t é uma plataforma de BI baseada em dados consolidados em JSON.

Fluxo final da arquitetura:

SQL → Python ETL → JSON → Frontend → Análise

A Inteligência Artificial deve utilizar os JSON como fonte primária para explicar comportamentos operacionais e financeiros da empresa.