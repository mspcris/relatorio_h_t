# XML das notas fiscais — Contabilidade - Notas e KPI Notas x RPS (2026-10-30)

## Pedido do Cristiano
1. Ao lado de cada nota, a opção de baixar o XML.
2. Baixar todos os XML de uma vez (ZIP).
3. Na Contabilidade, resumo claro e fácil para saber se alguma nota não tem XML.
4. (durante o trabalho) Na Contabilidade, depois de clicar numa empresa não havia
   como voltar a "todas"; a página abre com todas somadas e precisa de uma opção
   explícita "Todas as empresas".

## O que o DBA informou
- `Fin_NotaFiscalTodasEmitidas.XMLRecebido` — robô da Nota Carioca que buscava
  todas as notas do certificado, com ou sem receita; pode repetir entre postos.
- `Fin_NotaFiscalRetorno.xmlCompleto` — nota emitida pelo sistema, carioca ou
  nacional.

## O que foi medido (30/10/2026, só leitura)
- `Fin_NotaFiscalTodasEmitidas` parou em **31/03/2025** (A: 70.746 linhas,
  2023-01 → 2025-03). As telas começam em 2026-01 → tabela fora do período,
  **não é lida**.
- `Fin_NotaFiscalRetorno`: em set e out/2026, nos 13 postos, **100 %** das notas
  das três views casam com um retorno nacional com XML (~10,7 KB cada). Nova
  Iguaçu tem 1.062 retornos não nacionais sem XML em 2026, nenhum de set/out.
- Um mês da rede ≈ 17 mil notas ≈ 180 MB de XML → o ZIP não cabe num request
  (nginx corta em 60 s): é gerado em thread de fundo com progresso.

## Decisões (minhas; o pedido não as fixava)
- **Fonte única = `Fin_NotaFiscalRetorno.xmlCompleto`**, lida **ao vivo** no posto
  no momento do clique. Nada de XML em arquivo estático (nota com nome e CPF).
- O ETL grava por nota `id_retorno` e `tem_xml` (DATALENGTH > 50). A tela só pede
  XML de id que esteja no JSON do mês — a URL não serve de leitura livre.
- "Sem XML" = nota da view sem retorno nacional casado OU retorno sem XML. JSON
  gravado antes do campo aparece como "sem informação", nunca como "sem XML".
- ZIP: uma pasta por empresa (`<cnpj>_<nome>/NF_<n>_<posto>[_CANCELADA].xml`) na
  Contabilidade; `NF_<n>_<posto>.xml` no KPI. Sempre leva `LEIA-ME.txt` com o que
  entrou, o que faltou e posto que não respondeu. Arquivo vive 2 h em
  `NOTAS_XML_DIR` (padrão `/opt/camim-auth/notas_xml_tmp`). Só o dono baixa.
- Rotas da Contabilidade ficam sob `/api/contabilidade_notas/` por causa do
  usuário de escopo fechado; o KPI usa `/api/notas_xml/` com ACL de posto.
- "Todas as empresas" é a **primeira linha** da tabela Por empresa e o estado
  inicial; clicar na empresa já escolhida volta a todas.
