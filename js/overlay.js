// Sempre voltar ao topo
function resetScroll(){ setTimeout(()=>window.scrollTo(0,0),50); }
document.addEventListener("DOMContentLoaded", resetScroll);
window.addEventListener("load", resetScroll);
window.addEventListener("pageshow", resetScroll);

document.addEventListener("DOMContentLoaded", () => {
  const c = document.getElementById("overlay");
  if (!c) return;

  const overlayPath = "/templates/overlay.html";

  fetch(overlayPath, { credentials: "same-origin" })
    .then(r => {
      if (!r.ok) throw new Error("Erro ao carregar " + overlayPath);
      return r.text();
    })
    .then(html => {
      c.innerHTML = html;

              const frases = [
  "Métricas certas, decisões certas. <strong>Gerencie o que mede</strong> — inspirado em Peter Drucker",
  "Qualidade começa no processo. <strong>Padronize para melhorar</strong> — inspirado em W. Edwards Deming",
  "Valor nasce do cliente. <strong>Contabilidade com foco no mercado</strong> — inspirado em Philip Kotler",
  "Estratégia é escolha. <strong>Diga não ao que não gera margem</strong> — inspirado em Michael Porter",
  "Gestão é prática. <strong>Execute com disciplina</strong> — inspirado em Henry Mintzberg",
  "Crescimento é resultado do foco. <strong>O que não escala, simplifique</strong> — inspirado em Jim Collins",
  "Excelência é ação. <strong>Faça melhor hoje</strong> — inspirado em Tom Peters",
  "Aprender gera vantagem. <strong>Melhoria contínua no DRE</strong> — inspirado em Peter Senge",
  "Priorize o essencial. <strong>Agenda orientada a impacto</strong> — inspirado em Stephen R. Covey",
  "Inove na gestão. <strong>Quebre o status quo finance</strong> — inspirado em Gary Hamel",
  "Valor no core. <strong>Explore competências únicas</strong> — inspirado em C.K. Prahalad",
  "Disrupção é previsível. <strong>Proteja o caixa, teste o novo</strong> — inspirado em Clayton Christensen",
  "O que é medido, entrega. <strong>BSC na veia</strong> — inspirado em Robert Kaplan",
  "Traduza visão em metas. <strong>Mapa estratégico vivo</strong> — inspirado em David Norton",
  "Remove o gargalo. <strong>Fluxo de caixa sem fricção</strong> — inspirado em Eliyahu Goldratt",
  "Desperdício é custo. <strong>Lean no financeiro</strong> — inspirado em Taiichi Ohno",
  "Processo bom é simples. <strong>Padrões que liberam tempo</strong> — inspirado em Shigeo Shingo",
  "Colaboração gera resultado. <strong>Finance como parceiro</strong> — inspirado em Mary Parker Follett",
  "Método traz eficiência. <strong>Tempo padrão, custo certo</strong> — inspirado em Frederick W. Taylor",
  "Governança cria confiança. <strong>Controles que protegem valor</strong> — inspirado em Max Weber",
  "Estrutura segue estratégia. <strong>Centros de custo alinhados</strong> — inspirado em Alfred D. Chandler Jr.",
  "Planeje para crescer. <strong>Matriz de portfólio na mesa</strong> — inspirado em Igor Ansoff",
  "Só os paranoicos vencem. <strong>Gestão de riscos ativa</strong> — inspirado em Andrew Grove",
  "Meritocracia entrega. <strong>KPIs claros, bônus assertivo</strong> — inspirado em Jack Welch",
  "Delegue com números. <strong>Responsabilidade por P&L</strong> — inspirado em Alfred P. Sloan",
  "Cultura paga dividendos. <strong>Ética antes do lucro</strong> — inspirado em Edgar Schein",
  "Organizações existem para entregar. <strong>Propósito com P&L</strong> — inspirado em Chester Barnard",
  "Liderança é resultado. <strong>Traga clareza e ritmo</strong> — inspirado em John P. Kotter",
  "Conversa dura, dado limpo. <strong>Transparência fiscal</strong> — inspirado em Ram Charan",
  "Controle é linguagem. <strong>Planejamento e controle gerencial</strong> — inspirado em Robert N. Anthony",
  "Custo informa preço. <strong>CMV sem surpresas</strong> — inspirado em Charles T. Horngren",
  "Dupla partida, visão completa. <strong>Confiança nos números</strong> — inspirado em Luca Pacioli",
  "Integridade contábil é vantagem. <strong>Relate o que importa</strong> — inspirado em Abraham Briloff",
  "Princípios geram consistência. <strong>Paton-Littleton na prática</strong> — inspirado em William A. Paton e A.C. Littleton",
  "Medição importa. <strong>Estoque, receita, resultado</strong> — inspirado em Yuji Ijiri",
  "Padrões elevam credibilidade. <strong>Qualidade da informação</strong> — inspirado em Mary Barth",
  "Mercado premia transparência. <strong>Disclosure que cria valor</strong> — inspirado em Ray Ball",
  "Relevância e confiabilidade. <strong>Contabilidade que orienta ação</strong> — inspirado em Philip Brown",
  "Intangíveis contam. <strong>Mensure o que parece invisível</strong> — inspirado em Baruch Lev",
  "Risco tem viés. <strong>Evite atalhos cognitivos no budget</strong> — inspirado em Richard Thaler",
  "Margem primeiro. <strong>Fluxo de caixa é rei</strong> — inspirado em Warren Buffett",
  "Preço é o que paga. <strong>Valor é o que leva</strong> — inspirado em Benjamin Graham",
  "No longo prazo conta. <strong>Orçamento anticíclico</strong> — inspirado em John Maynard Keynes",
  "Invista no que entende. <strong>Alocação de capital disciplinada</strong> — inspirado em Peter Lynch",
  "Avalie com rigor. <strong>WACC, múltiplos e sensibilidade</strong> — inspirado em Aswath Damodaran",
  "Preto cisne existe. <strong>Reserva de liquidez sempre</strong> — inspirado em Nassim Nicholas Taleb",
  "O cliente é o centro. <strong>Receita recorrente saudável</strong> — inspirado em Theodore Levitt",
  "Processo vende. <strong>Pipeline e previsibilidade</strong> — inspirado em Neil Rackham",
  "Serviço com padrão. <strong>SLA financeiro respondendo ao negócio</strong> — inspirado em David Garvin",
  "Aprenda rápido. <strong>Fechamento contábil em T+3</strong> — inspirado em Kaizen (escola Lean)"
];
      const el = c.querySelector(".overlay-text");
      if (el) el.innerHTML = frases[Math.floor(Math.random()*frases.length)];

      setTimeout(() => {
        c.classList.add("fade-out");
        setTimeout(() => c.remove(), 1000);
      }, 5000);
    })
    .catch(err => {
      console.error("overlay:", err);
      c.remove();
    });
});
