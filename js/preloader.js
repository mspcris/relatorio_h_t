document.addEventListener("DOMContentLoaded", () => {
  const preloader = document.getElementById("preloader");
  const preloaderText = document.querySelector(".preloader-text");

  if (preloaderText) {
    // Lista de frases
    const frases = [
      `UPUP é mais que um projeto social:<br>é uma família unida por um propósito —<br><strong>Amar e servir</strong>`,
      `Quem ama, cuida.<br>Quem serve, transforma.<br><strong>UPUP é vida em movimento</strong>`,
      `Pequenos gestos de amor<br>geram grandes milagres.<br><strong>UPUP acredita em você</strong>`,
      `O cuidado nos une,<br>a esperança nos guia.<br><strong>UPUP é família</strong>`,
      `Mais que palavras,<br>nosso propósito é ação.<br><strong>UPUP — Amar é verbo</strong>`,
      `Quando cuidamos juntos,<br>a vida floresce.<br><strong>UPUP — Unidos para servir</strong>`,
      `UPUP é cuidado em cada detalhe:<br>um gesto simples pode mudar vidas.<br><strong>Amor que transforma</strong>`,
      `Servir é nossa missão.<br>Cuidar é nossa alegria.<br><strong>UPUP é coração em ação</strong>`,
      `Onde há dor,<br>UPUP leva esperança.<br><strong>Amar é o nosso propósito</strong>`,
      `Com carinho e dedicação,<br>UPUP acolhe cada história.<br><strong>Família que cuida</strong>`,
      `UPUP acredita que servir<br>é a maior forma de amar.<br><strong>Juntos pela vida</strong>`,
      `Cada sorriso que nasce<br>é fruto do cuidado da UPUP.<br><strong>Amor em movimento</strong>`,
      `UPUP é mais que palavras:<br>é serviço, é entrega.<br><strong>Amor verdadeiro</strong>`,
      `Quando alguém precisa,<br>UPUP está presente.<br><strong>Cuidar é servir</strong>`,
      `UPUP é abraço que aquece,<br>palavra que conforta.<br><strong>Família de amor</strong>`,
      `No caminho da esperança,<br>UPUP anda ao seu lado.<br><strong>Servir é viver</strong>`,
      `UPUP transforma cuidado<br>em amor multiplicado.<br><strong>Unidos pelo servir</strong>`,
      `Com cada ato de carinho,<br>UPUP semeia esperança.<br><strong>Serviço que floresce</strong>`,
      `UPUP é amor sem fronteiras,<br>solidariedade sem limites.<br><strong>Família que acolhe</strong>`,
      `No coração de cada vida,<br>UPUP planta amor.<br><strong>Cuidar é servir</strong>`,
      `UPUP é a mão estendida,<br>a palavra amiga.<br><strong>Esperança em ação</strong>`,
      `O amor se multiplica<br>quando a UPUP cuida.<br><strong>Família de fé</strong>`,
      `UPUP é carinho,<br>é atenção,<br><strong>é amor que abraça</strong>`,
      `No silêncio da dor,<br>UPUP leva conforto.<br><strong>Servir é amar</strong>`,
      `Com dedicação e amor,<br>UPUP constrói futuros.<br><strong>Esperança compartilhada</strong>`,
      `UPUP é vida que floresce<br>em cada cuidado.<br><strong>Amor transformador</strong>`,
      `Onde há necessidade,<br>UPUP responde com amor.<br><strong>Serviço e esperança</strong>`,
      `UPUP é presença constante,<br>é cuidado diário.<br><strong>Família que serve</strong>`,
      `Cada gesto de carinho<br>é o jeito UPUP de servir.<br><strong>Amor sem limites</strong>`,
      `UPUP acredita que amar<br>é cuidar de todos.<br><strong>Família unida</strong>`,
      `No sorriso de quem recebe,<br>vive a missão da UPUP.<br><strong>Amor em serviço</strong>`,
      `UPUP transforma realidades<br>através do amor.<br><strong>Cuidar é servir</strong>`,
      `Com UPUP,<br>ninguém caminha sozinho.<br><strong>Família de esperança</strong>`,
      `O cuidado da UPUP<br>é luz em meio à escuridão.<br><strong>Amor que guia</strong>`,
      `UPUP é ponte de amor,<br>ligando corações.<br><strong>Serviço que une</strong>`,
      `Em cada abraço da UPUP,<br>há vida renovada.<br><strong>Amar é servir</strong>`,
      `UPUP é amor que se doa,<br>cuidado que não cansa.<br><strong>Serviço de coração</strong>`,
      `Onde o mundo fecha portas,<br>UPUP abre os braços.<br><strong>Amor acolhedor</strong>`,
      `UPUP é chama de esperança<br>que nunca se apaga.<br><strong>Servir é viver</strong>`,
      `Com coragem e amor,<br>UPUP enfrenta desafios.<br><strong>Família que apoia</strong>`,
      `UPUP é vida compartilhada,<br>é coração em família.<br><strong>Amar e servir</strong>`,
      `O carinho da UPUP<br>é bálsamo para a alma.<br><strong>Serviço que cura</strong>`,
      `UPUP é amor que inspira,<br>cuidado que fortalece.<br><strong>Família unida</strong>`,
      `A cada passo com a UPUP,<br>renasce a esperança.<br><strong>Amor que guia</strong>`,
      `UPUP é entrega,<br>é serviço,<br><strong>é vida dedicada ao próximo</strong>`,
      `Com ternura e cuidado,<br>UPUP constrói laços.<br><strong>Amor que permanece</strong>`,
      `UPUP é amor presente<br>em todos os detalhes.<br><strong>Família que serve</strong>`,
      `Na jornada da vida,<br>UPUP caminha ao seu lado.<br><strong>Esperança em ação</strong>`,
      `UPUP é a certeza<br>de que amar sempre vale a pena.<br><strong>Servir é amar</strong>`,
      `Com cada mão estendida,<br>UPUP fortalece corações.<br><strong>Amor solidário</strong>`,
      `UPUP é fé que age,<br>amor que serve,<br><strong>esperança que acolhe</strong>`,
      `Mais que uma ONG,<br>UPUP é família.<br><strong>Amar e servir sempre</strong>`,
      `No coração da UPUP,<br>cabe o mundo inteiro.<br><strong>Amor que acolhe</strong>`,
      `UPUP é cuidado que abraça,<br>amor que transforma.<br><strong>Família de esperança</strong>`,
      `Com amor e serviço,<br>UPUP constrói pontes.<br><strong>Unidos pelo próximo</strong>`,
      `UPUP é sorriso,<br>é carinho,<br><strong>é amor em cada detalhe</strong>`,
      `Servir com amor<br>é a missão da UPUP.<br><strong>Família que transforma</strong>`
    ];

    // Sorteia uma frase
    const fraseAleatoria = frases[Math.floor(Math.random() * frases.length)];
    preloaderText.innerHTML = fraseAleatoria;
  }

  // Aplica fade-out ao preloader
  setTimeout(() => {
    if (preloader) preloader.classList.add("fade-out");

    // Após o fade-out, libera o site com fade-in
    setTimeout(() => {
      document.body.classList.add("loaded");
    }, 800); // tempo do fade-out
  }, 6000); // tempo de exibição do preloader
});

// Fallback (segurança)
setTimeout(() => {
  const preloader = document.getElementById("preloader");
  if (preloader && !preloader.classList.contains("fade-out")) {
    preloader.classList.add("fade-out");
    document.body.classList.add("loaded");
  }
}, 10000);
