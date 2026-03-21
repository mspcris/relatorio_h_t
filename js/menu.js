// Injeta link Admin no drawer se o usuário for admin
fetch("/session/me")
  .then(r => r.ok ? r.json() : null)
  .then(data => {
    if (!data || !data.is_admin) return;
    // Procura o <li> do Home Page e insere o Admin logo abaixo
    const links = document.querySelectorAll("#app-drawer a[href]");
    let homeItem = null;
    links.forEach(a => {
      if (a.getAttribute("href") === "/" || a.textContent.trim() === "Home Page") {
        homeItem = a.closest("li");
      }
    });
    const adminLi = document.createElement("li");
    adminLi.innerHTML =
      '<a href="/admin" style="display:flex;align-items:center;gap:10px;padding:10px 16px;' +
      'color:inherit;text-decoration:none;font-weight:600;border-left:3px solid #f39c12">' +
      '<i class="fas fa-users-cog" style="color:#f39c12;width:18px;text-align:center"></i>' +
      '<span>Admin</span></a>';
    if (homeItem && homeItem.parentNode) {
      homeItem.parentNode.insertBefore(adminLi, homeItem.nextSibling);
    }
  })
  .catch(() => {});

// Aguarda o HTML carregar
document.addEventListener("DOMContentLoaded", () => {
  // Seletores principais do menu
  const drawer = document.getElementById("app-drawer");      // O menu lateral
  const overlay = document.getElementById("drawerOverlay");  // Fundo escuro
  const toggleBtn = document.getElementById("menuToggle");   // Botão hamburguer
  const closeBtn = document.getElementById("drawerClose");   // Botão fechar (X)
  const accordions = document.querySelectorAll(".menu-accordion");
 // Botões que abrem submenus

  /**
   * Abre o menu lateral (drawer)
   */
function openDrawer() {
  drawer.hidden = false;
  overlay.hidden = false;
  drawer.setAttribute("data-open", "true");
  overlay.setAttribute("aria-hidden", "false");
  toggleBtn.setAttribute("aria-expanded", "true");
  document.body.style.overflow = "hidden";

  const splash = drawer.querySelector(".menu-splash");
  const brand = drawer.querySelector(".menu-brand");
  const menuItems = drawer.querySelectorAll(".menu-list li");

  // Reset
  menuItems.forEach(li => li.classList.remove("show"));
  brand.classList.remove("visible");

  // Reinicia animação do splash
  splash.style.display = "block";
  splash.style.animation = "none";        // zera animação
  splash.offsetHeight;                    // força reflow
  splash.style.animation = "splashFade 2s ease forwards"; // reaplica animação

  // Depois de 2s, troca pro brand + itens
  setTimeout(() => {
    splash.style.display = "none";
    brand.classList.add("visible");

    menuItems.forEach((li, i) => {
      setTimeout(() => li.classList.add("show"), i * 0);
    });
  }, 60);
}

  /**
   * Fecha o menu lateral (drawer)
   */
  function closeDrawer() {
    // Marca como fechado
    drawer.setAttribute("data-open", "false");
    overlay.setAttribute("aria-hidden", "true");
    toggleBtn.setAttribute("aria-expanded", "false");
    document.body.style.overflow = "";

    // Espera a transição terminar antes de esconder
    setTimeout(() => {
      drawer.hidden = true;
      overlay.hidden = true;
    }, 300); // tempo deve bater com o CSS transition do drawer
  }

  // === Eventos principais ===
  toggleBtn.addEventListener("click", () => {
    const isOpen = drawer.getAttribute("data-open") === "true";
    isOpen ? closeDrawer() : openDrawer();
  });

  closeBtn.addEventListener("click", closeDrawer);
  overlay.addEventListener("click", closeDrawer);

  // === Submenus (ex.: Cursos) ===
  accordions.forEach(btn => {
    const target = document.getElementById(btn.getAttribute("aria-controls"));

    btn.addEventListener("click", () => {
      const expanded = btn.getAttribute("aria-expanded") === "true";

      // Atualiza atributo de acessibilidade
      btn.setAttribute("aria-expanded", String(!expanded));

      // Alterna a classe que abre/fecha com animação CSS
      if (expanded) {
        target.classList.remove("open"); // fecha
      } else {
        target.classList.add("open");    // abre
      }
    });
  });
});

