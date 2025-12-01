document.addEventListener("DOMContentLoaded", () => {
  const headerPath = "/header.html";  // absoluto resolve em qualquer rota

  fetch(headerPath)
    .then(res => {
      if (!res.ok) throw new Error("Erro ao carregar " + headerPath);
      return res.text();
    })
    .then(html => {
      const el = document.getElementById("header");
      if (el) el.innerHTML = html;
    })
    .catch(err => console.error("Erro no fetch do header:", err));
});
