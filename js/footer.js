document.addEventListener("DOMContentLoaded", () => {
  const footerPath = "/footer.html"; // absoluto

  fetch(footerPath, { credentials: "same-origin" })
    .then(res => {
      if (!res.ok) throw new Error("Erro ao carregar " + footerPath);
      return res.text();
    })
    .then(html => {
      const el = document.querySelector("#footer");
      if (!el) return;
      el.innerHTML = html;

      const year = document.getElementById("y");
      if (year) year.textContent = new Date().getFullYear();
    })
    .catch(err => console.error(err));
});
