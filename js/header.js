// document.addEventListener("DOMContentLoaded", () => {
//   // Pega só o último segmento da URL (ex: "index.html" ou "curso1.html")
//   const page = window.location.pathname.split("/").pop();

//   // Se for index.html ou outra página da raiz → header.html
//   // Se vier de subpasta (tipo cursos/curso1.html) → ../header.html
//   const headerPath = window.location.pathname.includes("/courses/") 
//     ? "../header.html" 
//     : "header.html";

//   fetch(headerPath)
//     .then(res => {
//       if (!res.ok) throw new Error("Erro ao carregar " + headerPath);
//       return res.text();
//     })
//     .then(data => {
//       document.getElementById("header").innerHTML = data;
//     })
//     .catch(err => console.error("Erro no fetch do header:", err));
// });





document.addEventListener("DOMContentLoaded", () => {
  // Pega só o último segmento da URL (ex: "index.html" ou "curso1.html")
  const page = window.location.pathname.split("/").pop();

  // Se for index.html ou outra página da raiz → header.html
  // Se vier de subpasta (tipo cursos/curso1.html) → ../header.html
  const headerPath = window.location.pathname.includes("/courses/") 
    ? "../header.html" 
    : "header.html";

  fetch(headerPath)
    .then(res => {
      if (!res.ok) throw new Error("Erro ao carregar " + headerPath);
      return res.text();
    })
    .then(data => {
      document.getElementById("header").innerHTML = data;
    })
    .catch(err => console.error("Erro no fetch do header:", err));
});
