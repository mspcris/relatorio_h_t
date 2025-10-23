fetch("footer.html")
  .then(res => res.text())
  .then(data => {
    document.querySelector("#footer").innerHTML = data;
    // Atualiza ano automático
    document.getElementById("y").textContent = new Date().getFullYear();
  });
