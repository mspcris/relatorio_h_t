function initIAChat() {

    const box = document.createElement("div")

    box.innerHTML = `
    <div id="ia-chat" style="
        position:fixed;
        right:20px;
        bottom:20px;
        width:350px;
        background:#1e1e1e;
        color:white;
        padding:10px;
        border-radius:8px;
        z-index:9999;
    ">

    <div style="margin-bottom:8px;font-weight:bold">
    IA CAMIM
    </div>

    <textarea id="ia-pergunta" style="width:100%;height:60px"></textarea>

    <button id="ia-enviar">Perguntar</button>

    <div id="ia-resposta" style="margin-top:10px;font-size:13px"></div>

    </div>
    `

    document.body.appendChild(box)

    document.getElementById("ia-enviar").onclick = async () => {

        const pergunta = document.getElementById("ia-pergunta").value

        const contexto = window.__kpi_contexto || ""

        const resposta = await perguntarIA(pergunta, contexto)

        document.getElementById("ia-resposta").innerText = resposta
    }
}