async function perguntarIA(pergunta, contexto) {

    const resp = await fetch("/api/ia/pergunta", {
        method: "POST",
        headers: {
            "Content-Type": "application/json"
        },
        body: JSON.stringify({
            pergunta: pergunta,
            contexto: contexto
        })
    })

    const data = await resp.json()

    return data.resposta
}