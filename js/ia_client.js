async function perguntarIA(pergunta, contexto){

    try{

        const resp = await fetch("/api/ia/pergunta",{
            method:"POST",
            headers:{
                "Content-Type":"application/json"
            },
            body:JSON.stringify({
                pergunta:pergunta,
                contexto:contexto
            })
        })

        if(!resp.ok){

            if(resp.status===429){
                return "⚠️ IA indisponível: limite da API ou sem crédito."
            }

            return "⚠️ Falha ao conectar com a IA."
        }

        const data = await resp.json()
        return data.resposta

    }catch(e){

        return "⚠️ Erro de conexão com servidor."
    }
}