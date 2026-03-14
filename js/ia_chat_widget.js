function initIAChat(){

    const box=document.createElement("div")

    box.innerHTML=`

    <style>

    #ia-chat{
        position:fixed;
        right:20px;
        bottom:20px;
        width:360px;
        background:#1e1e1e;
        color:white;
        border-radius:10px;
        z-index:9999;
        font-family:Arial;
        box-shadow:0 4px 20px rgba(0,0,0,.4);
    }

    #ia-header{
        display:flex;
        justify-content:space-between;
        align-items:center;
        padding:10px;
        font-weight:bold;
        background:#2b2b2b;
        border-top-left-radius:10px;
        border-top-right-radius:10px;
    }

    #ia-body{
        padding:10px;
    }

    #ia-pergunta{
        width:100%;
        height:70px;
        border-radius:6px;
        border:none;
        padding:6px;
        resize:none;
    }

    .ia-btn{
        background:#3b82f6;
        border:none;
        color:white;
        padding:8px 14px;
        border-radius:6px;
        margin-top:6px;
        cursor:pointer;
    }

    .ia-btn:hover{
        background:#2563eb;
    }

    #ia-resposta{
        margin-top:10px;
        font-size:14px;
        max-height:300px;
        overflow-y:auto;
        line-height:1.5;
    }

    .ia-tools{
        margin-top:6px;
    }

    .ia-tools button{
        margin-right:5px;
        font-size:12px;
    }

    #ia-resposta{
        margin-top:10px;
        font-size:14px;
        max-height:300px;
        overflow-y:auto;
        line-height:1.5;
        white-space:pre-wrap;
    }
        
    </style>

    <div id="ia-chat">

        <div id="ia-header">

            <div>IA CAMIM</div>

            <button onclick="iaToggle()" style="background:none;border:none;color:white;font-size:18px;cursor:pointer">
            −
            </button>

        </div>

        <div id="ia-body">

            <textarea id="ia-pergunta"></textarea>

            <br>

            <button id="ia-enviar" class="ia-btn">Perguntar</button>

            <div class="ia-tools">
                <button onclick="iaFonteMaior()">A+</button>
                <button onclick="iaFonteNormal()">A</button>
            </div>

            <div id="ia-resposta"></div>

        </div>

    </div>
    `

    document.body.appendChild(box)

    document.getElementById("ia-enviar").onclick=async function(){

        const pergunta=document.getElementById("ia-pergunta").value

        if(!pergunta){
            return
        }

        const contexto=window.__kpi_contexto || ""

        const respBox=document.getElementById("ia-resposta")

        respBox.innerText="IA pensando..."

        try{

            const resposta=await perguntarIA(pergunta,contexto)

            respBox.innerHTML=renderMarkdown(resposta)

        }catch(e){

            respBox.innerText="⚠️ Falha ao conectar com a IA."

        }

    }

}



function iaFonteMaior(){

    const el=document.getElementById("ia-resposta")

    const size=parseInt(window.getComputedStyle(el).fontSize)

    el.style.fontSize=(size+2)+"px"
}



function iaFonteNormal(){

    document.getElementById("ia-resposta").style.fontSize="14px"
}



function iaToggle(){

    const body=document.getElementById("ia-body")

    if(body.style.display==="none"){

        body.style.display="block"

    }else{

        body.style.display="none"

    }


}

function renderMarkdown(text){

    return text
        .replace(/\*\*(.*?)\*\*/g,"<b>$1</b>")
        .replace(/### (.*?)/g,"<h4>$1</h4>")
        .replace(/\n/g,"<br>")
}