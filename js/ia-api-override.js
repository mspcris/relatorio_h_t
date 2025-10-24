// Override para usar a API on-demand com fallback ao JSON estático
(function(){
  const API_ANALISAR = "/ia/analisar";
  const IA_BASE = "../json_retorno_groq"; // legado, fallback

  const pad2 = n => String(n).padStart(2,"0");
  const monthToday = () => { const t = new Date(); return `${t.getFullYear()}-${pad2(t.getMonth()+1)}`; };
  const selectedRange = () => {
    const from = document.querySelector('#from')?.value || (window.MESES?.[0] || null);
    const to   = document.querySelector('#to')?.value   || (window.MESES?.[window.MESES.length-1] || null);
    return { from, to };
  };
  const includesCurrentMonth = () => selectedRange().to === monthToday();

  window.fetchIA = async function(kind){
    const { from, to } = selectedRange();
    const posto = document.querySelector('#postoSel')?.value || "ALL";
    if (includesCurrentMonth()) throw new Error("Selecione apenas meses completos.");
    try{
      const r = await fetch(API_ANALISAR, {
        method:"POST",
        headers:{ "Content-Type":"application/json" },
        body: JSON.stringify({ from_ym: from, to_ym: to, posto })
      });
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const data = await r.json();
      return data[kind];
    }catch(e){
      const urlStatic = `${IA_BASE}/${from}_${to}_${kind}.json`;
      const r2 = await fetch(urlStatic + '?t=' + Date.now(), { cache:'no-store' });
      if (!r2.ok) throw e;
      return await r2.json();
    }
  };

  window.loadIA = async function(kind, containerId){
    const box = document.getElementById(containerId);
    if (!box) return;
    if (box.dataset.open === "1"){
      box.dataset.open="0"; box.style.display="none"; box.innerHTML=""; return;
    }
    box.style.display='block'; box.dataset.open="1";
    try{
      const obj = await window.fetchIA(kind);
      const prose = window.toProse ? window.toProse(obj) : `<pre>${JSON.stringify(obj,null,2)}</pre>`;
      if (window.typeInto) { await window.typeInto(box, prose); } else { box.innerHTML = prose; }
    }catch(e){
      const { from, to } = selectedRange();
      const esc = s => String(s).replace(/[&<>"']/g, c => ({ "&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#039;" }[c]));
      box.innerHTML = `<div class="alert alert-warning p-2 mb-0">
        Não foi possível obter a análise para <code>${from}..${to}</code> (${kind}).<br>
        Detalhe: ${esc(e.message || e)}
      </div>`;
    }
  };
})();