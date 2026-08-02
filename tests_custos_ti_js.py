"""Carrega cada tela num Chrome de verdade e falha se houver erro de JS.

Existe porque um listener duplicado fora do {% if %} de integração matou a
página inteira de todo centro sem integração — e nada disso aparecia em log de
servidor. Erro de JS só o navegador vê.
"""
import os, subprocess, sys, tempfile
from jinja2 import Environment, FileSystemLoader

REPO = os.path.dirname(os.path.abspath(__file__))
env = Environment(loader=FileSystemLoader(REPO))

CAP = """<script>
window.__erros=[];
window.addEventListener('error',function(e){window.__erros.push((e.message||'')+' @linha '+(e.lineno||''));});
window.addEventListener('load',function(){setTimeout(function(){
  var d=document.createElement('div');d.id='AUTOTESTE';
  d.textContent='ERROS='+(window.__erros.join(' || ')||'NENHUM')
   +'|PERIODO='+((document.getElementById('fltDe')||{}).value||'VAZIO');
  document.body.appendChild(d);},2000);});
</script>"""

BASE = {"USER_EMAIL": "x@camim.com.br", "USER_IS_ADMIN": True, "USER_ALL_PAGES": True,
        "TI_CENTROS": [{"key": "comunicacao", "nome": "Comunicação",
                        "icone": "fab fa-whatsapp", "cor": "#eb6834",
                        "url": "/custos_ti/comunicacao", "fonte": "manual"}]}

def centro(key, integ):
    return {"key": key, "nome": key, "icone": "fas fa-box", "cor": "#eb6834",
            "fonte": "manual", "descricao": "x", "url": "/custos_ti/" + key,
            "integracao": integ}

# (nome, template, contexto, tem_filtro_de_periodo)
CASOS = [
    ("home",              "custos_ti.html",           {"TI_ATIVO": "_home"}),
    ("cadastros",         "custos_ti_cadastros.html", {"TI_ATIVO": "_cadastros"}),
    ("centro com meta",   "custos_ti_centro.html",
     {"TI_ATIVO": "comunicacao", "TI_CENTRO": centro("comunicacao", "meta")}),
    ("centro sem meta",   "custos_ti_centro.html",
     {"TI_ATIVO": "software", "TI_CENTRO": centro("software", None)}),
    ("centro fonte=ia",   "custos_ti_centro.html",
     {"TI_ATIVO": "ia", "TI_CENTRO": dict(centro("ia", None), fonte="ia")}),
]

falhas = []
tmp = tempfile.mkdtemp()
SEM_PERIODO = {"cadastros"}   # a tela de cadastros não tem filtro de período

for nome, tpl, extra in CASOS:
    html = env.get_template(tpl).render(**BASE, **extra).replace("<script>", CAP + "<script>", 1)
    caminho = os.path.join(tmp, nome.replace(" ", "_") + ".html")
    open(caminho, "w", encoding="utf-8").write(html)
    saida = subprocess.run(
        ["google-chrome", "--headless=new", "--disable-gpu", "--no-sandbox",
         "--virtual-time-budget=9000", "--dump-dom", "file://" + caminho],
        capture_output=True, text=True, timeout=120).stdout
    marca = saida.split('id="AUTOTESTE">')[-1].split("<")[0] if "AUTOTESTE" in saida else "SEM MARCA"
    ok = "ERROS=NENHUM" in marca and (
        nome in SEM_PERIODO or "PERIODO=VAZIO" not in marca)
    print(("  OK  " if ok else "  FALHOU  ") + f"{nome:<18} {marca}")
    if not ok:
        falhas.append(nome)

print()
if falhas:
    print(f"{len(falhas)} tela(s) com erro de JS: " + ", ".join(falhas))
    sys.exit(1)
print("TODAS AS TELAS CARREGAM SEM ERRO DE JS")
