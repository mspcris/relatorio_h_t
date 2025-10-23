# rel_kpi
Gerador de Relatórios para KPI e Governança

# build_relatorio_html.py
Gerador de página HTML index.html com css/js contido com dados do harvest e do trello(ronald). Abre o navegador padrão com a página.

# export_governanca.py
Baixa dados no sql usando os selects contidos em /sql e gera index_lp.html na pasta templates. Página possui dependências nas pastas contidas: (css, fonts, images, js)

# export_harvest.py
Exporta dados do harvest salvando csv em export_harvest.

# export_trello.py
Exporta dados do harvest salvando csv em export_trello.

# relatorio.ps1
script powershell que roda o export_trello.py e o export_harvest.py e, por fim, roda build_relatorio_html.py.

# stop-harvesttimers.ps1
Para todos os tractimenrs que estão em run no harvest. Não é usado neste projeto, mas, foi essencial para controlar os esquecidos.

# upload_report.ps1
Faz deploy na vm ubuntu.