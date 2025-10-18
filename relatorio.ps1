# caminho do projeto
$root = "C:\Users\csdg\Documents\GitHub\projetos\RELATORIO_H_T"
$venv = "$root\.venv\Scripts\python.exe"
$logd = "$root\logs"
New-Item -ItemType Directory -Force -Path $logd | Out-Null
$ts = Get-Date -Format "yyyy-MM-dd_HH-mm-ss"
$log = "$logd\run_$ts.log"

# roda no diretório do projeto (garante .env e paths relativos)
Set-Location $root

# executa e loga saída/erros
& $venv export_trello.py --board 1lAOxrLe --out .\export_trello *>> $log
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

& $venv export_harvest.py *>> $log
exit $LASTEXITCODE
