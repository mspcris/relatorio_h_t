param([switch]$FreshExports)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# Paths
$root = "C:\Users\csdg\Documents\GitHub\projetos\RELATORIO_H_T"
$venv = "$root\.venv\Scripts\python.exe"
$templates = Join-Path $root 'templates'
$target = Join-Path $templates 'trello_harvest.html'

# Logging
$logd = Join-Path $root 'logs'
New-Item -ItemType Directory -Force -Path $logd,$templates | Out-Null
$ts = Get-Date -Format "yyyy-MM-dd_HH-mm-ss"
$log = Join-Path $logd "trello_harvest_$ts.log"

Set-Location $root

if ($FreshExports) {
  & $venv export_trello.py --board 1lAOxrLe --out .\export_trello *>> $log
  if ($LASTEXITCODE -ne 0) { throw "export_trello falhou: $LASTEXITCODE" }

  & $venv export_harvest.py *>> $log
  if ($LASTEXITCODE -ne 0) { throw "export_harvest falhou: $LASTEXITCODE" }
}

# limpa alvo anterior
Remove-Item $target -Force -ErrorAction SilentlyContinue

# build focado
& $venv build_relatorio_html.py *>> $log
if ($LASTEXITCODE -ne 0) { throw "build_relatorio_html falhou: $LASTEXITCODE" }

# assert de entrega
if (-not (Test-Path $target)) {
  Write-Host "ERRO: '$target' não foi gerado. Veja o log: $log"
  exit 66
}

Write-Host "OK: gerado $target. Log: $log"
exit 0
