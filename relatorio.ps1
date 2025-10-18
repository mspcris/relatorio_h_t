# caminho do projeto
$root = "C:\Users\csdg\Documents\GitHub\projetos\RELATORIO_H_T"
$venv = "$root\.venv\Scripts\python.exe"
$logd = "$root\logs"
New-Item -ItemType Directory -Force -Path $logd | Out-Null
$ts = Get-Date -Format "yyyy-MM-dd_HH-mm-ss"
$log = "$logd\run_$ts.log"

Set-Location $root
$code = 0

& $venv export_trello.py --board 1lAOxrLe --out .\export_trello *>> $log
if ($LASTEXITCODE -ne 0) { $code = $LASTEXITCODE }

if ($code -eq 0) {
  & $venv export_harvest.py *>> $log
  if ($LASTEXITCODE -ne 0) { $code = $LASTEXITCODE }
}

if ($code -eq 0) {
  & $venv build_relatorio_html.py *>> $log
  $code = $LASTEXITCODE
}

if ($code -eq 0) {
  $reportDir = Join-Path $root 'relatorio'
  $html = Get-ChildItem $reportDir -Filter 'relatorio_*.html' | Sort-Object LastWriteTime -Descending | Select-Object -First 1
  if ($html) { Start-Process $html.FullName }  # abre no navegador padrão
}

exit $code


exit $code
