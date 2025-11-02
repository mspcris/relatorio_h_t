# python .\indicadores_etl.py --ptax 2020-01-01 today --ipca 2020-01 thismonth --igpm 2020-01 thismonth --out .\json_consolidado


# schedule0.ps1
param(
  [string]$ProjectDir = "$PSScriptRoot",
  [string]$OutDir     = "$PSScriptRoot\json_consolidado",
  [switch]$Site,
  [string]$IgpmCode   # opcional: ex. IGP12_IGPMG12
)

$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

Push-Location $ProjectDir
try {
  if (-not (Test-Path $OutDir)) { New-Item -ItemType Directory -Path $OutDir | Out-Null }

  $python = Join-Path $ProjectDir '.venv\Scripts\python.exe'
  if (-not (Test-Path $python)) { $python = 'python' }  # fallback

  $args = @(
    '.\indicadores_etl.py',
    '--ptax','2020-01-01','today',
    '--ipca','2020-01','thismonth',
    '--igpm','2020-01','thismonth',
    '--out', $OutDir
  )
  if ($IgpmCode) { $args += @('--igpm-code', $IgpmCode) }
  if ($Site)     { $args += '--site' }

  $log = Join-Path $OutDir ("etl_" + (Get-Date -Format 'yyyyMMdd_HHmmss') + '.log')
  Start-Transcript -Path $log -Append | Out-Null

  & $python @args
  $code = $LASTEXITCODE

  Stop-Transcript | Out-Null
  if ($code -ne 0) { throw "Python retornou código $code" }
}
catch {
  Write-Error $_
  exit 1
}
finally {
  Pop-Location
}
