param(
  [int]$PageSize = 100,
  [int]$MaxRetries = 3
)

$ErrorActionPreference = "Stop"

# Garante que o script rode na pasta dele (para achar .env ao agendar)
$PSScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $PSScriptRoot

# Carrega chaves do .env para a sessão atual
if (Test-Path ".env") {
  Get-Content ".env" | ForEach-Object {
    if ($_ -match '^\s*#' -or $_ -match '^\s*$') { return }
    $k,$v = $_ -split '=',2
    $k = $k.Trim()
    $v = $v.Trim().Trim("'`"")
    if ($k -and $v) { Set-Item -Path "Env:$k" -Value $v }
  }
}

$Base = "https://api.harvestapp.com/v2"
$Token = $env:HARVEST_TOKEN
$AccountId = $env:HARVEST_ACCOUNT_ID
if (-not $Token -or -not $AccountId) { Write-Error "Defina HARVEST_TOKEN e HARVEST_ACCOUNT_ID." }

$Headers = @{
  "Authorization"      = "Bearer $Token"
  "Harvest-Account-Id" = "$AccountId"
  "User-Agent"         = "Harvest-AutoStop/1.0"
  "Content-Type"       = "application/json"
}

function Invoke-WithRetry {
  param([scriptblock]$Script, [int]$Retries = $MaxRetries)
  for ($i=0; $i -le $Retries; $i++) {
    try { return & $Script } catch {
      $resp = $_.Exception.Response
      if ($resp -and $resp.StatusCode.value__ -eq 429) {
        $retryAfter = [int]$resp.Headers["Retry-After"]
        if ($retryAfter -gt 0) { Start-Sleep -Seconds $retryAfter; continue }
      }
      if ($i -eq $Retries) { throw }
      Start-Sleep -Seconds ([math]::Pow(2,$i))
    }
  }
}

function Get-RunningTimeEntries {
  param([int]$Page = 1)
  $uri = "$Base/time_entries?is_running=true&page=$Page&per_page=$PageSize"
  Invoke-WithRetry { Invoke-RestMethod -Method GET -Uri $uri -Headers $Headers }
}

function Stop-TimeEntry {
  param([long]$Id)
  $uri = "$Base/time_entries/$Id/stop"
  Invoke-WithRetry { Invoke-RestMethod -Method PATCH -Uri $uri -Headers $Headers }
}

# Paginação
$stopped = @()
$page    = 1
do {
  $resp = Get-RunningTimeEntries -Page $page
  $entries = @()
  if ($resp.time_entries) { $entries = $resp.time_entries } elseif ($resp) { $entries = $resp }
  foreach ($e in $entries) {
    try {
      Stop-TimeEntry -Id $e.id | Out-Null
      $stopped += [pscustomobject]@{
        id           = $e.id
        user         = $e.user.name
        project      = $e.project.name
        task         = $e.task.name
        started_time = $e.started_time
        notes        = $e.notes
        stopped_at   = (Get-Date).ToString("s")
      }
      Write-Output ("Stopped id={0} user={1} project={2} task={3}" -f $e.id,$e.user.name,$e.project.name,$e.task.name)
    } catch {
      Write-Warning ("Falha ao parar id={0}: {1}" -f $e.id, $_.Exception.Message)
    }
  }
  $page++
} while ($resp.links.next)

# Log CSV opcional
if ($stopped.Count -gt 0) {
  $logPath = Join-Path $env:USERPROFILE ("harvest_autostop_{0}.csv" -f (Get-Date -Format "yyyyMMdd"))
  $stopped | Export-Csv -NoTypeInformation -Append -Path $logPath
  Write-Output ("Log salvo em: {0}" -f $logPath)
} else {
  Write-Output "Nenhum timer em execução encontrado."
}
