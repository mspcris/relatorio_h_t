param(
  [int]$PageSize = 100,
  [int]$MaxRetries = 3
)

# Fail-fast para não mascarar erro
$ErrorActionPreference = "Stop"

# Garante execução a partir da pasta do script
Set-Location -Path $PSScriptRoot

# --- Loader de .env robusto ---
if (Test-Path ".env") {
  Get-Content ".env" | ForEach-Object {
    $line = $_

    # Ignora comentários e linhas em branco
    if ($line -match '^\s*(#|$)') { return }

    # Garante que há '='
    if ($line -notmatch '=') { return }

    # Split só no primeiro '='
    $parts = $line -split '=', 2
    $k = $parts[0].Trim()
    $v = $parts[1]

    if ($null -ne $v) {
      # Normaliza e remove aspas envolventes se existirem
      $v = $v.Trim()
      $v = $v.Trim("'")
      $v = $v.Trim('"')
    }

    if ($k) {
      Set-Item -Path ("Env:{0}" -f $k) -Value ($v | ForEach-Object { if ($_ -ne $null) { $_ } else { "" } })
    }
  }
} else {
  Write-Warning ("Arquivo .env não encontrado em {0}" -f (Get-Location).Path)
}

# --- Config Harvest ---
$Base      = "https://api.harvestapp.com/v2"
$Token     = $env:HARVEST_TOKEN
$AccountId = $env:HARVEST_ACCOUNT_ID

if (-not $Token -or -not $AccountId) {
  throw "Defina HARVEST_TOKEN e HARVEST_ACCOUNT_ID no .env ou no ambiente."
}

$Headers = @{
  "Authorization"      = "Bearer $Token"
  "Harvest-Account-Id" = "$AccountId"
  "User-Agent"         = "Harvest-AutoStop/1.1"
  "Content-Type"       = "application/json"
}

# --- Infra de retry com backoff exponencial e respeito a 429 Retry-After ---
function Invoke-WithRetry {
  param(
    [Parameter(Mandatory=$true)][scriptblock]$Script,
    [int]$Retries = $MaxRetries
  )
  for ($i = 0; $i -le $Retries; $i++) {
    try {
      return & $Script
    } catch {
      $resp = $_.Exception.Response
      $status = $null
      if ($resp) {
        try { $status = [int]$resp.StatusCode.value__ } catch { $status = $null }
      }

      # Rate limit explícito
      if ($status -eq 429) {
        $retryAfter = 0
        try { $retryAfter = [int]$resp.Headers["Retry-After"] } catch { $retryAfter = 0 }
        if ($retryAfter -gt 0) {
          Start-Sleep -Seconds $retryAfter
          continue
        }
      }

      # 5xx transiente
      if ($status -ge 500 -and $status -lt 600) {
        if ($i -lt $Retries) {
          Start-Sleep -Seconds ([math]::Pow(2, $i))
          continue
        }
      }

      if ($i -lt $Retries) {
        Start-Sleep -Seconds ([math]::Pow(2, $i))
        continue
      }

      throw
    }
  }
}

# --- Endpoints Harvest ---
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

# --- Execução: paginação e stop ---
$stopped = New-Object System.Collections.Generic.List[object]
$page    = 1
$hasNext = $true

while ($hasNext) {
  $resp = Get-RunningTimeEntries -Page $page

  # Normaliza shape da resposta
  $entries = @()
  if ($resp -and $resp.PSObject.Properties.Name -contains 'time_entries') {
    $entries = $resp.time_entries
  } elseif ($resp) {
    $entries = $resp
  }

  foreach ($e in $entries) {
    try {
      Stop-TimeEntry -Id $e.id | Out-Null
      $stopped.Add([pscustomobject]@{
        id           = $e.id
        user         = $e.user.name
        project      = $e.project.name
        task         = $e.task.name
        started_time = $e.started_time
        notes        = $e.notes
        stopped_at   = (Get-Date).ToString("s")
      })
      Write-Output ("Stopped id={0} user={1} project={2} task={3}" -f $e.id,$e.user.name,$e.project.name,$e.task.name)
    } catch {
      Write-Warning ("Falha ao parar id={0}: {1}" -f $e.id, $_.Exception.Message)
    }
  }

  # Harvest retorna links.next quando há próxima página
  $hasNext = $false
  if ($resp -and $resp.PSObject.Properties.Name -contains 'links') {
    $nextLink = $resp.links.next
    if ($nextLink) {
      $hasNext = $true
      $page++
    }
  }
}

# --- Logging CSV diário em HOME ---
if ($stopped.Count -gt 0) {
  $home    = $env:USERPROFILE
  if (-not $home) { $home = $HOME }
  if (-not $home) { $home = "." }
  $logPath = Join-Path $home ("harvest_autostop_{0}.csv" -f (Get-Date -Format "yyyyMMdd"))
  $stopped | Export-Csv -NoTypeInformation -Append -Path $logPath -Encoding UTF8
  Write-Output ("Log salvo em: {0}" -f $logPath)
} else {
  Write-Output "Nenhum timer em execução encontrado."
}
