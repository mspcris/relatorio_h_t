# =======================
# deploy.ps1  (stage + rsync)
# =======================

# ===== CONFIG (DEPLOY sem senha) =====
$scp = "$env:SystemRoot\System32\OpenSSH\scp.exe"
$ssh = "$env:SystemRoot\System32\OpenSSH\ssh.exe"

$key        = "$env:USERPROFILE\.ssh\contabo_deploy"
$remoteHost = "deploy@154.38.172.227"

$root = "C:\Users\csdg\Documents\GitHub\projetos\RELATORIO_H_T"
$py   = Join-Path $root 'analyze_groq.py'

# pastas do projeto
$dirsPublic      = @('js','css','images','fonts')     # -> /var/www/{...}
$dirsPublicJson  = @('json_consolidado')              # -> /var/www/json_consolidado/{...}


# HTMLs do RAIZ -> /var/www
$htmlFiles = Get-ChildItem $root -File -Filter *.html
$loginPath = Join-Path $root 'login.html'

if (-not (Test-Path $py))        { throw "analyze_groq.py não encontrado em $py" }
if (-not $htmlFiles)             { throw "nenhum .html encontrado no raiz do projeto $root" }
if (-not (Test-Path $loginPath)) { throw "login.html não encontrado no raiz do projeto" }

# ===== VARIÁVEIS DE STAGE =====
$build = (Get-Date).ToString('yyyyMMddHHmmss')
$stage = "/home/deploy/stage/$build"

# ===== PREP REMOTO: cria stage e reports =====
$prep = @"
set -Eeuo pipefail
umask 022
mkdir -p '$stage' '$stage/reports'
"@
($prep -replace "`r","") | & $ssh -i $key -o IdentitiesOnly=yes $remoteHost "/bin/bash -s"

# ===== SCP: diretórios de reports -> $stage/reports/<dir> =====
foreach ($d in $dirsReports) {
  $src = Join-Path $root $d
  if (Test-Path $src) {
    # garante pasta alvo específica (ex.: $stage/reports/json_retorno_groq)
    & $ssh -i $key -o IdentitiesOnly=yes $remoteHost "mkdir -p '$stage/reports/$d'"
    # envia CONTEÚDO do diretório (evita canonicalization do scp)
    & $scp -i $key -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new -r `
      (Join-Path $src '*') "${remoteHost}:$stage/reports/$d/"
  }
}

# ===== SCP: assets públicos -> $stage =====
foreach ($d in $dirsPublic) {
  $src = Join-Path $root $d
  if (Test-Path $src) {
    & $scp -i $key -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new -r `
      $src "${remoteHost}:$stage/"
  }
}

# ===== SCP: json_consolidado -> $stage =====
foreach ($d in $dirsPublicJson) {
  $src = Join-Path $root $d
  if (Test-Path $src) {
    & $scp -i $key -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new -r `
      $src "${remoteHost}:$stage/"
  }
}

# ===== SCP: HTMLs do raiz -> $stage =====
foreach ($h in $htmlFiles) {
  & $scp -i $key -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new `
    $h.FullName "${remoteHost}:$stage/$($h.Name)"
}

# ===== PROMOÇÃO: rsync stage -> /var/www, perms e checksum =====
$final = @"
set -Eeuo pipefail
umask 022

sudo -n rsync -a --delete '$stage/' /var/www/

sudo -n chown -R deploy:www-data /var/www
sudo -n find /var/www -type d -exec chmod 2775 {} +
sudo -n find /var/www -type f -exec chmod 0644 {} +
sudo -n chmod 755 /var /var/www

echo 'SHA256:'
sudo -n sha256sum /var/www/reports/analyze_groq.py 2>/dev/null || true

rm -rf '$stage'
"@
($final -replace "`r","") | & $ssh -i $key -o IdentitiesOnly=yes $remoteHost "/bin/bash -s"

Write-Host "Deploy finalizado via stage -> rsync."
