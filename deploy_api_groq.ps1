# =======================
# deploy.ps1  (stage + rsync)
# =======================

# --- SSH/Key ---
$scp = "$env:SystemRoot\System32\OpenSSH\scp.exe"
$ssh = "$env:SystemRoot\System32\OpenSSH\ssh.exe"
$key = "$env:USERPROFILE\.ssh\contabo_deploy"
$remoteHost = "deploy@154.38.172.227"

# --- Projeto local ---
$root = "C:\Users\csdg\Documents\GitHub\projetos\RELATORIO_H_T"
$py   = Join-Path $root 'analyze_groq.py'
if (-not (Test-Path $py)) { throw "analyze_groq.py não encontrado em $py" }

# Pastas públicas
$dirsPublic      = @('js','css','images','fonts')     # -> /var/www/{...}
$dirsPublicJson  = @('json_consolidado')              # -> /var/www/json_consolidado/{...}
# Se ainda houver conteúdo estático em /var/www/reports, mantenha aqui:
$dirsReportsWeb  = @('json_retorno_groq')             # -> /var/www/reports/{...}

# HTMLs do raiz -> /var/www
$htmlFiles = Get-ChildItem $root -File -Filter *.html
$loginPath = Join-Path $root 'login.html'
if (-not $htmlFiles)             { throw "nenhum .html encontrado no raiz do projeto $root" }
if (-not (Test-Path $loginPath)) { throw "login.html não encontrado no raiz do projeto" }

# --- Stage remoto ---
$build = (Get-Date).ToString('yyyyMMddHHmmss')
$stageRoot = "/home/deploy/stage/$build"
$stageWeb  = "$stageRoot/web"
$stageApp  = "$stageRoot/app"

# --- Prep remoto ---
@"
set -Eeuo pipefail
umask 022
mkdir -p '$stageWeb' '$stageApp' '$stageWeb/reports'
"@ -replace "`r","" | & $ssh -i $key -o IdentitiesOnly=yes $remoteHost "/bin/bash -s"

# --- Copia WEB (/var/www) ---
foreach ($d in $dirsPublic) {
  $src = Join-Path $root $d
  if (Test-Path $src) {
    & $scp -i $key -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new -r `
      $src "${remoteHost}:$stageWeb/"
  }
}
foreach ($d in $dirsPublicJson) {
  $src = Join-Path $root $d
  if (Test-Path $src) {
    & $scp -i $key -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new -r `
      $src "${remoteHost}:$stageWeb/"
  }
}
foreach ($d in $dirsReportsWeb) {
  $src = Join-Path $root $d
  if (Test-Path $src) {
    & $ssh -i $key -o IdentitiesOnly=yes $remoteHost "mkdir -p '$stageWeb/reports/$d'"
    & $scp -i $key -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new -r `
      (Join-Path $src '*') "${remoteHost}:$stageWeb/reports/$d/"
  }
}
foreach ($h in $htmlFiles) {
  & $scp -i $key -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new `
    $h.FullName "${remoteHost}:$stageWeb/$($h.Name)"
}

# --- Copia APP (/opt/ia-groq) ---
& $scp -i $key -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new `
  $py "${remoteHost}:$stageApp/analyze_groq.py"

# --- Promoção com rsync + restart da API ---
@"
set -Eeuo pipefail
umask 022

# WEB -> /var/www
sudo -n rsync -a --delete '$stageWeb/' /var/www/
sudo -n chown -R deploy:www-data /var/www
sudo -n find /var/www -type d -exec chmod 2775 {} +
sudo -n find /var/www -type f -exec chmod 0644 {} +
sudo -n chmod 755 /var /var/www

# APP -> /opt/ia-groq
sudo -n mkdir -p /opt/ia-groq
sudo -n rsync -a '$stageApp/' /opt/ia-groq/
sudo -n chown -R root:root /opt/ia-groq
sudo -n find /opt/ia-groq -type f -name '*.py' -exec chmod 0644 {} +
sudo -n chmod 0755 /opt/ia-groq

# (Opcional) garantir diretório de prompts se usado por env PROMPTS_DIR
sudo -n mkdir -p /var/lib/ia-groq/prompts
sudo -n chown -R deploy:www-data /var/lib/ia-groq
sudo -n chmod -R 2775 /var/lib/ia-groq

# Reinicia serviço
sudo -n systemctl restart ia-groq
sudo -n systemctl status --no-pager ia-groq || true

# Checks
echo 'SHA256 (app):'
sudo -n sha256sum /opt/ia-groq/analyze_groq.py 2>/dev/null || true

rm -rf '$stageRoot'
"@ -replace "`r","" | & $ssh -i $key -o IdentitiesOnly=yes $remoteHost "/bin/bash -s"

Write-Host "Deploy finalizado. Web -> /var/www | App -> /opt/ia-groq"
