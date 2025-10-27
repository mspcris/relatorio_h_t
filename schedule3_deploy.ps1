# ===== CONFIG (DEPLOY, sem senha) =====
$scp = "$env:SystemRoot\System32\OpenSSH\scp.exe"
$ssh = "$env:SystemRoot\System32\OpenSSH\ssh.exe"

$key        = "$env:USERPROFILE\.ssh\contabo_deploy"
$remoteHost = "deploy@154.38.172.227"

$root = "C:\Users\csdg\Documents\GitHub\projetos\RELATORIO_H_T"
$py   = Join-Path $root 'analyze_groq.py'

# pastas do projeto -> destinos reais
$dirsPublic      = @('js','css','images','fonts')     # -> /var/www/{...}
$dirsPublicJson  = @('json_consolidado')              # -> /var/www/json_consolidado/{...}
$dirsReports     = @('json_retorno_groq')             # -> /var/www/reports/{...}

# HTMLs do RAIZ -> /var/www
$htmlFiles = Get-ChildItem $root -File -Filter *.html
$loginPath = Join-Path $root 'login.html'

if (-not (Test-Path $py))        { throw "analyze_groq.py não encontrado em $py" }
if (-not $htmlFiles)             { throw "nenhum .html encontrado no raiz do projeto $root" }
if (-not (Test-Path $loginPath)) { throw "login.html não encontrado no raiz do projeto" }

# ===== PREP REMOTO =====
$prep = @"
set -Eeuo pipefail
umask 022

# garanta posse antes de limpar (evita Permission denied)
chown -R deploy:www-data /var/www 2>/dev/null || true
mkdir -p /var/www /var/www/reports

# zera destinos controlados por nós
rm -rf /var/www/css /var/www/js /var/www/images /var/www/fonts
rm -rf /var/www/json_consolidado
rm -rf /var/www/reports/json_retorno_groq

# recria estrutura
mkdir -p /var/www/css /var/www/js /var/www/images /var/www/fonts
mkdir -p /var/www/json_consolidado
mkdir -p /var/www/reports
"@
($prep -replace "`r","") | & $ssh -i $key -o IdentitiesOnly=yes $remoteHost "/bin/bash -s"

# ===== COPIA ASSETS PÚBLICOS -> /var/www =====
foreach ($d in $dirsPublic) {
  $src = Join-Path $root $d
  if (Test-Path $src) {
    & $scp -i $key -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new -r `
      $src "${remoteHost}:/var/www/"
  }
}

# ===== COPIA JSON CONSOLIDADO -> /var/www/json_consolidado =====
foreach ($d in $dirsPublicJson) {
  $src = Join-Path $root $d
  if (Test-Path $src) {
    & $scp -i $key -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new -r `
      $src "${remoteHost}:/var/www/"
  }
}

# ===== COPIA DIRETÓRIOS DE REPORTS -> /var/www/reports =====
foreach ($d in $dirsReports) {
  $src = Join-Path $root $d
  if (Test-Path $src) {
    & $scp -i $key -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new -r `
      $src "${remoteHost}:/var/www/reports/"
  }
}

# ===== COPIA HTMLs DO RAIZ -> /var/www =====
foreach ($h in $htmlFiles) {
  & $scp -i $key -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new `
    $h.FullName "${remoteHost}:/var/www/$($h.Name)"
}

# garante /var/www/login.html
& $scp -i $key -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new `
  $loginPath "${remoteHost}:/var/www/login.html"

# ===== PY DA API -> SOMENTE /var/www/reports =====
& $scp -i $key -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new `
  $py "${remoteHost}:/var/www/reports/analyze_groq.py"

# ===== PERMISSÕES, POSSE E CHECKSUM =====
$final = @"
set -Eeuo pipefail
umask 022

# donos corretos (deploy escreve; nginx lê via grupo)
chown -R deploy:www-data /var/www

# diretórios com setgid para manter grupo; arquivos 0644
find /var/www -type d -exec chmod 2775 {} +
find /var/www -type f -exec chmod 0644 {} +

# travessia
chmod 755 /var /var/www

echo 'SHA256:'
sha256sum /var/www/reports/analyze_groq.py 2>/dev/null || true
"@
($final -replace "`r","") | & $ssh -i $key -o IdentitiesOnly=yes $remoteHost "/bin/bash -s"
