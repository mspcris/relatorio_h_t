# upload_report.ps1 — tar + staging atômico
$ErrorActionPreference = "Stop"; Set-StrictMode -Version Latest

$scp = "$env:SystemRoot\System32\OpenSSH\scp.exe"
$ssh = "$env:SystemRoot\System32\OpenSSH\ssh.exe"
$tar = "$env:SystemRoot\System32\tar.exe"

$key     = "$env:USERPROFILE\.ssh\contabo_deploy"
$server  = "deploy@154.38.172.227"
$webroot = "/var/www/reports"
$stage   = "$webroot/.staging"

$root        = "C:\Users\csdg\Documents\GitHub\projetos\RELATORIO_H_T"
$srcTpl      = Join-Path $root 'templates'
$srcJs       = Join-Path $root 'js'
$srcCss      = Join-Path $root 'css'
$srcImg      = Join-Path $root 'images'
$srcFnt      = Join-Path $root 'fonts'
$srcJsonCons = Join-Path $root 'json_consolidado'
$srcJsonGroq = Join-Path $root 'json_retorno_groq'
$srcAnalyze  = Join-Path $root 'analyze_groq.py'
$nm          = Join-Path $root 'node_modules'

# subset necessário de node_modules
$nmList = @(
  'jquery/dist/jquery.min.js',
  'bootstrap/dist/js/bootstrap.bundle.min.js',
  'admin-lte/dist/css/adminlte.min.css',
  'admin-lte/dist/js/adminlte.min.js',
  '@fortawesome/fontawesome-free/css/all.min.css',
  '@fortawesome/fontawesome-free/webfonts',
  'chart.js/dist/chart.umd.js'
)

# workspace local temporário
$tmp = Join-Path $env:TEMP "relatorio_ht_deploy"
if (Test-Path $tmp) { Remove-Item -Recurse -Force $tmp }
New-Item -ItemType Directory -Force -Path $tmp | Out-Null

Copy-Item -Recurse -Force $srcTpl      (Join-Path $tmp 'templates')            -ErrorAction SilentlyContinue
Copy-Item -Recurse -Force $srcJs       (Join-Path $tmp 'js')                   -ErrorAction SilentlyContinue
Copy-Item -Recurse -Force $srcCss      (Join-Path $tmp 'css')                  -ErrorAction SilentlyContinue
Copy-Item -Recurse -Force $srcImg      (Join-Path $tmp 'images')               -ErrorAction SilentlyContinue
Copy-Item -Recurse -Force $srcFnt      (Join-Path $tmp 'fonts')                -ErrorAction SilentlyContinue
Copy-Item -Recurse -Force $srcJsonCons (Join-Path $tmp 'json_consolidado')
Copy-Item -Recurse -Force $srcJsonGroq (Join-Path $tmp 'json_retorno_groq')
if (Test-Path $srcAnalyze) {
  Copy-Item -Force $srcAnalyze (Join-Path $tmp 'analyze_groq.py')
}

# copiar subset node_modules
$nmTmp = Join-Path $tmp 'node_modules'
New-Item -ItemType Directory -Force -Path $nmTmp | Out-Null
foreach ($rel in $nmList) {
  $src = Join-Path $nm $rel
  if (-not (Test-Path $src)) { Write-Warning "Não encontrado: $src"; continue }
  $dest = Join-Path $nmTmp $rel
  New-Item -ItemType Directory -Force -Path (Split-Path $dest -Parent) | Out-Null
  Copy-Item -Recurse -Force $src $dest
}

# tarball único
$tgz = Join-Path $env:TEMP "reports_payload.tgz"
if (Test-Path $tgz) { Remove-Item -Force $tgz }
Push-Location $tmp; & $tar -czf $tgz *; Pop-Location

# staging remoto
& $ssh -i $key $server "/bin/mkdir -p $stage"

Write-Host "=> Enviando pacote único"
& $scp -i $key -o BatchMode=yes -o StrictHostKeyChecking=accept-new $tgz "${server}:$stage/"

# bloco remoto SEM interpolação
$remote = @'
set -e
dst="/var/www/reports"
stage="/var/www/reports/.staging"
pkg="$stage/reports_payload.tgz"

/bin/mkdir -p "$dst"
/bin/tar -xzf "$pkg" -C "$stage"

# criar diretórios destino com owner/grupo corretos e g+s
/usr/bin/install -d -o deploy -g www-data -m 2775 \
  "$dst" "$dst/templates" "$dst/js" "$dst/css" "$dst/images" "$dst/fonts" \
  "$dst/json_consolidado" "$dst/json_retorno_groq" "$dst/node_modules"

# normalizar ownership e diretórios antes de copiar
/bin/chown -R deploy:www-data "$dst"
/usr/bin/find "$dst" -type d -exec /bin/chmod 2775 {} +

# copiar payload em blocos
for d in js css images fonts json_consolidado json_retorno_groq node_modules; do
  /bin/cp -a "$stage/$d/." "$dst/$d/" 2>/dev/null || true
done
/bin/cp -a "$stage/templates/." "$dst/templates/" || true

# instalar analyze_groq.py com 0755, se veio no pacote
if [ -f "$stage/analyze_groq.py" ]; then
  /usr/bin/install -o deploy -g www-data -m 0755 "$stage/analyze_groq.py" "$dst/analyze_groq.py"
fi

# harden final de arquivos
/usr/bin/find "$dst" -type f -exec /bin/chmod 0644 {} +
# reatribuir exec ao analyze se existir
[ -f "$dst/analyze_groq.py" ] && /bin/chmod 0755 "$dst/analyze_groq.py"

# atualizações atômicas específicas (se aplicável)
[ -f "$stage/templates/kpi.html" ]   && /usr/bin/install -m 0644 "$stage/templates/kpi.html"   "$dst/templates/kpi.html.new"   && /bin/mv -f "$dst/templates/kpi.html.new"   "$dst/templates/kpi.html"
[ -f "$stage/templates/index.html" ] && /usr/bin/install -m 0644 "$stage/templates/index.html" "$dst/templates/index.html.new" && /bin/mv -f "$dst/templates/index.html.new" "$dst/templates/index.html"

# favicon no webroot
if [ -f "$dst/images/favicon.ico" ]; then
  [ -L "$dst/favicon.ico" ] && /bin/rm -f "$dst/favicon.ico"           # remove symlink pendurado
  /usr/bin/install -m 0644 "$dst/images/favicon.ico" "$dst/favicon.ico"
fi

# limpeza do staging
/bin/rm -f "$pkg"
/bin/rm -rf "$stage"/{js,css,images,fonts,json_consolidado,json_retorno_groq,node_modules,templates,analyze_groq.py}
'@

Write-Host "=> Aplicando no servidor"
($remote -replace "`r","") | & $ssh -i $key $server "/bin/bash -s"

Write-Host "=> OK. Abra: https://teste-ia.camim.com.br/templates/kpi.html"
