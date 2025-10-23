# upload_report.ps1
$ErrorActionPreference = "Stop"; Set-StrictMode -Version Latest

$scp    = "$env:SystemRoot\System32\OpenSSH\scp.exe"
$ssh    = "$env:SystemRoot\System32\OpenSSH\ssh.exe"
$key    = "$env:USERPROFILE\.ssh\contabo_deploy"
$server = "deploy@154.38.172.227"

$root      = "C:\Users\csdg\Documents\GitHub\projetos\RELATORIO_H_T"
$srcTpl    = Join-Path $root 'templates'
$srcJs     = Join-Path $root 'js'
$srcCss    = Join-Path $root 'css'
$srcImg    = Join-Path $root 'images'
$srcFnt    = Join-Path $root 'fonts'
$srcJson   = Join-Path $root 'json_consolidado'   # NOVO

# 1) staging no servidor
& $ssh -i $key $server "/bin/mkdir -p /var/www/reports/.staging/{templates,js,css,images,fonts,json_consolidado}"

Write-Host "=> Enviando para staging"
& $scp -i $key -o BatchMode=yes -o StrictHostKeyChecking=accept-new -r $srcTpl  "${server}:/var/www/reports/.staging/"
& $scp -i $key -o BatchMode=yes -o StrictHostKeyChecking=accept-new -r $srcJs   "${server}:/var/www/reports/.staging/"
& $scp -i $key -o BatchMode=yes -o StrictHostKeyChecking=accept-new -r $srcCss  "${server}:/var/www/reports/.staging/"
& $scp -i $key -o BatchMode=yes -o StrictHostKeyChecking=accept-new -r $srcImg  "${server}:/var/www/reports/.staging/"
& $scp -i $key -o BatchMode=yes -o StrictHostKeyChecking=accept-new -r $srcFnt  "${server}:/var/www/reports/.staging/"
& $scp -i $key -o BatchMode=yes -o StrictHostKeyChecking=accept-new -r $srcJson "${server}:/var/www/reports/.staging/"  # NOVO

# 2) aplicar e ajustar permissões usando bash via STDIN
$remote = @'
set -e
dst=/var/www/reports
stage=/var/www/reports/.staging

/bin/mkdir -p "$dst/templates" "$dst/js" "$dst/css" "$dst/images" "$dst/fonts" "$dst/json_consolidado"

# assets
for d in js css images fonts json_consolidado; do
  /bin/cp -a "$stage/$d/." "$dst/$d/" 2>/dev/null || true
done

# templates
/bin/cp -a "$stage/templates/." "$dst/templates/"

# permissões
/bin/chown -R deploy:www-data "$dst"
/usr/bin/find "$dst" -type d -exec /bin/chmod 2755 {} +
/usr/bin/find "$dst" -type f -exec /bin/chmod 0644 {} +

# index atômico
/usr/bin/install -m 0644 "$stage/templates/index.html" "$dst/templates/index.html.new"
/bin/mv -f "$dst/templates/index.html.new" "$dst/templates/index.html"

# favicon opcional na raiz
/bin/cp -f "$dst/images/favicon.ico" "$dst/favicon.ico" 2>/dev/null || true

/bin/rm -rf "$stage"
'@ -replace "`r",""

Write-Host "=> Aplicando no servidor"
$remote | & $ssh -i $key $server "/bin/bash -s"

Write-Host "=> OK"
