# ===== CONFIG (ROOT) =====
$scp = "$env:SystemRoot\System32\OpenSSH\scp.exe"
$ssh = "$env:SystemRoot\System32\OpenSSH\ssh.exe"

$key        = "$env:USERPROFILE\.ssh\contabo_deploy"   # se não houver chave para root, será pedido password
$remoteHost = "root@154.38.172.227"
$remoteTmp  = "/root/analyze_groq.py"

$root = "C:\Users\csdg\Documents\GitHub\projetos\RELATORIO_H_T"
$py   = Join-Path $root 'analyze_groq.py'

# ===== GUARD =====
if (-not (Test-Path $py)) { throw "analyze_groq.py não encontrado em $py" }
$localSha = (Get-FileHash -Algorithm SHA256 $py).Hash
"LOCAL SHA256: $localSha" | Write-Host

# ===== UPLOAD DIRETO PARA /root =====
& $scp -i $key -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new `
        $py "${remoteHost}:${remoteTmp}"

# ===== APPLY REMOTO (ROOT) =====
$remote = @'
set -Eeuo pipefail
src="/root/analyze_groq.py"
dst1="/var/www/seuapp/analyze_groq.py"
dst2="/var/www/reports/analyze_groq.py"

install -d -m 2775 -o root -g www-data /var/www/seuapp /var/www/reports
install -m 0644 "$src" "$dst1"
install -m 0644 "$src" "$dst2"
chown root:www-data "$dst1" "$dst2"

echo "REMOTE SHA256:"; sha256sum "$dst1" "$dst2"
echo "TIMESTAMPS (America/Sao_Paulo):"
TZ=America/Sao_Paulo stat -c "%n | birth=%w | mtime=%y | owner=%U:%G" "$dst1" "$dst2"
'@

($remote -replace "`r","") | & $ssh -i $key -o IdentitiesOnly=yes $remoteHost "/bin/bash -s"
