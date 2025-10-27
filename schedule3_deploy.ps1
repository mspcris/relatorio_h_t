# ===== CONFIG (DEPLOY, sem senha) =====
$scp = "$env:SystemRoot\System32\OpenSSH\scp.exe"
$ssh = "$env:SystemRoot\System32\OpenSSH\ssh.exe"

$key        = "$env:USERPROFILE\.ssh\contabo_deploy"
$remoteHost = "deploy@154.38.172.227"

$root = "C:\Users\csdg\Documents\GitHub\projetos\RELATORIO_H_T"
$py   = Join-Path $root 'analyze_groq.py'
$dirs = @('templates','js','css','images','fonts','json_consolidado','json_retorno_groq')

if (-not (Test-Path $py)) { throw "analyze_groq.py não encontrado em $py" }

# prepara e zera /var/www/reports
$prep = @'
set -Eeuo pipefail
mkdir -p /var/www/reports /var/www/seuapp
# apaga conteúdo atual do reports
rm -rf /var/www/reports/*
# limpeza de bytecode
find /var/www/seuapp -type d -name __pycache__ -prune -exec rm -rf {} + || true
'@
($prep -replace "`r","") | & $ssh -i $key -o IdentitiesOnly=yes $remoteHost "/bin/bash -s"

# copia diretórios para /var/www/reports
foreach ($d in $dirs) {
  $src = Join-Path $root $d
  if (Test-Path $src) {
    & $scp -i $key -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new -r `
      $src "${remoteHost}:/var/www/reports/"
  }
}

# copia analyze_groq.py para os dois destinos
& $scp -i $key -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new `
  $py "${remoteHost}:/var/www/reports/analyze_groq.py"
& $scp -i $key -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new `
  $py "${remoteHost}:/var/www/seuapp/analyze_groq.py"

# acerta permissões e confirma
$final = @'
set -Eeuo pipefail
# group e perms (deploy já é owner; setgid garante grupo www-data)
find /var/www/reports -type d -exec chmod 2775 {} +
find /var/www/reports -type f -exec chmod 0644 {} +
chmod 0644 /var/www/seuapp/analyze_groq.py
touch /var/www/seuapp/analyze_groq.py /var/www/reports/analyze_groq.py
echo "SHA256:"; sha256sum /var/www/seuapp/analyze_groq.py /var/www/reports/analyze_groq.py
'@
($final -replace "`r","") | & $ssh -i $key -o IdentitiesOnly=yes $remoteHost "/bin/bash -s"
