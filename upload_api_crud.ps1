# === COPY api_crud.py -> /opt/users_api (ROOT) ===
$scp = "$env:SystemRoot\System32\OpenSSH\scp.exe"
$ssh = "$env:SystemRoot\System32\OpenSSH\ssh.exe"

$keyRoot    = "$env:USERPROFILE\.ssh\contabo_deploy"
$remoteRoot = "root@154.38.172.227"

$localRoot  = "C:\Users\csdg\Documents\GitHub\projetos\RELATORIO_H_T"
$apiFile    = Join-Path $localRoot 'api_crud.py'
$remoteDir  = "/opt/users_api"
$remoteFile = "/opt/users_api/api_crud.py"

if (-not (Test-Path $apiFile)) { throw "api_crud.py não encontrado em $apiFile" }

# 1) prepara destino sem here-string
& $ssh -i $keyRoot -o IdentitiesOnly=yes $remoteRoot `
  "/bin/bash -lc 'set -euo pipefail; umask 022; install -d -m 0755 $remoteDir; chown root:root $remoteDir'"

# 2) copia arquivo
& $scp -i $keyRoot -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new `
  $apiFile "${remoteRoot}:${remoteFile}"

# 3) perms + validação, também inline
& $ssh -i $keyRoot -o IdentitiesOnly=yes $remoteRoot `
  "/bin/bash -lc 'set -euo pipefail; umask 022; chown root:root $remoteFile; chmod 0644 $remoteFile; echo REMOTE_SHA256:; sha256sum $remoteFile; ls -l $remoteFile'"
