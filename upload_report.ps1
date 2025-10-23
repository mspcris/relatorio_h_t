# upload_report.ps1
$ErrorActionPreference = "Stop"

$scp = "$env:SystemRoot\System32\OpenSSH\scp.exe"
$key = "$env:USERPROFILE\.ssh\contabo_deploy"

$srcRel = 'C:\Users\csdg\Documents\GitHub\projetos\RELATORIO_H_T\relatorio\*'
$srcTpl = 'C:\Users\csdg\Documents\GitHub\projetos\RELATORIO_H_T\templates\*'

$dstRoot = 'deploy@154.38.172.227:/var/www/reports/'
$dstTpl  = 'deploy@154.38.172.227:/var/www/reports/templates/'

# HTML e assets do relatório
& $scp -i $key -o BatchMode=yes -o StrictHostKeyChecking=accept-new -r $srcRel $dstRoot

# Templates completos
& $scp -i $key -o BatchMode=yes -o StrictHostKeyChecking=accept-new -r $srcTpl $dstTpl
