# Instance "friend": own database + secrets (backend/.env.friend overrides backend/.env).
param([int]$FrontendPort = 5174, [int]$BackendPort = 8010)
. (Join-Path $PSScriptRoot 'lan.ps1')
$envFile = Join-Path $BackendDir '.env.friend'
if (-not (Test-Path $envFile)) { throw "Missing $envFile (see SHARING.md)." }
Start-Instance -Name friend -FrontendPort $FrontendPort -BackendPort $BackendPort -EnvFile $envFile
