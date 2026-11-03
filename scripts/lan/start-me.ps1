# Instance "me": your normal data (backend/.env). Frontend on the LAN, backend on 127.0.0.1.
param([int]$FrontendPort = 5173, [int]$BackendPort = 8000)
. (Join-Path $PSScriptRoot 'lan.ps1')
Start-Instance -Name me -FrontendPort $FrontendPort -BackendPort $BackendPort
