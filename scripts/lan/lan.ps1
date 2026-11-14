# Shared helpers for the LAN start/stop scripts. Dot-sourced; not meant to be run directly.
# Each instance = one uvicorn backend on 127.0.0.1 + one Vite dev server on 0.0.0.0 whose
# /api proxy points at that backend. Browsers (yours or your friend's) only use the Vite port.

$ErrorActionPreference = 'Stop'
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$BackendDir = Join-Path $RepoRoot 'backend'
$FrontendDir = Join-Path $RepoRoot 'frontend'
$RunDir = Join-Path $RepoRoot '.lan-run'   # PIDs + logs (git-ignored)

function Get-LanIp {
    # IPv4 of the interface that carries the default route (lowest metric), e.g. your Wi-Fi.
    $route = Get-NetRoute -DestinationPrefix '0.0.0.0/0' -ErrorAction SilentlyContinue |
        Sort-Object { $_.RouteMetric + (Get-NetIPInterface -InterfaceIndex $_.InterfaceIndex -AddressFamily IPv4).InterfaceMetric } |
        Select-Object -First 1
    if (-not $route) { throw 'No default route found: are you connected to Wi-Fi/LAN?' }
    $ip = Get-NetIPAddress -InterfaceIndex $route.InterfaceIndex -AddressFamily IPv4 |
        Where-Object { $_.IPAddress -notlike '169.254.*' } | Select-Object -First 1
    if (-not $ip) { throw 'Could not determine the LAN IP address.' }
    return $ip.IPAddress
}

function Test-PortListening([int]$Port) {
    [bool](Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue)
}

function Test-BackendHealthy([int]$Port) {
    try {
        $r = Invoke-WebRequest -UseBasicParsing -TimeoutSec 3 "http://127.0.0.1:$Port/api/v1/health/live"
        return $r.StatusCode -eq 200
    } catch { return $false }
}

function Read-EnvFile([string]$Path) {
    # Minimal KEY=VALUE parser; strips one pair of surrounding quotes. No inline comments.
    $vars = [ordered]@{}
    foreach ($line in Get-Content -LiteralPath $Path) {
        $t = $line.Trim()
        if (-not $t -or $t.StartsWith('#')) { continue }
        $i = $t.IndexOf('=')
        if ($i -lt 1) { continue }
        $k = $t.Substring(0, $i).Trim()
        $v = $t.Substring($i + 1).Trim()
        if ($v.Length -ge 2 -and (($v[0] -eq "'" -and $v[-1] -eq "'") -or ($v[0] -eq '"' -and $v[-1] -eq '"'))) {
            $v = $v.Substring(1, $v.Length - 2)
        }
        $vars[$k] = $v
    }
    return $vars
}

function Start-WithEnv {
    # Start a hidden process with extra env vars, without leaking them into the caller's shell.
    param([hashtable]$Env, [string]$FilePath, [string[]]$ArgumentList, [string]$WorkingDirectory, [string]$LogName)
    $saved = @{}
    foreach ($k in $Env.Keys) {
        $saved[$k] = [Environment]::GetEnvironmentVariable($k, 'Process')
        [Environment]::SetEnvironmentVariable($k, [string]$Env[$k], 'Process')
    }
    try {
        return Start-Process -FilePath $FilePath -ArgumentList $ArgumentList -WorkingDirectory $WorkingDirectory `
            -WindowStyle Hidden -PassThru `
            -RedirectStandardOutput (Join-Path $RunDir "$LogName.out.log") `
            -RedirectStandardError (Join-Path $RunDir "$LogName.err.log")
    } finally {
        foreach ($k in $saved.Keys) { [Environment]::SetEnvironmentVariable($k, $saved[$k], 'Process') }
    }
}

function Start-Instance {
    param(
        [Parameter(Mandatory)][string]$Name,
        [Parameter(Mandatory)][int]$FrontendPort,
        [Parameter(Mandatory)][int]$BackendPort,
        [string]$EnvFile  # optional overrides on top of backend/.env
    )
    New-Item -ItemType Directory -Force $RunDir | Out-Null
    $pidFile = Join-Path $RunDir "$Name.pids"
    $lanIp = Get-LanIp
    $url = "http://${lanIp}:$FrontendPort"
    Write-Host "[$Name] LAN IP: $lanIp"

    if (Test-PortListening $FrontendPort) {
        throw "[$Name] Port $FrontendPort is already in use. Stop whatever runs there (e.g. a plain 'npm run dev', or scripts\lan\stop-all) and retry."
    }

    $backendEnv = @{}
    if ($EnvFile) { $backendEnv = [hashtable](Read-EnvFile $EnvFile) }
    # Allow both the localhost and the LAN origin of this instance's frontend.
    $backendEnv['CORS_ORIGINS'] = "[`"http://localhost:$FrontendPort`", `"http://127.0.0.1:$FrontendPort`", `"$url`"]"
    if ($EnvFile) { $backendEnv['PUBLIC_APP_URL'] = $url }

    $pids = @()
    if (Test-PortListening $BackendPort) {
        if (-not (Test-BackendHealthy $BackendPort)) {
            throw "[$Name] Port $BackendPort is in use by something that is not this app's backend."
        }
        if ($EnvFile) {
            throw "[$Name] A backend is already running on $BackendPort; refusing to reuse it for an isolated instance. Run scripts\lan\stop-$Name first."
        }
        Write-Host "[$Name] Backend already running on 127.0.0.1:$BackendPort (started by you) - reusing it, not managing it."
    } else {
        $python = Join-Path $BackendDir '.venv\Scripts\python.exe'
        $be = Start-WithEnv -Env $backendEnv -FilePath $python -WorkingDirectory $BackendDir -LogName "$Name-backend" `
            -ArgumentList @('-m', 'uvicorn', 'app.main:app', '--host', '127.0.0.1', '--port', "$BackendPort")
        $pids += $be.Id
        Write-Host "[$Name] Backend starting on 127.0.0.1:$BackendPort (pid $($be.Id))..."
        $deadline = (Get-Date).AddSeconds(90)
        while (-not (Test-BackendHealthy $BackendPort)) {
            if ($be.HasExited -or (Get-Date) -gt $deadline) {
                $pids | Set-Content $pidFile
                throw "[$Name] Backend did not become healthy. See $RunDir\$Name-backend.err.log"
            }
            Start-Sleep -Milliseconds 500
        }
    }

    $node = (Get-Command node).Source
    $feEnv = @{
        API_PROXY_TARGET = "http://127.0.0.1:$BackendPort"
        VITE_CACHE_DIR   = "node_modules/.vite-$Name"
    }
    $fe = Start-WithEnv -Env $feEnv -FilePath $node -WorkingDirectory $FrontendDir -LogName "$Name-frontend" `
        -ArgumentList @('node_modules/vite/bin/vite.js', '--host', '0.0.0.0', '--port', "$FrontendPort", '--strictPort')
    $pids += $fe.Id
    $pids | Set-Content $pidFile

    $deadline = (Get-Date).AddSeconds(60)
    while (-not (Test-PortListening $FrontendPort)) {
        if ($fe.HasExited -or (Get-Date) -gt $deadline) {
            throw "[$Name] Frontend did not start. See $RunDir\$Name-frontend.err.log"
        }
        Start-Sleep -Milliseconds 500
    }
    Write-Host ""
    Write-Host "[$Name] Ready:  $url   (on this PC also http://localhost:$FrontendPort)" -ForegroundColor Green
    Write-Host "[$Name] Logs:   $RunDir\$Name-*.log"
}

function Stop-Instance([string]$Name) {
    $pidFile = Join-Path $RunDir "$Name.pids"
    if (-not (Test-Path $pidFile)) { Write-Host "[$Name] Not running (no pid file)."; return }
    foreach ($p in Get-Content $pidFile) {
        if ($p -and (Get-Process -Id $p -ErrorAction SilentlyContinue)) {
            # /T kills the child processes too (the venv python launcher spawns the real interpreter).
            & taskkill.exe /PID $p /T /F | Out-Null
            Write-Host "[$Name] Stopped pid $p"
        }
    }
    Remove-Item $pidFile -Force
}
