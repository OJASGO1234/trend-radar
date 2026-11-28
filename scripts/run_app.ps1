<#
.SYNOPSIS
  Start/stop the API (FastAPI on :8000) and the React UI (Vite on :5173) with one command.
.EXAMPLE
  .\scripts\run_app.ps1 start     # both, in the background; open http://localhost:5173
  .\scripts\run_app.ps1 status
  .\scripts\run_app.ps1 stop
Logs: logs\api.log, logs\web.log. PIDs: .state\app_pids.json.
#>
param([Parameter(Position = 0)][ValidateSet("start", "stop", "status")][string]$Action = "status")

$Root = Split-Path -Parent $PSScriptRoot
$Py = Join-Path $Root ".venv\Scripts\python.exe"
$StateDir = Join-Path $Root ".state"
$LogDir = Join-Path $Root "logs"
$PidFile = Join-Path $StateDir "app_pids.json"
New-Item -ItemType Directory -Force $StateDir, $LogDir | Out-Null

function Get-Pids { if (Test-Path $PidFile) { Get-Content $PidFile -Raw | ConvertFrom-Json } else { [pscustomobject]@{} } }
function Test-Alive($procId) { [bool]($procId -and (Get-Process -Id $procId -ErrorAction SilentlyContinue)) }
function Stop-Tree($procId) { taskkill /PID $procId /T /F | Out-Null }   # uvicorn/npm spawn children; /T kills them too
function Test-Port($port) { [bool](Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue) }

switch ($Action) {
    "start" {
        $pids = Get-Pids
        foreach ($port in 8000, 5173) {
            if (Test-Port $port -and -not (Test-Alive ($pids.api, $pids.web | Where-Object { $_ }))) {
                throw "port $port is already in use by another process (run .\scripts\run_app.ps1 stop, or free the port)"
            }
        }
        $api = Start-Process -FilePath $Py -ArgumentList @("-m", "uvicorn", "api.main:app", "--port", "8000") -WorkingDirectory $Root `
            -WindowStyle Hidden -PassThru -RedirectStandardError (Join-Path $LogDir "api.log") -RedirectStandardOutput (Join-Path $LogDir "api.out.log")
        $web = Start-Process -FilePath "npm.cmd" -ArgumentList @("run", "dev") -WorkingDirectory (Join-Path $Root "web") `
            -WindowStyle Hidden -PassThru -RedirectStandardError (Join-Path $LogDir "web.err.log") -RedirectStandardOutput (Join-Path $LogDir "web.log")
        @{ api = $api.Id; web = $web.Id } | ConvertTo-Json | Set-Content $PidFile -Encoding utf8
        for ($i = 0; $i -lt 30 -and -not ((Test-Port 8000) -and (Test-Port 5173)); $i++) { Start-Sleep 1 }
        Write-Host "API http://localhost:8000/docs   UI http://localhost:5173   (logs in logs\)"
    }
    "stop" {
        $pids = Get-Pids
        foreach ($n in "api", "web") { if (Test-Alive $pids.$n) { Stop-Tree $pids.$n; Write-Host "stopped $n" } }
        Remove-Item $PidFile -ErrorAction SilentlyContinue
    }
    "status" {
        $pids = Get-Pids
        Write-Host ("api  " + $(if (Test-Alive $pids.api) { "running pid=$($pids.api) port 8000 listening=$(Test-Port 8000)" } else { "stopped" }))
        Write-Host ("web  " + $(if (Test-Alive $pids.web) { "running pid=$($pids.web) port 5173 listening=$(Test-Port 5173)" } else { "stopped" }))
    }
}
