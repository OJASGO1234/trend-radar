<#
.SYNOPSIS
  Start / stop / inspect the whole streaming pipeline (Kafka + 3 producers + db_sink).
.EXAMPLE
  .\scripts\run_stream.ps1 start      # Kafka (docker), 3 producers, db_sink - all in the background
  .\scripts\run_stream.ps1 status
  .\scripts\run_stream.ps1 stop       # db_sink stops gracefully; producers and kafka are stopped too
  .\scripts\run_stream.ps1 stop -KeepKafka
  .\scripts\run_stream.ps1 restart-sink
Logs go to logs\<name>.log, PIDs to .state\pids.json.
#>
param(
    [Parameter(Position = 0)][ValidateSet("start", "stop", "status", "restart-sink", "kill-sink")][string]$Action = "status",
    [switch]$KeepKafka
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
$Py = Join-Path $Root ".venv\Scripts\python.exe"
$StateDir = Join-Path $Root ".state"
$LogDir = Join-Path $Root "logs"
$PidFile = Join-Path $StateDir "pids.json"
$Compose = Join-Path $Root "infra\docker-compose.kafka.yml"
$Procs = [ordered]@{
    bluesky    = "streaming.producers.bluesky"
    wikipedia  = "streaming.producers.wikipedia"
    hackernews = "streaming.producers.hackernews"
    db_sink    = "streaming.consumers.db_sink"
}
New-Item -ItemType Directory -Force $StateDir, $LogDir | Out-Null

function Get-Pids {
    if (Test-Path $PidFile) { Get-Content $PidFile -Raw | ConvertFrom-Json } else { [pscustomobject]@{} }
}
function Save-Pids($obj) { $obj | ConvertTo-Json | Set-Content $PidFile -Encoding utf8 }
function Test-Alive($procId) { [bool]($procId -and (Get-Process -Id $procId -ErrorAction SilentlyContinue)) }

function Start-One($name) {
    $log = Join-Path $LogDir "$name.log"
    # python logging writes to stderr -> <name>.log ; stdout -> <name>.log.out
    $p = Start-Process -FilePath $Py -ArgumentList @("-u", "-m", $Procs[$name]) -WorkingDirectory $Root `
        -WindowStyle Hidden -PassThru -RedirectStandardError $log -RedirectStandardOutput "$log.out"
    $pids = Get-Pids
    $pids | Add-Member -NotePropertyName $name -NotePropertyValue $p.Id -Force
    Save-Pids $pids
    Write-Host ("started {0,-11} pid={1}  log={2}" -f $name, $p.Id, $log)
}

function Stop-Tree($procId) {
    # the venv python.exe is a launcher that spawns the real interpreter; /T kills both
    taskkill /PID $procId /T /F | Out-Null
}

function Stop-Sink([switch]$Hard) {
    $sinkPid = (Get-Pids).db_sink
    if (-not (Test-Alive $sinkPid)) { Write-Host "db_sink not running"; return }
    if ($Hard) { Stop-Tree $sinkPid; Write-Host "db_sink KILLED (no flush, no commit)"; return }
    # graceful: the sink polls for this file, flushes its batch, commits offsets, exits
    New-Item -ItemType File -Force (Join-Path $StateDir "db_sink.stop") | Out-Null
    for ($i = 0; $i -lt 40 -and (Test-Alive $sinkPid); $i++) { Start-Sleep -Milliseconds 500 }
    if (Test-Alive $sinkPid) { Write-Warning "db_sink did not exit in 20 s, killing"; Stop-Tree $sinkPid }
    else { Write-Host "db_sink stopped gracefully" }
}

switch ($Action) {
    "start" {
        cmd /c "docker info >nul 2>&1"
        if ($LASTEXITCODE -ne 0) { throw "Docker is not running. Start Docker Desktop first." }
        cmd /c "docker compose -f `"$Compose`" up -d >nul 2>&1"
        Write-Host "waiting for Kafka to be healthy..."
        for ($i = 0; $i -lt 30; $i++) {
            if ((cmd /c "docker inspect -f {{.State.Health.Status}} kafka 2>&1") -eq "healthy") { break }
            Start-Sleep 3
        }
        $pids = Get-Pids
        foreach ($n in $Procs.Keys) {
            if (Test-Alive $pids.$n) { Write-Host "$n already running pid=$($pids.$n)" } else { Start-One $n }
        }
    }
    "restart-sink" { Stop-Sink; Start-One "db_sink" }
    "kill-sink"    { Stop-Sink -Hard }
    "stop" {
        Stop-Sink
        $pids = Get-Pids
        foreach ($n in $Procs.Keys) {
            if ($n -ne "db_sink" -and (Test-Alive $pids.$n)) { Stop-Tree $pids.$n; Write-Host "stopped $n" }
        }
        Remove-Item $PidFile -ErrorAction SilentlyContinue
        if (-not $KeepKafka) { cmd /c "docker compose -f `"$Compose`" stop >nul 2>&1"; Write-Host "kafka stopped (data kept in volume)" }
    }
    "status" {
        $pids = Get-Pids
        foreach ($n in $Procs.Keys) {
            $state = if (Test-Alive $pids.$n) { "running pid=$($pids.$n)" } else { "stopped" }
            Write-Host ("{0,-11} {1}" -f $n, $state)
        }
        Write-Host ("kafka       " + (cmd /c "docker inspect -f ""{{.State.Status}} ({{.State.Health.Status}})"" kafka 2>&1"))
    }
}
