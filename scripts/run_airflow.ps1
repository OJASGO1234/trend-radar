<#
.SYNOPSIS
  Start/stop Airflow (single container) and trigger the DAG.
.EXAMPLE
  .\scripts\run_airflow.ps1 up          # build image (first time) + start; UI http://localhost:8080
  .\scripts\run_airflow.ps1 trigger     # run trend_radar_daily once now
  .\scripts\run_airflow.ps1 status      # latest runs of the DAG
  .\scripts\run_airflow.ps1 tasks       # task states of the latest run
  .\scripts\run_airflow.ps1 password    # admin password for the UI
  .\scripts\run_airflow.ps1 down
#>
param([Parameter(Position = 0)][ValidateSet("up", "down", "trigger", "status", "tasks", "password", "logs")][string]$Action = "status")

$Root = Split-Path -Parent $PSScriptRoot
$Compose = Join-Path $Root "infra\docker-compose.airflow.yml"
$EnvFile = Join-Path $Root ".env"
$Dag = "trend_radar_daily"

# cmd /c so docker's stderr chatter is not treated as a PowerShell error
function Invoke-Docker([string]$cmdline) { cmd /c "$cmdline 2>&1" }

switch ($Action) {
    "up" {
        Invoke-Docker "docker compose --env-file `"$EnvFile`" -f `"$Compose`" up -d --build"
        Write-Host "UI: http://localhost:8080  (first start takes 1-2 min: it creates the metadata DB and an admin user)"
    }
    "down"     { Invoke-Docker "docker compose --env-file `"$EnvFile`" -f `"$Compose`" down" }
    "password" { Invoke-Docker "docker exec airflow cat /opt/airflow/standalone_admin_password.txt" }
    "logs"     { Invoke-Docker "docker logs --tail 60 airflow" }
    "trigger"  {
        Invoke-Docker "docker exec airflow airflow dags unpause $Dag"
        Invoke-Docker "docker exec airflow airflow dags trigger $Dag"
    }
    "status"   { Invoke-Docker "docker exec airflow airflow dags list-runs -d $Dag -o table" }
    "tasks"    {
        # states-for-dag-run wants the run's logical date; take it from the newest run
        $row = Invoke-Docker "docker exec airflow airflow dags list-runs -d $Dag -o table" | Where-Object { $_ -match "^$Dag \|" } | Select-Object -First 1
        $date = ($row -split "\|")[3].Trim()
        Invoke-Docker "docker exec airflow airflow tasks states-for-dag-run $Dag $date -o table" | Where-Object { $_ -notmatch " INFO " }
    }
}
