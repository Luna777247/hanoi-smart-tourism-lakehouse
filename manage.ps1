# File: manage.ps1
# Hanoi Smart Tourism Lakehouse Management Script for Windows

param (
    [Parameter(Mandatory = $true, Position = 0)]
    [ValidateSet("build", "package", "init", "up", "up-foundation", "up-orchestration", "up-etl", "up-governance", "up-apps", "down", "clean", "logs", "status", "health-check")]
    [string]$Command,

    [Parameter(Position = 1)]
    [string]$Service = ""
)

$DOCKER_COMPOSE = "docker compose"

# Load .env variables
if (Test-Path ".env") {
    Get-Content .env | Foreach-Object {
        if ($_ -match "^\s*([^#\s=]+)\s*=\s*(.*)$") {
            $name = $Matches[1]
            $value = $Matches[2].Trim()
            Set-Item -Path "Env:$name" -Value $value
        }
    }
}

switch ($Command) {
    "build" {
        Write-Host "==> Building all project images..." -ForegroundColor Cyan
        Invoke-Expression "$DOCKER_COMPOSE build"
    }
    "package" {
        Write-Host "==> Packaging project..." -ForegroundColor Cyan
        Invoke-Expression "$DOCKER_COMPOSE build"
    }
    "init" {
        Write-Host "==> Bootstrapping Lakehouse infrastructure..." -ForegroundColor Cyan
        Invoke-Expression "$DOCKER_COMPOSE up -d postgres redis minio vault"
        Write-Host "Waiting for core services (20s)..." -ForegroundColor Yellow
        Start-Sleep -Seconds 20
        Invoke-Expression "$DOCKER_COMPOSE run --rm minio-init"
        Invoke-Expression "$DOCKER_COMPOSE run --rm airflow-init"
        Write-Host "Initialization complete." -ForegroundColor Green
    }
    "up" {
        Write-Host "==> Starting ALL Lakehouse services..." -ForegroundColor Cyan
        Invoke-Expression "$DOCKER_COMPOSE up -d"
    }
    "up-foundation" {
        Write-Host "==> LAYER 1: Starting Foundation (Storage & DB)..." -ForegroundColor Green
        Invoke-Expression "$DOCKER_COMPOSE up -d postgres redis minio vault keycloak"
        Write-Host "Waiting 15s for core services to stabilize..." -ForegroundColor Yellow
        Start-Sleep -Seconds 15
        Invoke-Expression "$DOCKER_COMPOSE run --rm minio-init"
        Write-Host "Foundation layer is READY." -ForegroundColor Green
    }
    "up-orchestration" {
        Write-Host "==> LAYER 2: Starting Orchestration (Airflow 2.10.x)..." -ForegroundColor Cyan
        Invoke-Expression "$DOCKER_COMPOSE run --rm airflow-init"
        Invoke-Expression "$DOCKER_COMPOSE up -d airflow-api-server airflow-scheduler airflow-triggerer airflow-flower"
        Write-Host "Orchestration layer is READY." -ForegroundColor Cyan
    }
    "up-etl" {
        Write-Host "==> LAYER 3: Starting ETL Engine (Spark & Trino)..." -ForegroundColor Yellow
        
        # Tự động tạo database và bảng metadata nếu chưa có
        Write-Host "Configuring Iceberg Metadata..." -ForegroundColor Gray
        Invoke-Expression "docker exec lakehouse-postgres psql -U `$env:POSTGRES_USER -d postgres -c 'CREATE DATABASE lakehouse_meta;'"
        $CREATE_TABLE_SQL = "CREATE TABLE IF NOT EXISTS iceberg_tables (catalog_name VARCHAR(255) NOT NULL, table_namespace VARCHAR(255) NOT NULL, table_name VARCHAR(255) NOT NULL, metadata_location VARCHAR(1000) NOT NULL, previous_metadata_location VARCHAR(1000), PRIMARY KEY (catalog_name, table_namespace, table_name));"
        Invoke-Expression "docker exec lakehouse-postgres psql -U `$env:POSTGRES_USER -d lakehouse_meta -c `"$CREATE_TABLE_SQL`""

        Invoke-Expression "$DOCKER_COMPOSE up -d spark-master spark-worker trino"
        Write-Host "ETL layer is READY." -ForegroundColor Yellow
    }
    "up-governance" {
        Write-Host "==> LAYER 4: Starting Governance (OpenMetadata)..." -ForegroundColor Magenta
        Invoke-Expression "$DOCKER_COMPOSE up -d elasticsearch"
        Write-Host "Waiting 30s for Elasticsearch..." -ForegroundColor Yellow
        Start-Sleep -Seconds 30
        Invoke-Expression "$DOCKER_COMPOSE run --rm openmetadata-init"
        Invoke-Expression "$DOCKER_COMPOSE up -d openmetadata-server ingestion"
        Write-Host "Governance layer is READY." -ForegroundColor Magenta
    }
    "up-apps" {
        Write-Host "==> LAYER 5: Starting Consumption Apps (Superset & Custom Apps)..." -ForegroundColor Cyan
        Invoke-Expression "$DOCKER_COMPOSE up -d superset backend frontend"
        Write-Host "Application layer is READY." -ForegroundColor Cyan
    }
    "down" {
        Invoke-Expression "$DOCKER_COMPOSE down"
    }
    "clean" {
        Write-Host "WARNING: This will delete all persistent data." -ForegroundColor Red
        Invoke-Expression "$DOCKER_COMPOSE down -v"
    }
    "logs" {
        if ($Service -eq "") {
            Invoke-Expression "$DOCKER_COMPOSE logs -f --tail=100"
        }
        else {
            Invoke-Expression "$DOCKER_COMPOSE logs -f --tail=100 $Service"
        }
    }
    "health-check" {
        Write-Host "==> SYSTEM HEALTH CHECK (6 PILLARS) <==" -ForegroundColor Cyan
        
        Write-Host "[1] Checking Infrastructure..." -ForegroundColor Gray
        $status = Invoke-Expression "$DOCKER_COMPOSE ps --format '{{.Names}}: {{.Status}}'"
        $down = $status | Select-String "Exited", "Dead"
        if ($down) { Write-Host "  WARN: Some containers are DOWN!" -ForegroundColor Yellow; $down } else { Write-Host "  OK: All containers running." -ForegroundColor Green }

        Write-Host "[2] Checking Security (Vault)..." -ForegroundColor Gray
        try { $v = Invoke-WebRequest -Uri "http://localhost:8200/v1/sys/health" -UseBasicParsing; Write-Host "  OK: Vault is responsive." -ForegroundColor Green } catch { Write-Host "  FAIL: Vault is inaccessible." -ForegroundColor Red }

        Write-Host "[3] Checking Storage (MinIO)..." -ForegroundColor Gray
        try { $m = Invoke-WebRequest -Uri "http://localhost:9001" -UseBasicParsing; Write-Host "  OK: MinIO Console is up." -ForegroundColor Green } catch { Write-Host "  FAIL: MinIO is down." -ForegroundColor Red }

        Write-Host "[4] Checking Data Engine (Trino)..." -ForegroundColor Gray
        try { $t = Invoke-WebRequest -Uri "http://localhost:8888" -UseBasicParsing; Write-Host "  OK: Trino Web UI is up." -ForegroundColor Green } catch { Write-Host "  FAIL: Trino is down." -ForegroundColor Red }

        Write-Host "[5] Checking Governance (OpenMetadata)..." -ForegroundColor Gray
        try { $o = Invoke-WebRequest -Uri "http://localhost:8585" -UseBasicParsing; Write-Host "  OK: OpenMetadata is up." -ForegroundColor Green } catch { Write-Host "  FAIL: OpenMetadata is down." -ForegroundColor Red }

        Write-Host "[6] Checking Monitoring (Prometheus)..." -ForegroundColor Gray
        try { $p = Invoke-WebRequest -Uri "http://localhost:9090" -UseBasicParsing; Write-Host "  OK: Prometheus is scraping." -ForegroundColor Green } catch { Write-Host "  FAIL: Prometheus is down." -ForegroundColor Red }
    }
    "status" {
        Invoke-Expression 'docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | findstr lakehouse'
    }
}
