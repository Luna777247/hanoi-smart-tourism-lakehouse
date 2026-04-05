param (
    [Parameter(Mandatory=$true)]
    [ValidateSet("start-tourism", "start-full", "stop", "status", "build")]
    $Action
)

switch ($Action) {
    "start-tourism" {
        Write-Host "🚀 Khởi động Hanoi Smart Tourism Lakehouse..." -ForegroundColor Cyan
        docker-compose --profile core --profile airflow --profile travel up -d
    }
    "start-full" {
        Write-Host "🌟 Khởi động TOÀN BỘ hệ thống (Cực kỳ nặng!)..." -ForegroundColor Yellow
        docker-compose --profile core --profile airflow --profile travel --profile monitoring --profile trino --profile clickhouse up -d
    }
    "stop" {
        Write-Host "🛑 Đang dừng hệ thống..." -ForegroundColor Red
        docker-compose down
    }
    "status" {
        docker-compose ps
    }
    "build" {
        Write-Host "🛠️ Đang xây dựng lại các Image..."
        docker-compose --profile core --profile airflow build
    }
}
