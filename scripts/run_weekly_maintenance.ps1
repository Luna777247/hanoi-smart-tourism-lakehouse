# File: scripts/run_weekly_maintenance.ps1
# Script chạy weekly maintenance: start services, trigger DAGs, backup

Write-Host "🚀 Starting weekly maintenance for Hanoi Tourism Lakehouse..."
$StartTime = Get-Date

# 1. Start Docker services
Write-Host "📦 Starting Docker services..."
& "$PSScriptRoot\start_docker_services.sh"

# Wait for services to be ready
Start-Sleep -Seconds 60

# 2. Trigger DAGs in Airflow
Write-Host "🔄 Triggering Airflow DAGs..."
# Note: This would require Airflow CLI or API call
# For now, manual trigger recommended

# 3. Run backup
Write-Host "💾 Running MinIO backup..."
& "$PSScriptRoot\backup_minio.sh"

# 4. Monitor and report
Write-Host "📊 Generating status report..."
& "$PSScriptRoot\monitor_dags.sh" > "$PSScriptRoot\..\logs\weekly_report_$(Get-Date -Format 'yyyyMMdd').txt"

$EndTime = Get-Date
$Duration = $EndTime - $StartTime

Write-Host "✅ Weekly maintenance completed in $($Duration.TotalMinutes) minutes"
Write-Host "📄 Report saved to logs\weekly_report_$(Get-Date -Format 'yyyyMMdd').txt"