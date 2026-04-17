# File: scripts/setup_windows_scheduler.ps1
# PowerShell script để setup Windows Task Scheduler cho automation

# Yêu cầu: Chạy với quyền Administrator

$TaskName = "HanoiTourismLakehouse-Weekly"
$ScriptPath = "$PSScriptRoot\..\scripts\start_docker_services.sh"
$BackupScript = "$PSScriptRoot\..\scripts\backup_minio.sh"

# Tạo task chạy vào Chủ Nhật lúc 9:00 AM (hàng tuần)
$Action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument "-ExecutionPolicy Bypass -File $PSScriptRoot\run_weekly_maintenance.ps1"
$Trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Sunday -At 9am
$Settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable
$Principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType InteractiveToken

# Đăng ký task
Register-ScheduledTask -TaskName $TaskName -Action $Action -Trigger $Trigger -Settings $Settings -Principal $Principal -Description "Weekly maintenance for Hanoi Tourism Lakehouse"

Write-Host "✅ Task Scheduler setup completed!"
Write-Host "📅 Task will run every Sunday at 9:00 AM"
Write-Host "🔧 To modify: Task Scheduler > Task Scheduler Library > $TaskName"