param (
    [Parameter(Mandatory=$true)]
    [ValidateSet("start-tourism", "start-full", "stop", "status", "build", "down-clean")]
    $Action
)

# ==========================================
# 1. KHAI BÁO BIẾN & CẤU HÌNH DỮ LIỆU (Data/Variables)
# Tách biệt hoàn toàn các cấu hình Profile để dễ dàng mở rộng thêm profile mới sau này.
# ==========================================
$ProfilesConfig = @{
    Tourism = @("--profile", "core", "--profile", "airflow", "--profile", "travel")
    Full    = @("--profile", "core", "--profile", "airflow", "--profile", "travel", "--profile", "monitoring", "--profile", "trino", "--profile", "clickhouse")
    Build   = @("--profile", "core", "--profile", "airflow")
}

# ==========================================
# 2. ĐỊNH NGHĨA HÀM LOGIC (Functions)
# Tập trung xử lý các lệnh Docker tại một nơi để trách lặp lại code.
# ==========================================
function Invoke-DockerComposeTask {
    param(
        [string]$Message,
        [ConsoleColor]$Color,
        [string[]]$Profiles,
        [string]$DockerCommand
    )

    Write-Host ""
    Write-Host $Message -ForegroundColor $Color

    # Lắp ráp câu lệnh
    $Command = "docker-compose"
    if ($Profiles.Count -gt 0) {
        $Command += " " + ($Profiles -join " ")
    }
    $Command += " " + $DockerCommand

    # Hiển thị câu lệnh đang chạy để dễ debug
    Write-Host "-> Executing: $Command" -ForegroundColor DarkGray
    Write-Host ""

    # Chạy câu lệnh
    Invoke-Expression $Command
}

# ==========================================
# 3. ĐIỀU HƯỚNG YÊU CẦU (Main Execution)
# ==========================================
switch ($Action) {
    "start-tourism" {
        Invoke-DockerComposeTask -Message "🚀 Khởi động Hanoi Smart Tourism Lakehouse..." -Color Cyan -Profiles $ProfilesConfig.Tourism -DockerCommand "up -d"
    }
    "start-full" {
        Invoke-DockerComposeTask -Message "🌟 Khởi động TOÀN BỘ hệ thống (Tốn nhiều tài nguyên!)..." -Color Yellow -Profiles $ProfilesConfig.Full -DockerCommand "up -d"
    }
    "build" {
        Invoke-DockerComposeTask -Message "🛠️ Đang xây dựng lại các Image (Build)..." -Color Magenta -Profiles $ProfilesConfig.Build -DockerCommand "build"
    }
    "stop" {
        Invoke-DockerComposeTask -Message "🛑 Đang dừng hệ thống..." -Color Red -Profiles @() -DockerCommand "down"
    }
    "down-clean" {
        Invoke-DockerComposeTask -Message "🧹 Đang dọn dẹp hệ thống (Xóa Volumes + Orphans)..." -Color Red -Profiles @() -DockerCommand "down -v --remove-orphans"
    }
    "status" {
        Invoke-DockerComposeTask -Message "📊 Kiểm tra trạng thái hệ thống..." -Color Green -Profiles @() -DockerCommand "ps"
    }
}
