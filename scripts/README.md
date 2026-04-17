# 📋 Hanoi Tourism Lakehouse - Maintenance Guide

## 🎯 Tổng quan

Hệ thống hiện chỉ còn **một luồng ingestion chính**:

- `bronze_ingest_osm_google_enriched` lúc `02:00`
- `silver_transform_enriched_data` lúc `05:00`
- `gold_transform_tourism_marts` lúc `06:00`

## 📂 Scripts Available

### Core Scripts
- `start_docker_services.sh` - Khởi động tất cả services cần thiết
- `stop_docker_services.sh` - Dừng services (giữ data)
- `backup_minio.sh` - Backup dữ liệu MinIO
- `monitor_dags.sh` - Monitor trạng thái DAGs

### Automation Scripts
- `run_weekly_maintenance.ps1` - Chạy full maintenance workflow
- `setup_windows_scheduler.ps1` - Setup Windows Task Scheduler

## 🚀 Quick Start

### 1. Chạy Manual
```bash
# Windows PowerShell
cd d:\hn-smart-tourism-lakehouse
.\scripts\start_docker_services.sh

# Sau đó truy cập Airflow UI để trigger DAGs
# http://localhost:8080
```

### 2. Setup Automation (Windows)
```powershell
# Chạy với quyền Administrator
.\scripts\setup_windows_scheduler.ps1
```

## 📊 Monitoring & Alerts

### Airflow UI Monitoring
- URL: http://localhost:8080
- Check DAG runs status
- View logs khi fail

### Email Alerts Setup
1. Cập nhật `.env` với thông tin SMTP:
   ```
   SMTP_USER=your-email@gmail.com
   SMTP_PASSWORD=your-app-password
   ```
2. Thêm email vào DAG default_args:
   ```python
   default_args = {
       'email_on_failure': True,
       'email_on_retry': False,
       'email': ['admin@tourism-lakehouse.local']
   }
   ```

### Backup Monitoring
- Backups lưu trong `./backups/`
- Tự động cleanup, giữ 5 backup gần nhất
- Check size và status trong logs

## 📈 Maintenance Schedule

| Task | Frequency | Script |
|------|-----------|--------|
| Start Services | Khi cần | `start_docker_services.sh` |
| Trigger DAGs | Manual | Airflow UI |
| Backup Data | Weekly | `backup_minio.sh` |
| Monitor Status | Daily | `monitor_dags.sh` |

## 🔧 Troubleshooting

### Services không start
```bash
# Check Docker status
docker ps

# View logs
docker logs lakehouse-airflow-webserver
```

### DAGs fail
```bash
# Check Airflow logs
docker exec lakehouse-airflow-worker airflow dags list

# Manual trigger
docker exec lakehouse-airflow-worker airflow dags trigger bronze_ingest_osm_google_enriched
```

### Backup fail
```bash
# Check disk space
df -h

# Manual backup
docker run --rm -v lakehouse_minio-data:/data -v $(pwd)/backups:/backup alpine tar czf /backup/manual_backup.tar.gz -C /data .
```

## 📝 Logs & Reports

- Maintenance logs: `./logs/`
- Backup files: `./backups/`
- Airflow logs: Docker volume `airflow-logs`

## 🎯 Best Practices

1. **Chạy ít nhất 1 lần/tuần** để tránh miss data
2. **Monitor Airflow UI** sau khi start
3. **Backup trước khi thay đổi** cấu hình
4. **Check logs** khi có vấn đề
5. **Update scripts** khi có thay đổi hệ thống
