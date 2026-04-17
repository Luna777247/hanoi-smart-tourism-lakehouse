#!/bin/bash
# File: scripts/backup_minio.sh
# Script để backup dữ liệu từ MinIO

BACKUP_DIR="./backups"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
BACKUP_FILE="minio_backup_${TIMESTAMP}.tar.gz"

echo "📦 Starting MinIO backup..."

# Tạo thư mục backup
mkdir -p "$BACKUP_DIR"

# Backup MinIO data từ Docker volume
docker run --rm \
    -v lakehouse_minio-data:/data \
    -v "$(pwd)/$BACKUP_DIR:/backup" \
    alpine:latest \
    sh -c "cd /data && tar czf /backup/$BACKUP_FILE ."

if [ $? -eq 0 ]; then
    echo "✅ Backup completed: $BACKUP_DIR/$BACKUP_FILE"
    echo "📊 Backup size: $(du -h "$BACKUP_DIR/$BACKUP_FILE" | cut -f1)"

    # Cleanup old backups (giữ 5 backup gần nhất)
    echo "🧹 Cleaning up old backups..."
    ls -t "$BACKUP_DIR"/minio_backup_*.tar.gz | tail -n +6 | xargs -r rm
    echo "✅ Cleanup completed"
else
    echo "❌ Backup failed!"
    exit 1
fi