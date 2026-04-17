#!/bin/sh
# File: infra/minio/init-buckets.sh
set -e
 
echo 'Waiting for MinIO to be ready...'
sleep 5
 
mc alias set local http://minio:9000 \
  "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}"
 
# Tao 3 buckets chinh cho Bronze, Silver, Gold
for bucket in tourism-bronze tourism-silver tourism-gold; do
  mc mb --ignore-existing local/${bucket}
  echo "Created bucket: ${bucket}"
done
 
# Cau hinh versioning cho Bronze (bao toan du lieu goc)
mc version enable local/tourism-bronze
 
echo 'MinIO init complete!'
