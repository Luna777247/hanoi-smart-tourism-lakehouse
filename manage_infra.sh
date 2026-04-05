#!/bin/bash

# --- Hanoi Smart Tourism - Infrastructure Manager ---

# Các nhóm profile đã định nghĩa trong docker-compose.yml:
# core: MinIO, Postgres, Spark, Redis, Vault, Elasticsearch
# airflow: Toàn bộ cụm Airflow (Scheduler, Worker, Webserver)
# travel: Profile tùy chỉnh chứa các công cụ du lịch (Spark, Kafka, Trino)
# monitoring: Prometheus, Grafana, Loki (Để giám sát hệ thống)

case "$1" in
  "start-tourism")
    echo "🚀 Đang khởi động hệ thống Hanoi Smart Tourism Lakehouse..."
    docker-compose --profile core --profile airflow --profile travel up -d
    ;;
  "start-full")
    echo "🌟 Đang khởi động TOÀN BỘ hệ thống (Cực kỳ nặng!)..."
    docker-compose --profile core --profile airflow --profile travel --profile monitoring --profile trino --profile clickhouse up -d
    ;;
  "stop")
    echo "🛑 Đang dừng toàn bộ hệ thống..."
    docker-compose down
    ;;
  "status")
    docker-compose ps
    ;;
  "build")
    echo "🛠️ Đang xây dựng lại các Image..."
    docker-compose --profile core --profile airflow build
    ;;
  *)
    echo "Sử dụng: ./manage_infra.sh [start-tourism | start-full | stop | status | build]"
    ;;
esac
