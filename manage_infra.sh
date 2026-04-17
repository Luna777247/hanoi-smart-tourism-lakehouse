#!/bin/bash
# File: manage_infra.sh
# Cach dung: ./manage_infra.sh [start|stop|restart|logs|status]
 
CMD=${1:-help}
 
case $CMD in
  start)
    echo '==> Starting Hanoi Tourism Lakehouse...'
    docker compose up -d postgres redis minio vault
    sleep 15
    docker compose run --rm minio-init
    bash scripts/setup_vault.sh
    docker compose run --rm airflow-init
    docker compose up -d airflow-webserver airflow-scheduler airflow-worker airflow-triggerer
    docker compose up -d trino superset openmetadata-server
    docker compose up -d backend frontend
    bash scripts/health_check.sh
    ;;
  stop)
    docker compose down
    ;;
  restart)
    docker compose restart
    ;;
  logs)
    docker compose logs -f --tail=100 ${2:-airflow-webserver}
    ;;
  status)
    bash scripts/health_check.sh
    ;;
  *)
    echo 'Usage: ./manage_infra.sh [start|stop|restart|logs|status]'
    ;;
esac

