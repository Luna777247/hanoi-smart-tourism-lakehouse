#!/bin/bash
# File: scripts/health_check.sh
set -e
 
PASS='\033[0;32m✓\033[0m'
FAIL='\033[0;31m✗\033[0m'
WARN='\033[0;33m?\033[0m'
 
check_http() {
    local name=$1 url=$2
    if curl -sf -o /dev/null --max-time 5 "$url"; then
        echo -e "${PASS} ${name}"
    else
        echo -e "${FAIL} ${name} (${url})"
    fi
}
 
echo '=== Hanoi Tourism Lakehouse Health Check ==='; echo
 
echo '--- Web Services ---'
check_http 'MinIO Console'         'http://localhost:9001'
check_http 'Airflow Webserver'     'http://localhost:8080/health'
check_http 'Apache Superset'       'http://localhost:8400/health'
check_http 'OpenMetadata UI'       'http://localhost:8585'
check_http 'HashiCorp Vault'       'http://localhost:8200/v1/sys/health'
check_http 'Management Portal API' 'http://localhost:8000/health'
check_http 'Management Portal UI'  'http://localhost:3000'
 
echo; echo '--- Docker Containers ---'
docker compose ps --format 'table {{.Name}}\t{{.Status}}'
 
echo; echo '--- Disk Usage ---'
docker system df
 
echo; echo '--- Trino Connection ---'
docker compose exec trino trino \
    --server http://localhost:8080 \
    --execute 'SHOW CATALOGS' 2>/dev/null && \
    echo -e "${PASS} Trino reachable" || echo -e "${FAIL} Trino unreachable"
 
echo; echo '=== Done ==='

