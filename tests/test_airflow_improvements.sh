#!/bin/bash

# Test script for Airflow production improvements
# This script validates all the enhanced Airflow components

echo "🧪 Testing Airflow Production Improvements"
echo "=========================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to check service health
check_service() {
    local service_name=$1
    local url=$2
    local expected_code=${3:-200}

    echo -n "Checking $service_name... "
    if curl -s -o /dev/null -w "%{http_code}" "$url" | grep -q "$expected_code"; then
        echo -e "${GREEN}✅ OK${NC}"
        return 0
    else
        echo -e "${RED}❌ FAILED${NC}"
        return 1
    fi
}

# Check Airflow Webserver
check_service "Airflow Webserver" "http://localhost:8080/health"

# Check Flower UI
check_service "Flower UI" "http://localhost:5555/"

# Check Prometheus Metrics
check_service "Prometheus Metrics" "http://localhost:8001/metrics"

# Check Redis connectivity
echo -n "Checking Redis connectivity... "
if docker compose exec -T redis redis-cli ping | grep -q "PONG"; then
    echo -e "${GREEN}✅ OK${NC}"
else
    echo -e "${RED}❌ FAILED${NC}"
fi

# Check PostgreSQL connectivity
echo -n "Checking PostgreSQL connectivity... "
if docker compose exec -T postgres pg_isready -U ${POSTGRES_USER} -d ${POSTGRES_DB} >/dev/null 2>&1; then
    echo -e "${GREEN}✅ OK${NC}"
else
    echo -e "${RED}❌ FAILED${NC}"
fi

# Check DAGs are loaded
echo -n "Checking DAGs loaded... "
dag_count=$(docker compose exec -T airflow-webserver airflow dags list --output=json | jq length 2>/dev/null || echo "0")
if [ "$dag_count" -gt 0 ]; then
    echo -e "${GREEN}✅ $dag_count DAGs loaded${NC}"
else
    echo -e "${RED}❌ No DAGs loaded${NC}"
fi

# Check Celery workers
echo -n "Checking Celery workers... "
worker_count=$(docker compose exec -T airflow-flower celery -A airflow.executors.celery_executor inspect active 2>/dev/null | grep -c "celery@" || echo "0")
if [ "$worker_count" -gt 0 ]; then
    echo -e "${GREEN}✅ $worker_count workers active${NC}"
else
    echo -e "${YELLOW}⚠️  Workers status unknown (Flower may still be starting)${NC}"
fi

# Check log rotation configuration
echo -n "Checking log rotation config... "
if [ -f "infra/airflow/logrotate.conf" ]; then
    echo -e "${GREEN}✅ Log rotation configured${NC}"
else
    echo -e "${RED}❌ Log rotation config missing${NC}"
fi

echo ""
echo "📊 Component Status Summary:"
echo "==========================="
docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"

echo ""
echo "🎯 Next Steps:"
echo "- Access Airflow UI: http://localhost:8080"
echo "- Monitor Celery: http://localhost:5555"
echo "- View Metrics: http://localhost:8001/metrics"
echo "- Check logs: docker compose logs -f [service-name]"