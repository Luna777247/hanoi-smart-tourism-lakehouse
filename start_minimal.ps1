# Script khoi dong toi gian cho Hanoi Smart Tourism Lakehouse
# Chi chay cac service cot loi de tiet kiem tai nguyen

Write-Host "--- Dang dung cac service cu... ---" -ForegroundColor Yellow
docker-compose down

Write-Host "--- Dang khoi dong cum service MINIMAL (Airflow + MinIO + Postgres + Vault) ---" -ForegroundColor Green
docker-compose up -d postgres redis minio vault airflow-init airflow-webserver airflow-scheduler airflow-worker airflow-triggerer

Write-Host "--- Hoan tat! ---" -ForegroundColor Cyan
Write-Host "Theo doi trang thai Airflow tai: http://localhost:8085"
Write-Host "Kiem tra log Webserver: docker logs -f hanoi-smart-tourism-lakehouse-airflow-webserver-1"
