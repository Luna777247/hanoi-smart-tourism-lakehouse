# File: scripts/init_openmetadata_services.py
import requests
import json
import os

# ─── Cấu hình OpenMetadata ─────────────────────────────────────
# URL này dành cho việc chạy từ Host đến Docker. 
# Nếu chạy TRONG container, hãy đổi thành http://lakehouse-openmetadata:8585/api/v1
OM_URL = "http://localhost:8585/api/v1"

# TOKEN: Bạn cần lấy JWT Token từ giao diện OpenMetadata:
# Settings -> Bots -> Ingestion-bot -> Copy JWT Token
OM_TOKEN = os.getenv("OM_TOKEN", "REPLACE_WITH_YOUR_TOKEN")

HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {OM_TOKEN}"
}

def create_database_service(name, service_type, connection_config):
    """
    Tạo một Database Service trong OpenMetadata qua API.
    """
    url = f"{OM_URL}/services/databaseServices"
    
    payload = {
        "name": name,
        "serviceType": service_type,
        "connection": {
            "config": connection_config
        }
    }
    
    print(f"--- Đang đăng ký dịch vụ: {name} ({service_type}) ---")
    try:
        response = requests.post(url, headers=HEADERS, data=json.dumps(payload))
        if response.status_code in [200, 201]:
            print(f"✅ Thành công: {name}")
        elif response.status_code == 409:
            print(f"ℹ️ Thông báo: {name} đã tồn tại.")
        elif response.status_code == 401:
            print(f"❌ Lỗi 401: Token không hợp lệ hoặc thiếu. Vui lòng cập nhật OM_TOKEN.")
        else:
            print(f"❌ Thất bại: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"❌ Lỗi kết nối: {e}")

def main():
    if OM_TOKEN == "REPLACE_WITH_YOUR_TOKEN":
        print("⚠️ CẢNH BÁO: Bạn chưa nhập OM_TOKEN. Script có thể sẽ thất bại (401).")
        print("💡 Hãy lấy token tại: http://localhost:8585 -> Settings -> Bots -> Ingestion-bot")
        print("-" * 50)

    # 1. Đăng ký Postgres (Bronze/Metadata storage)
    # OpenMetadata 1.12+: password phải nằm trong authType.basicAuth
    postgres_config = {
        "type": "Postgres",
        "scheme": "postgresql+psycopg2",
        "username": "lakehouse_admin",
        "authType": {
            "password": "ChangeMe_Strong123!"
        },
        "hostPort": "lakehouse-postgres:5432",
        "database": "lakehouse_admin"
    }
    create_database_service("hanoi_postgres", "Postgres", postgres_config)

    # 2. Đăng ký Trino (Lakehouse Query Engine)
    trino_config = {
        "type": "Trino",
        "scheme": "trino",
        "username": "admin",
        "hostPort": "lakehouse-trino:8080",
        "catalog": "iceberg"
    }
    create_database_service("hanoi_trino", "Trino", trino_config)

    print("\n🚀 HOÀN TẤT: Kiểm tra lại trên giao diện OpenMetadata.")

if __name__ == "__main__":
    main()
