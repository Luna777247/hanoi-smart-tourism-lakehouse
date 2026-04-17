# File: tests/test_live_ingestion.py
import os
import sys
import logging

# Setup path
sys.path.append(os.path.join(os.getcwd(), 'infra', 'airflow', 'dags'))

from libs.osm_google_enrichor import OSMGoogleEnrichor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_real_test():
    # 1. Thiet lap moi truong ket noi thuc te toi Minio tren Host
    os.environ['MINIO_ENDPOINT'] = 'localhost:9000'
    os.environ['MINIO_ACCESS_KEY'] = 'minio_admin'
    os.environ['MINIO_SECRET_KEY'] = 'ChangeMe_Minio123!'
    
    print("\n🚀 BẮT ĐẦU CHẠY THỰC PIPELINE TẠI LOCAL...")
    
    try:
        # TĂNG TỐC: Chi test voi 1 khu vuc nho
        print("\n[STEP 1] Running main ingestion flow (OSM -> Google enrichment)...")
        enrichor = OSMGoogleEnrichor()
        results = enrichor.collect(limit=2)

        print("\n[STEP 2] Saving enriched result to MinIO Bronze landing...")
        enrichor.source_name = "osm_google_enriched_test"
        object_name = enrichor.save_to_bronze(results)
        
        print("\n" + "="*50)
        print(f"✅ CHẠY THỰC THÀNH CÔNG!")
        print(f"📂 Dữ liệu đã lưu tại: {object_name}")
        print("="*50)
        
    except Exception as e:
        print("\n" + "!"*50)
        print(f"❌ PHÁT HIỆN LỖI KHI CHẠY THỰC: {e}")
        print("!"*50)
        sys.exit(1)

if __name__ == "__main__":
    run_real_test()
