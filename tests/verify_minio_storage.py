# File: tests/verify_minio_storage.py
import os
import sys
from datetime import datetime, timezone

# Thiet lap moi truong de ket noi duoc tu ben ngoai container (host machine)
os.environ['MINIO_ENDPOINT'] = 'localhost:9000'
os.environ['MINIO_ACCESS_KEY'] = 'minio_admin'
os.environ['MINIO_SECRET_KEY'] = 'ChangeMe_Minio123!'

# Them duong dan de import duoc libs
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'infra', 'airflow', 'dags'))

from libs.base_collector import BaseLakehouseIngestor
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TestIngestor(BaseLakehouseIngestor):
    def collect(self):
        return {"test_id": 123, "name": "Test Location", "status": "Success"}

def verify():

    logger.info("Initializing TestIngestor...")
    ingestor = TestIngestor(source_name="verification_test")
    
    logger.info("Running ingestion and saving to MinIO...")
    try:
        # 1. Chay luu du lieu
        object_name = ingestor.run(force=True)
        
        if object_name and object_name != "SKIPPED":
            logger.info(f"✅ SUCCESS: Data saved as {object_name}")
            
            # 2. Kiem tra lai tren MinIO
            objects = ingestor._minio_client.list_objects(ingestor.bucket_name, prefix="source=verification_test/", recursive=True)
            found = False
            for obj in objects:
                logger.info(f"Checking object: {obj.object_name}")
                if obj.object_name == object_name:
                    found = True
                    break
            
            if found:
                print("\n" + "="*50)
                print("Result: PHẦN MỀM ĐÃ LƯU DỮ LIỆU LÊN MINIO THÀNH CÔNG!")
                print("="*50)
            else:
                print("\n❌ LỖI: File đã báo lưu nhưng không tìm thấy khi list objects.")
        else:
            print("\n❌ LỖI: Không nhận được object_name sau khi chạy.")
            
    except Exception as e:
        logger.error(f"❌ Kết nối thất bại: {e}")
        print("\n💡 Gợi ý: Hãy đảm bảo Docker đang chạy và MinIO port 9000 đã sẵn sàng.")

if __name__ == "__main__":
    verify()
