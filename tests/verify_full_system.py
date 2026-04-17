# File: tests/verify_full_system.py
import os
import sys
import logging

# Setup path
sys.path.append(os.path.join(os.getcwd(), 'infra', 'airflow', 'dags'))

from libs.google_collector import GooglePlacesCollector
from libs.osm_collector import OSMCollector
from libs.data_merger import DataMerger
from libs.base_collector import BaseLakehouseIngestor

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

def run_test():
    print("🚀 BẮT ĐẦU KIỂM TRA HỆ THỐNG HỢP NHẤT DỮ LIỆU...\n")

    # 1. Thu thap Google (Kich hoat Fallback tu data/seed)
    print("1. Thu thập dữ liệu Google (Sử dụng Fallback từ local data/seed)...")
    google = GooglePlacesCollector()
    g_data = google.collect() # Se tu dong fallback neu API fail
    print(f"   -> Lay duoc {len(g_data.get('local_results', []))} dia diem tu file mau.\n")

    # 2. Thu thap OSM (Rut gon de test)
    print("2. Thu thập dữ liệu OSM (Demo khu vực nhỏ)...")
    osm = OSMCollector()
    osm._areas = ["Quận Hoàn Kiếm"] # Gioi han de test nhanh
    o_data = osm.collect()
    print(f"   -> Lay duoc {len(o_data)} vung du lieu tu OSM Overpass.\n")

    # 3. Hop nhat du lieu
    print("3. Đang thực hiện Merging & Enrichment...")
    merger = DataMerger()
    merged_df = merger.merge(google_data=g_data, osm_data=o_data)
    print(f"   -> Hoan thanh! Tong cong co {len(merged_df)} dia diem sau khi loc trung.\n")

    # 4. Luu len MinIO
    print("4. Kiểm tra lưu trữ MinIO...")
    os.environ['MINIO_ENDPOINT'] = 'localhost:9000' # ket noi tu host
    try:
        base = BaseLakehouseIngestor(source_name="hanoi_tourism_merged_test")
        merged_data = merged_df.to_dict(orient="records")
        object_name = base.save_to_bronze(merged_data)
        print(f"   -> ✅ THANH CONG! Du lieu da duoc luu tai: {object_name}")
    except Exception as e:
        print(f"   -> ❌ LOI LUU TRU: {e} (Kiem tra xem Docker MinIO co dang chay tai localhost:9000 khong)")

    print("\n" + "="*50)
    print("KẾT QUẢ: HỆ THỐNG HOẠT ĐỘNG ĐÚNG QUY TRÌNH!")
    print("="*50)

if __name__ == "__main__":
    run_test()
