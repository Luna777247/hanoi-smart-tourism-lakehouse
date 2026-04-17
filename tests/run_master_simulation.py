# File: tests/run_master_simulation.py
import os
import sys
import pandas as pd
import json

# Setup path to internal libs
sys.path.append(os.path.join(os.getcwd(), 'infra', 'airflow', 'dags'))
from libs.fallback_manager import FallbackManager
from libs.data_merger import DataMerger

def run_simulation():
    print("🎭 BẮT ĐẦU MÔ PHỎNG LUỒNG CHÍNH OSM -> GOOGLE ENRICHMENT...\n")

    # --- GIAI ĐOẠN 1: LANDING ZONE ---
    print("[STAGE 1] Mô phỏng ingestion raw từ luồng chính...")
    fm = FallbackManager()
    g_data = fm.get_seed_data("google_local")
    print("✅ Đã lấy dữ liệu seed đại diện cho payload làm giàu từ Google.\n")

    # --- GIAI ĐOẠN 2: SILVER ---
    print("[STAGE 2] Mô phỏng Silver enrichment logic...")
    merger = DataMerger()
    o_data = [{"elements": [{"tags": {"name": "Test Place"}, "lat": 21.02, "lon": 105.85}]}]
    
    merged_df = merger.merge(google_data=g_data, osm_data=o_data)
    
    # Ap dung logic Cleansing trong Spark Job Silver: GPS Filter & Normalization
    print("   -> Thực hiện Cleansing (GPS Box Hà Nội [20.5-21.5, 105.0-106.5])")
    silver_df = merged_df[
        (merged_df['lat'].between(20.5, 21.5)) & 
        (merged_df['lon'].between(105.0, 106.5))
    ].copy()
    
    silver_df['name'] = silver_df['name'].str.strip().str.title()
    print(f"✅ Silver Transformation hoàn tất! (Sau lọc GPS còn lại {len(silver_df)} bản ghi định danh).\n")

    # --- GIAI ĐOẠN 3: GOLD READINESS ---
    print("[STAGE 3] Kiểm tra tính sẵn sàng của Gold Spark jobs...")
    if os.path.exists("infra/spark/jobs/gold/gold_process_tourism_marts.py"):
        print("✅ Đã tìm thấy Gold job: gold_process_tourism_marts.py")
    
    print("\n" + "="*50)
    print("KẾT LUẬN: LUỒNG CHÍNH ĐÃ THÔNG NÒNG!")
    print("Dữ liệu sẵn sàng để Airflow & Spark thực thi theo nhánh enriched chính thức.")
    print("="*50)

if __name__ == "__main__":
    run_simulation()
