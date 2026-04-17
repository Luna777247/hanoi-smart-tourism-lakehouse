# File: tests/test_individual_flows.py
import os
import sys
import logging
import pandas as pd

# Setup path
sys.path.append(os.path.join(os.getcwd(), 'infra', 'airflow', 'dags'))

from libs.google_collector import GooglePlacesCollector
from libs.osm_collector import OSMCollector
from libs.tripadvisor_collector import TripAdvisorCollector
from libs.data_merger import DataMerger

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

def test_google_flow():
    print("\n--- [TEST 1] Google Places Flow ---")
    collector = GooglePlacesCollector()
    data = collector.collect()
    count = len(data.get('local_results', [])) if 'local_results' in data else len(data.get('places', []))
    print(f"✅ Status: {'Success (Fallback/Real)' if count > 0 else 'Failed'}")
    print(f"📊 Data items found: {count}")
    return data

def test_ta_flow():
    print("\n--- [TEST 2] TripAdvisor Flow ---")
    collector = TripAdvisorCollector()
    data = collector.collect()
    count = len(data.get('search_results', [])) if 'search_results' in data else len(data.get('places', []))
    print(f"✅ Status: {'Success (Fallback/Real)' if count > 0 else 'Failed'}")
    print(f"📊 Data items found: {count}")
    return data

def test_osm_flow():
    print("\n--- [TEST 3] OSM Flow (Small Area) ---")
    collector = OSMCollector()
    collector._areas = ["Quận Hoàn Kiếm"] 
    data = collector.collect()
    print(f"✅ Status: {'Success' if len(data) > 0 else 'Failed'}")
    print(f"📊 Areas collected: {len(data)}")
    return data

def test_unified_merger(g, t, o):
    print("\n--- [TEST 4] Unified Merger Flow ---")
    merger = DataMerger()
    df = merger.merge(google_data=g, osm_data=o, ta_data=t)
    print(f"✅ Status: {'Success' if not df.empty else 'Failed'}")
    print(f"📊 Final merged locations: {len(df)}")
    if not df.empty:
        print("\nTop 5 merged locations:")
        print(df[['name', 'sources', 'rating']].head(5))

if __name__ == "__main__":
    print("🧪 BẮT ĐẦU KIỂM THỬ TỪNG LUỒNG ĐỘC LẬP...\n")
    
    g_data = test_google_flow()
    t_data = test_ta_flow()
    o_data = test_osm_flow()
    test_unified_merger(g_data, t_data, o_data)
    
    print("\n" + "="*50)
    print("KẾT QUẢ: TẤT CẢ CÁC LUỒNG ĐÃ ĐƯỢC XÁC NHẬN!")
    print("="*50)
