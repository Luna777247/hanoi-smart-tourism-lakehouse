# File: scripts/test_google_enrichment_logic.py
import os
import sys
import json
import logging
import time

# Add dags/libs to path
sys.path.append(os.path.join(os.getcwd(), "infra/airflow/dags"))

from libs.osm_google_enrichor import OSMGoogleEnrichor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_google_logic():
    print("🧪 Starting Mock OSM -> Google Enrichment Test...")
    
    # 1. Create Mock OSM Data
    mock_candidates = [
        {
            "osm_id": 12345,
            "name": "Văn Miếu Quốc Tử Giám",
            "tags": {"tourism": "attraction", "historic": "monument"},
            "lat": 21.0285,
            "lon": 105.8355
        },
        {
            "osm_id": 67890,
            "name": "Hồ Hoàn Kiếm",
            "tags": {"tourism": "attraction"},
            "lat": 21.0285,
            "lon": 105.8542
        }
    ]
    
    # 2. Initialize Enrichor
    enrichor = OSMGoogleEnrichor()
    
    enriched_results = []
    print(f"Enriching {len(mock_candidates)} locations via Google RapidAPI...")
    
    for item in mock_candidates:
        print(f"  Enriching: {item['name']}...")
        details = enrichor._get_google_details(f"{item['name']}, Hanoi")
        
        if details:
            print(f"  ✅ SUCCESS: Got rating {details.get('rating')} and address {details.get('formatted_address')}")
            enriched_results.append({
                "osm_data": item,
                "google_data": details,
                "ingested_at": "2026-04-15"
            })
        else:
            print(f"  ❌ FAILED: Could not enrich {item['name']}")
        
        time.sleep(1) # Delay
        
    # 3. Final Verification
    if len(enriched_results) == len(mock_candidates):
        print("\n🎉 ALL TESTS PASSED!")
        print(f"Total Enriched: {len(enriched_results)}")
        # Check a sample field for the Silver stage
        sample = enriched_results[0]
        print(f"Schema Check: {sample['google_data']['geometry']['location']} is present for Spark mapping.")
    else:
        print("\n⚠️ PARTIAL SUCCESS: Some items failed enrichment.")

if __name__ == "__main__":
    test_google_logic()
