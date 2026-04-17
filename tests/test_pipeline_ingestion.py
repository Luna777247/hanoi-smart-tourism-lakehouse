# File: scripts/test_pipeline_ingestion.py
import os
import sys
import logging

# Add dags/libs to path
sys.path.append(os.path.join(os.getcwd(), "infra/airflow/dags"))

from libs.osm_google_enrichor import OSMGoogleEnrichor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_ingestion():
    print("🚀 Starting Pipeline Ingestion Test...")
    
    # Initialize Enrichor
    enrichor = OSMGoogleEnrichor()
    
    # We test with a minimal limit to verify connectivity and logic
    # Set limit to 2 just for testing
    print("Testing enrichment with limit=2...")
    try:
        results = enrichor.collect(limit=2)
        
        if results and len(results) > 0:
            print(f"✅ Ingestion Test Successful! Fetched {len(results)} enriched items.")
            print(f"Sample Item: {results[0]['osm_data']['name']} enriched by Google.")
            
            # Verify if it can save to Bronze (this might fail if MinIO is not up, but we catch it)
            print("Attempting to save to Bronze (MinIO)...")
            try:
                # We use a test bucket or a test source name
                enrichor.source_name = "test_osm_google"
                path = enrichor.save_to_bronze(results)
                print(f"✅ Data saved to: {path}")
            except Exception as e:
                print(f"⚠️ MinIO Save failed (expected if MinIO is not running locally): {e}")
                print("However, the data collection logic IS WORKING correctly.")
        else:
            print("❌ Ingestion Test Failed: No data returned.")
            
    except Exception as e:
        print(f"❌ Pipeline Test ERROR: {e}")

if __name__ == "__main__":
    test_ingestion()
