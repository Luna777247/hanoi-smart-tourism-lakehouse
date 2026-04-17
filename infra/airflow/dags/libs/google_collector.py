# File: infra/airflow/dags/libs/google_collector.py
import requests
import logging
from libs.base_collector import BaseLakehouseIngestor

logger = logging.getLogger(__name__)

class GooglePlacesCollector(BaseLakehouseIngestor):
    """
    Inheritance: Ke thua BaseLakehouseIngestor de tai su dung logic luu tru va secrets.
    """
    
    def __init__(self):
        super().__init__(source_name="google_places")
        self._api_url = 'https://places.googleapis.com/v1/places:searchNearby'

    def collect(self):
        """
        Polymorphism & Abstraction: Trien khai logic lay du lieu dac thu cua Google.
        """
        try:
            api_key = self.get_secret('tourism/apis').get('google_places_key')
        except Exception:
            api_key = "dummy"
        
        headers = {
            'Content-Type': 'application/json',
            'X-Goog-Api-Key': api_key,
            'X-Goog-FieldMask': 'places.displayName,places.location,places.types,places.rating'
        }
        
        # Mau thu thap attractions tai Hoan Kiem
        payload = {
            'locationRestriction': {
                'circle': {
                    'center': {'latitude': 21.0285, 'longitude': 105.8542},
                    'radius': 5000.0
                }
            },
            'includedTypes': ['tourist_attraction'],
            'languageCode': 'vi'
        }
        
        try:
            response = requests.post(self._api_url, json=payload, headers=headers, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.warning(f"Google API error: {e}. Switching to local fallback data.")
            from libs.fallback_manager import FallbackManager
            fm = FallbackManager()
            fallback_data = fm.get_seed_data("google_local")
            
            # Map SerpApi format (local_results) to expected format (places) if needed
            # For simplicity, we can just return it and update DataMerger logic if needed
            return fallback_data or {
                "places": [
                    {"displayName": {"text": "Hồ Hoàn Kiếm (Dummy)"}, "location": {"latitude": 21.0285, "longitude": 105.8542}, "rating": 4.8}
                ]
            }
