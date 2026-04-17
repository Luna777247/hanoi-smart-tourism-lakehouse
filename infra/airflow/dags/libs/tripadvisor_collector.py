# File: infra/airflow/dags/libs/tripadvisor_collector.py
import requests
import logging
import os
from libs.base_collector import BaseLakehouseIngestor

logger = logging.getLogger(__name__)

class TripAdvisorCollector(BaseLakehouseIngestor):
    def __init__(self):
        super().__init__(source_name="tripadvisor")
        self._api_url = "https://serpapi.com/search"

    def collect(self):
        # Lay key tu bien moi truong hoac Vault
        api_key = os.getenv("SERPAPI_KEY") or self.get_secret('tourism/apis').get('tripadvisor_key')
        
        params = {
            "engine": "tripadvisor",
            "q": "Hanoi attractions",
            "api_key": api_key
        }
        
        try:
            response = requests.get(self._api_url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.warning(f"TripAdvisor API error: {e}. Switching to local fallback data.")
            from libs.fallback_manager import FallbackManager
            fm = FallbackManager()
            return fm.get_seed_data("tripadvisor")
