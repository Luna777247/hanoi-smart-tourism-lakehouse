# File: infra/airflow/dags/libs/osm_collector.py
# Rewritten to match the proven logic from scripts/fetch_osm_data.py
import requests
import json
import logging
from libs.base_collector import BaseLakehouseIngestor

logger = logging.getLogger(__name__)

class OSMCollector(BaseLakehouseIngestor):
    def __init__(self):
        super().__init__(source_name="osm")
        # Use the same mirrors as the working script
        self._overpass_urls = [
            "https://lz4.overpass-api.de/api/interpreter",
            "https://z.overpass-api.de/api/interpreter",
            "http://overpass-api.de/api/interpreter",
        ]

    def _fetch_all_hanoi(self):
        """
        Use a SINGLE query for all of Hanoi (same as scripts/fetch_osm_data.py).
        This avoids rate limiting from sending 30 separate queries.
        """
        # Check Redis cache first
        cache_key = "all_hanoi_pois"
        cached_data = self.get_cached_data(cache_key)
        if cached_data:
            logger.info(f"Cache hit for all Hanoi POIs: {len(cached_data.get('elements', []))} items")
            return cached_data

        # Single query for entire Hanoi - proven to work in script
        query = """
        [out:json][timeout:90];
        area["name"="Thành phố Hà Nội"]->.searchArea;
        (
          node["tourism"](area.searchArea);
          way["tourism"](area.searchArea);
          node["historic"](area.searchArea);
          way["historic"](area.searchArea);
          node["amenity"="place_of_worship"](area.searchArea);
          way["amenity"="place_of_worship"](area.searchArea);
          node["amenity"="museum"](area.searchArea);
          node["amenity"="arts_centre"](area.searchArea);
          node["leisure"="park"](area.searchArea);
          way["leisure"="park"](area.searchArea);
        );
        out center;
        """

        # Try each mirror until one works
        for url in self._overpass_urls:
            try:
                logger.info(f"Querying Overpass mirror: {url}")
                response = requests.post(url, data={"data": query}, timeout=120)

                if response.status_code in (429, 504):
                    logger.warning(f"Mirror {url} returned {response.status_code}. Trying next...")
                    continue

                response.raise_for_status()
                data = response.json()

                element_count = len(data.get("elements", []))
                logger.info(f"✅ Overpass success! Got {element_count} POIs from {url}")

                # Cache for 7 days
                self.set_cached_data(cache_key, data, ttl=604800)
                return data

            except requests.exceptions.Timeout:
                logger.warning(f"Timeout on {url}. Trying next mirror...")
                continue
            except requests.exceptions.RequestException as e:
                logger.warning(f"Error on {url}: {e}. Trying next mirror...")
                continue

        # All mirrors failed — try loading from local seed file as fallback
        logger.error("All Overpass mirrors failed. Attempting local seed fallback...")
        return self._load_local_seed()

    def _load_local_seed(self):
        """Fallback: load from the seed data saved by the original script."""
        import os
        seed_path = "/opt/airflow/data/seed/osm/overpass_attractions.json"
        if os.path.exists(seed_path):
            with open(seed_path, 'r', encoding='utf-8') as f:
                items = json.load(f)
            logger.info(f"Loaded {len(items)} items from local seed: {seed_path}")
            # Wrap in Overpass-style format
            return {"elements": items}
        
        logger.error(f"No local seed found at {seed_path}")
        return {"elements": []}

    def collect(self):
        """Main collection method — returns a list with one data group."""
        data = self._fetch_all_hanoi()
        if data and data.get("elements"):
            return [data]
        return [{"elements": []}]
