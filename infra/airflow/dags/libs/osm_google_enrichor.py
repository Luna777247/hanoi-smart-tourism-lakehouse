# File: infra/airflow/dags/libs/osm_google_enrichor.py
import requests
import json
import os
import time
import logging
from typing import List, Dict, Any
from libs.base_collector import BaseLakehouseIngestor
from libs.osm_collector import OSMCollector

logger = logging.getLogger(__name__)

class OSMGoogleEnrichor(BaseLakehouseIngestor):
    """
    Enrichment Flow: OSM -> Filter -> Google Places (RapidAPI) -> Bronze
    """
    
    def __init__(self):
        super().__init__(source_name="osm_google_enriched")
        self.rapid_api_keys = [
            os.getenv("RAPID_API_KEY1", "02ad4fd6f3msh1f0390da51ae627p19a5cfjsn7f2b23cadfdb"),
            os.getenv("RAPID_API_KEY2"),
            os.getenv("RAPID_API_KEY3"),
            os.getenv("RAPID_API_KEY4"),
            os.getenv("RAPID_API_KEY5"),
            os.getenv("RAPID_API_KEY6"),
            os.getenv("RAPID_API_KEY7")
        ]
        # Filter out empty keys
        self.rapid_api_keys = [k for k in self.rapid_api_keys if k]
        self.host = "google-map-places.p.rapidapi.com"

    def _get_google_details(self, query: str, key_idx: int = 0) -> Dict[str, Any]:
        """Fetch details from Google via RapidAPI with key rotation."""
        if key_idx >= len(self.rapid_api_keys):
            logger.error("All RapidAPI keys exhausted.")
            return None

        headers = {
            "x-rapidapi-key": self.rapid_api_keys[key_idx],
            "x-rapidapi-host": self.host
        }
        
        try:
            # 1. Find Place ID
            search_url = f"https://{self.host}/maps/api/place/findplacefromtext/json"
            params = {
                "input": query,
                "inputtype": "textquery",
                "fields": "place_id",
                "language": "vi"
            }
            
            resp = requests.get(search_url, headers=headers, params=params, timeout=20)
            if resp.status_code == 429:
                logger.info(f"Key {key_idx} rate limited, trying next...")
                return self._get_google_details(query, key_idx + 1)
            
            search_data = resp.json()
            candidates = search_data.get("candidates", [])
            if not candidates:
                return None
            
            place_id = candidates[0].get("place_id")
            
            # 2. Get Details
            details_url = f"https://{self.host}/maps/api/place/details/json"
            detail_params = {
                "place_id": place_id,
                "fields": "name,formatted_address,geometry,rating,user_ratings_total,opening_hours,website,international_phone_number,reviews,photos",
                "language": "vi"
            }
            
            resp_details = requests.get(details_url, headers=headers, params=detail_params, timeout=20)
            return resp_details.json().get("result")
            
        except Exception as e:
            logger.error(f"Error enriching {query}: {e}")
            return None

    def collect(self, limit: int = 500) -> List[Dict[str, Any]]:
        """
        Main collection logic:
        1. Get current OSM attractions.
        2. Filter for high-value targets.
        3. Enrich with Google data.
        """
        logger.info("Starting OSM-Google enrichment flow...")
        
        # In a real DAG, we might pull the latest OSM file from Bronze.
        # Here we use the OSMCollector to get fresh data or fallback to local seed.
        osm_collector = OSMCollector()
        osm_data_raw = osm_collector.collect()
        
        # Flatten and filter
        all_elements = []
        if isinstance(osm_data_raw, list):
            for area_group in osm_data_raw:
                all_elements.extend(area_group.get("elements", []))
        else:
            all_elements = osm_data_raw.get("elements", [])
            
        # Filter logic (Attractions only, exclude hotels/hostels)
        excluded_types = ['hotel', 'guest_house', 'hostel', 'information', 'apartment']
        candidates = []
        for el in all_elements:
            tags = el.get("tags", {})
            name = tags.get("name")
            if not name: continue
            
            tourism = tags.get("tourism")
            if tourism in excluded_types: continue
            
            # If it's a known attraction type
            if tourism == 'attraction' or 'museum' in (tourism or '') or tags.get('historic'):
                candidates.append({
                    "osm_id": el.get("id"),
                    "name": name,
                    "tags": tags,
                    "lat": el.get("lat") or el.get("center", {}).get("lat"),
                    "lon": el.get("lon") or el.get("center", {}).get("lon")
                })

        logger.info(f"Found {len(candidates)} candidates for enrichment.")
        
        # Strategy: Batch Enrichment (Sweep the city every ~30 days)
        # Use day of the year to determine the offset
        import datetime
        day_of_year = datetime.datetime.now().timetuple().tm_yday
        batch_size = limit
        total_candidates = len(candidates)
        
        # Calculate start index based on day of year
        # This ensures we process a different subset each day
        start_idx = (day_of_year * batch_size) % total_candidates
        end_idx = min(start_idx + batch_size, total_candidates)
        
        selected = candidates[start_idx:end_idx]
        
        # If we reached the end of the list but batch isn't full, wrap around
        if len(selected) < batch_size and total_candidates > batch_size:
            extra_needed = batch_size - len(selected)
            selected.extend(candidates[0:extra_needed])
            
        logger.info(f"Day of year: {day_of_year}, Processing batch indices {start_idx} to {end_idx}")
        
        enriched_results = []
        all_keys_exhausted = False
        
        for i, item in enumerate(selected):
            if all_keys_exhausted:
                logger.warning("Skipping remaining items in batch due to exhausted keys.")
                break
                
            logger.info(f"Processing {i+1}/{len(selected)}: {item['name']}")
            details = self._get_google_details(f"{item['name']}, Hanoi")
            
            if details:
                enriched_results.append({
                    "osm_data": item,
                    "google_data": details,
                    "ingested_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                })
                # Add short delay
                time.sleep(0.5)
            else:
                # If we couldn't get details, it might be because all keys are out
                # Let's verify by checking a dummy or just checking the current key index in log 
                # (Assuming _get_google_details logs the error)
                logger.warning(f"Failed to enrich {item['name']}. Might have reached quota limits.")
                # We could add an explicit check here if needed.
            
        logger.info(f"Enrichment complete. Total enriched: {len(enriched_results)}")
        return enriched_results
