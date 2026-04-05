import httpx
import logging
import asyncio
from typing import List, Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)

OVERPASS_URL = "http://overpass-api.de/api/interpreter"
NOMINATIM_URL = "https://nominatim.openstreetmap.org/reverse"

class OSMService:
    def __init__(self):
        self.session_timeout = httpx.Timeout(45.0)
        self.headers = {
            "User-Agent": "HanoiSmartTourism/1.0 (contact: support@hanoitourism.vn)"
        }

    async def fetch_hanoi_attractions_from_overpass(self) -> List[Dict]:
        """
        Query Overpass API for tourist attractions and historical sites in Hanoi.
        """
        # Overpass QL query for Hanoi area
        query = """
        [out:json][timeout:60];
        area["name"="Thành phố Hà Nội"]->.a;
        (
          node["tourism"="attraction"](area.a);
          way["tourism"="attraction"](area.a);
          node["historic"](area.a);
          way["historic"](area.a);
          node["tourism"="museum"](area.a);
          way["tourism"="museum"](area.a);
        );
        out body;
        >;
        out skel qt;
        """
        
        async with httpx.AsyncClient(timeout=self.session_timeout) as client:
            try:
                response = await client.post(OVERPASS_URL, data={"data": query})
                response.raise_for_status()
                data = response.json()
                elements = data.get("elements", [])
                
                results = []
                for elem in elements:
                    if elem.get("type") in ["node", "way"] and "tags" in elem:
                        results.append({
                            "osm_id": str(elem.get("id")),
                            "osm_type": elem.get("type"),
                            "lat": elem.get("lat") or elem.get("center", {}).get("lat"),
                            "lon": elem.get("lon") or elem.get("center", {}).get("lon"),
                            "tags": elem.get("tags"),
                            "name": elem.get("tags", {}).get("name"),
                            "name_en": elem.get("tags", {}).get("name:en"),
                            "amenity": elem.get("tags", {}).get("amenity"),
                            "tourism": elem.get("tags", {}).get("tourism"),
                            "historic": elem.get("tags", {}).get("historic"),
                            "collected_at": datetime.utcnow().isoformat()
                        })
                return results
            except Exception as e:
                logger.error(f"Overpass API error: {e}")
                return []

    async def get_details_from_nominatim(self, osm_id: str, osm_type: str) -> Dict:
        """
        Get detailed address and information from Nominatim for a specific OSM object.
        """
        osm_type_char = osm_type[0].upper() # N, W, R
        params = {
            "osm_id": osm_id,
            "osm_type": osm_type_char,
            "format": "jsonv2",
            "addressdetails": 1,
            "accept-language": "vi,en"
        }
        
        async with httpx.AsyncClient(timeout=self.session_timeout, headers=self.headers) as client:
            try:
                # IMPORTANT: Nominatim requires 1 sec delay per request
                await asyncio.sleep(1.1) 
                response = await client.get(NOMINATIM_URL, params=params)
                response.raise_for_status()
                return response.json()
            except Exception as e:
                logger.error(f"Nominatim API error for {osm_type}:{osm_id}: {e}")
                return {}
