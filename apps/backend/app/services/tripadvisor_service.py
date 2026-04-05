import httpx
import logging
from typing import List, Dict, Any
from datetime import datetime
from app.core.config import settings

logger = logging.getLogger(__name__)

SERPAPI_URL = "https://serpapi.com/search"

TRIPADVISOR_DEFAULT_QUERIES = [
    "Hanoi attractions",
    "Things to do in Hanoi",
    "Hanoi landmarks",
]

class TripadvisorService:
    def __init__(self, api_key: str = None):
        self.api_key = api_key or settings.SERPAPI_KEY
        self.session_timeout = httpx.Timeout(30.0)

    async def search_tripadvisor(self, query: str) -> List[Dict]:
        """Search Tripadvisor using SerpApi."""
        if not self.api_key:
            logger.error("No SerpApi key provided")
            return []

        params = {
            "engine": "tripadvisor",
            "q": query,
            "ssrc": "A",  # A = Attractions
            "hl": "vi",   # Vietnamese results
            "api_key": self.api_key,
        }

        async with httpx.AsyncClient(timeout=self.session_timeout) as client:
            try:
                response = await client.get(SERPAPI_URL, params=params)
                response.raise_for_status()
                data = response.json()
                
                # Retrieve from local_results or similar depending on the SerpApi's tripadvisor engine response
                # Based on seed_data_collector.py, it was results_dict.get("places", []) or results_dict.get("results", [])
                places = data.get("places", []) or data.get("results", []) or []
                
                enriched_places = []
                for p in places:
                    enriched_places.append({
                        "source": "tripadvisor",
                        "query_used": query,
                        "title": p.get("title"),
                        "place_id": p.get("place_id"),
                        "rating": p.get("rating"),
                        "reviews_count": p.get("reviews") or p.get("num_reviews"),
                        "location_string": p.get("location_string"),
                        "tripadvisor_url": p.get("link"),
                        "thumbnail": p.get("thumbnail"),
                        "description": p.get("description"),
                        "collected_at": datetime.utcnow().isoformat()
                    })
                
                return enriched_places
            except Exception as e:
                logger.error(f"Tripadvisor search error for '{query}': {e}")
                return []

    async def fetch_all_hanoi_attractions(self, queries: List[str] = None) -> List[Dict]:
        """Collect attractions for all predefined queries."""
        active_queries = queries or TRIPADVISOR_DEFAULT_QUERIES
        all_results = []
        
        for query in active_queries:
            results = await self.search_tripadvisor(query)
            all_results.extend(results)
            
        # Deduplication by title or place_id if available
        seen = set()
        unique_results = []
        for r in all_results:
            key = r.get("place_id") or r.get("title")
            if key not in seen:
                seen.add(key)
                unique_results.append(r)
                
        return unique_results
