import httpx
import asyncio
import logging
from typing import List, Optional
from datetime import datetime
from app.core.config import settings

logger = logging.getLogger(__name__)

PLACES_BASE_URL = "https://maps.googleapis.com/maps/api/place"

HANOI_ATTRACTION_TYPES = [
    "tourist_attraction" # Điểm tham quan chính
    # "museum",             # Bảo tàng
    # "art_gallery",        # Phòng tranh/Nghệ thuật
    # "landmark",           # Các cột mốc di tích (Cột cờ Hà Nội, Văn Miếu...)
    # "place_of_worship",   # Các nơi tâm linh (Chùa, Đền)
    # "church",             # Nhà thờ (Nhà thờ Lớn...)
    # "hindu_temple",       # Đền chùa (Chùa Trấn Quốc...)
    # "park",               # Công viên trung tâm (Hồ Gươm, Hồ Tây...)
    # "amusement_park",     # Khu vui chơi giải trí
    # "zoo",                # Vườn thú/Thủy cung
    # "aquarium",
    # "memorial",           # Đài tưởng niệm/Di tích lịch sử
]


class GooglePlacesService:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session_timeout = httpx.Timeout(30.0)

    async def search_nearby(
        self,
        lat: float = settings.HANOI_LAT,
        lon: float = settings.HANOI_LON,
        radius: int = settings.HANOI_RADIUS_METERS,
        place_type: str = "tourist_attraction",
        page_token: Optional[str] = None,
    ) -> dict:
        params = {
            "location": f"{lat},{lon}",
            "radius": radius,
            "type": place_type,
            "key": self.api_key,
            "language": "vi",
        }
        if page_token:
            params["pagetoken"] = page_token

        async with httpx.AsyncClient(timeout=self.session_timeout) as client:
            response = await client.get(
                f"{PLACES_BASE_URL}/nearbysearch/json", params=params
            )
            response.raise_for_status()
            return response.json()

    async def get_place_details(self, place_id: str) -> dict:
        fields = [
            "place_id", "name", "formatted_address", "geometry",
            "rating", "user_ratings_total", "price_level",
            "opening_hours", "photos", "types", "website",
            "international_phone_number", "reviews",
        ]
        params = {
            "place_id": place_id,
            "fields": ",".join(fields),
            "key": self.api_key,
            "language": "vi",
        }
        async with httpx.AsyncClient(timeout=self.session_timeout) as client:
            response = await client.get(
                f"{PLACES_BASE_URL}/details/json", params=params
            )
            response.raise_for_status()
            return response.json().get("result", {})

    async def fetch_all_hanoi_attractions(
        self, limit_per_type: int = 60
    ) -> List[dict]:
        all_places = {}

        for place_type in HANOI_ATTRACTION_TYPES:
            logger.info(f"Fetching type: {place_type}")
            page_token = None
            count = 0

            while count < limit_per_type:
                try:
                    result = await self.search_nearby(
                        place_type=place_type, page_token=page_token
                    )
                    places = result.get("results", [])

                    for place in places:
                        pid = place.get("place_id")
                        if pid and pid not in all_places:
                            all_places[pid] = {
                                **place,
                                "fetched_type": place_type,
                                "ingested_at": datetime.utcnow().isoformat(),
                            }

                    page_token = result.get("next_page_token")
                    count += len(places)

                    if not page_token:
                        break

                    # Google requires 2s delay between page requests
                    await asyncio.sleep(2)

                except Exception as e:
                    logger.error(f"Error fetching {place_type}: {e}")
                    break

        logger.info(f"Total unique attractions fetched: {len(all_places)}")
        return list(all_places.values())

    async def test_connection(self) -> dict:
        try:
            result = await self.search_nearby(radius=1000)
            status = result.get("status")
            return {
                "success": status == "OK",
                "message": f"API status: {status}",
                "sample_count": len(result.get("results", [])),
            }
        except Exception as e:
            return {"success": False, "message": str(e)}