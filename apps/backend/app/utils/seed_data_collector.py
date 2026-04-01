import os
import json
import time
from datetime import datetime
from typing import List, Dict, Any

import pandas as pd
from serpapi import Client
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

SERPAPI_KEY = os.getenv("SERPAPI_KEY")
if not SERPAPI_KEY:
    raise ValueError("❌ Vui lòng đặt SERPAPI_KEY trong file .env")

client = Client(api_key=SERPAPI_KEY)

# ====================== CONFIG ======================
OUTPUT_DIR = "data/seed"
os.makedirs(OUTPUT_DIR, exist_ok=True)

HANOI_QUERIES = [
    "attractions in Hanoi Vietnam",
    "things to do in Hanoi",
    "Hanoi landmarks",
    "di tích lịch sử Hà Nội",
    "phố cổ Hà Nội",
    "Hồ Gươm Hà Nội",
    "Văn Miếu Quốc Tử Giám",
    "Hoàng thành Thăng Long",
    "chùa Một Cột Hà Nội",
    "Làng cổ Đường Lâm",
]

TRIPADVISOR_QUERIES = [
    "Hanoi attractions",
    "Things to do in Hanoi",
    "Hanoi landmarks",
]

def safe_save_raw(data: Any, filename: str):
    """Lưu raw response an toàn"""
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"✅ Saved raw: {filename}")


# ====================== GOOGLE LOCAL API (SỬA LỖI) ======================
def collect_google_local() -> List[Dict]:
    all_places: List[Dict] = []
    print("🚀 Bắt đầu thu thập từ **Google Local API**...")

    for query in tqdm(HANOI_QUERIES, desc="Google Local"):
        try:
            params = {
                "engine": "google_local",
                "q": query,
                "location": "Hanoi, Vietnam",
                "hl": "vi",
                "gl": "vn",
                "google_domain": "google.com.vn",
            }

            results = client.search(params)          # Trả về SerpResults object
            results_dict = results.as_dict()         # ← Cách đúng nhất hiện nay

            # Lấy local_results (có thể là list hoặc dict có key 'places')
            local_results = results_dict.get("local_results", {})
            if isinstance(local_results, dict):
                places = local_results.get("places", []) or local_results.get("results", [])
            else:
                places = local_results if isinstance(local_results, list) else []

            for place in places:
                gps = place.get("gps_coordinates", {}) or {}
                place_data = {
                    "source": "google_local",
                    "query_used": query,
                    "title": place.get("title"),
                    "place_id": place.get("place_id") or place.get("data_id"),
                    "data_id": place.get("data_id"),
                    "rating": place.get("rating"),
                    "reviews": place.get("reviews"),
                    "types": place.get("types") or place.get("type"),
                    "address": place.get("address"),
                    "gps": {
                        "lat": gps.get("latitude"),
                        "lng": gps.get("longitude")
                    },
                    "thumbnail": place.get("thumbnail"),
                    "description": place.get("description"),
                    "hours": place.get("hours"),
                    "collected_at": datetime.utcnow().isoformat()
                }
                all_places.append(place_data)

            safe_save_raw(results_dict, f"google_local_{query.replace(' ', '_')}_{int(time.time())}.json")
            time.sleep(1.8)   # Tiết kiệm quota

        except Exception as e:
            print(f"❌ Lỗi Google Local '{query}': {e}")
            time.sleep(3)

    print(f"📊 Thu thập được **{len(all_places)}** places từ Google Local")
    return all_places


# ====================== TRIPADVISOR (SỬA LỖI) ======================
def collect_tripadvisor() -> List[Dict]:
    all_places: List[Dict] = []
    print("\n🚀 Bắt đầu thu thập từ **Tripadvisor API**...")

    for query in tqdm(TRIPADVISOR_QUERIES, desc="Tripadvisor"):
        try:
            params = {
                "engine": "tripadvisor",
                "q": query,
                "ssrc": "A",        # A = Attractions / Things to Do
                "hl": "vi",
            }

            results = client.search(params)
            results_dict = results.as_dict()

            places = results_dict.get("places", []) or results_dict.get("results", [])

            for place in places:
                place_data = {
                    "source": "tripadvisor",
                    "query_used": query,
                    "title": place.get("title"),
                    "place_id": place.get("place_id"),
                    "rating": place.get("rating"),
                    "reviews_count": place.get("reviews") or place.get("num_reviews"),
                    "location_string": place.get("location_string"),
                    "tripadvisor_url": place.get("link"),
                    "thumbnail": place.get("thumbnail"),
                    "description": place.get("description"),
                    "collected_at": datetime.utcnow().isoformat()
                }
                all_places.append(place_data)

            safe_save_raw(results_dict, f"tripadvisor_{query.replace(' ', '_')}_{int(time.time())}.json")
            time.sleep(2.0)

        except Exception as e:
            print(f"❌ Lỗi Tripadvisor '{query}': {e}")
            time.sleep(3)

    print(f"📊 Thu thập được **{len(all_places)}** places từ Tripadvisor")
    return all_places


# ====================== MAIN ======================
def main():
    print("=== HANOI SMART TOURISM - SEED DATA COLLECTOR (Fixed) ===\n")

    google_places = collect_google_local()
    tripadvisor_places = collect_tripadvisor()

    all_data = google_places + tripadvisor_places

    if not all_data:
        print("⚠️ Không thu thập được dữ liệu nào. Kiểm tra SERPAPI_KEY và quota!")
        return

    df = pd.DataFrame(all_data)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M")

    df.to_parquet(f"{OUTPUT_DIR}/hanoi_attractions_seed_{timestamp}.parquet", index=False)
    df.to_json(f"{OUTPUT_DIR}/hanoi_attractions_seed_{timestamp}.json", 
               orient="records", force_ascii=False, indent=2)

    print(f"\n🎉 **HOÀN THÀNH!**")
    print(f"   Tổng places: {len(df)}")
    print(f"   File lưu tại: {OUTPUT_DIR}/")
    print(f"   • Parquet: hanoi_attractions_seed_{timestamp}.parquet")
    print(f"   • JSON:    hanoi_attractions_seed_{timestamp}.json\n")

    print("📈 Thống kê theo nguồn:")
    print(df.groupby("source").size())
    if "rating" in df.columns:
        print("\nTop 5 rating cao nhất:")
        print(df.nlargest(5, "rating")[["title", "rating", "source"]].to_string(index=False))


if __name__ == "__main__":
    main()