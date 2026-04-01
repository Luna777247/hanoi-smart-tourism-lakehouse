from fastapi import APIRouter, Query, HTTPException
from typing import Optional, List
from app.core.config import settings
import httpx

router = APIRouter()

TRINO_BASE = f"http://{settings.TRINO_HOST if hasattr(settings, 'TRINO_HOST') else 'localhost'}:8081/v1"


async def run_trino_query(sql: str) -> List[dict]:
    """Execute SQL on Trino and return results as list of dicts."""
    headers = {
        "X-Trino-User": "admin",
        "X-Trino-Catalog": "iceberg",
        "X-Trino-Schema": "gold",
        "Content-Type": "application/json",
    }
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(f"{TRINO_BASE}/statement", content=sql, headers=headers)
        resp.raise_for_status()
        data = resp.json()

        columns = [c["name"] for c in data.get("columns", [])]
        rows = data.get("data", [])

        # Follow next_uri for paginated results
        next_uri = data.get("nextUri")
        while next_uri:
            r = await client.get(next_uri, headers=headers)
            r.raise_for_status()
            page = r.json()
            rows.extend(page.get("data", []))
            next_uri = page.get("nextUri")

        return [dict(zip(columns, row)) for row in rows]


@router.get("/top")
async def get_top_attractions(
    limit: int = Query(10, ge=1, le=50),
    district: Optional[str] = None,
    category: Optional[str] = None,
):
    """Top điểm du lịch theo rating từ Gold layer."""
    where_clauses = ["rating IS NOT NULL", "total_ratings > 10"]
    if district:
        where_clauses.append(f"district = '{district}'")
    if category:
        where_clauses.append(f"category = '{category}'")

    where = " AND ".join(where_clauses)
    sql = f"""
        SELECT
            place_id, name, district, category,
            ROUND(rating, 2) AS rating,
            total_ratings,
            avg_sentiment_score,
            is_overcrowded,
            latitude, longitude
        FROM gold.fact_attractions
        WHERE {where}
        ORDER BY rating DESC, total_ratings DESC
        LIMIT {limit}
    """
    try:
        return await run_trino_query(sql)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")


@router.get("/summary")
async def get_dashboard_summary():
    """Tổng hợp số liệu cho Dashboard header cards."""
    sql = """
        SELECT
            COUNT(*) AS total_attractions,
            ROUND(AVG(rating), 2) AS avg_rating,
            SUM(total_ratings) AS total_reviews,
            COUNT(CASE WHEN is_overcrowded THEN 1 END) AS overcrowded_count
        FROM gold.fact_attractions
        WHERE rating IS NOT NULL
    """
    try:
        results = await run_trino_query(sql)
        return results[0] if results else {}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/weather-correlation")
async def get_weather_correlation():
    """Correlation thời tiết → rating / lượt khách."""
    sql = """
        SELECT
            w.weather_condition,
            ROUND(AVG(f.rating), 2) AS avg_rating,
            COUNT(*) AS sample_count,
            ROUND(AVG(w.temperature_celsius), 1) AS avg_temp
        FROM gold.fact_attractions f
        JOIN gold.dim_weather w ON f.weather_snapshot_id = w.snapshot_id
        GROUP BY w.weather_condition
        ORDER BY avg_rating DESC
    """
    try:
        return await run_trino_query(sql)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/districts")
async def get_districts_summary():
    """Thống kê theo quận/huyện."""
    sql = """
        SELECT
            district,
            COUNT(*) AS attraction_count,
            ROUND(AVG(rating), 2) AS avg_rating,
            SUM(total_ratings) AS total_reviews
        FROM gold.fact_attractions
        WHERE district IS NOT NULL AND rating IS NOT NULL
        GROUP BY district
        ORDER BY attraction_count DESC
    """
    try:
        return await run_trino_query(sql)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))