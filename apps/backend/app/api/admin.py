from fastapi import APIRouter
from app.services.airflow_service import airflow_service
from app.services.minio_service import minio_service
from app.core.database import get_db
import redis.asyncio as aioredis
from app.core.config import settings

router = APIRouter()


@router.get("/health")
async def health_check():
    checks = {}

    # MongoDB
    try:
        await get_db().command("ping")
        checks["mongodb"] = "ok"
    except Exception:
        checks["mongodb"] = "error"

    # Redis
    try:
        r = aioredis.from_url(settings.REDIS_URL)
        await r.ping()
        await r.aclose()
        checks["redis"] = "ok"
    except Exception:
        checks["redis"] = "error"

    # MinIO
    checks["minio"] = "ok" if minio_service.health_check() else "error"

    # Airflow
    checks["airflow"] = "ok" if await airflow_service.health_check() else "error"

    overall = "healthy" if all(v == "ok" for v in checks.values()) else "degraded"

    return {"status": overall, "services": checks}


@router.get("/storage/stats")
async def storage_stats():
    return minio_service.get_bucket_stats()