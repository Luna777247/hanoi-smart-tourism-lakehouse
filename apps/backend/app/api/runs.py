from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from app.services.airflow_service import airflow_service

router = APIRouter()

ALL_DAGS = [
    "hanoi_tourism_ingestion",
    "hanoi_bronze_to_silver",
    "hanoi_silver_to_gold",
    "hanoi_weather_sync",
    "hanoi_full_pipeline",
]


@router.get("/")
async def list_recent_runs(limit: int = Query(20, ge=1, le=100)):
    """Lấy các pipeline run gần nhất từ tất cả DAGs."""
    all_runs = []
    for dag_id in ALL_DAGS:
        try:
            runs = await airflow_service.get_all_dag_runs(dag_id, limit=limit)
            for r in runs:
                r["dag_id"] = dag_id
            all_runs.extend(runs)
        except Exception:
            pass

    all_runs.sort(key=lambda x: x.get("execution_date", ""), reverse=True)
    return all_runs[:limit]


@router.get("/{dag_id}/{run_id}")
async def get_run_detail(dag_id: str, run_id: str):
    try:
        return await airflow_service.get_dag_run_status(dag_id, run_id)
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/stats/summary")
async def get_run_stats():
    """Thống kê trạng thái pipeline runs."""
    stats = {"total": 0, "success": 0, "failed": 0, "running": 0}
    for dag_id in ALL_DAGS:
        try:
            runs = await airflow_service.get_all_dag_runs(dag_id, limit=50)
            for r in runs:
                stats["total"] += 1
                state = r.get("state", "")
                if state == "success":
                    stats["success"] += 1
                elif state == "failed":
                    stats["failed"] += 1
                elif state == "running":
                    stats["running"] += 1
        except Exception:
            pass
    return stats