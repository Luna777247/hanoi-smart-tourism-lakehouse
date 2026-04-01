from fastapi import APIRouter, HTTPException, status
from typing import List
from datetime import datetime

from app.schemas.schedule import ScheduleCreate, ScheduleResponse, RunTriggerRequest
from app.core.database import get_collection
from app.services.airflow_service import airflow_service
from bson import ObjectId

router = APIRouter()


def _serialize(doc: dict) -> dict:
    doc["id"] = str(doc.pop("_id"))
    return doc


DAG_MAP = {
    "full_ingestion": "hanoi_tourism_ingestion",
    "bronze_to_silver": "hanoi_bronze_to_silver",
    "silver_to_gold": "hanoi_silver_to_gold",
    "weather_sync": "hanoi_weather_sync",
    "full_pipeline": "hanoi_full_pipeline",
    "incremental": "hanoi_tourism_incremental",
}


@router.get("/", response_model=List[ScheduleResponse])
async def list_schedules():
    col = get_collection("schedules")
    docs = await col.find({}).to_list(100)
    return [_serialize(d) for d in docs]


@router.post("/", response_model=ScheduleResponse, status_code=status.HTTP_201_CREATED)
async def create_schedule(payload: ScheduleCreate):
    col = get_collection("schedules")

    existing = await col.find_one({"name": payload.name})
    if existing:
        raise HTTPException(status_code=409, detail=f"Schedule '{payload.name}' already exists")

    dag_id = DAG_MAP.get(payload.pipeline_type, f"hanoi_{payload.pipeline_type}")

    doc = {
        **payload.model_dump(),
        "dag_id": dag_id,
        "last_run": None,
        "next_run": None,
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow(),
    }
    result = await col.insert_one(doc)
    doc["_id"] = result.inserted_id

    # Activate the DAG in Airflow
    try:
        await airflow_service.toggle_dag(dag_id, is_paused=not payload.is_active)
    except Exception as e:
        # Don't fail if Airflow is not ready yet
        pass

    return _serialize(doc)


@router.post("/{schedule_id}/trigger")
async def trigger_schedule(schedule_id: str, payload: RunTriggerRequest):
    col = get_collection("schedules")
    doc = await col.find_one({"_id": ObjectId(schedule_id)})
    if not doc:
        raise HTTPException(status_code=404, detail="Schedule not found")

    dag_id = doc.get("dag_id")
    conf = {**doc.get("config", {}), **(payload.config_override or {})}

    try:
        run = await airflow_service.trigger_dag(dag_id=dag_id, conf=conf)
        await col.update_one(
            {"_id": ObjectId(schedule_id)},
            {"$set": {"last_run": datetime.utcnow()}},
        )
        return {"dag_run_id": run.get("dag_run_id"), "state": run.get("state"), "dag_id": dag_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to trigger DAG: {e}")


@router.delete("/{schedule_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_schedule(schedule_id: str):
    col = get_collection("schedules")
    result = await col.delete_one({"_id": ObjectId(schedule_id)})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Schedule not found")