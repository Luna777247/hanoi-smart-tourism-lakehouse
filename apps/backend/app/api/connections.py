from fastapi import APIRouter, HTTPException, Depends, status
from typing import List
from datetime import datetime
import time

from app.schemas.connection import (
    ConnectionCreate, ConnectionUpdate, ConnectionResponse,
    ConnectionTestResult, ConnectionType,
)
from app.core.database import get_collection
from app.services.google_places_service import GooglePlacesService
from bson import ObjectId

router = APIRouter()


def _serialize(doc: dict) -> dict:
    doc["id"] = str(doc.pop("_id"))
    return doc


@router.get("/", response_model=List[ConnectionResponse])
async def list_connections():
    col = get_collection("connections")
    docs = await col.find({}).to_list(100)
    return [_serialize(d) for d in docs]


@router.post("/", response_model=ConnectionResponse, status_code=status.HTTP_201_CREATED)
async def create_connection(payload: ConnectionCreate):
    col = get_collection("connections")

    existing = await col.find_one({"name": payload.name})
    if existing:
        raise HTTPException(status_code=409, detail=f"Connection '{payload.name}' already exists")

    doc = {
        **payload.model_dump(),
        "status": "active",
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow(),
        "last_tested_at": None,
    }
    result = await col.insert_one(doc)
    doc["_id"] = result.inserted_id
    return _serialize(doc)


@router.get("/{connection_id}", response_model=ConnectionResponse)
async def get_connection(connection_id: str):
    col = get_collection("connections")
    doc = await col.find_one({"_id": ObjectId(connection_id)})
    if not doc:
        raise HTTPException(status_code=404, detail="Connection not found")
    return _serialize(doc)


@router.patch("/{connection_id}", response_model=ConnectionResponse)
async def update_connection(connection_id: str, payload: ConnectionUpdate):
    col = get_collection("connections")
    update_data = {k: v for k, v in payload.model_dump().items() if v is not None}
    update_data["updated_at"] = datetime.utcnow()

    result = await col.find_one_and_update(
        {"_id": ObjectId(connection_id)},
        {"$set": update_data},
        return_document=True,
    )
    if not result:
        raise HTTPException(status_code=404, detail="Connection not found")
    return _serialize(result)


@router.delete("/{connection_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_connection(connection_id: str):
    col = get_collection("connections")
    result = await col.delete_one({"_id": ObjectId(connection_id)})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Connection not found")


@router.post("/{connection_id}/test", response_model=ConnectionTestResult)
async def test_connection(connection_id: str):
    col = get_collection("connections")
    doc = await col.find_one({"_id": ObjectId(connection_id)})
    if not doc:
        raise HTTPException(status_code=404, detail="Connection not found")

    start = time.time()
    result = {"success": False, "message": "Unknown connection type", "latency_ms": None}

    try:
        if doc["type"] == ConnectionType.GOOGLE_PLACES:
            svc = GooglePlacesService(api_key=doc.get("api_key", ""))
            test = await svc.test_connection()
            result = {**test, "latency_ms": round((time.time() - start) * 1000, 2)}

        elif doc["type"] == ConnectionType.OPENWEATHER:
            import httpx
            params = {"q": "Hanoi,VN", "appid": doc.get("api_key", ""), "cnt": 1}
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get("https://api.openweathermap.org/data/2.5/weather", params=params)
                result = {
                    "success": resp.status_code == 200,
                    "message": "OK" if resp.status_code == 200 else resp.text,
                    "latency_ms": round((time.time() - start) * 1000, 2),
                }

        # Update last tested timestamp
        await col.update_one(
            {"_id": ObjectId(connection_id)},
            {"$set": {"last_tested_at": datetime.utcnow(),
                      "status": "active" if result["success"] else "error"}},
        )

    except Exception as e:
        result = {"success": False, "message": str(e), "latency_ms": None}

    return result