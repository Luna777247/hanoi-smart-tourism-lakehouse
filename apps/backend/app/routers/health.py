# File: apps/backend/app/routers/health.py
from fastapi import APIRouter
from datetime import datetime
 
router = APIRouter()
 
@router.get('/health')
async def health_check():
    return {
        'status': 'ok',
        'service': 'hanoi-tourism-lakehouse-api',
        'timestamp': datetime.utcnow().isoformat(),
        'version': '1.0.0',
    }

