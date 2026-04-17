# File: apps/backend/app/routers/pipeline.py
import requests
from fastapi import APIRouter, HTTPException, Depends
from app.config import get_settings, Settings
 
router = APIRouter()
 
def _airflow_auth(s: Settings):
    return ('admin', s.airflow_admin_password)
 
@router.post('/trigger/{dag_id}')
async def trigger_dag(dag_id: str, settings: Settings = Depends(get_settings)):
    """Kich hoat thu cong mot DAG qua Airflow REST API."""
    url = f'{settings.airflow_api_url}/api/v1/dags/{dag_id}/dagRuns'
    resp = requests.post(
        url,
        json={'logical_date': None},
        auth=('admin', 'admin'),   # Lay tu Vault trong production
        timeout=10,
    )
    if resp.status_code not in (200, 201):
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return {'dag_id': dag_id, 'run_id': resp.json().get('dag_run_id')}
 
@router.get('/status/{dag_id}')
async def dag_status(dag_id: str, settings: Settings = Depends(get_settings)):
    """Lay trang thai cac DAG run gan nhat."""
    url = f'{settings.airflow_api_url}/api/v1/dags/{dag_id}/dagRuns?limit=5&order_by=-start_date'
    resp = requests.get(url, auth=('admin', 'admin'), timeout=10)
    return resp.json()

