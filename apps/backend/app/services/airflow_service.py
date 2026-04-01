import httpx
import logging
from datetime import datetime
from typing import Optional
from app.core.config import settings

logger = logging.getLogger(__name__)


class AirflowService:
    def __init__(self):
        self.base_url = settings.AIRFLOW_BASE_URL
        self.auth = (settings.AIRFLOW_USER, settings.AIRFLOW_PASSWORD)
        self.headers = {"Content-Type": "application/json"}

    async def trigger_dag(
        self,
        dag_id: str,
        conf: Optional[dict] = None,
        run_id: Optional[str] = None,
    ) -> dict:
        run_id = run_id or f"manual__{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}"
        payload = {"dag_run_id": run_id, "conf": conf or {}}

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns",
                json=payload,
                auth=self.auth,
                headers=self.headers,
            )
            response.raise_for_status()
            return response.json()

    async def get_dag_run_status(self, dag_id: str, run_id: str) -> dict:
        async with httpx.AsyncClient(timeout=15.0) as client:
            response = await client.get(
                f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns/{run_id}",
                auth=self.auth,
            )
            response.raise_for_status()
            return response.json()

    async def get_all_dag_runs(self, dag_id: str, limit: int = 10) -> list:
        async with httpx.AsyncClient(timeout=15.0) as client:
            response = await client.get(
                f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns",
                params={"limit": limit, "order_by": "-execution_date"},
                auth=self.auth,
            )
            response.raise_for_status()
            return response.json().get("dag_runs", [])

    async def toggle_dag(self, dag_id: str, is_paused: bool) -> dict:
        async with httpx.AsyncClient(timeout=15.0) as client:
            response = await client.patch(
                f"{self.base_url}/api/v1/dags/{dag_id}",
                json={"is_paused": is_paused},
                auth=self.auth,
                headers=self.headers,
            )
            response.raise_for_status()
            return response.json()

    async def health_check(self) -> bool:
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(f"{self.base_url}/health")
                return response.status_code == 200
        except Exception:
            return False

    def build_dag_id(self, pipeline_type: str, connection_name: str) -> str:
        safe_name = connection_name.lower().replace(" ", "_").replace("-", "_")
        return f"tourism_{pipeline_type}_{safe_name}"


airflow_service = AirflowService()