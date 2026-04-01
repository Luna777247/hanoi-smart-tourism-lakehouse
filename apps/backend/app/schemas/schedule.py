from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime
from enum import Enum


class ScheduleInterval(str, Enum):
    HOURLY = "@hourly"
    DAILY = "@daily"
    WEEKLY = "@weekly"
    MONTHLY = "@monthly"
    CUSTOM = "custom"


class PipelineType(str, Enum):
    FULL_INGESTION = "full_ingestion"        # Bronze layer
    INCREMENTAL = "incremental"              # Bronze incremental
    BRONZE_TO_SILVER = "bronze_to_silver"   # PySpark transform
    SILVER_TO_GOLD = "silver_to_gold"       # dbt models
    WEATHER_SYNC = "weather_sync"           # Weather data
    FULL_PIPELINE = "full_pipeline"         # All steps


class ScheduleCreate(BaseModel):
    name: str = Field(..., example="daily_hanoi_ingestion")
    connection_id: str
    pipeline_type: PipelineType
    interval: ScheduleInterval = ScheduleInterval.DAILY
    cron_expression: Optional[str] = Field(None, example="0 2 * * *")
    config: dict = Field(default_factory=dict)
    is_active: bool = True
    description: Optional[str] = None


class ScheduleResponse(BaseModel):
    id: str
    name: str
    pipeline_type: PipelineType
    interval: ScheduleInterval
    dag_id: Optional[str] = None
    is_active: bool
    next_run: Optional[datetime] = None
    last_run: Optional[datetime] = None
    created_at: datetime

    class Config:
        from_attributes = True


class RunTriggerRequest(BaseModel):
    schedule_id: str
    config_override: Optional[dict] = None
    note: Optional[str] = None