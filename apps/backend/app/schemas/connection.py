from pydantic import BaseModel, Field
from typing import Optional, Literal
from datetime import datetime
from enum import Enum


class ConnectionType(str, Enum):
    GOOGLE_PLACES = "google_places"
    OPENWEATHER = "openweather"
    CSV_UPLOAD = "csv_upload"
    POSTGRES = "postgres"
    MONGODB = "mongodb"


class ConnectionStatus(str, Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    ERROR = "error"


class ConnectionCreate(BaseModel):
    name: str = Field(..., min_length=3, max_length=100, example="google_places_hanoi")
    type: ConnectionType
    api_key: Optional[str] = Field(None, example="AIzaSy...")
    base_url: Optional[str] = Field(None, example="https://maps.googleapis.com")
    extra_params: Optional[dict] = Field(default_factory=dict)
    description: Optional[str] = None


class ConnectionUpdate(BaseModel):
    name: Optional[str] = None
    api_key: Optional[str] = None
    extra_params: Optional[dict] = None
    description: Optional[str] = None
    status: Optional[ConnectionStatus] = None


class ConnectionResponse(BaseModel):
    id: str
    name: str
    type: ConnectionType
    status: ConnectionStatus
    base_url: Optional[str]
    description: Optional[str]
    created_at: datetime
    updated_at: datetime
    last_tested_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class ConnectionTestResult(BaseModel):
    success: bool
    message: str
    latency_ms: Optional[float] = None
    sample_data: Optional[dict] = None