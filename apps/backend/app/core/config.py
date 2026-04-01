from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    # App
    APP_NAME: str = "Hanoi Smart Tourism Lakehouse"
    DEBUG: bool = False

    # MongoDB
    MONGO_URI: str = "mongodb://admin:tourism2024@localhost:27017/tourism_connections?authSource=admin"

    # PostgreSQL
    POSTGRES_URI: str = "postgresql://airflow:airflow@localhost/tourism_catalog"

    # Redis
    REDIS_URL: str = "redis://localhost:6379/0"

    # Airflow
    AIRFLOW_BASE_URL: str = "http://localhost:8080"
    AIRFLOW_USER: str = "admin"
    AIRFLOW_PASSWORD: str = "admin"

    # MinIO
    MINIO_ENDPOINT: str = "localhost:9000"
    MINIO_ACCESS_KEY: str = "minioadmin"
    MINIO_SECRET_KEY: str = "minioadmin123"
    MINIO_SECURE: bool = False

    # Buckets
    BRONZE_BUCKET: str = "tourism-bronze"
    SILVER_BUCKET: str = "tourism-silver"
    GOLD_BUCKET: str = "tourism-gold"

    # API Keys
    GOOGLE_PLACES_API_KEY: str = ""
    OPENWEATHER_API_KEY: str = ""

    # Hanoi config
    HANOI_LAT: float = 21.0285
    HANOI_LON: float = 105.8542
    HANOI_RADIUS_METERS: int = 30000

    class Config:
        env_file = ".env"
        case_sensitive = True


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()