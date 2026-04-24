import os
import hvac
from pydantic_settings import BaseSettings
from functools import lru_cache

class Settings(BaseSettings):
    # App
    APP_NAME: str = "Hanoi Smart Tourism Lakehouse"
    DEBUG: bool = False

    # Vault Configuration
    VAULT_ADDR: str = os.getenv("VAULT_ADDR", "http://vault:8200")
    VAULT_TOKEN: str = os.getenv("VAULT_TOKEN", "root")

    # DB & Services
    POSTGRES_URI: str = os.getenv("POSTGRES_URI", "postgresql://airflow:airflow@postgres/tourism_catalog")
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://redis:6379/0")
    AIRFLOW_BASE_URL: str = os.getenv("AIRFLOW_BASE_URL", "http://airflow-webserver:8080")
    
    # MinIO
    MINIO_ENDPOINT: str = os.getenv("MINIO_ENDPOINT", "minio:9000")
    MINIO_ACCESS_KEY: str = os.getenv("MINIO_ACCESS_KEY", "minio_admin")
    MINIO_SECRET_KEY: str = os.getenv("MINIO_SECRET_KEY", "ChangeMe_Minio123!")
    
    # API Keys (Will be overridden by Vault if available)
    GOOGLE_PLACES_API_KEY: str = os.getenv("GOOGLE_PLACES_API_KEY", "")
    SERPAPI_KEY: str = os.getenv("SERPAPI_KEY", "")

    # Hanoi config
    HANOI_LAT: float = 21.0285
    HANOI_LON: float = 105.8542
    HANOI_RADIUS_METERS: int = 30000

    class Config:
        env_file = ".env"
        extra = "ignore"

def fetch_vault_secrets(vault_addr: str, vault_token: str):
    """Fetch secrets from HashiCorp Vault"""
    secrets = {}
    try:
        # Check if we are running inside docker or local
        client = hvac.Client(url=vault_addr, token=vault_token)
        if client.is_authenticated():
            # Read Database credentials
            try:
                db_resp = client.secrets.kv.v2.read_secret_version(path='database')
                db_data = db_resp['data']['data']
                if db_data.get('username') and db_data.get('password'):
                    # Update URI if needed
                    user = db_data.get('username')
                    pw = db_data.get('password')
                    secrets['POSTGRES_URI'] = f"postgresql://{user}:{pw}@postgres/tourism_catalog"
            except:
                pass

            # Read API Keys
            try:
                api_resp = client.secrets.kv.v2.read_secret_version(path='api-keys')
                api_data = api_resp['data']['data']
                if api_data.get('google_places'):
                    secrets['GOOGLE_PLACES_API_KEY'] = api_data.get('google_places')
                if api_data.get('serpapi'):
                    secrets['SERPAPI_KEY'] = api_data.get('serpapi')
            except:
                pass
            
            print(f"INFO: Successfully fetched secrets from Vault: {vault_addr}")
    except Exception as e:
        print(f"WARNING: Vault connection failed ({e}). Falling back to Environment Variables.")
    return secrets

@lru_cache()
def get_settings() -> Settings:
    # 1. Start with baseline settings from ENV
    current_settings = Settings()
    
    # 2. Merge with Vault secrets (Override ENV)
    vault_secrets = fetch_vault_secrets(current_settings.VAULT_ADDR, current_settings.VAULT_TOKEN)
    for key, value in vault_secrets.items():
        if hasattr(current_settings, key):
            setattr(current_settings, key, value)
            
    return current_settings

settings = get_settings()