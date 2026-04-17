# File: apps/backend/app/config.py
import os
import hvac
from functools import lru_cache
from pydantic_settings import BaseSettings
 
class Settings(BaseSettings):
    vault_addr:  str = os.getenv('VAULT_ADDR',  'http://vault:8200')
    vault_token: str = os.getenv('VAULT_TOKEN', 'root')
    airflow_api_url: str = os.getenv('AIRFLOW_API_URL', 'http://airflow-webserver:8080')
    trino_host: str = os.getenv('TRINO_HOST', 'trino')
    trino_port: int = int(os.getenv('TRINO_PORT', '8080'))
 
    class Config:
        env_file = '.env'
 
@lru_cache()
def get_settings() -> Settings:
    return Settings()
 
def get_vault_client() -> hvac.Client:
    s = get_settings()
    return hvac.Client(url=s.vault_addr, token=s.vault_token)
 
def get_secret(path: str, key: str) -> str:
    client = get_vault_client()
    secret = client.secrets.kv.v2.read_secret_version(path=path)
    return secret['data']['data'][key]

