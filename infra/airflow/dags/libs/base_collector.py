import json
import logging
import os
import time
import hashlib
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from io import BytesIO

import hvac
import redis
from minio import Minio
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logger = logging.getLogger(__name__)

class BaseLakehouseIngestor(ABC):
    """
    Base class ap dung cac nguyen tac OOP cho viec ingestion du lieu.
    - Abstraction: User chi can quan tam den collect() va transform().
    - Encapsulation: Cac secrets va ket noi duoc quan ly noi bo.
    - Resilience: Su dung tenacity cho retry va redis cho caching.
    """
    
    def __init__(self, source_name: str, bucket_name: str = "tourism-bronze"):
        self.source_name = source_name
        self.bucket_name = bucket_name
        self._vault_client = self._init_vault_client()
        self._minio_client = self._init_minio_client()
        self._redis_client = self._init_redis_client()

    def _init_vault_client(self):
        """Encapsulation: Khoi tao ket noi Vault bao mat."""
        return hvac.Client(
            url=os.getenv('VAULT_ADDR', 'http://vault:8200'),
            token=os.getenv('VAULT_TOKEN', 'root')
        )

    def _init_minio_client(self):
        """Encapsulation: Khoi tao ket noi MinIO."""
        try:
            creds = self.get_secret('storage/minio')
            access_key = creds.get('access_key')
            secret_key = creds.get('secret_key')
        except Exception:
            access_key = os.getenv('MINIO_ACCESS_KEY', 'minio_admin')
            secret_key = os.getenv('MINIO_SECRET_KEY', 'ChangeMe_Minio123!')

        return Minio(
            os.getenv('MINIO_ENDPOINT', 'minio:9000').replace('http://', '').replace('https://', ''),
            access_key=access_key,
            secret_key=secret_key,
            secure=False
        )

    def _init_redis_client(self):
        """Khoi tao Redis client cho caching."""
        return redis.Redis(
            host=os.getenv('REDIS_HOST', 'redis'),
            port=int(os.getenv('REDIS_PORT', 6379)),
            db=0,
            decode_responses=True
        )

    def get_secret(self, path: str):
        """Common logic de lay secrets tu Vault. Co fallback de tranh crash khi test."""
        try:
            response = self._vault_client.secrets.kv.v2.read_secret_version(path=path)
            return response['data']['data']
        except Exception as e:
            logger.warning(f"Could not fetch secret from Vault ({path}): {e}. Using empty defaults.")
            return {}

    @abstractmethod
    def collect(self) -> any:
        """Abstraction: Moi nguon du lieu se co cach lay rieng (Google, OSM, TA)."""
        pass

    def get_cached_data(self, key: str):
        """Lay du lieu tu cache. Resilient: Neu loi Redis van tiep tuc chay."""
        try:
            cached = self._redis_client.get(f"ingest:{self.source_name}:{key}")
            if cached:
                logger.info(f"Cache hit for {self.source_name}:{key}")
                return json.loads(cached)
        except Exception as e:
            logger.warning(f"Redis get error: {e}")
        return None

    def set_cached_data(self, key: str, data: any, ttl: int = 86400):
        """Luu du lieu vao cache. Resilient: Neu loi Redis van tiep tuc chay."""
        try:
            self._redis_client.setex(
                f"ingest:{self.source_name}:{key}",
                ttl,
                json.dumps(data)
            )
        except Exception as e:
            logger.warning(f"Redis set error: {e}")

    def save_to_bronze(self, data: any, extension: str = "json", logical_date: str = None, run_id: str = None):
        """Inheritance: logic luu vao MinIO duoc tai su dung cho moi subclass. Thuc hien ghi de idempotent."""
        date_str = logical_date if logical_date else datetime.now(timezone.utc).strftime("%Y-%m-%d")
        rid = run_id if run_id else f"manual_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
        
        object_name = f"source={self.source_name}/snapshot_date={date_str}/run_id={rid}/data.{extension}"
        
        if extension == "json":
            content = json.dumps(data, ensure_ascii=False).encode("utf-8")
        else:
            content = data

        self._minio_client.put_object(
            bucket_name=self.bucket_name,
            object_name=object_name,
            data=BytesIO(content),
            length=len(content),
            content_type=f"application/{extension}",
        )
        logger.info(f"Successfully saved data to {self.bucket_name}/{object_name}")
        return object_name

    def _check_exists_for_date(self, date_str: str):
        prefix = f"source={self.source_name}/snapshot_date={date_str}/"
        objects = list(self._minio_client.list_objects(self.bucket_name, prefix=prefix, recursive=True))
        return len(objects) > 0

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type(Exception),
        reraise=True
    )
    def run_with_retry(self):
        """Goi collect voi retry logic."""
        logger.info(f"Starting ingestion for {self.source_name}...")
        return self.collect()

    def run(self, force=False, logical_date=None, run_id=None):
        """
        Main entry point cho ingestion.
        """
        date_str = logical_date if logical_date else datetime.now(timezone.utc).strftime("%Y-%m-%d")

        if not force and self._check_exists_for_date(date_str):
            logger.info(f"Data for {self.source_name} already exists for {date_str}. Skipping.")
            return "SKIPPED"

        try:
            data = self.run_with_retry()
            if data:
                return self.save_to_bronze(data, logical_date=date_str, run_id=run_id)
            else:
                logger.warning(f"No data collected for {self.source_name}")
                return None
        except Exception as e:
            logger.error(f"Ingestion failed after retries: {e}")
            raise e
