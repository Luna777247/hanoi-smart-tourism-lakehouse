import json
import logging
from datetime import datetime
from typing import List, Optional
from io import BytesIO
from minio import Minio
from minio.error import S3Error
from app.core.config import settings

logger = logging.getLogger(__name__)


class MinioService:
    def __init__(self):
        self.client = Minio(
            settings.MINIO_ENDPOINT,
            access_key=settings.MINIO_ACCESS_KEY,
            secret_key=settings.MINIO_SECRET_KEY,
            secure=settings.MINIO_SECURE,
        )

    def _get_bronze_path(self, source: str, date: str) -> str:
        """Partition path: source=google_places/date=2024-01-15/"""
        return f"source={source}/date={date}"

    def upload_json_to_bronze(
        self,
        data: List[dict],
        source: str,
        filename: Optional[str] = None,
    ) -> str:
        date_str = datetime.utcnow().strftime("%Y-%m-%d")
        ts = datetime.utcnow().strftime("%H%M%S")
        filename = filename or f"raw_{ts}.json"
        object_name = f"{self._get_bronze_path(source, date_str)}/{filename}"

        json_bytes = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
        buffer = BytesIO(json_bytes)

        try:
            self.client.put_object(
                bucket_name=settings.BRONZE_BUCKET,
                object_name=object_name,
                data=buffer,
                length=len(json_bytes),
                content_type="application/json",
                metadata={
                    "source": source,
                    "record_count": str(len(data)),
                    "ingested_at": datetime.utcnow().isoformat(),
                },
            )
            full_path = f"s3a://{settings.BRONZE_BUCKET}/{object_name}"
            logger.info(f"Uploaded {len(data)} records to {full_path}")
            return full_path
        except S3Error as e:
            logger.error(f"MinIO upload failed: {e}")
            raise

    def list_bronze_partitions(self, source: str) -> List[str]:
        prefix = f"source={source}/"
        objects = self.client.list_objects(
            settings.BRONZE_BUCKET, prefix=prefix, recursive=False
        )
        return [obj.object_name for obj in objects]

    def get_bucket_stats(self) -> dict:
        stats = {}
        for bucket in [settings.BRONZE_BUCKET, settings.SILVER_BUCKET, settings.GOLD_BUCKET]:
            try:
                objects = list(self.client.list_objects(bucket, recursive=True))
                total_size = sum(obj.size or 0 for obj in objects)
                stats[bucket] = {
                    "object_count": len(objects),
                    "total_size_mb": round(total_size / 1024 / 1024, 2),
                }
            except Exception as e:
                stats[bucket] = {"error": str(e)}
        return stats

    def health_check(self) -> bool:
        try:
            self.client.list_buckets()
            return True
        except Exception:
            return False


minio_service = MinioService()