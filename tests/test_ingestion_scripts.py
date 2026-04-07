import os
import sys
import asyncio
import unittest
import json
from unittest.mock import patch, MagicMock, AsyncMock

# 1. Định nghĩa ROOT và PATHS
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INGESTION_PATH = os.path.join(ROOT_DIR, "infra", "airflow", "processing", "ingestion")

if INGESTION_PATH not in sys.path:
    sys.path.insert(0, INGESTION_PATH)

# Mock app.core.config để tránh lỗi Pydantic
mock_config = MagicMock()
sys.modules["app.core.config"] = mock_config

# Import các script sau khi mock env
import fetch_hanoi_osm
import fetch_hanoi_tripadvisor
import fetch_hanoi_google
import fetch_hanoi_weather

class TestIngestionScripts(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        os.environ["MINIO_ENDPOINT"] = "localhost:9000"
        os.environ["MINIO_ACCESS_KEY"] = "minio"
        os.environ["MINIO_SECRET_KEY"] = "minio123"
        os.environ["SERPAPI_KEY"] = "fake_key"
        os.environ["OPENWEATHER_API_KEY"] = "fake_key"

    @patch("fetch_hanoi_osm.Minio")
    @patch("fetch_hanoi_osm.httpx.AsyncClient")
    async def test_osm_ingestion(self, mock_client_class, mock_minio_class):
        # Mock OSM Raw Response
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"generator": "Overpass", "elements": [{"id": 1, "tags": {"name": "Test"}}]}
        mock_resp.raise_for_status = MagicMock()
        
        client_instance = mock_client_class.return_value.__aenter__.return_value
        client_instance.post = AsyncMock(return_value=mock_resp)
        
        mock_mc = mock_minio_class.return_value
        mock_mc.bucket_exists.return_value = True
        
        await fetch_hanoi_osm.run_ingestion()
        self.assertTrue(mock_mc.put_object.called)
        print("\n✅ OSM Ingestion TEST: PASSED (Raw JSON captured)")

    @patch("fetch_hanoi_tripadvisor.Minio")
    @patch("fetch_hanoi_tripadvisor.httpx.AsyncClient")
    async def test_tripadvisor_ingestion(self, mock_client_class, mock_minio_class):
        # Mock SerpApi Tripadvisor Response
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"search_metadata": {"id": "123"}, "tripadvisor_results": []}
        mock_resp.raise_for_status = MagicMock()
        
        client_instance = mock_client_class.return_value.__aenter__.return_value
        client_instance.get = AsyncMock(return_value=mock_resp)
        
        mock_mc = mock_minio_class.return_value
        mock_mc.bucket_exists.return_value = True
        
        await fetch_hanoi_tripadvisor.run_ingestion()
        self.assertTrue(mock_mc.put_object.called)
        print("✅ TripAdvisor Ingestion TEST: PASSED (Raw JSON captured)")

    @patch("fetch_hanoi_google.Minio")
    @patch("fetch_hanoi_google.httpx.AsyncClient")
    async def test_google_serpapi_ingestion(self, mock_client_class, mock_minio_class):
        # Mock SerpApi Google Maps Response
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"local_results": [{"title": "Test Google"}]}
        mock_resp.raise_for_status = MagicMock()
        
        client_instance = mock_client_class.return_value.__aenter__.return_value
        client_instance.get = AsyncMock(return_value=mock_resp)
        
        mock_mc = mock_minio_class.return_value
        mock_mc.bucket_exists.return_value = True
        
        await fetch_hanoi_google.run_ingestion()
        self.assertTrue(mock_mc.put_object.called)
        print("✅ Google Maps Ingestion TEST: PASSED (Raw JSON captured)")

    @patch("fetch_hanoi_weather.Minio")
    @patch("fetch_hanoi_weather.httpx.get")
    def test_weather_ingestion(self, mock_httpx_get, mock_minio_class):
        # Mock Weather Response
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"main": {"temp": 25}, "weather": []}
        mock_resp.raise_for_status = MagicMock()
        mock_httpx_get.return_value = mock_resp
        
        mock_mc = mock_minio_class.return_value
        mock_mc.bucket_exists.return_value = True
        
        fetch_hanoi_weather.run_weather_ingestion()
        self.assertTrue(mock_mc.put_object.called)
        print("✅ Weather Ingestion TEST: PASSED (Raw JSON captured)")

if __name__ == "__main__":
    unittest.main()
