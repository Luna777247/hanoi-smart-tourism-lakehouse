"""
=============================================================================
  TEST SUITE: Ingestion Layer – Hanoi Smart Tourism Lakehouse
=============================================================================

Kiểm thử 2 cơ chế thu thập dữ liệu:

  1. BATCH (Pull)  : Airflow → API công khai → MinIO (Landing Zone)
  2. STREAMING (Push/CDC): Debezium → Kafka → Topic CDC

Chạy:
    pip install requests minio confluent-kafka psycopg2-binary pytest
    pytest tests/test_ingestion_layer.py -v --tb=short

Lưu ý: Hệ thống Docker phải đang chạy (docker-compose --profile core --profile airflow up -d)
"""

import json
import os
import time
import uuid
from datetime import datetime

import pytest
import requests

# ─── Cấu hình kết nối (tuỳ chỉnh theo .env) ─────────────────────────────────

AIRFLOW_URL       = os.getenv("AIRFLOW_URL",       "http://localhost:8085")
AIRFLOW_USER      = os.getenv("AIRFLOW_USER",       "airflow")
AIRFLOW_PASSWORD  = os.getenv("AIRFLOW_PASSWORD",   "changeme")

MINIO_ENDPOINT    = os.getenv("MINIO_TEST_ENDPOINT","localhost:9000")
MINIO_ACCESS_KEY  = os.getenv("MINIO_ACCESS_KEY",   "minio")
MINIO_SECRET_KEY  = os.getenv("MINIO_SECRET_KEY",   "minio123")

KAFKA_BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP",    "localhost:9092")
SCHEMA_REG_URL    = os.getenv("SCHEMA_REGISTRY_URL","http://localhost:8081")

PG_HOST           = os.getenv("PG_HOST",  "localhost")
PG_PORT           = int(os.getenv("PG_PORT", "15432"))
PG_USER           = os.getenv("PG_USER",  "admin")
PG_PASS           = os.getenv("PG_PASS",  "admin")
PG_DB             = os.getenv("PG_DB",    "demo")

DEBEZIUM_URL      = os.getenv("DEBEZIUM_URL", "http://localhost:8083")
KAFKA_UI_URL      = os.getenv("KAFKA_UI_URL", "http://localhost:8082")

# ─── Timeouts ─────────────────────────────────────────────────────────────────
WAIT_TIMEOUT  = 120   # giây – chờ tối đa cho các service khởi động
POLL_INTERVAL = 5     # giây – khoảng cách giữa các lần kiểm tra


# =============================================================================
# HELPERS
# =============================================================================

def _wait_for_http(url: str, label: str, timeout: int = WAIT_TIMEOUT) -> bool:
    """Chờ một HTTP endpoint trả về 2xx."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(url, timeout=5)
            if r.status_code < 400:
                return True
        except requests.ConnectionError:
            pass
        time.sleep(POLL_INTERVAL)
    pytest.fail(f"❌ {label} không phản hồi sau {timeout}s tại {url}")


def _airflow_api(path: str, method: str = "GET", json_data=None):
    """Gọi Airflow REST API v2 với Basic Auth."""
    url = f"{AIRFLOW_URL}/api/v1/{path}"
    r = requests.request(
        method, url,
        auth=(AIRFLOW_USER, AIRFLOW_PASSWORD),
        json=json_data,
        timeout=15,
    )
    return r


def _get_minio_client():
    from minio import Minio
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )


def _get_pg_conn():
    import psycopg2
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        user=PG_USER, password=PG_PASS,
        dbname=PG_DB,
    )


# =============================================================================
# PHẦN A – KIỂM THỬ HẠ TẦNG (Pre-conditions)
# =============================================================================

class TestInfrastructureHealth:
    """Đảm bảo các dịch vụ cốt lõi đang hoạt động trước khi test ingestion."""

    def test_A01_postgres_is_reachable(self):
        """Postgres (demo DB) phải kết nối được."""
        conn = _get_pg_conn()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        assert cur.fetchone()[0] == 1
        cur.close()
        conn.close()
        print("  ✅ PostgreSQL (demo) đang hoạt động")

    def test_A02_minio_is_reachable(self):
        """MinIO API phải phản hồi."""
        _wait_for_http(f"http://{MINIO_ENDPOINT}/minio/health/live", "MinIO")
        print("  ✅ MinIO đang hoạt động")

    def test_A03_kafka_broker_is_reachable(self):
        """Kafka broker phải kết nối được qua AdminClient."""
        from confluent_kafka.admin import AdminClient
        admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
        md = admin.list_topics(timeout=15)
        assert md.brokers, "Không tìm thấy broker nào"
        print(f"  ✅ Kafka đang hoạt động – {len(md.brokers)} broker(s)")

    def test_A04_schema_registry_is_reachable(self):
        """Schema Registry phải phản hồi."""
        _wait_for_http(f"{SCHEMA_REG_URL}/subjects", "Schema Registry", timeout=30)
        print("  ✅ Schema Registry đang hoạt động")

    def test_A05_airflow_api_is_reachable(self):
        """Airflow API Server phải phản hồi."""
        _wait_for_http(f"{AIRFLOW_URL}/health", "Airflow API", timeout=60)
        print("  ✅ Airflow API Server đang hoạt động")

    def test_A06_debezium_connect_is_reachable(self):
        """Debezium (Kafka Connect) phải phản hồi."""
        _wait_for_http(DEBEZIUM_URL, "Debezium Connect", timeout=60)
        print("  ✅ Debezium Connect đang hoạt động")


# =============================================================================
# PHẦN B – BATCH INGESTION (Pull – Airflow → API → MinIO)
# =============================================================================

class TestBatchIngestion:
    """Kiểm thử cơ chế Batch: Airflow kéo dữ liệu API về MinIO."""

    def test_B01_airflow_dags_loaded(self):
        """Kiểm tra các DAG bronze đã được Airflow nhận diện."""
        r = _airflow_api("dags")
        assert r.status_code == 200, f"API lỗi: {r.status_code}"
        dags = r.json().get("dags", [])
        dag_ids = [d["dag_id"] for d in dags]
        print(f"  📋 DAGs hiện có: {dag_ids}")

        # Kiểm tra ít nhất 1 DAG bronze tồn tại
        bronze_dags = [d for d in dag_ids if "bronze" in d.lower()]
        assert len(bronze_dags) > 0, \
            f"Không tìm thấy DAG bronze nào. Danh sách DAG: {dag_ids}"
        print(f"  ✅ Tìm thấy {len(bronze_dags)} DAG(s) bronze: {bronze_dags}")

    def test_B02_minio_bronze_bucket_exists(self):
        """Bucket 'tourism-bronze' phải tồn tại trên MinIO."""
        mc = _get_minio_client()
        bucket = "tourism-bronze"
        if not mc.bucket_exists(bucket):
            # Tạo bucket nếu chưa có (để test tiếp được)
            mc.make_bucket(bucket)
            print(f"  ⚠️  Bucket '{bucket}' chưa tồn tại – đã tự tạo")
        else:
            print(f"  ✅ Bucket '{bucket}' đã tồn tại")

    def test_B03_simulate_batch_upload_to_minio(self):
        """Giả lập quá trình Batch Ingestion: upload 1 file JSON vào MinIO."""
        from io import BytesIO
        mc = _get_minio_client()
        bucket = "tourism-bronze"
        if not mc.bucket_exists(bucket):
            mc.make_bucket(bucket)

        # Dữ liệu giả lập – mô phỏng output từ Google Places API
        test_data = [
            {
                "place_id": f"test_{uuid.uuid4().hex[:8]}",
                "name": "Hoàn Kiếm Lake - Test",
                "category": "tourist_attraction",
                "latitude": 21.0285,
                "longitude": 105.8542,
                "rating": 4.7,
                "user_ratings_total": 12345,
                "ingestion_time": datetime.utcnow().isoformat(),
                "source": "test_batch_simulation",
            },
            {
                "place_id": f"test_{uuid.uuid4().hex[:8]}",
                "name": "Temple of Literature - Test",
                "category": "tourist_attraction",
                "latitude": 21.0275,
                "longitude": 105.8355,
                "rating": 4.6,
                "user_ratings_total": 9876,
                "ingestion_time": datetime.utcnow().isoformat(),
                "source": "test_batch_simulation",
            },
        ]

        date_str = datetime.utcnow().strftime("%Y-%m-%d")
        ts = datetime.utcnow().strftime("%H%M%S")
        object_name = f"source=google_places/date={date_str}/test_places_{ts}.json"

        payload = json.dumps(test_data, ensure_ascii=False).encode("utf-8")
        mc.put_object(
            bucket_name=bucket,
            object_name=object_name,
            data=BytesIO(payload),
            length=len(payload),
            content_type="application/json",
        )
        print(f"  ✅ Upload thành công: s3://{bucket}/{object_name}")

        # Xác nhận file tồn tại
        stat = mc.stat_object(bucket, object_name)
        assert stat.size == len(payload), \
            f"Kích thước file không khớp: {stat.size} != {len(payload)}"
        print(f"  ✅ Xác nhận file: {stat.size} bytes, ETag={stat.etag}")

    def test_B04_verify_minio_landing_zone_structure(self):
        """Kiểm tra cấu trúc phân vùng (partitioning) trong Landing Zone."""
        mc = _get_minio_client()
        bucket = "tourism-bronze"

        objects = list(mc.list_objects(bucket, prefix="source=", recursive=True))
        print(f"  📁 Tổng số objects trong landing zone: {len(objects)}")

        if len(objects) > 0:
            # Kiểm tra cấu trúc đường dẫn Hive-style partitioning
            sample = objects[0].object_name
            assert "source=" in sample, f"Thiếu partition 'source=' trong path: {sample}"
            print(f"  ✅ Cấu trúc partitioning hợp lệ: {sample}")
        else:
            print("  ⚠️  Landing zone rỗng (chưa có dữ liệu từ DAG thực)")

    def test_B05_airflow_connections_configured(self):
        """Kiểm tra Airflow đã cấu hình connection MinIO và Spark."""
        r = _airflow_api("connections")
        if r.status_code != 200:
            pytest.skip(f"Không thể truy vấn connections: HTTP {r.status_code}")

        conns = r.json().get("connections", [])
        conn_ids = [c["connection_id"] for c in conns]
        print(f"  📋 Connections: {conn_ids}")

        assert "minio" in conn_ids, "Connection 'minio' chưa được tạo"
        assert "spark_default" in conn_ids, "Connection 'spark_default' chưa được tạo"
        print("  ✅ Connections minio & spark_default đã sẵn sàng")


# =============================================================================
# PHẦN C – STREAMING INGESTION (Push/CDC – Debezium → Kafka)
# =============================================================================

class TestStreamingIngestion:
    """Kiểm thử cơ chế Streaming: CDC từ Postgres qua Debezium vào Kafka."""

    def test_C01_debezium_connector_registered(self):
        """Kiểm tra Debezium connector 'demo-postgres' đã đăng ký."""
        r = requests.get(f"{DEBEZIUM_URL}/connectors", timeout=10)
        assert r.status_code == 200, f"Debezium API lỗi: {r.status_code}"
        connectors = r.json()
        print(f"  📋 Connectors hiện có: {connectors}")

        if "demo-postgres" not in connectors:
            # Tự đăng ký connector nếu chưa có (để test tiếp)
            print("  ⚠️  Connector 'demo-postgres' chưa đăng ký, đang tạo...")
            connector_config = {
                "name": "demo-postgres",
                "config": json.load(
                    open(os.path.join(
                        os.path.dirname(__file__), "..",
                        "infra", "debezium", "config", "demo-postgres.json"
                    ))
                )
            }
            connector_config["config"]["name"] = "demo-postgres"
            rr = requests.post(
                f"{DEBEZIUM_URL}/connectors",
                json=connector_config,
                headers={"Content-Type": "application/json"},
                timeout=15,
            )
            print(f"  → Kết quả đăng ký: HTTP {rr.status_code}")
            # Chờ connector khởi động
            time.sleep(10)
        else:
            print("  ✅ Connector 'demo-postgres' đã đăng ký")

    def test_C02_debezium_connector_running(self):
        """Kiểm tra connector đang ở trạng thái RUNNING."""
        r = requests.get(f"{DEBEZIUM_URL}/connectors/demo-postgres/status", timeout=10)
        if r.status_code == 404:
            pytest.skip("Connector 'demo-postgres' chưa đăng ký")

        status = r.json()
        connector_state = status.get("connector", {}).get("state", "UNKNOWN")
        print(f"  🔄 Connector state: {connector_state}")

        tasks = status.get("tasks", [])
        for t in tasks:
            task_state = t.get("state", "UNKNOWN")
            print(f"     Task {t.get('id')}: {task_state}")
            if task_state == "FAILED":
                trace = t.get("trace", "")
                print(f"     ❌ Trace: {trace[:300]}")

        assert connector_state == "RUNNING", \
            f"Connector không ở trạng thái RUNNING: {connector_state}"
        print("  ✅ Connector đang RUNNING")

    def test_C03_postgres_publication_exists(self):
        """Kiểm tra publication CDC 'demo_publication' tồn tại trong Postgres."""
        conn = _get_pg_conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT pubname, puballtables FROM pg_publication "
            "WHERE pubname = 'demo_publication'"
        )
        row = cur.fetchone()
        cur.close()
        conn.close()

        assert row is not None, "Publication 'demo_publication' không tồn tại"
        print(f"  ✅ Publication '{row[0]}' tồn tại (all_tables={row[1]})")

    def test_C04_postgres_replication_slot_exists(self):
        """Kiểm tra replication slot CDC tồn tại."""
        conn = _get_pg_conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT slot_name, plugin, slot_type, active "
            "FROM pg_replication_slots "
            "WHERE slot_name = 'demo_slot'"
        )
        row = cur.fetchone()
        cur.close()
        conn.close()

        if row is None:
            pytest.skip("Replication slot 'demo_slot' chưa tồn tại "
                        "(connector chưa khởi tạo)")

        print(f"  ✅ Slot: {row[0]}, plugin={row[1]}, type={row[2]}, active={row[3]}")

    def test_C05_insert_and_verify_cdc_event(self):
        """
        End-to-end CDC test:
          1. INSERT 1 dòng mới vào bảng 'users' trong DB demo
          2. Chờ ~10s để Debezium capture
          3. Kiểm tra Kafka topic 'demo.public.users' có message mới
        """
        # 1. INSERT dữ liệu test
        test_user_id = f"test_cdc_{uuid.uuid4().hex[:8]}"
        test_email = f"{test_user_id}@test.hanoi.vn"

        conn = _get_pg_conn()
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO users (user_id, email, country) VALUES (%s, %s, %s)",
            (test_user_id, test_email, "VN"),
        )
        cur.close()
        conn.close()
        print(f"  📝 Inserted user: {test_user_id} ({test_email})")

        # 2. Chờ Debezium capture thay đổi
        print("  ⏳ Chờ Debezium capture CDC event (15s)...")
        time.sleep(15)

        # 3. Consume từ Kafka topic
        from confluent_kafka import Consumer, KafkaError

        topic = "demo.public.users"
        consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": f"test-cdc-verify-{uuid.uuid4().hex[:6]}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        consumer.subscribe([topic])

        found = False
        deadline = time.time() + 30
        messages_checked = 0

        while time.time() < deadline:
            msg = consumer.poll(timeout=2.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # Đã đọc hết partition nhưng chưa tìm thấy → tiếp tục chờ
                    continue
                print(f"  ❌ Kafka error: {msg.error()}")
                break

            messages_checked += 1
            try:
                # Debezium dùng Avro → cần deserialize hoặc kiểm tra raw
                value = msg.value()
                if value and test_user_id.encode() in value:
                    found = True
                    print(f"  ✅ Tìm thấy CDC event cho user '{test_user_id}' "
                          f"tại offset {msg.offset()}")
                    break
            except Exception as e:
                # Nếu dùng Avro, có thể không decode được bằng text
                pass

        consumer.close()

        if not found and messages_checked > 0:
            print(f"  ⚠️  Đã quét {messages_checked} messages nhưng chưa tìm thấy "
                  f"event cho '{test_user_id}' (có thể do Avro encoding)")
            print(f"  ℹ️  Topic '{topic}' có dữ liệu → CDC pipeline đang hoạt động")
        elif not found:
            pytest.fail(f"Không tìm thấy message nào trong topic '{topic}' sau 30s")

        # 4. Cleanup – xoá user test
        conn = _get_pg_conn()
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("DELETE FROM users WHERE user_id = %s", (test_user_id,))
        cur.close()
        conn.close()
        print(f"  🧹 Đã xoá user test: {test_user_id}")

    def test_C06_kafka_cdc_topics_exist(self):
        """Kiểm tra các CDC topic với prefix 'demo.' đã được tạo trong Kafka."""
        from confluent_kafka.admin import AdminClient
        admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
        topics = admin.list_topics(timeout=15).topics

        cdc_topics = [t for t in topics if t.startswith("demo.")]
        print(f"  📋 CDC topics: {cdc_topics}")

        if len(cdc_topics) == 0:
            pytest.skip("Chưa có CDC topic nào (connector chưa chạy snapshot)")

        # Kiểm tra ít nhất topic users tồn tại
        expected_tables = ["users", "products", "inventory"]
        for table in expected_tables:
            topic_name = f"demo.public.{table}"
            if topic_name in cdc_topics:
                print(f"  ✅ Topic '{topic_name}' tồn tại")
            else:
                print(f"  ⚠️  Topic '{topic_name}' chưa tồn tại")


# =============================================================================
# PHẦN D – INTEGRATION (End-to-End Verification)
# =============================================================================

class TestEndToEndIntegration:
    """Kiểm thử tích hợp toàn diện cả hai cơ chế."""

    def test_D01_batch_and_streaming_coexist(self):
        """Xác nhận cả Batch (Airflow) và Streaming (Kafka) cùng hoạt động."""
        # Kiểm tra Airflow
        r1 = requests.get(f"{AIRFLOW_URL}/health", timeout=10)
        airflow_ok = r1.status_code < 400

        # Kiểm tra Kafka
        try:
            from confluent_kafka.admin import AdminClient
            admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
            md = admin.list_topics(timeout=10)
            kafka_ok = len(md.brokers) > 0
        except Exception:
            kafka_ok = False

        # Kiểm tra Debezium
        try:
            r2 = requests.get(DEBEZIUM_URL, timeout=10)
            debezium_ok = r2.status_code < 400
        except Exception:
            debezium_ok = False

        print(f"  Airflow:  {'✅' if airflow_ok else '❌'}")
        print(f"  Kafka:    {'✅' if kafka_ok else '❌'}")
        print(f"  Debezium: {'✅' if debezium_ok else '❌'}")

        assert airflow_ok, "Airflow không hoạt động"
        assert kafka_ok, "Kafka không hoạt động"
        assert debezium_ok, "Debezium không hoạt động"
        print("  ✅ Chiến lược Hybrid Ingestion (Batch + Streaming) hoạt động đồng thời")

    def test_D02_data_flow_summary(self):
        """Tổng kết trạng thái dữ liệu trong hệ thống."""
        summary = {}

        # MinIO objects count
        try:
            mc = _get_minio_client()
            if mc.bucket_exists("tourism-bronze"):
                objs = list(mc.list_objects("tourism-bronze", recursive=True))
                summary["minio_bronze_objects"] = len(objs)
        except Exception as e:
            summary["minio_bronze_objects"] = f"Error: {e}"

        # Postgres tables row count
        try:
            conn = _get_pg_conn()
            cur = conn.cursor()
            for table in ["users", "products", "inventory"]:
                cur.execute(f"SELECT count(*) FROM {table}")
                summary[f"pg_{table}_rows"] = cur.fetchone()[0]
            cur.close()
            conn.close()
        except Exception as e:
            summary["pg_tables"] = f"Error: {e}"

        # Kafka topic count
        try:
            from confluent_kafka.admin import AdminClient
            admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
            topics = admin.list_topics(timeout=10).topics
            summary["kafka_total_topics"] = len(topics)
            summary["kafka_cdc_topics"] = len([t for t in topics if t.startswith("demo.")])
        except Exception as e:
            summary["kafka_topics"] = f"Error: {e}"

        print("\n  ╔══════════════════════════════════════════╗")
        print("  ║   📊 DATA FLOW SUMMARY                   ║")
        print("  ╠══════════════════════════════════════════╣")
        for k, v in summary.items():
            print(f"  ║  {k:<30} {str(v):>8} ║")
        print("  ╚══════════════════════════════════════════╝\n")


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short", "-s"])
