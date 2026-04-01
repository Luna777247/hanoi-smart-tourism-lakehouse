# Lakehouse OSS System Access Report (Updated)

This report summarizes the verified access points, ports, and credentials for various components within the Lakehouse OSS platform.

> [!IMPORTANT]
> These credentials have been verified against the active `.env` and `docker-compose.yml` files.

## 🗄️ Database Systems

| Service | Port | Default Username | Default Password | Notes |
| :--- | :--- | :--- | :--- | :--- |
| **PostgreSQL** | `15432` | `admin` | `admin` | Main metastore database |
| **PostgreSQL (CDC)** | `15432` | `cdc_reader` | `cdc_reader_pwd` | Dedicated replication user |
| **ClickHouse** | `8123` | `admin` | `admin` | Analytics database (HTTP) |
| **ClickHouse (Native)**| `9002` | `admin` | `admin` | Native protocol access |
| **Hive Metastore** | `9083` | `admin` | `admin` | Metadata service (Thrift) |
| **Oracle (External)** | `1521` | `PG_T24CORE` | `PG_T24CORE` | Host: `192.168.26.180`, SID: `dbpdb` |
| **Redis** | `6379` | *None* | *None* | Result backend/cache |

---

## 🔐 Identity & Access Management

| Service | Port | Default Username | Default Password | Notes |
| :--- | :--- | :--- | :--- | :--- |
| **Keycloak** | `8084` | `admin` | `admin123` | Identity and Access Management |
| **HashiCorp Vault** | `8200` | *Token* | `dev-root-token` | Secrets management |
| **Apache Ranger** | `6080` | `rangerAdmin` | `rangeradmin` | Security policy manager |
| **Ranger Users** | `6080` | `rangerUsersync` | `Rangeradmin1` | default in properties |
| **Ranger Tags** | `6080` | `rangerTagsync` | `Rangeradmin1` | default in properties |

---

## ⚙️ Data Orchestration & Processing

| Service | Port | Default Username | Default Password | Notes |
| :--- | :--- | :--- | :--- | :--- |
| **Apache Airflow** | `8085` | `airflow` | `airflow` | Workflow orchestration |
| **MinIO Console** | `9001` | `minio` | `minio123` | Object storage Web UI |
| **MinIO API** | `9000` | `minio` | `minio123` | S3-compatible API |
| **Apache NiFi** | `8443` | `admin` | `adminadminadmin` | Data flow (HTTPS) |
| **Spark Master UI** | `8088` | *None* | *None* | Spark cluster management |
| **JupyterLab** | `8888` | *None* | *None* | Interactive notebooks |

---

## 📊 Monitoring & Visualization

| Service | Port | Default Username | Default Password | Notes |
| :--- | : :--- | :--- | :--- | :--- |
| **Apache Superset** | `8089` | `admin` | `admin` | BI Visualization |
| **Grafana** | `3000` | `admin` | `admin` | Dashboards |
| **Prometheus** | `9090` | `admin` | `admin123` | Metrics collection |
| **Kafka UI** | `8082` | *None* | *None* | Kafka management UI |

---

## 🧊 Other Components

- **Kafka**: `9092` (PLAINTEXT, no auth by default)
- **Schema Registry**: `8081`
- **Flink JobManager**: `8095`
- **Elasticsearch**: `9200` (Security disabled: `xpack.security.enabled: "false"`)
- **Debezium**: `8083` (Connector management)
