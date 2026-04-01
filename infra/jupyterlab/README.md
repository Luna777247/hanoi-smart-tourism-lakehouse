# 🧩 JupyterLab

Why: Interactive notebooks for exploration with Spark, Trino, and Python libs.

## ⚙️ Profile

- `explore`

## 🔗 Dependencies

- Optional: core services for connections (Trino, Spark, MinIO, Postgres, ClickHouse)

## 🚀 How

- Start service:
  - `docker compose --profile explore up -d jupyterlab`

- UI: `http://localhost:8888`

## 📝 Notes

- Notebooks mounted from `./pipelines/notebooks` (repo root mount trong compose).
- Environment exposes `SPARK_MASTER_URL`, `TRINO_URL`, and service URLs.

