#!/bin/bash
# =============================================================================
# vault-entrypoint.sh — Fetch Airflow secrets from Vault then exec the real
# process. Used as the ENTRYPOINT wrapper for all Airflow services.
#
# Pattern (exec form in docker-compose):
#   Normal services:  ["bash", "/vault-entrypoint.sh", "/usr/bin/dumb-init", "--", "/entrypoint"]
#   airflow-init:     ["bash", "/vault-entrypoint.sh", "/bin/bash"]
#   In both cases CMD args are appended and exec "$@" hands off cleanly.
#
# Fallback: if VAULT_ADDR / VAULT_TOKEN are unset or Vault is unreachable,
# every variable keeps its docker-compose .env value (safe for local dev).
# =============================================================================

set -e

# ---------------------------------------------------------------------------
# vault_fetch <kv_path> <field> <default>
# ---------------------------------------------------------------------------
vault_fetch() {
    local path="$1" field="$2" default="$3"
    if [ -z "${VAULT_ADDR:-}" ] || [ -z "${VAULT_TOKEN:-}" ]; then
        printf '%s' "$default"; return
    fi
    local raw val
    raw=$(curl -sf --connect-timeout 3 \
        -H "X-Vault-Token: ${VAULT_TOKEN}" \
        "${VAULT_ADDR}/v1/secret/data/${path}" 2>/dev/null) || true
    val=$(printf '%s' "${raw}" \
        | grep -o "\"${field}\":\"[^\"]*\"" \
        | head -1 \
        | sed 's/^"[^"]*":"\(.*\)"$/\1/')
    printf '%s' "${val:-$default}"
}

# ---------------------------------------------------------------------------
# MinIO credentials (also used by Spark/Airflow S3 connections)
# ---------------------------------------------------------------------------
MINIO_ROOT_USER=$(vault_fetch "platform/minio" "root_user" "${MINIO_ROOT_USER:-minio}")
export MINIO_ROOT_USER
MINIO_ROOT_PASSWORD=$(vault_fetch "platform/minio" "root_password" "${MINIO_ROOT_PASSWORD:-minio123}")
export MINIO_ROOT_PASSWORD

# MinIO aliases used by Airflow DAGs and Spark
export S3_ACCESS_KEY="${MINIO_ROOT_USER}"
export S3_SECRET_KEY="${MINIO_ROOT_PASSWORD}"
export AWS_ACCESS_KEY_ID="${MINIO_ROOT_USER}"
export AWS_SECRET_ACCESS_KEY="${MINIO_ROOT_PASSWORD}"

# ---------------------------------------------------------------------------
# Airflow core secrets
# ---------------------------------------------------------------------------
AIRFLOW_ADMIN_PASSWORD=$(vault_fetch "platform/airflow" "admin_password" "${AIRFLOW_ADMIN_PASSWORD:-airflow}")
export AIRFLOW_ADMIN_PASSWORD

AIRFLOW__CORE__FERNET_KEY=$(vault_fetch "platform/airflow" "fernet_key" "${AIRFLOW__CORE__FERNET_KEY:-}")
export AIRFLOW__CORE__FERNET_KEY

AIRFLOW__WEBSERVER__SECRET_KEY=$(vault_fetch "platform/airflow" "webserver_secret_key" "${AIRFLOW__WEBSERVER__SECRET_KEY:-}")
export AIRFLOW__WEBSERVER__SECRET_KEY

_JWT=$(vault_fetch "platform/airflow" "jwt_secret_key" "${AIRFLOW__API_AUTH__JWT_SECRET_KEY:-}")
export AIRFLOW__API_AUTH__JWT_SECRET_KEY="${_JWT}"
export AIRFLOW__API_AUTH__JWT_SECRET="${_JWT}"

AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=$(vault_fetch "platform/airflow" "db_conn_dsn" "${AIRFLOW__DATABASE__SQL_ALCHEMY_CONN:-}")
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN

AIRFLOW__CELERY__RESULT_BACKEND=$(vault_fetch "platform/airflow" "celery_result_backend" "${AIRFLOW__CELERY__RESULT_BACKEND:-}")
export AIRFLOW__CELERY__RESULT_BACKEND

# ---------------------------------------------------------------------------
# Oracle credentials (used by airflow-init to register the oracle_default conn)
# ---------------------------------------------------------------------------
ORACLE_USER=$(vault_fetch "platform/oracle" "user" "${ORACLE_USER:-}")
export ORACLE_USER
ORACLE_PASSWORD=$(vault_fetch "platform/oracle" "password" "${ORACLE_PASSWORD:-}")
export ORACLE_PASSWORD

# ---------------------------------------------------------------------------
# Hand off to the real process (dumb-init + /entrypoint, or /bin/bash, etc.)
# ---------------------------------------------------------------------------
exec "$@"
