#!/bin/bash
# File: scripts/setup_vault.sh
set -e
 
export VAULT_ADDR='http://localhost:8200'
export VAULT_TOKEN=${VAULT_DEV_ROOT_TOKEN_ID:-root}
 
echo '==> Kich hoat KV Secrets Engine v2'
vault secrets enable -path=secret kv-v2 2>/dev/null || true
 
echo '==> Nap secrets ban dau'
vault kv put secret/tourism/apis \
  google_places_key="${GOOGLE_PLACES_API_KEY}" \
  tripadvisor_key="${TRIPADVISOR_API_KEY}"
 
vault kv put secret/db/postgres \
  host=postgres port=5432 \
  user="${POSTGRES_USER}" password="${POSTGRES_PASSWORD}"
 
vault kv put secret/storage/minio \
  endpoint=http://minio:9000 \
  access_key="${MINIO_ROOT_USER}" \
  secret_key="${MINIO_ROOT_PASSWORD}"
 
echo '==> Tao Vault Policy cho Airflow'
vault policy write airflow-policy - <<EOF
path "secret/data/tourism/*" { capabilities = ["read"] }
path "secret/data/db/*"      { capabilities = ["read"] }
path "secret/data/storage/*" { capabilities = ["read"] }
EOF
 
echo '==> Cau hinh AppRole cho Airflow'
vault auth enable approle 2>/dev/null || true
vault write auth/approle/role/airflow \
  token_policies="airflow-policy" \
  token_ttl=1h token_max_ttl=4h
 
echo '==> Vault setup complete!'

