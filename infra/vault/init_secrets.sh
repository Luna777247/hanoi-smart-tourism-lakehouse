docker exec -e VAULT_ADDR='http://127.0.0.1:8200' -e VAULT_TOKEN='root' lakehouse-vault vault kv put secret/tourism/apis google_places_key="${GOOGLE_PLACES_KEY}" tripadvisor_key="${TRIPADVISOR_KEY}"
docker exec -e VAULT_ADDR='http://127.0.0.1:8200' -e VAULT_TOKEN='root' lakehouse-vault vault kv put secret/storage/minio access_key="minio_admin" secret_key="ChangeMe_Minio123!"
