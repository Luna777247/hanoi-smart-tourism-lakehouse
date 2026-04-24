# File: scripts/seed_vault.ps1
# Script to seed secrets from .env to HashiCorp Vault

Write-Host "==> Starting Centralized Configuration Sync to Vault..." -ForegroundColor Cyan

$VAULT_CONTAINER = "lakehouse-vault"
$VAULT_TOKEN = "root"

# Function to set secret in Vault
function Set-VaultSecret {
    param($Path, $Key, $Value)
    Write-Host "Setting $Key in $Path..." -ForegroundColor Gray
    docker exec -e VAULT_ADDR='http://127.0.0.1:8200' -e VAULT_TOKEN='root' $VAULT_CONTAINER vault kv put "secret/$Path" "$Key=$Value"
}

# 1. Enable KV engine (if not enabled)
Write-Host "Enabling KV secret engine..." -ForegroundColor Gray
docker exec -e VAULT_ADDR='http://127.0.0.1:8200' -e VAULT_TOKEN='root' $VAULT_CONTAINER vault secrets enable -path=secret kv 2>$null

# 2. Sync Database Credentials
Set-VaultSecret -Path "database" -Key "username" -Value $env:POSTGRES_USER
Set-VaultSecret -Path "database" -Key "password" -Value $env:POSTGRES_PASSWORD

# 3. Sync AI/Data API Keys
Set-VaultSecret -Path "api-keys" -Key "google_places" -Value $env:GOOGLE_PLACES_API_KEY
Set-VaultSecret -Path "api-keys" -Key "serpapi" -Value $env:SERPAPI_KEY

# 4. Sync RapidAPI Keys (multi-source)
Set-VaultSecret -Path "rapidapi" -Key "key1" -Value $env:RAPID_API_KEY1
Set-VaultSecret -Path "rapidapi" -Key "key2" -Value $env:RAPID_API_KEY2

Write-Host "==> Centralized Configuration Sync COMPLETE!" -ForegroundColor Green
Write-Host "Secrets are now managed by Vault at http://localhost:8200" -ForegroundColor Green
