#!/bin/bash
# File: scripts/stop_docker_services.sh
# Script để dừng các services (giữ data trong volumes)

echo "🛑 Stopping Docker services..."

# Dừng tất cả services nhưng giữ volumes (data không bị mất)
docker compose --profile "*" down

echo "✅ Services stopped. Data preserved in Docker volumes."
echo "💡 To start again: ./scripts/start_docker_services.sh"