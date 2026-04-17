#!/bin/bash
set -e

echo "Installing prometheus-client..."
pip install prometheus-client

echo "Starting metrics exporter..."
python3 /opt/airflow/metrics_exporter.py