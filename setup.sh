#!/bin/bash

if ! command -v python3 &>/dev/null; then
    echo "Python3 is not installed. Installing Python3..."
    sudo apt update && sudo apt install -y python3 python3-venv python3-pip
else
    echo "Python3 is already installed."
fi

echo "Setting up virtual environment..."
python3 -m venv venv
source venv/bin/activate

if [ -f requirements.txt ]; then
    echo "Installing dependencies from requirements.txt..."
    pip install -r requirements.txt
else
    echo "requirements.txt not found. Skipping dependency installation."
fi

cd ./setup-scripts

# Checking to see if env file exists
ENV_FILE=".env"

if [[ ! -f "$ENV_FILE" ]]; then
    echo "Error: .env file not found."
    exit 1
fi

required_params=("DB_HOST" "DB_PORT" "DB_USERNAME" "DB_PASSWORD" "KAFKA_CLUSTER_NAME" "NUM_ROWS_IN_DATASET")

missing_or_empty_params=()
for param in "${required_params[@]}"; do
    if ! grep -q "^$param=" "$ENV_FILE" || [[ -z $(grep "^$param=" "$ENV_FILE" | cut -d'=' -f2-) ]]; then
        missing_or_empty_params+=("$param")
    fi
done

if [[ ${#missing_or_empty_params[@]} -ne 0 ]]; then
    echo "Error: The following required parameters are missing in the .env file:"
    for param in "${missing_or_empty_params[@]}"; do
        echo "- $param"
    done
    exit 1
else
    echo "All required parameters are present in the .env file."
fi

echo "Generating the tables..."
python3 generate-tables.py
echo "Generating materialized views"
python3 generate-materialized-views.py
echo "Loading the metadata..."
python3 metadata-clickhouse-loader.py
echo "Generating historical data..."
python3 generate-historical-data.py

cd ..

