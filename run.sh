#!/bin/bash

# Make script executable
chmod +x run_snapshot_extract.sh

echo "APHRC Data Extraction from OpenAlex Snapshot"
echo "-------------------------------------------"

# Start Docker containers if not already running
if [ "$(docker ps -q -f name=openalex_db)" == "" ]; then
    echo "Starting Docker containers..."
    docker-compose up -d
    
    # Wait for PostgreSQL to be ready
    echo "Waiting for PostgreSQL to be ready..."
    sleep 10
fi

# Install required packages
echo "Installing required packages..."
docker exec -it openalex_python pip install requests pandas psycopg2-binary boto3 2>/dev/null

# Copy the extraction script to the container
echo "Copying extraction script to container..."
docker cp extract_aphrc_from_snapshot.py openalex_python:/app/

# Clear existing data if any exists
echo "Clearing any existing data..."
docker exec -it openalex_db psql -U user -d openalex -c "TRUNCATE aphrc_works;"

# Run the extraction script
echo "Running the extraction script..."
docker exec -it openalex_python python extract.py

# Run verification script
echo "Running verification on extracted data..."
docker exec -it openalex_python pip install tabulate >/dev/null 2>&1
docker exec -it openalex_python python verify_aphrc.py

echo "Process complete!"
echo "If you want to export this data to your remote database, run: ./export.sh"