#!/bin/bash

# Make script executable
chmod +x export.sh

echo "APHRC Publications Database Export Utility"
echo "-----------------------------------------"
echo "This script will export data from the Docker container to your remote PostgreSQL database."
echo

# Check if there's data to export
RECORD_COUNT=$(docker exec -it openalex_db psql -U user -d openalex -t -c "SELECT COUNT(*) FROM aphrc_works;")
RECORD_COUNT=$(echo $RECORD_COUNT | xargs)  # Trim whitespace

if [ "$RECORD_COUNT" -eq "0" ]; then
    echo "No records found to export. Please run the data import first."
    exit 1
fi

echo "Found $RECORD_COUNT records to export."
echo

# Get remote database details
read -p "Remote database name: " DB_NAME
read -p "Remote username: " DB_USER
read -s -p "Remote password: " DB_PASSWORD
echo
read -p "Remote host: " DB_HOST
read -p "Remote port [5432]: " DB_PORT
DB_PORT=${DB_PORT:-5432}

echo
echo "Exporting data to remote PostgreSQL database..."

# Copy the export script to the container
docker cp export_to_remote.py openalex_python:/app/

# Run the export script with the provided parameters
docker exec -it openalex_python python export_to_remote.py "$DB_NAME" "$DB_USER" "$DB_PASSWORD" "$DB_HOST" "$DB_PORT"

echo
echo "Export process completed."