import os
import psycopg2
import sys

# Local DB connection (Docker container)
LOCAL_DB = {
    "dbname": os.getenv("DB_NAME", "openalex"),
    "user": os.getenv("DB_USER", "user"),
    "password": os.getenv("DB_PASSWORD", "password"),
    "host": os.getenv("DB_HOST", "db"),
    "port": "5432"
}

# Get remote connection details from arguments or prompt
def get_remote_db_config():
    if len(sys.argv) >= 6:
        return {
            "dbname": sys.argv[1],
            "user": sys.argv[2],
            "password": sys.argv[3],
            "host": sys.argv[4],
            "port": sys.argv[5]
        }
    else:
        print("Please enter your remote PostgreSQL database details:")
        return {
            "dbname": input("Database name: "),
            "user": input("Username: "),
            "password": input("Password: "),
            "host": input("Host: "),
            "port": input("Port (default 5432): ") or "5432"
        }

def export_data_to_remote():
    # Get remote database configuration
    remote_db = get_remote_db_config()
    
    try:
        # Connect to local database
        print("Connecting to local database...")
        local_conn = psycopg2.connect(**LOCAL_DB)
        local_cur = local_conn.cursor()
        
        # Get the data from local database
        local_cur.execute("SELECT * FROM aphrc_works")
        rows = local_cur.fetchall()
        
        # Get column names
        local_cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'aphrc_works' ORDER BY ordinal_position")
        columns = [col[0] for col in local_cur.fetchall()]
        
        if not rows:
            print("No data found in local database.")
            return
            
        print(f"Found {len(rows)} records in local database.")
        
        # Connect to remote database
        print(f"Connecting to remote database at {remote_db['host']}...")
        remote_conn = psycopg2.connect(**remote_db)
        remote_cur = remote_conn.cursor()
        
        # Create table in remote database if it doesn't exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS aphrc_works (
            id TEXT PRIMARY KEY,
            title TEXT,
            authors TEXT,
            publication_year INTEGER,
            publication_date DATE,
            doi TEXT,
            open_access BOOLEAN,
            journal_name TEXT,
            volume TEXT,
            issue TEXT,
            cited_by_count INTEGER,
            type TEXT,
            abstract TEXT
        )
        """
        remote_cur.execute(create_table_query)
        remote_conn.commit()
        
        # Insert data into remote database
        print("Transferring data to remote database...")
        
        # Build placeholders for the INSERT statement
        placeholders = ', '.join(['%s'] * len(columns))
        columns_str = ', '.join(columns)
        
        # Use batch inserts for better performance
        batch_size = 100
        total_inserted = 0
        
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i+batch_size]
            remote_cur.executemany(
                f"INSERT INTO aphrc_works ({columns_str}) VALUES ({placeholders}) ON CONFLICT (id) DO NOTHING",
                batch
            )
            remote_conn.commit()
            total_inserted += len(batch)
            print(f"Progress: {total_inserted}/{len(rows)} records transferred")
        
        print(f"Successfully transferred {total_inserted} records to remote database.")
        
        # Close connections
        local_cur.close()
        local_conn.close()
        remote_cur.close()
        remote_conn.close()
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    export_data_to_remote()