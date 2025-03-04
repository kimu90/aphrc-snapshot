import os
import psycopg2
import json
from tabulate import tabulate

# Database connection settings from environment variables
DB_PARAMS = {
    "dbname": os.getenv("DB_NAME", "openalex"),
    "user": os.getenv("DB_USER", "user"),
    "password": os.getenv("DB_PASSWORD", "password"),
    "host": os.getenv("DB_HOST", "db"),
    "port": "5432"
}

def run_verification():
    """
    Run verification checks on the APHRC publications data
    """
    print("\n📊 APHRC PUBLICATIONS VERIFICATION REPORT 📊")
    print("==========================================\n")
    
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()
        
        # 1. Count total publications
        cur.execute("SELECT COUNT(*) FROM aphrc_works")
        total_count = cur.fetchone()[0]
        print(f"Total APHRC publications found: {total_count}\n")
        
        # 2. Publications by year
        print("Publications by Year:")
        cur.execute("""
            SELECT publication_year, COUNT(*) 
            FROM aphrc_works 
            WHERE publication_year IS NOT NULL
            GROUP BY publication_year 
            ORDER BY publication_year DESC
        """)
        years_data = cur.fetchall()
        print(tabulate(years_data, headers=["Year", "Count"], tablefmt="simple"))
        print()
        
        # 3. Most cited publications
        print("Top 5 Most Cited Publications:")
        cur.execute("""
            SELECT title, publication_year, cited_by_count 
            FROM aphrc_works 
            ORDER BY cited_by_count DESC 
            LIMIT 5
        """)
        cited_data = cur.fetchall()
        print(tabulate(cited_data, headers=["Title", "Year", "Citations"], tablefmt="simple"))
        print()
        
        # 4. Common journals
        print("Top 5 Journals with APHRC Publications:")
        cur.execute("""
            SELECT journal_name, COUNT(*) 
            FROM aphrc_works 
            WHERE journal_name != ''
            GROUP BY journal_name 
            ORDER BY COUNT(*) DESC 
            LIMIT 5
        """)
        journal_data = cur.fetchall()
        print(tabulate(journal_data, headers=["Journal", "Publications"], tablefmt="simple"))
        print()
        
        # 5. Open access status
        cur.execute("""
            SELECT 
                CASE 
                    WHEN open_access = true THEN 'Open Access' 
                    WHEN open_access = false THEN 'Closed Access'
                    ELSE 'Unknown'
                END as access_type,
                COUNT(*)
            FROM aphrc_works 
            GROUP BY access_type
        """)
        access_data = cur.fetchall()
        print("Open Access Statistics:")
        print(tabulate(access_data, headers=["Access Type", "Count"], tablefmt="simple"))
        print()
        
        # 6. Most frequent authors
        print("Top 10 Authors in APHRC Publications:")
        cur.execute("""
            WITH author_split AS (
                SELECT 
                    TRIM(unnest(string_to_array(authors, ','))) AS author_name
                FROM 
                    aphrc_works
            )
            SELECT 
                author_name, 
                COUNT(*) as publication_count
            FROM 
                author_split
            WHERE
                author_name != ''
            GROUP BY 
                author_name
            ORDER BY 
                publication_count DESC
            LIMIT 10
        """)
        author_data = cur.fetchall()
        print(tabulate(author_data, headers=["Author", "Publications"], tablefmt="simple"))
        
        # Close connections
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"Error during verification: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    try:
        import tabulate
    except ImportError:
        print("Installing tabulate package...")
        import subprocess
        subprocess.check_call(["pip", "install", "tabulate"])
    
    run_verification()