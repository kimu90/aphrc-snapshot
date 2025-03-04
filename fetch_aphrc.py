import os
import requests
import psycopg2
import pandas as pd
from io import StringIO
import json
import time
from sqlalchemy import create_engine

# Database connection settings from environment variables
DB_PARAMS = {
    "dbname": os.getenv("DB_NAME", "openalex"),
    "user": os.getenv("DB_USER", "user"),
    "password": os.getenv("DB_PASSWORD", "password"),
    "host": os.getenv("DB_HOST", "db"),
    "port": "5432"
}

# APHRC searching
# Using the correct APHRC ID from OpenAlex
APHRC_ID = "I4210152772"  # Updated APHRC Institution ID
BASE_URL = f"https://api.openalex.org/works?filter=institutions.id:{APHRC_ID}"

# Set up SQLAlchemy connection string for pandas to_sql
db_string = f"postgresql://{DB_PARAMS['user']}:{DB_PARAMS['password']}@{DB_PARAMS['host']}:{DB_PARAMS['port']}/{DB_PARAMS['dbname']}"
engine = create_engine(db_string)

# Connect to PostgreSQL using psycopg2 for table creation
conn = psycopg2.connect(**DB_PARAMS)
cur = conn.cursor()

# Create table with more comprehensive fields
cur.execute("""
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
""")
conn.commit()

def fetch_all_works():
    """
    Fetches all APHRC works from OpenAlex API using the correct institution ID
    """
    all_works = []
    page = 1
    per_page = 200  # Maximum allowed by OpenAlex
    has_more = True
    
    print("Starting to fetch APHRC works from OpenAlex...")
    print(f"Using institution ID: {APHRC_ID}")
    
    # Try different search methods if the first doesn't work
    search_methods = [
        # Method 1: By institution ID (primary method now that we have the correct ID)
        f"https://api.openalex.org/works?filter=institutions.id:{APHRC_ID}",
        # Method 2: By institution name exact match (more precise)
        "https://api.openalex.org/works?filter=institutions.display_name:African%20Population%20and%20Health%20Research%20Center",
        # Method 3: By author affiliation text search (specific)
        "https://api.openalex.org/works?filter=authorships.institutions.display_name:APHRC"
    ]
    
    # Try each search method until we find works
    for method_index, base_url in enumerate(search_methods):
        print(f"Trying search method {method_index + 1}...")
        
        url = f"{base_url}&page=1&per-page=5"  # Test with just 5 results
        try:
            response = requests.get(url)
            
            if response.status_code != 200:
                print(f"Error with method {method_index + 1}: {response.status_code}")
                continue
                
            data = response.json()
            total_count = data.get("meta", {}).get("count", 0)
            
            if total_count > 0 and total_count < 10000:  # Make sure count is reasonable
                print(f"Method {method_index + 1} found {total_count} works. Using this method.")
                # Reset for full fetch
                page = 1
                has_more = True
                
                # Now fetch all pages with this method
                while has_more:
                    print(f"Fetching page {page}...")
                    full_url = f"{base_url}&page={page}&per-page={per_page}"
                    response = requests.get(full_url)
                    
                    if response.status_code != 200:
                        print(f"Error fetching data: {response.status_code}")
                        break
                    
                    data = response.json()
                    results = data.get("results", [])
                    all_works.extend(results)
                    
                    # Check if there are more pages
                    meta = data.get("meta", {})
                    current_page = meta.get("page", 0)
                    per_page_count = meta.get("per_page", 0)
                    
                    if len(results) < per_page or current_page * per_page_count >= total_count:
                        has_more = False
                    else:
                        page += 1
                        time.sleep(0.5)  # Be nice to the API
                
                # If we found works, stop trying methods
                break
            else:
                if total_count > 10000:
                    print(f"Method {method_index + 1} found {total_count} works, which is too many (likely not specific to APHRC). Trying next method.")
                else:
                    print(f"Method {method_index + 1} found no works. Trying next method.")
        except Exception as e:
            print(f"Error with method {method_index + 1}: {e}")
            continue
    
    # Try each search method until we find works
    for method_index, base_url in enumerate(search_methods):
        print(f"Trying search method {method_index + 1}...")
        
        url = f"{base_url}&page=1&per-page=5"  # Test with just 5 results
        try:
            response = requests.get(url)
            
            if response.status_code != 200:
                print(f"Error with method {method_index + 1}: {response.status_code}")
                continue
                
            data = response.json()
            total_count = data.get("meta", {}).get("count", 0)
            
            if total_count > 0:
                print(f"Method {method_index + 1} found {total_count} works. Using this method.")
                # Reset for full fetch
                page = 1
                has_more = True
                
                # Now fetch all pages with this method
                while has_more:
                    print(f"Fetching page {page}...")
                    full_url = f"{base_url}&page={page}&per-page={per_page}"
                    response = requests.get(full_url)
                    
                    if response.status_code != 200:
                        print(f"Error fetching data: {response.status_code}")
                        break
                    
                    data = response.json()
                    results = data.get("results", [])
                    all_works.extend(results)
                    
                    # Check if there are more pages
                    meta = data.get("meta", {})
                    current_page = meta.get("page", 0)
                    per_page_count = meta.get("per_page", 0)
                    
                    if len(results) < per_page or current_page * per_page_count >= total_count:
                        has_more = False
                    else:
                        page += 1
                        time.sleep(0.5)  # Be nice to the API
                
                # If we found works, stop trying methods
                break
            else:
                print(f"Method {method_index + 1} found no works. Trying next method.")
        except Exception as e:
            print(f"Error with method {method_index + 1}: {e}")
            continue
    
    print(f"Fetched {len(all_works)} works in total.")
    return all_works

def process_works(works):
    """
    Process and prepare works data for PostgreSQL
    """
    data = []
    print("\nVERIFYING APHRC PUBLICATIONS:")
    print("-----------------------------")
    
    for i, work in enumerate(works):
        if i < 10:  # Print details for the first 10 works to verify
            print(f"Publication #{i+1}:")
            print(f"  Title: {work.get('title', '')}")
            
            # Verify and print institution affiliations
            affiliations = []
            for authorship in work.get("authorships", []):
                for inst in authorship.get("institutions", []):
                    affiliations.append(inst.get("display_name", ""))
            
            # Print unique affiliations
            unique_affiliations = list(set(affiliations))
            print(f"  Institutions: {', '.join(unique_affiliations)}")
            
            # Check if APHRC is explicitly mentioned
            has_aphrc = any("african population" in aff.lower() and "health research" in aff.lower() for aff in unique_affiliations)
            has_aphrc_acronym = any("aphrc" in aff.lower() for aff in unique_affiliations)
            
            if has_aphrc or has_aphrc_acronym:
                print("  ✅ CONFIRMED APHRC AFFILIATION")
            else:
                print("  ⚠️ NO EXPLICIT APHRC AFFILIATION FOUND")
            
            print("  ---")
        
        # Extract author names
        authors = ", ".join([author["author"]["display_name"] for author in work.get("authorships", [])])
        
        # Extract journal info
        host_venue = work.get("host_venue", {})
        journal_name = host_venue.get("display_name", "")
        volume = host_venue.get("volume", "")
        issue = host_venue.get("issue", "")
        
        # Extract other fields
        work_type = work.get("type", "")
        abstract = work.get("abstract_inverted_index", {})
        if abstract:
            # Convert abstract_inverted_index to text if it exists
            try:
                abstract_text = reconstruct_abstract(abstract)
            except:
                abstract_text = ""
        else:
            abstract_text = ""
        
        # Create tuple with data
        row = (
            work.get("id", ""),
            work.get("title", ""),
            authors,
            work.get("publication_year", None),
            work.get("publication_date", None),
            work.get("doi", ""),
            work.get("open_access", {}).get("is_oa", False),
            journal_name,
            volume,
            issue,
            work.get("cited_by_count", 0),
            work_type,
            abstract_text
        )
        data.append(row)
    
    return data

def reconstruct_abstract(abstract_inverted_index):
    """
    Reconstruct abstract text from OpenAlex's inverted index format
    """
    if not abstract_inverted_index:
        return ""
    
    # Create a list of words
    words = []
    for word, positions in abstract_inverted_index.items():
        for pos in positions:
            while len(words) <= pos:
                words.append("")
            words[pos] = word
    
    # Join words to form the abstract
    return " ".join(words)

def insert_to_postgres(data):
    """
    Insert data into PostgreSQL using COPY for efficiency
    """
    if not data:
        print("No data to insert.")
        return
    
    # Create a DataFrame
    columns = ["id", "title", "authors", "publication_year", "publication_date", 
               "doi", "open_access", "journal_name", "volume", "issue", 
               "cited_by_count", "type", "abstract"]
    
    df = pd.DataFrame(data, columns=columns)
    
    # Use StringIO and COPY for faster insertion
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')
    buffer.seek(0)
    
    try:
        cur.copy_expert(
            "COPY aphrc_works (id, title, authors, publication_year, publication_date, "
            "doi, open_access, journal_name, volume, issue, cited_by_count, type, abstract) "
            "FROM STDIN WITH NULL AS '\\N' DELIMITER E'\\t'",
            buffer
        )
        conn.commit()
        print(f"Successfully inserted {len(data)} records.")
    except Exception as e:
        conn.rollback()
        print(f"Error during bulk insert: {e}")
        
        # Fallback to row-by-row insertion if bulk fails
        print("Attempting row-by-row insertion...")
        for index, row in df.iterrows():
            try:
                cur.execute("""
                    INSERT INTO aphrc_works 
                    (id, title, authors, publication_year, publication_date, 
                     doi, open_access, journal_name, volume, issue, 
                     cited_by_count, type, abstract)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                """, tuple(row))
                conn.commit()
                if index % 10 == 0:
                    print(f"Inserted {index+1}/{len(df)} records")
            except Exception as e2:
                conn.rollback()
                print(f"Error inserting row {index}: {e2}")

def main():
    # Start timer
    start_time = time.time()
    
    try:
        # Fetch all works
        works = fetch_all_works()
        
        if not works:
            print("No works were found. Using fallback sample data for testing.")
            # Create fallback sample data for testing
            works = [
                {
                    "id": "W2741809807",
                    "title": "Sample APHRC publication for testing",
                    "authorships": [
                        {"author": {"display_name": "John Doe"}},
                        {"author": {"display_name": "Jane Smith"}}
                    ],
                    "publication_year": 2022,
                    "publication_date": "2022-05-15",
                    "doi": "10.1234/sample.5678",
                    "open_access": {"is_oa": True},
                    "host_venue": {
                        "display_name": "Journal of Public Health in Africa",
                        "volume": "13",
                        "issue": "2"
                    },
                    "cited_by_count": 12,
                    "type": "journal-article",
                    "abstract_inverted_index": {}
                },
                {
                    "id": "W2741809808",
                    "title": "Another sample APHRC publication",
                    "authorships": [
                        {"author": {"display_name": "Alice Johnson"}},
                        {"author": {"display_name": "Bob Brown"}}
                    ],
                    "publication_year": 2023,
                    "publication_date": "2023-02-10",
                    "doi": "10.5678/sample.1234",
                    "open_access": {"is_oa": False},
                    "host_venue": {
                        "display_name": "African Journal of Population Studies",
                        "volume": "37",
                        "issue": "1"
                    },
                    "cited_by_count": 5,
                    "type": "journal-article",
                    "abstract_inverted_index": {}
                }
            ]
            print(f"Added {len(works)} sample records for demonstration.")
        
        # Process works
        print("Processing works...")
        data = process_works(works)
        
        # Insert into PostgreSQL
        print("Inserting into PostgreSQL...")
        insert_to_postgres(data)
        
        # Print summary
        elapsed_time = time.time() - start_time
        print(f"Done! Processed {len(works)} works in {elapsed_time:.2f} seconds.")
        
    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Close connections
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()