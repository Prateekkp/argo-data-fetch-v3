import psycopg2
from glob import glob
import os
import csv
from io import StringIO
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys

# Add parent directory to path to import database_setup
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database_setup import get_db_config, get_table_name, get_max_workers

# Get configuration from centralized setup
db_config = get_db_config()
DATA_FOLDER = "./argo_index"
TABLE_NAME = get_table_name()
MAX_WORKERS = get_max_workers()

def ingest_file(file_path):
    try:
        # Extract region/year from folder structure
        parts = file_path.split(os.sep)
        region = parts[-3]   # e.g., 'atlantic'
        year = int(parts[-2]) # e.g., '2020'

        # Prepare in-memory CSV with extra columns
        buffer = StringIO()
        with open(file_path, 'r') as f:
            reader = csv.reader(f)
            headers = next(reader)
            headers += ['region', 'year']
            writer = csv.writer(buffer)
            writer.writerow(headers)
            for row in reader:
                row += [region, year]
                writer.writerow(row)

        buffer.seek(0)

        # Connect inside the thread
        conn = psycopg2.connect(
            host=db_config['host'],
            database=db_config['database'],
            user=db_config['user'],
            password=db_config['password'],
            port=db_config['port']
        )
        cur = conn.cursor()
        cur.copy_expert(f"COPY {TABLE_NAME} FROM STDIN WITH CSV HEADER", buffer)
        conn.commit()
        cur.close()
        conn.close()

        print(f"✅ Inserted: {file_path} into table {TABLE_NAME}")
    except Exception as e:
        print(f"❌ Failed: {file_path} | {e}")

def save_to_database(region, year):
    """Save index files for a specific region and year to database"""
    print(f"💾 Saving index data to database for {region}/{year}...")
    
    try:
        # Get files for specific region and year
        pattern = os.path.join(DATA_FOLDER, region, str(year), "*.txt")
        txt_files = glob(pattern)
        
        if not txt_files:
            print(f"⚠️ No index files found for {region}/{year}")
            return False
        
        print(f"Found {len(txt_files)} files to save to database...")
        
        success_count = 0
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(ingest_file, file) for file in txt_files]
            for future in as_completed(futures):
                try:
                    future.result()
                    success_count += 1
                except Exception as e:
                    print(f"⚠️ Failed to process file: {e}")
        
        if success_count > 0:
            print(f"✅ Successfully saved {success_count} files to database")
            return True
        else:
            print(f"❌ Failed to save any files to database")
            return False
            
    except Exception as e:
        print(f"❌ Database save failed: {e}")
        return False

def main():
    """Legacy main function for backward compatibility"""
    # Recursively get all .txt files
    txt_files = glob(os.path.join(DATA_FOLDER, "*", "*", "*.txt"))

    print(f"Found {len(txt_files)} files to ingest...")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(ingest_file, file) for file in txt_files]
        for _ in as_completed(futures):
            pass  # printing is handled inside ingest_file()

if __name__ == "__main__":
    main()
