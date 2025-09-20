import os
import asyncio
import aiohttp
import aiofiles
import psycopg2
import sys

# Add parent directory to path to import database_setup
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database_setup import get_db_config, get_table_name

# Get configuration from centralized setup
DB_CONFIG = get_db_config()
TABLE_NAME = get_table_name()

LOCAL_FOLDER = "./argo_float_data"
BASE_URL = "https://www.ncei.noaa.gov/data/oceans/argo/gadr/data/"
MAX_CONNECTIONS = 100
RETRIES = 0

def is_file_valid(local_path):
    """Check if local file exists and is not empty"""
    return os.path.exists(local_path) and os.path.getsize(local_path) > 0

def clean_file_path(file_path):
    """Clean DB path: remove duplicate 'data/' and fix .nnc typo"""
    if file_path.startswith("data/data/"):
        file_path = file_path[len("data/"):]  # remove first 'data/'
    elif file_path.startswith("data/"):
        file_path = file_path[len("data/"):]
    if not file_path.endswith(".nc"):
        file_path = file_path.replace(".nnc", ".nc")
    return file_path

def query_index(region, year, lat_range=None, lon_range=None):
    """Query PostgreSQL for relevant files and return unique paths"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    sql = f"SELECT file_path FROM {TABLE_NAME} WHERE region=%s AND year=%s"
    params = [region, year]

    if lat_range:
        sql += " AND latitude_min >= %s AND latitude_max <= %s"
        params += lat_range
    if lon_range:
        sql += " AND longitude_min >= %s AND longitude_max <= %s"
        params += lon_range

    cur.execute(sql, params)
    results = cur.fetchall()
    cur.close()
    conn.close()

    # Extract unique file paths only
    unique_files = list({clean_file_path(fp[0]) for fp in results})
    return unique_files

async def download_nc_file(session, url, local_path, retries=RETRIES):
    """Async download with silent retries"""
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    for _ in range(retries):
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as resp:
                if resp.status == 200:
                    async with aiofiles.open(local_path, 'wb') as f:
                        async for chunk in resp.content.iter_chunked(1024*256):
                            await f.write(chunk)
                    return True
                elif resp.status == 404:
                    return False
        except:
            await asyncio.sleep(1)
    return False

async def fetch_data(region, year, lat_range=None, lon_range=None):
    """Query DB, check local folder, download missing files"""
    print(f"\n🔍 Querying index table for region={region}, year={year}...")
    files = query_index(region, year, lat_range, lon_range)
    total_files = len(files)
    print(f"📄 Total unique files in DB: {total_files:,}")

    if total_files == 0:
        print("⚠️ No files found. Exiting.")
        return []

    download_tasks = []
    local_paths = []
    failed_downloads = []

    for file_path in files:
        local_path = os.path.join(LOCAL_FOLDER, region, file_path.split(f"{region}/")[-1])
        local_paths.append(local_path)
        url = BASE_URL + file_path
        if not is_file_valid(local_path):
            download_tasks.append((url, local_path))

    already_existing = total_files - len(download_tasks)
    downloaded_count = 0

    if download_tasks:
        connector = aiohttp.TCPConnector(limit=MAX_CONNECTIONS)
        timeout = aiohttp.ClientTimeout(total=300)
        headers = {'User-Agent': 'Mozilla/5.0'}

        async with aiohttp.ClientSession(connector=connector, timeout=timeout, headers=headers) as session:
            tasks = [
                download_nc_file(session, url, path)
                for url, path in download_tasks
            ]
            results = await asyncio.gather(*tasks)

            for i, success in enumerate(results):
                if success:
                    downloaded_count += 1
                else:
                    failed_downloads.append(download_tasks[i][0])

    # Final summary
    print("\n" + "="*60)
    print(f"📊 SUMMARY for region={region}, year={year}")
    print(f"Total unique files in DB:       {total_files:,}")
    print(f"Already exists locally:          {already_existing:,}")
    print(f"Successfully downloaded:        {downloaded_count:,}")
    print(f"Failed / not found:              {len(failed_downloads):,}")
    print("="*60 + "\n")

    return local_paths

def check_and_download_files(region, year, lat_range=None, lon_range=None):
    """Check database and download missing NetCDF files for a specific region and year"""
    print(f"🔍 Checking and downloading files for {region}/{year}...")
    
    try:
        # Run the async fetch_data function
        local_paths = asyncio.run(fetch_data(region, year, lat_range, lon_range))
        
        if local_paths:
            print(f"✅ File check completed for {region}/{year}")
            return local_paths
        else:
            print(f"⚠️ No files found for {region}/{year}")
            return []
            
    except Exception as e:
        print(f"❌ File existence check failed: {e}")
        return []

# ---------------- Example Usage ----------------
if __name__ == "__main__":
    REGION = "indian"
    YEAR = 2020
    LAT_RANGE = None    # optional: (min, max)
    LON_RANGE = None    # optional: (min, max)

    asyncio.run(fetch_data(REGION, YEAR, LAT_RANGE, LON_RANGE))
