import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import time

# Configuration
BASE_URL = "https://www.ncei.noaa.gov/data/oceans/argo/gadr/inv/basins"
SAVE_BASE = "./argo_index"
CHUNK_SIZE = 4 * 1024 * 1024
MAX_WORKERS = 5
MAX_RETRIES = 3

def get_file_links(region, year):
    """Get all *_argoinv.txt files for a region/year"""
    url = f"{BASE_URL}/{region}/{year}/"
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        files = [a['href'] for a in soup.find_all('a', href=True) if a['href'].endswith("_argoinv.txt")]
        return files
    except Exception as e:
        print(f"❌ Failed to list files for {region}/{year}: {e}")
        return []

def is_file_complete(file_path, url):
    """Check if file is already complete by comparing with server"""
    if not os.path.exists(file_path):
        return False
    
    try:
        # Get file size from server using HEAD request
        response = requests.head(url, timeout=10)
        if response.status_code == 200:
            server_size = int(response.headers.get('content-length', 0))
            local_size = os.path.getsize(file_path)
            
            # File is complete if sizes match or local file is the expected size
            if server_size > 0 and local_size == server_size:
                return True
            # For very small files (like empty index files), if local file exists and server returns same size
            elif server_size == 0 and local_size == 0:
                return True
    except:
        pass
    
    return False

def download_file(url, save_path):
    """Download a single file with resume and retries"""
    os.makedirs(os.path.dirname(save_path), exist_ok=True)

    # Check if file is already complete
    if is_file_complete(save_path, url):
        size_mb = os.path.getsize(save_path) / (1024 * 1024)
        print(f"⏭️ Skipping (already complete): {save_path} | Size: {size_mb:.2f} MB")
        return

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            existing_size = 0
            if os.path.exists(save_path):
                existing_size = os.path.getsize(save_path)

            # Try resume if file exists and has content
            headers = {}
            mode = "wb"
            if existing_size > 0:
                headers = {"Range": f"bytes={existing_size}-"}
                mode = "ab"

            start_time = time.time()
            with requests.get(url, stream=True, timeout=60, headers=headers) as r:
                # Handle different response codes
                if r.status_code == 416:  # Range Not Satisfiable
                    print(f"⚠️ Resume failed for {save_path}, downloading fresh...")
                    # Remove the partial file and try fresh download
                    if os.path.exists(save_path):
                        os.remove(save_path)
                    headers = {}
                    mode = "wb"
                    # Retry with fresh download
                    with requests.get(url, stream=True, timeout=60, headers=headers) as r2:
                        r2.raise_for_status()
                        with open(save_path, mode) as f:
                            for chunk in r2.iter_content(chunk_size=CHUNK_SIZE):
                                if chunk:
                                    f.write(chunk)
                elif r.status_code in [200, 206]:  # OK or Partial Content
                    with open(save_path, mode) as f:
                        for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                            if chunk:
                                f.write(chunk)
                else:
                    r.raise_for_status()

            elapsed = time.time() - start_time
            size_mb = os.path.getsize(save_path) / (1024 * 1024)
            print(f"✅ Downloaded: {save_path} | Size: {size_mb:.2f} MB | Time: {elapsed:.2f} sec")
            return
        except Exception as e:
            print(f"⚠️ Attempt {attempt} failed for {url}: {e}")
            # Remove partial file on error to ensure fresh download next time
            if os.path.exists(save_path):
                try:
                    os.remove(save_path)
                except:
                    pass
            time.sleep(5)  # wait before retry
    print(f"❌ Failed to download after {MAX_RETRIES} attempts: {url}")

def download_index_files(region, year):
    """Download index files for a specific region and year"""
    print(f"📥 Downloading index files for {region}/{year}...")
    
    tasks = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        files = get_file_links(region, year)
        if not files:
            print(f"⚠️ No index files found for {region}/{year}")
            return False
            
        for file_name in files:
            url = f"{BASE_URL}/{region}/{year}/{file_name}"
            save_path = os.path.join(SAVE_BASE, region, str(year), file_name)
            tasks.append(executor.submit(download_file, url, save_path))

        # Wait for all downloads to complete
        success_count = 0
        for future in as_completed(tasks):
            try:
                future.result()  # This will raise an exception if the download failed
                success_count += 1
            except Exception as e:
                print(f"⚠️ Download task failed: {e}")
        
        if success_count > 0:
            print(f"✅ Successfully downloaded {success_count} index files for {region}/{year}")
            return True
        else:
            print(f"❌ Failed to download any index files for {region}/{year}")
            return False

def main():
    """Legacy main function for backward compatibility"""
    # Default values for standalone execution
    regions = ["atlantic", "pacific", "indian"]
    years = [2019]
    
    tasks = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for region in regions:
            for year in years:
                files = get_file_links(region, year)
                if not files:
                    continue
                for file_name in files:
                    url = f"{BASE_URL}/{region}/{year}/{file_name}"
                    save_path = os.path.join(SAVE_BASE, region, str(year), file_name)
                    tasks.append(executor.submit(download_file, url, save_path))

        # Wait for all downloads to complete
        for future in as_completed(tasks):
            pass  # all print statements inside download_file

if __name__ == "__main__":
    main()
