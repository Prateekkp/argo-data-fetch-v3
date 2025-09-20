import requests
import os
import asyncio
import aiohttp
import aiofiles
from concurrent.futures import ThreadPoolExecutor
import time
import re
from collections import defaultdict


class UltraFastArgoDownloader:
    def __init__(self, region="indian", years=[2020], max_connections=100):
        self.region = region
        self.years = years
        self.base_url = "https://www.ncei.noaa.gov/data/oceans/argo/gadr/data/"
        self.max_connections = max_connections
        
    def parse_apache_directory_fast(self, html_content):
        """Ultra-fast regex parsing"""
        if not html_content:
            return []
        
        # Optimized regex for Apache directory listings
        pattern = r'<a href="([^"]+\.nc)"[^>]*>([^<]+\.nc)</a>'
        matches = re.findall(pattern, html_content, re.IGNORECASE)
        
        files = []
        for href, name in matches:
            files.append({'name': name.strip(), 'href': href})
        
        # Also catch directory links for deeper scanning
        dir_pattern = r'<a href="([^"]+/)"[^>]*>([^<]+)/</a>'
        dir_matches = re.findall(dir_pattern, html_content, re.IGNORECASE)
        
        return files, dir_matches
    
    async def fetch_url(self, session, url):
        """Async fetch with timeout"""
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as response:
                if response.status == 200:
                    return await response.text()
                return None
        except:
            return None
    
    async def download_file(self, session, semaphore, file_url, local_path):
        """Async file download"""
        async with semaphore:
            try:
                # Create directory
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                
                # Skip if exists
                if os.path.exists(local_path):
                    return True
                
                async with session.get(file_url, timeout=aiohttp.ClientTimeout(total=60)) as response:
                    if response.status == 200:
                        async with aiofiles.open(local_path, 'wb') as f:
                            async for chunk in response.content.iter_chunked(32768):
                                await f.write(chunk)
                        return True
                    return False
            except:
                return False
    
    async def scan_and_download(self):
        """Ultra-fast async scanning and downloading"""
        print(f"ULTRA-FAST download for {self.region}, years: {self.years}")
        print(f"Max connections: {self.max_connections}")
        print("="*60)
        
        # Configure aiohttp for maximum speed
        connector = aiohttp.TCPConnector(
            limit=self.max_connections,
            limit_per_host=self.max_connections,
            ttl_dns_cache=300,
            use_dns_cache=True,
            keepalive_timeout=60
        )
        
        timeout = aiohttp.ClientTimeout(total=300)
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive'
        }
        
        start_time = time.time()
        
        async with aiohttp.ClientSession(
            connector=connector, 
            timeout=timeout, 
            headers=headers
        ) as session:
            
            # Step 1: Fast parallel scanning
            print("Ultra-fast parallel scanning...")
            all_files = []
            
            for year in self.years:
                print(f"Scanning {year}...")
                year_url = f"{self.base_url}{self.region}/{year}/"
                
                # Get year directory
                year_html = await self.fetch_url(session, year_url)
                if not year_html:
                    continue
                
                files, dirs = self.parse_apache_directory_fast(year_html)
                
                # Add direct .nc files
                for file_info in files:
                    file_url = year_url + file_info['href']
                    local_path = os.path.join("argo_float_data", self.region, str(year), file_info['name'])
                    all_files.append((file_url, local_path))
                
                # Scan subdirectories in parallel
                if dirs:
                    folder_tasks = []
                    for dir_href, dir_name in dirs:
                        folder_url = year_url + dir_href
                        folder_tasks.append(self.scan_folder(session, folder_url, year, dir_name))
                    
                    # Execute folder scanning in parallel
                    folder_results = await asyncio.gather(*folder_tasks, return_exceptions=True)
                    
                    for result in folder_results:
                        if isinstance(result, list):
                            all_files.extend(result)
            
            scan_time = time.time() - start_time
            print(f"Found {len(all_files):,} files in {scan_time:.1f} seconds")
            
            if not all_files:
                print("No files found!")
                return
            
            # Step 2: Ultra-fast parallel downloading
            print("Starting ultra-fast downloads...")
            download_start = time.time()
            
            # Create semaphore to limit concurrent downloads
            semaphore = asyncio.Semaphore(self.max_connections)
            
            # Create download tasks
            download_tasks = [
                self.download_file(session, semaphore, file_url, local_path)
                for file_url, local_path in all_files
            ]
            
            # Execute downloads with progress tracking
            completed = 0
            batch_size = 1000
            
            for i in range(0, len(download_tasks), batch_size):
                batch = download_tasks[i:i + batch_size]
                results = await asyncio.gather(*batch, return_exceptions=True)
                
                # Count successes
                batch_success = sum(1 for r in results if r is True)
                completed += len(batch)
                
                # Progress update
                progress = (completed / len(download_tasks)) * 100
                elapsed = time.time() - download_start
                speed = completed / elapsed if elapsed > 0 else 0
                
                print(f"Progress: {progress:.1f}% | Processed: {completed:,}/{len(download_tasks):,} | Speed: {speed:.0f} files/sec")
            
            # Final summary
            total_time = time.time() - start_time
            download_time = time.time() - download_start
            
            print("="*60)
            print(f"ULTRA-FAST DOWNLOAD COMPLETED!")
            print(f"Total files: {len(all_files):,}")
            print(f"Scan time: {scan_time:.1f} seconds")
            print(f"Download time: {download_time/60:.1f} minutes") 
            print(f"Total time: {total_time/60:.1f} minutes")
            print(f"Average speed: {(len(all_files)/(download_time/60)):.0f} files/minute")
    
    async def scan_folder(self, session, folder_url, year, folder_name):
        """Scan a single folder for .nc files"""
        files = []
        try:
            folder_html = await self.fetch_url(session, folder_url)
            if folder_html:
                nc_files, subdirs = self.parse_apache_directory_fast(folder_html)
                
                # Add .nc files from this folder
                for file_info in nc_files:
                    file_url = folder_url + file_info['href']
                    local_path = os.path.join("argo_float_data", self.region, str(year), folder_name, file_info['name'])
                    files.append((file_url, local_path))
                
                # Handle one level of subdirectories
                for subdir_href, subdir_name in subdirs:
                    subdir_url = folder_url + subdir_href
                    subdir_html = await self.fetch_url(session, subdir_url)
                    if subdir_html:
                        sub_files, _ = self.parse_apache_directory_fast(subdir_html)
                        for file_info in sub_files:
                            file_url = subdir_url + file_info['href']
                            local_path = os.path.join("argo_float_data", self.region, str(year), folder_name, subdir_name, file_info['name'])
                            files.append((file_url, local_path))
        except:
            pass
        
        return files


def download_netcdf_files(region, year, max_connections=100):
    """Download NetCDF files for a specific region and year"""
    print(f"📦 Downloading NetCDF files for {region}/{year}...")
    
    try:
        # Run the ultra-fast downloader
        downloader = UltraFastArgoDownloader(region, [year], max_connections)
        asyncio.run(downloader.scan_and_download())
        
        print(f"✅ NetCDF download completed for {region}/{year}")
        return True
        
    except Exception as e:
        print(f"❌ NetCDF download failed: {e}")
        return False

# Synchronous wrapper
def run_ultra_fast_download(region="indian", years=[2020], max_connections=150):
    """Run the ultra-fast async downloader"""
    downloader = UltraFastArgoDownloader(region, years, max_connections)
    asyncio.run(downloader.scan_and_download())


# ============================================================================
# MANUAL CONFIGURATION SECTION - EDIT THESE SETTINGS
# ============================================================================

if __name__ == "__main__":
    # EASY CONFIGURATION - CHANGE THESE VALUES AS NEEDED
    
    # 1. SET REGION (available: indian, atlantic, pacific, arctic, southern)
    REGION = "pacific"
    
    # 2. SET YEARS (can be single year or list of years)
    YEARS = [2020]  # Example options:
    # YEARS = [2018, 2019, 2020]           # Multiple specific years
    # YEARS = list(range(2015, 2021))      # Range: 2015 to 2020
    # YEARS = [2020]                       # Single year
    
    # 3. SET DOWNLOAD PERFORMANCE (optional)
    MAX_CONNECTIONS = 150  # Higher = faster downloads (but may overwhelm server)
    
    print("="*60)
    print("ARGO FLOAT DATA DOWNLOADER")
    print("="*60)
    print(f"Region: {REGION}")
    print(f"Years: {YEARS}")
    print(f"Max Connections: {MAX_CONNECTIONS}")
    print("="*60)
    
    # Run without confirmation for workflow compatibility
    run_ultra_fast_download(
        region=REGION,
        years=YEARS,
        max_connections=MAX_CONNECTIONS
    )
