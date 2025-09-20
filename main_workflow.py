"""
ARGO Data Processing Workflow
============================

Simple script to download and process ARGO float data for any region and year.
Just provide region and year, and all steps will execute automatically:

1. Index Download
2. Database Save  
3. File Existence Check
4. NetCDF Download
5. Data Conversion
6. Parquet Merge (Region & Year wise)
7. Final Inspection

Usage:
python main_workflow.py
"""

import os
import sys
import time
from datetime import datetime

# Import workflow modules
from modules.index_file_download import download_index_files
from modules.db_config import save_to_database
from modules.file_exisitance_checking import check_and_download_files
from modules.netcdf_file_dowload import download_netcdf_files
from modules.data_loading_module import convert_data
from modules.parquet_merger_module import merge_parquet_files
from modules.parquet_opener_module import inspect_final_data

def print_header(step_num, step_name):
    """Print a nice header for each step"""
    print("\n" + "="*60)
    print(f"STEP {step_num}: {step_name}")
    print("="*60)

def print_success(message):
    """Print success message"""
    print(f"✅ {message}")

def print_error(message):
    """Print error message"""
    print(f"❌ {message}")

def get_user_input():
    """Get region and year from user"""
    print("="*60)
    print("ARGO DATA PROCESSING WORKFLOW")
    print("="*60)
    
    # Available regions
    regions = ["atlantic", "pacific", "indian"]
    print("\nAvailable regions:")
    for i, region in enumerate(regions, 1):
        print(f"{i}. {region}")
    
    # Get region
    while True:
        try:
            region_choice = input(f"\nSelect region (1-{len(regions)}): ").strip()
            region_idx = int(region_choice) - 1
            if 0 <= region_idx < len(regions):
                region = regions[region_idx]
                break
            else:
                print("Invalid choice. Please try again.")
        except ValueError:
            print("Please enter a number.")
    
    # Get year
    while True:
        try:
            year = int(input("Enter year (e.g., 2020): ").strip())
            if 2000 <= year <= datetime.now().year:
                break
            else:
                print("Please enter a valid year between 2000 and current year.")
        except ValueError:
            print("Please enter a valid year.")
    
    return region, year

def main_workflow():
    """Execute the complete ARGO data processing workflow"""
    start_time = time.time()
    
    try:
        # Get user input
        region, year = get_user_input()
        
        print(f"\nStarting workflow for region: {region}, year: {year}")
        print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Step 1: Index Download
        print_header(1, "Index Download")
        success = download_index_files(region, year)
        if success:
            print_success("Index files downloaded successfully")
        else:
            print_error("Index download failed")
            return False
        
        # Step 2: Database Save
        print_header(2, "Database Save")
        success = save_to_database(region, year)
        if success:
            print_success("Data saved to database successfully")
        else:
            print_error("Database save failed")
            return False
        
        # Step 3: File Existence Check
        print_header(3, "File Existence Check")
        file_paths = check_and_download_files(region, year)
        if file_paths:
            print_success(f"File check completed. Found {len(file_paths)} files")
        else:
            print_error("File existence check failed")
            return False
        
        # Step 4: NetCDF Download (if needed)
        print_header(4, "NetCDF Download")
        success = download_netcdf_files(region, year)
        if success:
            print_success("NetCDF files downloaded successfully")
        else:
            print_error("NetCDF download failed")
            return False
        
        # Step 5: Data Conversion
        print_header(5, "Data Conversion")
        success = convert_data(region, year)
        if success:
            print_success("Data conversion completed successfully")
        else:
            print_error("Data conversion failed")
            return False
        
        # Step 6: Parquet Merge
        print_header(6, "Parquet Merge (Region & Year wise)")
        success = merge_parquet_files(region, year)
        if success:
            print_success("Parquet files merged successfully")
        else:
            print_error("Parquet merge failed")
            return False
        
        # Step 7: Final Inspection
        print_header(7, "Final Inspection")
        success = inspect_final_data(region, year)
        if success:
            print_success("Final inspection completed successfully")
        else:
            print_error("Final inspection failed")
            return False
        
        # Workflow completed successfully
        end_time = time.time()
        total_time = end_time - start_time
        
        print("\n" + "="*60)
        print("🎉 WORKFLOW COMPLETED SUCCESSFULLY! 🎉")
        print("="*60)
        print(f"Region: {region}")
        print(f"Year: {year}")
        print(f"Total processing time: {total_time/60:.2f} minutes")
        print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("\nYour ARGO data is ready for analysis!")
        print("="*60)
        
        return True
        
    except KeyboardInterrupt:
        print("\n\n❌ Workflow interrupted by user")
        return False
    except Exception as e:
        print(f"\n\n❌ Workflow failed with error: {str(e)}")
        return False

if __name__ == "__main__":
    success = main_workflow()
    if success:
        sys.exit(0)
    else:
        sys.exit(1)