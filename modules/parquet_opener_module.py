"""
Parquet Opener Module
====================

Inspects and displays information about the final merged parquet file.
This module provides a summary of the processed data for verification.
"""

import pandas as pd
import os

def inspect_final_data(region, year):
    """Inspect the final merged parquet file for a region and year"""
    print(f"🔍 Inspecting final data for {region}/{year}...")
    
    try:
        # Construct file path in the new merged folder structure
        file_path = os.path.join("processed_data", "merged", region, f"{region}_{year}_full.parquet")
        
        # Check if file exists
        if not os.path.exists(file_path):
            print(f"❌ Final parquet file not found: {file_path}")
            return False
        
        # Read the parquet file
        df = pd.read_parquet(file_path)
        
        # Display basic information
        print(f"\n📊 DATA SUMMARY for {region.upper()} {year}")
        print("="*50)
        print(f"File path: {file_path}")
        print(f"File size: {os.path.getsize(file_path) / (1024*1024):.2f} MB")
        print(f"Total rows: {len(df):,}")
        print(f"Total columns: {len(df.columns)}")
        
        print(f"\n📋 COLUMN INFORMATION:")
        print(f"Columns: {list(df.columns)}")
        
        print(f"\n📈 DATA SHAPE:")
        print(f"Shape: {df.shape}")
        
        print(f"\n🎯 SAMPLE DATA (First 5 rows):")
        print(df.head())
        
        print(f"\n📊 DATA TYPES:")
        print(df.dtypes)
        
        print(f"\n📐 BASIC STATISTICS:")
        try:
            print(df.describe())
        except Exception as e:
            print(f"Could not generate statistics: {e}")
        
        print(f"\n✅ Final data inspection completed successfully!")
        print("="*50)
        
        return True
        
    except Exception as e:
        print(f"❌ Final data inspection failed: {e}")
        return False

def quick_preview(region, year, num_rows=10):
    """Quick preview of the data without full inspection"""
    try:
        file_path = os.path.join("processed_data", "merged", region, f"{region}_{year}_full.parquet")
        
        if not os.path.exists(file_path):
            print(f"❌ File not found: {file_path}")
            return False
        
        df = pd.read_parquet(file_path)
        print(f"\n📋 Quick Preview - {region.upper()} {year}")
        print(f"Rows: {len(df):,} | Columns: {len(df.columns)}")
        print(f"\nFirst {num_rows} rows:")
        print(df.head(num_rows))
        
        return True
        
    except Exception as e:
        print(f"❌ Quick preview failed: {e}")
        return False

if __name__ == "__main__":
    # Example usage for standalone execution
    region = "indian"
    year = 2020
    
    success = inspect_final_data(region, year)
    if success:
        print("Data inspection completed successfully!")
    else:
        print("Data inspection failed!")