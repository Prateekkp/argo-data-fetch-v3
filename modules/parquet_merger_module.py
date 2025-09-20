"""
Parquet Merger Module
====================

Merges individual parquet batch files into a single parquet file for a region and year.
This module combines all processed data into one final dataset.
"""

import pandas as pd
import glob
import os

def merge_parquet_files(region, year):
    """Merge all parquet batch files for a specific region and year"""
    print(f"🔗 Merging parquet files for {region}/{year}...")
    
    try:
        # Setup region-specific directories
        region_folder = os.path.join("processed_data", region)
        merged_folder = os.path.join("processed_data", "merged", region)
        os.makedirs(merged_folder, exist_ok=True)
        
        # Find all batch files for this region and year in the region folder
        pattern = os.path.join(region_folder, f"{region}_{year}_batch*.parquet")
        batch_files = glob.glob(pattern)
        
        if not batch_files:
            print(f"⚠️ No batch parquet files found in {region_folder}")
            return False
        
        print(f"Found {len(batch_files)} batch files to merge")
        print(f"Saving merged file to: {merged_folder}")
        
        all_dfs = []
        valid_files = 0
        
        for f in batch_files:
            if os.path.getsize(f) == 0:
                print(f"⚠️ Skipping empty file: {f}")
                continue
            
            try:
                df = pd.read_parquet(f)
                if not df.empty:
                    all_dfs.append(df)
                    valid_files += 1
                else:
                    print(f"⚠️ Skipping empty dataframe from: {f}")
            except Exception as e:
                print(f"⚠️ Failed to read {f}: {e}")
        
        if all_dfs:
            # Combine all dataframes
            final_df = pd.concat(all_dfs, ignore_index=True)
            
            # Create final file path in the merged region folder
            final_file = os.path.join(merged_folder, f"{region}_{year}_full.parquet")
            
            # Save merged file
            final_df.to_parquet(final_file, index=False)
            
            print(f"✅ Successfully merged {valid_files} files into {final_file}")
            print(f"   Total rows: {len(final_df):,}")
            print(f"   File size: {os.path.getsize(final_file) / (1024*1024):.2f} MB")
            
            return True
        else:
            print(f"❌ No valid parquet files found to merge for {region}/{year}")
            return False
            
    except Exception as e:
        print(f"❌ Parquet merge failed: {e}")
        return False

if __name__ == "__main__":
    # Example usage for standalone execution
    region = "indian"
    year = 2020
    
    success = merge_parquet_files(region, year)
    if success:
        print("Parquet merge completed successfully!")
    else:
        print("Parquet merge failed!")