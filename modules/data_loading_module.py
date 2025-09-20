"""
Data Loading Module
==================

Converts NetCDF files to Parquet format for a specific region and year.
This module processes ARGO float data and creates structured parquet files.
"""

import xarray as xr
import glob
import pandas as pd
import numpy as np
import os
import dask
from dask.diagnostics import ProgressBar

# Configuration
OUTPUT_FOLDER = "processed_data"
BASE_FOLDER = "argo_float_data"
BATCH_SIZE = 100
NUM_WORKERS = 1

def get_variable(ds, var_name, default_value=None):
    """Get variable from dataset, trying both uppercase and lowercase versions"""
    # Try lowercase first
    if var_name.lower() in ds.variables:
        return ds[var_name.lower()]
    # Try uppercase
    elif var_name.upper() in ds.variables:
        return ds[var_name.upper()]
    # Try original case
    elif var_name in ds.variables:
        return ds[var_name]
    else:
        if default_value is not None:
            return default_value
        else:
            raise KeyError(f"Variable '{var_name}' not found in dataset (tried lowercase, uppercase)")

def get_variable_safe(ds, var_name, shape=None, fill_value=None):
    """Safely get variable, create with NaN if missing"""
    try:
        return get_variable(ds, var_name)
    except KeyError:
        if shape is not None:
            print(f"⚠️ Variable '{var_name}' missing, creating with NaN values")
            if fill_value is None:
                fill_value = float('nan')
            return xr.DataArray(
                data=np.full(shape, fill_value),
                dims=['N_PROF', 'N_LEVELS'] if len(shape) == 2 else ['N_PROF']
            )
        else:
            raise

# Global counter for missing PSAL files
missing_psal_count = 0

def process_file(fpath, region_name):
    """Process a single NetCDF file and return DataFrame"""
    global missing_psal_count
    
    try:
        ds = xr.open_dataset(fpath)
        
        # Get essential variables (these should always exist)
        juld = pd.to_datetime(get_variable(ds, 'juld').values)
        lat = get_variable(ds, 'latitude').values
        lon = get_variable(ds, 'longitude').values
        pres = get_variable(ds, 'pres').values
        temp = get_variable(ds, 'temp').values
        
        # Get PSAL safely - create with NaN if missing
        try:
            psal = get_variable(ds, 'psal').values
        except KeyError:
            # Silent handling - just count missing PSAL files
            missing_psal_count += 1
            psal = np.full_like(pres, np.nan)
        
        # Handle different array shapes
        if pres.ndim == 1:
            # 1D array - single profile
            n_levels = len(pres)
            n_prof = 1
            pres = pres.reshape(1, -1)
            temp = temp.reshape(1, -1)
            psal = psal.reshape(1, -1)
            if juld.ndim == 0:
                juld = [juld]
            if lat.ndim == 0:
                lat = [lat]
            if lon.ndim == 0:
                lon = [lon]
        else:
            # 2D array - multiple profiles
            n_prof, n_levels = pres.shape

        dfs = []
        for p in range(n_prof):
            try:
                # Handle scalar vs array cases
                current_juld = juld[p] if hasattr(juld, '__len__') and len(juld) > p else juld[0] if hasattr(juld, '__len__') else juld
                current_lat = lat[p] if hasattr(lat, '__len__') and len(lat) > p else lat[0] if hasattr(lat, '__len__') else lat
                current_lon = lon[p] if hasattr(lon, '__len__') and len(lon) > p else lon[0] if hasattr(lon, '__len__') else lon
                
                df = pd.DataFrame({
                    'region': region_name,
                    'date': [current_juld.date()] * n_levels,
                    'time': [current_juld.time()] * n_levels,
                    'latitude': current_lat,
                    'longitude': current_lon,
                    'pres': pres[p],
                    'temp': temp[p],
                    'psal': psal[p]
                })
                dfs.append(df)
            except Exception as profile_error:
                print(f"⚠️ Skipping profile {p} in {fpath}: {profile_error}")
                continue
        
        if dfs:
            return pd.concat(dfs, ignore_index=True)
        else:
            print(f"⚠️ No valid profiles found in {fpath}")
            return None
            
    except Exception as e:
        print(f"⚠️ Skipping corrupted file: {fpath} ({e})")
        return None

def convert_data(region, year):
    """Convert NetCDF data to Parquet format for a specific region and year"""
    global missing_psal_count
    missing_psal_count = 0  # Reset counter for this conversion
    
    print(f"🔄 Converting data for {region}/{year}...")
    
    try:
        # Setup directories with region-specific structure
        region_output_folder = os.path.join(OUTPUT_FOLDER, region)
        os.makedirs(region_output_folder, exist_ok=True)
        
        # Find all NetCDF files for the region and year
        year_path = os.path.join(BASE_FOLDER, region, str(year))
        files = glob.glob(f"{year_path}/**/*.nc", recursive=True)
        
        if not files:
            print(f"⚠️ No NetCDF files found for {region}/{year}")
            return False
        
        print(f"Processing {len(files)} files for region '{region}', year '{year}'")
        print(f"Saving batch files to: {region_output_folder}")
        
        # Configure dask
        dask.config.set(scheduler='threads', num_workers=NUM_WORKERS)
        
        batch_number = 0
        successful_batches = 0
        
        # Process files in batches
        for i in range(0, len(files), BATCH_SIZE):
            batch_files = files[i:i+BATCH_SIZE]
            print(f"\nProcessing batch {batch_number+1}, files {i+1} to {i+len(batch_files)}")

            delayed_dfs = [dask.delayed(process_file)(f, region) for f in batch_files]

            try:
                with ProgressBar():
                    batch_dfs = dask.compute(*delayed_dfs)
            except Exception as e:
                print(f"⚠️ Batch {batch_number+1} failed: {e}")
                batch_number += 1
                continue

            batch_dfs = [df for df in batch_dfs if df is not None]
            if batch_dfs:
                batch_combined = pd.concat(batch_dfs, ignore_index=True)
                output_file = os.path.join(region_output_folder, f"{region}_{year}_batch{batch_number}.parquet")
                batch_combined.to_parquet(output_file, index=False)
                print(f"✅ Batch {batch_number+1} saved to {output_file}")
                successful_batches += 1
            else:
                print(f"⚠️ Batch {batch_number+1} has no valid data")

            batch_number += 1
        
        if successful_batches > 0:
            print(f"\n✅ Data conversion completed! {successful_batches} batches processed for {region}/{year}")
            
            # Report missing PSAL summary
            if missing_psal_count > 0:
                total_files = len(files)
                psal_coverage = ((total_files - missing_psal_count) / total_files) * 100
                print(f"📊 Data summary: {missing_psal_count:,} files missing PSAL data ({psal_coverage:.1f}% have salinity)")
            else:
                print(f"📊 Data summary: All files have complete PSAL data")
            
            return True
        else:
            print(f"\n❌ No batches were successfully processed for {region}/{year}")
            return False
            
    except Exception as e:
        print(f"❌ Data conversion failed: {e}")
        return False

if __name__ == "__main__":
    # Example usage for standalone execution
    region = "indian"
    year = 2020
    
    success = convert_data(region, year)
    if success:
        print("Data conversion completed successfully!")
    else:
        print("Data conversion failed!")