# ARGO Ocean Data Processing Workflow

A comprehensive Python workflow for downloading, processing, and analyzing ARGO oceanographic float data from NOAA servers. This project provides an automated 7-step pipeline to convert raw ARGO data into structured Parquet files for analysis.

## 🌊 Features

- **Automated Data Pipeline**: Complete 7-step workflow from data discovery to processing
- **Parallel Downloads**: High-performance concurrent downloading with resume capability
- **Data Validation**: Robust error handling and data quality checks
- **Database Integration**: PostgreSQL storage for efficient data indexing and querying
- **Flexible Output**: Converts NetCDF to Parquet format for analysis
- **Beginner Friendly**: Simple interface - just input region and year

## 🚀 Quick Start

### Prerequisites

- Python 3.7+
- PostgreSQL database
- Internet connection for data downloads

### Installation

1. **Clone the repository**:
   ```bash
   git clone <your-repo-url>
   cd "Data Fetching v3"
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up environment variables**:
   ```bash
   copy .env.example .env
   ```
   Edit `.env` with your database credentials:
   ```env
   DB_HOST=localhost
   DB_NAME=argo_index
   DB_USER=postgres
   DB_PASSWORD=your_password_here
   DB_PORT=5432
   ```

4. **Create PostgreSQL database**:
   ```sql
   CREATE DATABASE argo_index;
   ```

5. **Run the workflow**:
   ```bash
   python main_workflow.py
   ```

## 📊 Usage

The workflow supports various ocean regions:
- `atlantic` - Atlantic Ocean
- `pacific` - Pacific Ocean 
- `indian` - Indian Ocean
- `arctic` - Arctic Ocean
- `southern` - Southern Ocean

Simply run the main script and follow the prompts:

```bash
python main_workflow.py
```

**Example interaction**:
```
Enter ocean region (atlantic/pacific/indian): atlantic
Enter year (2000-2024): 2020
```

## What this workflow does:

1. **Index Download** - Downloads ARGO index files for the specified region and year
2. **Database Save** - Saves index data to PostgreSQL database  
3. **File Existence Check** - Checks which NetCDF files exist and downloads missing ones
4. **NetCDF Download** - Downloads all required NetCDF float data files
5. **Data Conversion** - Converts NetCDF files to structured Parquet format
6. **Parquet Merge** - Combines all data into one final file (region & year specific)
7. **Final Inspection** - Shows summary and sample of the processed data

## 📁 Project Structure

```
ARGO Data Processing/
├── main_workflow.py           # Main script - run this!
├── database_setup.py          # Database configuration - edit this first!
├── README.md                  # This file
├── modules/                   # Processing modules (don't edit these)
│   ├── index_file_download.py
│   ├── db_config.py
│   ├── file_exisitance_checking.py
│   ├── netcdf_file_dowload.py
│   ├── data_loading_module.py
│   ├── parquet_merger_module.py
│   └── parquet_opener_module.py
├── argo_index/               # Downloaded index files
├── argo_float_data/          # Downloaded NetCDF files
└── processed_data/           # Final parquet files
    ├── {region}/             # Batch files by region
    └── merged/{region}/      # Final merged files by region
```

## ⚙️ Configuration

### Database Setup
Edit `database_setup.py` with your PostgreSQL settings:
- Host, database name, username, password
- Table name for storing index data
- Connection settings

### Advanced Usage
You can run individual modules if needed, but the main workflow handles everything automatically.

## 📋 Requirements

- Python 3.7+
- PostgreSQL database
- Required Python packages:
  - requests, beautifulsoup4, psycopg2
  - aiohttp, aiofiles
  - xarray, pandas, dask

## 🎯 Output

After running the workflow for region="indian" and year="2020":
- **Batch files**: `processed_data/indian/indian_2020_batch*.parquet`
- **Final file**: `processed_data/merged/indian/indian_2020_full.parquet`

## ✨ Features

✅ **One-click setup** - Edit database config once, run anywhere  
✅ **Interactive interface** - Simple menu-driven workflow  
✅ **Organized output** - Clean folder structure by region  
✅ **Robust downloads** - Handles network issues gracefully  
✅ **Progress tracking** - Shows progress at each step  
✅ **Resumable** - Can restart if interrupted