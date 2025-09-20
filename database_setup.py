"""
Database Configuration Setup
============================

Centralized database configuration for ARGO Data Processing Workflow.
Uses environment variables for secure configuration.

Create a .env file from .env.example and update with your database settings.
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# =============================================================================
# DATABASE CONFIGURATION - LOADED FROM ENVIRONMENT VARIABLES
# =============================================================================

# PostgreSQL Database Connection Settings
DATABASE_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'database': os.getenv('DB_NAME', 'argo_index'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', ''),  # Must be set in .env file
    'port': int(os.getenv('DB_PORT', 5432))
}

# Database Table Settings
TABLE_NAME = os.getenv('TABLE_NAME', 'argo_index_testing_2')

# Performance Settings
MAX_DB_WORKERS = int(os.getenv('MAX_DB_WORKERS', 12))

# =============================================================================
# INSTRUCTIONS FOR SETUP
# =============================================================================

"""
To set up your database:

1. Install PostgreSQL on your system
2. Create a database (e.g., 'argo_index')
3. Update the DATABASE_CONFIG above with your settings:
   - host: Your database server (usually 'localhost')
   - database: Your database name
   - user: Your PostgreSQL username
   - password: Your PostgreSQL password
   - port: Your PostgreSQL port (default 5432)
4. Update TABLE_NAME if you want a different table name

The workflow will automatically create the required table structure.

Example PostgreSQL commands:
```sql
CREATE DATABASE argo_index;
-- The table will be created automatically by the workflow
```
"""

# =============================================================================
# DO NOT EDIT BELOW THIS LINE
# =============================================================================

def get_db_config():
    """Get database configuration"""
    return DATABASE_CONFIG.copy()

def get_table_name():
    """Get table name"""
    return TABLE_NAME

def get_max_workers():
    """Get maximum database workers"""
    return MAX_DB_WORKERS

# Validation function
def validate_config():
    """Validate database configuration"""
    required_keys = ['host', 'database', 'user', 'password', 'port']
    for key in required_keys:
        if key not in DATABASE_CONFIG:
            raise ValueError(f"Missing required database config: {key}")
        if not DATABASE_CONFIG[key]:
            raise ValueError(f"Empty database config value: {key}")
    
    if not TABLE_NAME:
        raise ValueError("TABLE_NAME cannot be empty")
    
    return True

# Test connection function
def test_connection():
    """Test database connection"""
    try:
        import psycopg2
        conn = psycopg2.connect(**DATABASE_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        conn.close()
        print("✅ Database connection successful!")
        return True
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False

if __name__ == "__main__":
    print("="*60)
    print("DATABASE CONFIGURATION TEST")
    print("="*60)
    
    try:
        validate_config()
        print("✅ Configuration validation passed")
        
        print("\nTesting database connection...")
        if test_connection():
            print("\n🎉 Database setup is working correctly!")
        else:
            print("\n⚠️ Please check your database settings and ensure PostgreSQL is running.")
            
    except Exception as e:
        print(f"❌ Configuration error: {e}")
    
    print("\nCurrent configuration:")
    print(f"Host: {DATABASE_CONFIG['host']}")
    print(f"Database: {DATABASE_CONFIG['database']}")
    print(f"User: {DATABASE_CONFIG['user']}")
    print(f"Table: {TABLE_NAME}")
    print("="*60)