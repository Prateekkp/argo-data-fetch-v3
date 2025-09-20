#!/usr/bin/env python3
"""
ARGO Data Processing - Initial Setup Script
===========================================

This script helps you set up the ARGO data processing environment.
Run this before using the main workflow for the first time.
"""

import os
import sys
import subprocess
import psycopg2
from database_setup import DATABASE_CONFIG, TABLE_NAME

def check_python_version():
    """Check if Python version is compatible."""
    if sys.version_info < (3, 7):
        print("❌ Error: Python 3.7 or higher is required")
        print(f"Current version: {sys.version}")
        return False
    print(f"✅ Python version: {sys.version.split()[0]}")
    return True

def check_packages():
    """Check if required packages are installed."""
    required_packages = [
        'aiohttp', 'aiofiles', 'psycopg2', 'pandas', 
        'xarray', 'dask', 'numpy', 'python-dotenv'
    ]
    
    missing_packages = []
    for package in required_packages:
        try:
            __import__(package.replace('-', '_'))
            print(f"✅ {package}")
        except ImportError:
            missing_packages.append(package)
            print(f"❌ {package}")
    
    if missing_packages:
        print(f"\n📦 Installing missing packages: {', '.join(missing_packages)}")
        try:
            subprocess.check_call([sys.executable, '-m', 'pip', 'install'] + missing_packages)
            print("✅ All packages installed successfully")
        except subprocess.CalledProcessError:
            print("❌ Failed to install packages. Please install manually:")
            print(f"pip install {' '.join(missing_packages)}")
            return False
    
    return True

def test_database_connection():
    """Test PostgreSQL database connection."""
    try:
        conn = psycopg2.connect(**DATABASE_CONFIG)
        cursor = conn.cursor()
        
        # Test connection
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        print(f"✅ Database connection successful")
        print(f"PostgreSQL version: {version.split()[1]}")
        
        # Check if table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = %s
            );
        """, (TABLE_NAME,))
        
        table_exists = cursor.fetchone()[0]
        if table_exists:
            print(f"✅ Table '{TABLE_NAME}' exists")
        else:
            print(f"ℹ️  Table '{TABLE_NAME}' will be created when needed")
        
        cursor.close()
        conn.close()
        return True
        
    except psycopg2.Error as e:
        print(f"❌ Database connection failed: {e}")
        print("\nTroubleshooting:")
        print("1. Make sure PostgreSQL is running")
        print("2. Check your .env file database credentials")
        print("3. Ensure the database exists")
        print(f"4. Try: CREATE DATABASE {DATABASE_CONFIG['database']};")
        return False

def create_directories():
    """Create necessary directories."""
    directories = [
        'argo_index',
        'argo_float_data', 
        'processed_data',
        'processed_data/atlantic',
        'processed_data/pacific',
        'processed_data/indian',
        'processed_data/arctic',
        'processed_data/southern'
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        print(f"✅ Directory: {directory}")

def check_env_file():
    """Check if .env file exists and is configured."""
    if not os.path.exists('.env'):
        print("❌ .env file not found")
        print("Please copy .env.example to .env and configure it:")
        print("copy .env.example .env  (Windows)")
        print("cp .env.example .env    (Linux/Mac)")
        return False
    
    print("✅ .env file found")
    
    # Check if password is set
    with open('.env', 'r') as f:
        content = f.read()
        if 'DB_PASSWORD=' in content and 'your_password_here' not in content:
            print("✅ Database password configured")
        else:
            print("⚠️  Please set your database password in .env file")
    
    return True

def main():
    """Run the complete setup check."""
    print("🌊 ARGO Data Processing - Setup Check")
    print("=" * 50)
    
    steps = [
        ("Checking Python version", check_python_version),
        ("Checking .env configuration", check_env_file),
        ("Checking Python packages", check_packages),
        ("Testing database connection", test_database_connection),
        ("Creating directories", create_directories)
    ]
    
    all_passed = True
    for step_name, step_func in steps:
        print(f"\n{step_name}...")
        if not step_func():
            all_passed = False
    
    print("\n" + "=" * 50)
    if all_passed:
        print("🎉 Setup complete! You can now run:")
        print("python main_workflow.py")
    else:
        print("❌ Setup incomplete. Please fix the issues above.")
    
    return all_passed

if __name__ == "__main__":
    main()