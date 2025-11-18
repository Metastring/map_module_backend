"""
Project Setup and Deployment Guide
Run this script to set up the complete unified data management system
"""
import subprocess
import sys
import os
from pathlib import Path

def run_command(command, description, shell=True):
    """Run a command and handle errors"""
    print(f"⏳ {description}...")
    try:
        result = subprocess.run(command, shell=shell, check=True, capture_output=True, text=True)
        print(f"✅ {description} completed")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed: {e.stderr}")
        return False

def check_requirements():
    """Check system requirements"""
    print("🔍 Checking system requirements...")
    
    # Check Python version
    python_version = sys.version_info
    if python_version.major < 3 or (python_version.major == 3 and python_version.minor < 8):
        print(f"❌ Python 3.8+ required. Current: {python_version.major}.{python_version.minor}")
        return False
    print(f"✅ Python {python_version.major}.{python_version.minor} detected")
    
    # Check if PostgreSQL with PostGIS is available
    # This would typically require connecting to test, but we'll assume it's configured
    print("✅ Assuming PostgreSQL with PostGIS is configured (update secure.ini)")
    
    return True

def install_dependencies():
    """Install Python dependencies"""
    if not run_command("pip install -r requirements.txt", "Installing Python dependencies"):
        return False
    return True

def setup_environment():
    """Set up environment variables and configuration"""
    print("📝 Setting up environment configuration...")
    
    # Check if secure.ini exists
    if not os.path.exists("secure.ini"):
        print("⚠️  secure.ini not found. Creating template...")
        template_config = """[database]
host = localhost
port = 5432
dbname = your_database_name
user = your_username
password = your_password

[geoserver]
url = http://localhost:8080/geoserver
username = admin
password = geoserver
workspace = default

[app]
debug = true
cors_origins = http://localhost:3000,http://localhost:8080
"""
        with open("secure.ini", "w") as f:
            f.write(template_config)
        print("📝 Created secure.ini template - please update with your settings")
        return False
    
    print("✅ secure.ini configuration found")
    return True

def initialize_database():
    """Initialize the database schema"""
    return run_command("python scripts/init_database.py", "Initializing database schema")

def start_application():
    """Start the FastAPI application"""
    print("🚀 Starting FastAPI application...")
    print("📖 API Documentation will be available at: http://localhost:8000/docs")
    print("🔍 GraphQL interface will be available at: http://localhost:8000/graphql")
    print("💡 Press Ctrl+C to stop the server")
    
    try:
        subprocess.run("uvicorn main:app --reload --host 0.0.0.0 --port 8000", shell=True)
    except KeyboardInterrupt:
        print("\n🛑 Server stopped")

def main():
    """Main setup routine"""
    print("🌍 Unified Data Management System Setup")
    print("=" * 50)
    
    # Step 1: Check requirements
    if not check_requirements():
        print("❌ Requirements check failed")
        sys.exit(1)
    
    # Step 2: Install dependencies
    if not install_dependencies():
        print("❌ Dependency installation failed")
        sys.exit(1)
    
    # Step 3: Setup environment
    if not setup_environment():
        print("❌ Please configure secure.ini and run setup again")
        sys.exit(1)
    
    # Step 4: Initialize database
    if not initialize_database():
        print("❌ Database initialization failed")
        print("💡 Make sure PostgreSQL with PostGIS is running and secure.ini is configured")
        sys.exit(1)
    
    print("\n🎉 Setup completed successfully!")
    print("\nSystem Capabilities:")
    print("📁 Multi-format data upload (Shapefile, GeoJSON, CSV, Raster)")
    print("🗃️  Dynamic attribute storage with JSONB")
    print("🔍 Advanced spatial and attribute querying")
    print("🗺️  Automatic GeoServer layer publishing")
    print("📊 Statistical analysis and aggregation")
    print("🔄 Bulk operations and data management")
    print("📈 Real-time system monitoring")
    
    print("\nAPI Endpoints Overview:")
    print("• POST /unified-data/upload - Upload datasets")
    print("• GET /unified-data/datasets - List all datasets")
    print("• GET /unified-data/datasets/{id}/features - Query features")
    print("• POST /unified-data/query/spatial - Spatial queries")
    print("• POST /unified-data/query/attributes - Attribute-based search")
    print("• GET /unified-data/categories - Manage categories")
    print("• GET /unified-data/health - System health check")
    
    # Ask if user wants to start the application
    response = input("\n🚀 Start the application now? (y/N): ").lower().strip()
    if response in ['y', 'yes']:
        start_application()
    else:
        print("\n💡 To start manually: uvicorn main:app --reload")
        print("📖 Visit http://localhost:8000/docs for API documentation")

if __name__ == "__main__":
    main()