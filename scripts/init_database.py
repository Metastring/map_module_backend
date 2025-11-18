"""
Database initialization script for unified data management system
Run this script to create the necessary database schema and tables
"""
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import logging

from utils.config import database_url
from unified_data.models.schema import Base
from unified_data.models.model import DatasetCategoryCreate
from unified_data.dao.dao import UnifiedDataDAO

logger = logging.getLogger(__name__)


def create_database_schema():
    """Create the database schema and tables"""
    try:
        engine = create_engine(database_url)
        
        # Create schema if it doesn't exist
        with engine.connect() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS unified_data"))
            conn.commit()
        
        # Create all tables
        Base.metadata.create_all(bind=engine)
        
        # Create indexes for better performance
        with engine.connect() as conn:
            indexes = [
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dataset_features_geometry ON unified_data.dataset_features USING GIST (geometry)",
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dataset_features_attributes ON unified_data.dataset_features USING GIN (attributes)",
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_datasets_category ON unified_data.datasets (category_id)",
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_datasets_type ON unified_data.datasets (dataset_type)",
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_datasets_status ON unified_data.datasets (status)",
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_datasets_bbox ON unified_data.datasets (bbox_minx, bbox_miny, bbox_maxx, bbox_maxy)"
            ]
            
            for index_sql in indexes:
                try:
                    conn.execute(text(index_sql))
                    conn.commit()
                except Exception as e:
                    logger.warning(f"Index creation warning (may already exist): {e}")
        
        logger.info("Database schema created successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error creating database schema: {e}")
        return False


def create_default_categories():
    """Create default dataset categories"""
    try:
        engine = create_engine(database_url)
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        db = SessionLocal()
        
        default_categories = [
            {
                "name": "climate",
                "display_name": "Climate & Weather",
                "description": "Temperature, precipitation, humidity, and other meteorological data"
            },
            {
                "name": "biodiversity", 
                "display_name": "Biodiversity & Species",
                "description": "Species occurrence records, distribution ranges, and ecological data"
            },
            {
                "name": "environment",
                "display_name": "Environmental",
                "description": "Land cover, soil types, pollution, and environmental monitoring data"
            },
            {
                "name": "conservation",
                "display_name": "Conservation & Protected Areas", 
                "description": "Protected areas, conservation status, and management data"
            },
            {
                "name": "remote_sensing",
                "display_name": "Remote Sensing",
                "description": "Satellite imagery, NDVI, land surface temperature, and derived products"
            },
            {
                "name": "socioeconomic",
                "display_name": "Socioeconomic",
                "description": "Population, infrastructure, land use, and human activity data"
            },
            {
                "name": "research",
                "display_name": "Research Data",
                "description": "Field surveys, experimental data, and research datasets"
            }
        ]
        
        # Check if categories already exist
        existing_categories = UnifiedDataDAO.get_categories(db)
        existing_names = {cat.name for cat in existing_categories}
        
        created_count = 0
        for category_data in default_categories:
            if category_data["name"] not in existing_names:
                UnifiedDataDAO.create_category(db, category_data)
                created_count += 1
                logger.info(f"Created category: {category_data['display_name']}")
        
        db.close()
        logger.info(f"Created {created_count} default categories")
        return True
        
    except Exception as e:
        logger.error(f"Error creating default categories: {e}")
        return False


def initialize_database():
    """Complete database initialization"""
    print("🚀 Initializing Unified Data Management Database...")
    
    # Step 1: Create schema and tables
    print("📋 Creating database schema and tables...")
    if not create_database_schema():
        print("❌ Failed to create database schema")
        return False
    print("✅ Database schema created")
    
    # Step 2: Create default categories  
    print("📂 Creating default dataset categories...")
    if not create_default_categories():
        print("❌ Failed to create default categories")
        return False
    print("✅ Default categories created")
    
    print("🎉 Database initialization completed successfully!")
    print("\nDefault Categories Created:")
    print("• Climate & Weather")
    print("• Biodiversity & Species") 
    print("• Environmental")
    print("• Conservation & Protected Areas")
    print("• Remote Sensing")
    print("• Socioeconomic")
    print("• Research Data")
    print("\nNext Steps:")
    print("1. Start the application: uvicorn main:app --reload")
    print("2. Visit /docs for API documentation")
    print("3. Upload your first dataset via /unified-data/upload")
    
    return True


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    
    # Initialize database
    success = initialize_database()
    
    if not success:
        exit(1)