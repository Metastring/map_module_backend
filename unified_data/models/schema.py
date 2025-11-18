"""
Unified Data Management Database Schema
Supports vector/raster/shapefile storage with dynamic attributes
"""
from sqlalchemy import Column, Integer, String, DateTime, Text, Float, Boolean, ForeignKey, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from geoalchemy2 import Geometry, Raster
import uuid

Base = declarative_base()


class DatasetCategory(Base):
    """
    Categories for organizing datasets (climate, biodiversity, environment, etc.)
    """
    __tablename__ = "dataset_categories"
    __table_args__ = {"schema": "unified_data"}

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), unique=True, nullable=False)  # climate, biodiversity, etc.
    display_name = Column(String(200), nullable=False)
    description = Column(Text)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Relationships
    datasets = relationship("Dataset", back_populates="category")


class Dataset(Base):
    """
    Main dataset table - stores metadata for any type of spatial dataset
    """
    __tablename__ = "datasets"
    __table_args__ = {"schema": "unified_data"}

    id = Column(Integer, primary_key=True, index=True)
    uuid = Column(UUID(as_uuid=True), default=uuid.uuid4, unique=True, nullable=False)
    
    # Basic Information
    name = Column(String(200), nullable=False)
    display_name = Column(String(300))
    description = Column(Text)
    
    # Dataset Type and Format
    dataset_type = Column(String(50), nullable=False)  # vector, raster, shapefile, geojson, csv
    geometry_type = Column(String(50))  # POINT, LINESTRING, POLYGON, MULTIPOLYGON, RASTER
    file_format = Column(String(50))  # .shp, .tif, .geojson, .csv, etc.
    
    # Spatial Information
    crs = Column(String(50), default="EPSG:4326")  # Coordinate Reference System
    bbox_minx = Column(Float)  # Bounding box
    bbox_miny = Column(Float)
    bbox_maxx = Column(Float)
    bbox_maxy = Column(Float)
    
    # Source and Metadata
    source = Column(String(200))  # IMD, NRSC, FSI, etc.
    source_url = Column(Text)
    update_frequency = Column(String(50))  # daily, monthly, yearly
    spatial_resolution = Column(String(100))  # For raster data
    temporal_coverage = Column(String(100))  # 1969-2024, etc.
    
    # File Storage
    original_file_path = Column(Text)  # Path to original uploaded file
    processed_file_path = Column(Text)  # Path to processed file (if applicable)
    file_size_mb = Column(Float)
    
    # Publishing Information
    geoserver_workspace = Column(String(100))
    geoserver_layer_name = Column(String(200))
    is_published = Column(Boolean, default=False)
    wms_url = Column(Text)
    wfs_url = Column(Text)
    
    # Status and Tracking
    status = Column(String(50), default="uploaded")  # uploaded, processing, processed, published, error
    error_message = Column(Text)
    uploaded_by = Column(String(100), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    # Relationships
    category_id = Column(Integer, ForeignKey("unified_data.dataset_categories.id"))
    category = relationship("DatasetCategory", back_populates="datasets")
    features = relationship("DatasetFeature", back_populates="dataset", cascade="all, delete-orphan")


class DatasetFeature(Base):
    """
    Individual features/records within a dataset
    Stores geometry and dynamic attributes in JSONB
    """
    __tablename__ = "dataset_features"
    __table_args__ = {"schema": "unified_data"}

    id = Column(Integer, primary_key=True, index=True)
    dataset_id = Column(Integer, ForeignKey("unified_data.datasets.id"), nullable=False)
    
    # Geometry storage - supports all types
    geometry = Column(Geometry('GEOMETRY', srid=4326))  # Points, Lines, Polygons, Multi*
    
    # Raster storage (optional - for raster datasets)
    raster_data = Column(Raster)  # PostGIS raster column
    
    # Dynamic attributes stored as JSONB
    attributes = Column(JSONB, nullable=False, default={})
    
    # Feature metadata
    feature_id = Column(String(100))  # Original feature ID from source
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Relationships
    dataset = relationship("Dataset", back_populates="features")
    
    # Add indexes for performance
    __table_args__ = (
        {"schema": "unified_data"},
    )


class DatasetProcessingLog(Base):
    """
    Log processing steps and transformations applied to datasets
    """
    __tablename__ = "dataset_processing_logs"
    __table_args__ = {"schema": "unified_data"}

    id = Column(Integer, primary_key=True, index=True)
    dataset_id = Column(Integer, ForeignKey("unified_data.datasets.id"), nullable=False)
    
    processing_step = Column(String(100), nullable=False)  # upload, validate, transform, publish
    status = Column(String(50), nullable=False)  # success, error, in_progress
    message = Column(Text)
    details = Column(JSONB)  # Additional processing details
    
    started_at = Column(DateTime(timezone=True), server_default=func.now())
    completed_at = Column(DateTime(timezone=True))
    duration_seconds = Column(Float)


class DatasetAttributeSchema(Base):
    """
    Optional: Store common attribute schemas for validation and UI generation
    """
    __tablename__ = "dataset_attribute_schemas"
    __table_args__ = {"schema": "unified_data"}

    id = Column(Integer, primary_key=True, index=True)
    dataset_id = Column(Integer, ForeignKey("unified_data.datasets.id"))
    category_id = Column(Integer, ForeignKey("unified_data.dataset_categories.id"))
    
    # Schema definition
    attribute_name = Column(String(100), nullable=False)
    data_type = Column(String(50), nullable=False)  # string, float, integer, boolean, date
    display_name = Column(String(200))
    description = Column(Text)
    is_required = Column(Boolean, default=False)
    default_value = Column(String(200))
    
    # Validation rules
    min_value = Column(Float)
    max_value = Column(Float)
    allowed_values = Column(JSONB)  # For enumerated values
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())


# Add indexes for better query performance
"""
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dataset_features_geometry 
ON unified_data.dataset_features USING GIST (geometry);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dataset_features_attributes 
ON unified_data.dataset_features USING GIN (attributes);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_datasets_category 
ON unified_data.datasets (category_id);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_datasets_type 
ON unified_data.datasets (dataset_type);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_datasets_status 
ON unified_data.datasets (status);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_datasets_bbox 
ON unified_data.datasets (bbox_minx, bbox_miny, bbox_maxx, bbox_maxy);
"""