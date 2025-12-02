"""
Geospatial Hierarchy Ingestion System
=====================================

This module handles hierarchical geospatial data (State -> District -> Taluka -> Point)
with support for multiple data formats and automated ingestion workflows.

Architecture:
- geo_hierarchy/: Core hierarchy models and services
- ingestion/: Data format handlers and processors  
- spatial/: Geometry operations and indexing
- classification/: Layer type detection and metadata extraction
"""

from sqlalchemy import Column, Integer, String, Text, DateTime, Float, Boolean, ForeignKey, Index
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import relationship, declarative_base
from sqlalchemy.sql import func
from geoalchemy2 import Geometry
import uuid

Base = declarative_base()

class AdministrativeLevel(Base):
    """Administrative hierarchy levels (state, district, taluka, etc.)"""
    __tablename__ = "administrative_levels"
    
    id = Column(Integer, primary_key=True)
    level_code = Column(String(20), unique=True, nullable=False)  # STATE, DISTRICT, TALUKA, VILLAGE
    level_name = Column(String(100), nullable=False)
    hierarchy_order = Column(Integer, nullable=False)  # 1=state, 2=district, 3=taluka, 4=village
    created_on = Column(DateTime, default=func.now())
    
    # Relationships
    boundaries = relationship("AdministrativeBoundary", back_populates="level")
    
    __table_args__ = (
        Index('idx_admin_level_order', 'hierarchy_order'),
    )

class AdministrativeBoundary(Base):
    """Individual administrative boundaries at any level"""
    __tablename__ = "administrative_boundaries"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    level_id = Column(Integer, ForeignKey("administrative_levels.id"), nullable=False)
    parent_id = Column(UUID(as_uuid=True), ForeignKey("administrative_boundaries.id"), nullable=True)
    
    # Identity
    name = Column(String(200), nullable=False)
    code = Column(String(50))  # Official code (state code, district code, etc.)
    name_local = Column(String(200))  # Local language name
    
    # Geometry
    geometry = Column(Geometry('MULTIPOLYGON', srid=4326), nullable=False)
    centroid = Column(Geometry('POINT', srid=4326))
    area_sq_km = Column(Float)
    
    # Metadata
    properties = Column(JSONB)  # Flexible properties (population, area, etc.)
    source_dataset = Column(String(200))
    data_source = Column(String(200))  # Survey of India, Census, etc.
    accuracy_level = Column(String(20))  # HIGH, MEDIUM, LOW
    
    # Timestamps
    created_on = Column(DateTime, default=func.now())
    updated_on = Column(DateTime, default=func.now(), onupdate=func.now())
    
    # Relationships
    level = relationship("AdministrativeLevel", back_populates="boundaries")
    parent = relationship("AdministrativeBoundary", remote_side=[id], backref="children")
    point_features = relationship("PointFeature", back_populates="admin_boundary")
    
    __table_args__ = (
        Index('idx_admin_boundary_level', 'level_id'),
        Index('idx_admin_boundary_parent', 'parent_id'),
        Index('idx_admin_boundary_geom', 'geometry', postgresql_using='gist'),
        Index('idx_admin_boundary_name', 'name'),
    )

class DatasetType(Base):
    """Classification of geospatial datasets"""
    __tablename__ = "dataset_types"
    
    id = Column(Integer, primary_key=True)
    type_code = Column(String(50), unique=True, nullable=False)  # BOUNDARY, INFRASTRUCTURE, NATURAL, DEMOGRAPHIC
    type_name = Column(String(100), nullable=False)
    description = Column(Text)
    default_style = Column(JSONB)  # Default styling rules
    
    # Relationships
    datasets = relationship("GeospatialDataset", back_populates="dataset_type")

class GeospatialDataset(Base):
    """Metadata for any geospatial dataset"""
    __tablename__ = "geospatial_datasets"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    dataset_type_id = Column(Integer, ForeignKey("dataset_types.id"), nullable=False)
    
    # Identity
    name = Column(String(200), nullable=False)
    title = Column(String(300))
    description = Column(Text)
    
    # Source
    source_format = Column(String(50))  # SHAPEFILE, GEOJSON, CSV, KML, GPX, POSTGIS
    source_path = Column(String(500))
    geoserver_workspace = Column(String(100))
    geoserver_layer = Column(String(200))
    
    # Spatial metadata
    geometry_type = Column(String(50))  # POINT, LINESTRING, POLYGON, MULTIPOLYGON
    coordinate_system = Column(String(50), default="EPSG:4326")
    bbox_min_x = Column(Float)
    bbox_min_y = Column(Float) 
    bbox_max_x = Column(Float)
    bbox_max_y = Column(Float)
    
    # Administrative context
    admin_level_id = Column(Integer, ForeignKey("administrative_levels.id"), nullable=True)
    coverage_area = Column(String(200))  # "Maharashtra", "All India", etc.
    
    # Processing metadata
    feature_count = Column(Integer)
    attributes_schema = Column(JSONB)  # Column definitions and sample values
    processing_status = Column(String(50), default="UPLOADED")  # UPLOADED, PROCESSED, PUBLISHED, ERROR
    processing_log = Column(Text)
    
    # Style and visualization
    default_style = Column(JSONB)
    legend_config = Column(JSONB)
    
    # Timestamps
    uploaded_on = Column(DateTime, default=func.now())
    processed_on = Column(DateTime)
    published_on = Column(DateTime)
    
    # Relationships
    dataset_type = relationship("DatasetType", back_populates="datasets")
    admin_level = relationship("AdministrativeLevel")
    point_features = relationship("PointFeature", back_populates="dataset")
    
    __table_args__ = (
        Index('idx_dataset_type', 'dataset_type_id'),
        Index('idx_dataset_status', 'processing_status'),
        Index('idx_dataset_geoserver', 'geoserver_workspace', 'geoserver_layer'),
    )

class PointFeature(Base):
    """Individual point features within datasets"""
    __tablename__ = "point_features"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    dataset_id = Column(UUID(as_uuid=True), ForeignKey("geospatial_datasets.id"), nullable=False)
    admin_boundary_id = Column(UUID(as_uuid=True), ForeignKey("administrative_boundaries.id"), nullable=True)
    
    # Identity
    name = Column(String(300))
    feature_type = Column(String(100))  # hospital, school, wetland, etc.
    
    # Geometry
    geometry = Column(Geometry('POINT', srid=4326), nullable=False)
    
    # Attributes (flexible JSON storage)
    properties = Column(JSONB, nullable=False, default={})
    
    # Classification
    category = Column(String(100))
    subcategory = Column(String(100))
    status = Column(String(50))
    
    # Timestamps
    created_on = Column(DateTime, default=func.now())
    updated_on = Column(DateTime, default=func.now(), onupdate=func.now())
    
    # Relationships
    dataset = relationship("GeospatialDataset", back_populates="point_features")
    admin_boundary = relationship("AdministrativeBoundary", back_populates="point_features")
    
    __table_args__ = (
        Index('idx_point_dataset', 'dataset_id'),
        Index('idx_point_admin', 'admin_boundary_id'),
        Index('idx_point_geom', 'geometry', postgresql_using='gist'),
        Index('idx_point_category', 'category', 'subcategory'),
    )

class IngestionJob(Base):
    """Track data ingestion jobs and their progress"""
    __tablename__ = "ingestion_jobs"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_type = Column(String(50), nullable=False)  # BOUNDARY_INGEST, POINT_INGEST, BATCH_PROCESS
    status = Column(String(50), default="PENDING")  # PENDING, RUNNING, COMPLETED, FAILED
    
    # Input parameters
    source_path = Column(String(500))
    target_dataset_id = Column(UUID(as_uuid=True), ForeignKey("geospatial_datasets.id"))
    parameters = Column(JSONB)  # Job-specific configuration
    
    # Progress tracking
    total_records = Column(Integer)
    processed_records = Column(Integer, default=0)
    failed_records = Column(Integer, default=0)
    
    # Results
    result_log = Column(Text)
    error_details = Column(Text)
    
    # Timestamps
    started_on = Column(DateTime, default=func.now())
    completed_on = Column(DateTime)
    
    # Relationships
    target_dataset = relationship("GeospatialDataset")
    
    __table_args__ = (
        Index('idx_ingestion_status', 'status'),
l̥    )