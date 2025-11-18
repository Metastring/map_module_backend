"""
Pydantic models for the unified data management system
"""
from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict, Any, Union
from datetime import datetime
from enum import Enum
import uuid


class DatasetType(str, Enum):
    VECTOR = "vector"
    RASTER = "raster"
    SHAPEFILE = "shapefile"
    GEOJSON = "geojson"
    CSV = "csv"
    GEOPACKAGE = "geopackage"
    KML = "kml"


class GeometryType(str, Enum):
    POINT = "POINT"
    LINESTRING = "LINESTRING"
    POLYGON = "POLYGON"
    MULTIPOINT = "MULTIPOINT"
    MULTILINESTRING = "MULTILINESTRING"
    MULTIPOLYGON = "MULTIPOLYGON"
    RASTER = "RASTER"


class DatasetStatus(str, Enum):
    UPLOADED = "uploaded"
    PROCESSING = "processing"
    PROCESSED = "processed"
    PUBLISHED = "published"
    ERROR = "error"


class UpdateFrequency(str, Enum):
    REAL_TIME = "real_time"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    YEARLY = "yearly"
    ONE_TIME = "one_time"


# Category Models
class DatasetCategoryBase(BaseModel):
    name: str = Field(..., max_length=100, description="Category identifier (e.g., 'climate')")
    display_name: str = Field(..., max_length=200, description="Human-readable category name")
    description: Optional[str] = Field(None, description="Category description")


class DatasetCategoryCreate(DatasetCategoryBase):
    pass


class DatasetCategoryOut(DatasetCategoryBase):
    id: int
    created_at: datetime
    
    class Config:
        orm_mode = True


# Dataset Models
class DatasetBase(BaseModel):
    name: str = Field(..., max_length=200, description="Dataset identifier")
    display_name: Optional[str] = Field(None, max_length=300, description="Human-readable dataset name")
    description: Optional[str] = Field(None, description="Dataset description")
    dataset_type: DatasetType = Field(..., description="Type of dataset")
    geometry_type: Optional[GeometryType] = Field(None, description="Geometry type for spatial data")
    crs: str = Field(default="EPSG:4326", description="Coordinate Reference System")
    source: Optional[str] = Field(None, max_length=200, description="Data source (e.g., 'IMD', 'NRSC')")
    source_url: Optional[str] = Field(None, description="Source URL or reference")
    update_frequency: Optional[UpdateFrequency] = Field(None, description="How often data is updated")
    spatial_resolution: Optional[str] = Field(None, max_length=100, description="Spatial resolution (for raster)")
    temporal_coverage: Optional[str] = Field(None, max_length=100, description="Temporal coverage period")


class DatasetCreate(DatasetBase):
    category_id: int = Field(..., description="Category this dataset belongs to")
    uploaded_by: str = Field(..., max_length=100, description="User who uploaded the dataset")


class DatasetUpdate(BaseModel):
    display_name: Optional[str] = None
    description: Optional[str] = None
    source: Optional[str] = None
    source_url: Optional[str] = None
    update_frequency: Optional[UpdateFrequency] = None
    spatial_resolution: Optional[str] = None
    temporal_coverage: Optional[str] = None


class BoundingBox(BaseModel):
    minx: float = Field(..., description="Minimum X coordinate")
    miny: float = Field(..., description="Minimum Y coordinate") 
    maxx: float = Field(..., description="Maximum X coordinate")
    maxy: float = Field(..., description="Maximum Y coordinate")


class DatasetOut(DatasetBase):
    id: int
    uuid: uuid.UUID
    file_format: Optional[str]
    bbox: Optional[BoundingBox] = None
    file_size_mb: Optional[float]
    geoserver_workspace: Optional[str]
    geoserver_layer_name: Optional[str]
    is_published: bool
    wms_url: Optional[str]
    wfs_url: Optional[str]
    status: DatasetStatus
    error_message: Optional[str]
    uploaded_by: str
    created_at: datetime
    updated_at: Optional[datetime]
    category: Optional[DatasetCategoryOut]
    feature_count: Optional[int] = Field(None, description="Number of features in dataset")
    
    class Config:
        orm_mode = True
    
    @validator('bbox', pre=True, always=True)
    def create_bbox(cls, v, values):
        if 'bbox_minx' in values and values.get('bbox_minx') is not None:
            return BoundingBox(
                minx=values.get('bbox_minx'),
                miny=values.get('bbox_miny'),
                maxx=values.get('bbox_maxx'),
                maxy=values.get('bbox_maxy')
            )
        return v


# Feature Models
class DatasetFeatureBase(BaseModel):
    attributes: Dict[str, Any] = Field(default_factory=dict, description="Dynamic attributes as key-value pairs")
    feature_id: Optional[str] = Field(None, max_length=100, description="Original feature ID")


class DatasetFeatureCreate(DatasetFeatureBase):
    geometry: Optional[Dict[str, Any]] = Field(None, description="GeoJSON geometry object")


class DatasetFeatureOut(DatasetFeatureBase):
    id: int
    dataset_id: int
    geometry: Optional[Dict[str, Any]] = Field(None, description="GeoJSON geometry object")
    created_at: datetime
    
    class Config:
        orm_mode = True


# Query and Filter Models
class SpatialFilter(BaseModel):
    geometry: Dict[str, Any] = Field(..., description="GeoJSON geometry for spatial filtering")
    operation: str = Field(default="intersects", description="Spatial operation: intersects, within, contains")


class AttributeFilter(BaseModel):
    field: str = Field(..., description="Attribute field name")
    operator: str = Field(..., description="Comparison operator: eq, ne, gt, lt, gte, lte, like, ilike, in")
    value: Union[str, int, float, List[Union[str, int, float]]] = Field(..., description="Filter value(s)")


class DatasetQuery(BaseModel):
    dataset_ids: Optional[List[int]] = Field(None, description="Specific dataset IDs to query")
    category_ids: Optional[List[int]] = Field(None, description="Category IDs to include")
    dataset_types: Optional[List[DatasetType]] = Field(None, description="Dataset types to include")
    spatial_filter: Optional[SpatialFilter] = Field(None, description="Spatial filter criteria")
    attribute_filters: Optional[List[AttributeFilter]] = Field(None, description="Attribute filter criteria")
    bbox: Optional[BoundingBox] = Field(None, description="Bounding box filter")
    limit: int = Field(default=1000, ge=1, le=10000, description="Maximum number of features to return")
    offset: int = Field(default=0, ge=0, description="Number of features to skip")
    include_geometry: bool = Field(default=True, description="Whether to include geometry in response")


# Upload Models
class DatasetUploadRequest(BaseModel):
    name: str = Field(..., description="Dataset name")
    category_id: int = Field(..., description="Category ID")
    description: Optional[str] = Field(None, description="Dataset description")
    source: Optional[str] = Field(None, description="Data source")
    uploaded_by: str = Field(..., description="User uploading the dataset")
    
    # Processing options
    auto_publish: bool = Field(default=True, description="Automatically publish to GeoServer")
    geoserver_workspace: str = Field(default="unified_data", description="GeoServer workspace")
    
    # Override detection
    force_dataset_type: Optional[DatasetType] = Field(None, description="Force specific dataset type")
    force_crs: Optional[str] = Field(None, description="Force specific CRS")


class DatasetUploadResponse(BaseModel):
    dataset_id: int
    dataset_uuid: uuid.UUID
    status: DatasetStatus
    message: str
    processing_details: Optional[Dict[str, Any]] = None


# Processing Log Models
class ProcessingLogCreate(BaseModel):
    dataset_id: int
    processing_step: str
    status: str
    message: Optional[str] = None
    details: Optional[Dict[str, Any]] = None


class ProcessingLogOut(BaseModel):
    id: int
    dataset_id: int
    processing_step: str
    status: str
    message: Optional[str]
    details: Optional[Dict[str, Any]]
    started_at: datetime
    completed_at: Optional[datetime]
    duration_seconds: Optional[float]
    
    class Config:
        orm_mode = True


# Statistics and Summary Models
class DatasetStatistics(BaseModel):
    total_datasets: int
    datasets_by_type: Dict[str, int]
    datasets_by_category: Dict[str, int]
    datasets_by_status: Dict[str, int]
    total_features: int
    total_storage_mb: float


class CategoryStatistics(BaseModel):
    category: DatasetCategoryOut
    dataset_count: int
    feature_count: int
    storage_mb: float
    latest_update: Optional[datetime]


# Bulk Operations
class BulkDatasetQuery(BaseModel):
    dataset_ids: List[int] = Field(..., description="List of dataset IDs")
    operation: str = Field(..., description="Operation: publish, unpublish, delete")
    parameters: Optional[Dict[str, Any]] = Field(None, description="Operation-specific parameters")


class BulkOperationResponse(BaseModel):
    total_requested: int
    successful: int
    failed: int
    results: List[Dict[str, Any]]
    errors: List[Dict[str, Any]]