from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, ConfigDict
from uuid import UUID
from datetime import datetime

# Use configured default schema instead of hardcoding "public"
from utils.config import db_schema as DEFAULT_DB_SCHEMA

# Import from styles model for style configuration
from styles.models.model import (
    ClassificationMethod,
    LayerType,
    DataSource,
)


class StyleConfigForColumn(BaseModel):
    """Configuration for styling a single column."""
    color_by: str = Field(..., description="Column name to style by")
    layer_type: Optional[LayerType] = None
    classification_method: Optional[ClassificationMethod] = None
    num_classes: Optional[int] = Field(None, ge=2, le=12)
    color_palette: Optional[str] = None
    custom_colors: Optional[List[str]] = None
    manual_breaks: Optional[List[float]] = None
    fill_opacity: Optional[float] = Field(None, ge=0, le=1)
    stroke_color: Optional[str] = None
    stroke_width: Optional[float] = Field(None, ge=0)


class RegisterDatasetFormData(BaseModel):
    """Form data model for register dataset endpoint to avoid schema shadowing in FastAPI."""
    model_config = ConfigDict(populate_by_name=True)
    
    table_name: str = Field(..., description="Name for the database table")
    db_schema: str = Field(default=DEFAULT_DB_SCHEMA, alias="schema", description="Database schema")
    uploaded_by: Optional[str] = Field(None, description="User who uploaded the dataset")
    layer_name: Optional[str] = Field(None, description="Name for the GeoServer layer")
    tags: Optional[str] = Field(None, description="Comma-separated tags")
    workspace: str = Field(default="metastring", description="GeoServer workspace")
    store_name: Optional[str] = Field(None, description="GeoServer store name")
    name_of_dataset: str = Field(..., description="Name of the dataset")
    theme: Optional[str] = None
    keywords: Optional[str] = Field(None, description="Comma-separated keywords")
    purpose_of_creating_data: Optional[str] = None
    access_constraints: Optional[str] = None
    use_constraints: Optional[str] = None
    data_type: Optional[str] = None
    contact_person: Optional[str] = None
    organization: Optional[str] = None
    mailing_address: Optional[str] = None
    city_locality_country: Optional[str] = None
    country: Optional[str] = None
    contact_email: Optional[str] = None
    style_configs_json: str = Field(
        ...,
        description="JSON array of style configurations"
    )
    data_source: str = Field(default="postgis", description="Data source: 'postgis' or 'geoserver'")
    publish_styles_to_geoserver: bool = Field(default=True)
    attach_styles_to_layer: bool = Field(default=True)
    user_id: Optional[str] = None
    user_email: Optional[str] = None


class RegisterDatasetRequest(BaseModel):
    """Request model for registering a complete dataset."""
    model_config = ConfigDict(populate_by_name=True)
    
    # Parameters for create-table-and-insert1
    table_name: str = Field(..., description="Name for the database table")
    db_schema: str = Field(default=DEFAULT_DB_SCHEMA, alias="schema", description="Database schema")
    uploaded_by: Optional[str] = Field(None, description="User who uploaded the dataset")
    layer_name: Optional[str] = Field(None, description="Name for the GeoServer layer")
    tags: Optional[List[str]] = Field(None, description="Tags for the dataset")
    workspace: str = Field(default="metastring", description="GeoServer workspace")
    store_name: Optional[str] = Field(None, description="GeoServer store name")
    
    # Metadata parameters (mapped from create-table-and-insert1 where possible)
    name_of_dataset: str = Field(..., description="Name of the dataset")
    theme: Optional[str] = None
    keywords: Optional[List[str]] = None
    purpose_of_creating_data: Optional[str] = None
    access_constraints: Optional[str] = None
    use_constraints: Optional[str] = None
    data_type: Optional[str] = None
    contact_person: Optional[str] = None
    organization: Optional[str] = None
    mailing_address: Optional[str] = None
    city_locality_country: Optional[str] = None
    country: Optional[str] = None
    contact_email: Optional[str] = None
    
    # Style generation parameters
    style_configs: List[StyleConfigForColumn] = Field(
        ..., 
        description="List of style configurations, one for each column to style"
    )
    data_source: DataSource = Field(
        default=DataSource.POSTGIS,
        description="Data source type for style generation"
    )
    publish_styles_to_geoserver: bool = Field(
        default=True,
        description="Whether to publish styles to GeoServer"
    )
    attach_styles_to_layer: bool = Field(
        default=True,
        description="Whether to attach styles as default styles to the layer"
    )
    user_id: Optional[str] = None
    user_email: Optional[str] = None


class RegisterDatasetResponse(BaseModel):
    """Response model for dataset registration."""
    success: bool
    message: str
    dataset_id: Optional[UUID] = None
    upload_log_id: Optional[UUID] = None
    table_name: str
    layer_name: str
    workspace: str
    metadata_id: Optional[UUID] = None
    styles_created: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="List of created styles with their details"
    )
    created_at: datetime = Field(default_factory=datetime.now)


class RegisterShapefileFormData(BaseModel):
    """Form data model for register shapefile endpoint to avoid schema shadowing in FastAPI."""
    model_config = ConfigDict(populate_by_name=True)
    
    uploaded_by: Optional[str] = Field(None, description="User who uploaded the shapefile")
    store_name: Optional[str] = Field(None, description="GeoServer store name (datastore)")
    layer_name: Optional[str] = Field(None, description="Name for the GeoServer layer (feature type)")
    tags: Optional[str] = Field(None, description="Comma-separated tags")
    workspace: str = Field(default="metastring", description="GeoServer workspace")
    name_of_dataset: str = Field(..., description="Name of the dataset")
    theme: Optional[str] = None
    keywords: Optional[str] = Field(None, description="Comma-separated keywords")
    purpose_of_creating_data: Optional[str] = None
    access_constraints: Optional[str] = None
    use_constraints: Optional[str] = None
    data_type: Optional[str] = None
    contact_person: Optional[str] = None
    organization: Optional[str] = None
    mailing_address: Optional[str] = None
    city_locality_country: Optional[str] = None
    country: Optional[str] = None
    contact_email: Optional[str] = None
    style_configs_json: str = Field(
        ...,
        description="JSON array of style configurations"
    )
    publish_styles_to_geoserver: bool = Field(default=True)
    attach_styles_to_layer: bool = Field(default=True)
    user_id: Optional[str] = None
    user_email: Optional[str] = None


class RegisterShapefileRequest(BaseModel):
    """Request model for registering a complete shapefile."""
    model_config = ConfigDict(populate_by_name=True)
    
    # Parameters for shapefile upload
    uploaded_by: Optional[str] = Field(None, description="User who uploaded the shapefile")
    store_name: Optional[str] = Field(None, description="GeoServer store name (datastore)")
    layer_name: Optional[str] = Field(None, description="Name for the GeoServer layer (feature type)")
    tags: Optional[List[str]] = Field(None, description="Tags for the dataset")
    workspace: str = Field(default="metastring", description="GeoServer workspace")
    
    # Metadata parameters
    name_of_dataset: str = Field(..., description="Name of the dataset")
    theme: Optional[str] = None
    keywords: Optional[List[str]] = None
    purpose_of_creating_data: Optional[str] = None
    access_constraints: Optional[str] = None
    use_constraints: Optional[str] = None
    data_type: Optional[str] = None
    contact_person: Optional[str] = None
    organization: Optional[str] = None
    mailing_address: Optional[str] = None
    city_locality_country: Optional[str] = None
    country: Optional[str] = None
    contact_email: Optional[str] = None
    
    # Style generation parameters
    style_configs: List[StyleConfigForColumn] = Field(
        ..., 
        description="List of style configurations, one for each column to style"
    )
    data_source: DataSource = Field(
        default=DataSource.GEOSERVER,
        description="Data source type for style generation (always 'geoserver' for shapefiles)"
    )
    publish_styles_to_geoserver: bool = Field(
        default=True,
        description="Whether to publish styles to GeoServer"
    )
    attach_styles_to_layer: bool = Field(
        default=True,
        description="Whether to attach styles as default styles to the layer"
    )
    user_id: Optional[str] = None
    user_email: Optional[str] = None


class RegisterShapefileResponse(BaseModel):
    """Response model for shapefile registration."""
    success: bool
    message: str
    dataset_id: Optional[UUID] = None
    upload_log_id: Optional[UUID] = None
    store_name: str
    layer_name: str
    workspace: str
    metadata_id: Optional[UUID] = None
    styles_created: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="List of created styles with their details"
    )
    created_at: datetime = Field(default_factory=datetime.now)

