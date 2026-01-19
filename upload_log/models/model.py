from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional
import uuid

from pydantic import BaseModel, Field, ConfigDict, validator


class DataType(str, Enum):
    VECTOR = "vector"
    RASTER = "raster"
    UNKNOWN = "unknown"


class UploadLogBase(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    # Backward-compatible field:
    # - internal attribute remains `layer_name` (so existing code keeps working)
    # - external JSON key becomes `store_name`
    layer_name: str = Field(
        ...,
        alias="store_name",
        description="GeoServer datastore/store name for the uploaded dataset",
    )
    file_format: str = Field(..., description="Source file format or extension")
    data_type: DataType = Field(..., description="Spatial data classification (vector/raster)")
    crs: Optional[str] = Field(None, description="Coordinate reference system identifier")
    bbox: Optional[Dict[str, float]] = Field(
        default=None,
        description="Spatial bounding box in geographic coordinates",
    )
    source_path: str = Field(..., description="Filesystem path where the original upload is stored")
    geoserver_layer: Optional[str] = Field(
        default=None,
        description="GeoServer layer name if published",
    )
    tags: Optional[List[str]] = Field(default=None, description="Arbitrary labels for grouping")
    uploaded_by: str = Field(..., description="Identifier for the user that performed the upload")


class UploadLogCreate(UploadLogBase):
    uploaded_on: Optional[datetime] = Field(
        default=None, description="Explicit upload timestamp override"
    )

    @validator("tags", pre=True)
    def _normalize_tags(cls, value):
        if isinstance(value, str):
            value = [tag.strip() for tag in value.split(",") if tag.strip()]
        return value


class UploadLogOut(UploadLogBase):
    id: uuid.UUID
    uploaded_on: datetime

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


class UploadLogFilter(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: Optional[uuid.UUID] = None
    layer_name: Optional[str] = Field(default=None, alias="store_name")
    file_format: Optional[str] = None
    data_type: Optional[DataType] = None
    crs: Optional[str] = None
    bbox: Optional[Dict[str, float]] = None
    source_path: Optional[str] = None
    geoserver_layer: Optional[str] = None
    tags: Optional[List[str]] = None
    uploaded_by: Optional[str] = None
    uploaded_on: Optional[datetime] = None

    # model_config above enables accepting both `layer_name` and `store_name`
