from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional

from pydantic import BaseModel, Field, validator


class DataType(str, Enum):
    VECTOR = "vector"
    RASTER = "raster"
    UNKNOWN = "unknown"


class UploadLogBase(BaseModel):
    layer_name: str = Field(..., description="Canonical name for the uploaded layer or dataset")
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
    id: int
    uploaded_on: datetime

    class Config:
        orm_mode = True


class UploadLogFilter(BaseModel):
    id: Optional[int] = None
    layer_name: Optional[str] = None
    file_format: Optional[str] = None
    data_type: Optional[DataType] = None
    crs: Optional[str] = None
    bbox: Optional[Dict[str, float]] = None
    source_path: Optional[str] = None
    geoserver_layer: Optional[str] = None
    tags: Optional[List[str]] = None
    uploaded_by: Optional[str] = None
    uploaded_on: Optional[datetime] = None

