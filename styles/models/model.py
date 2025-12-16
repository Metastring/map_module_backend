from datetime import datetime
from enum import Enum
import re
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, validator


class ClassificationMethod(str, Enum):

    EQUAL_INTERVAL = "equal_interval"
    QUANTILE = "quantile"
    JENKS = "jenks"
    CATEGORICAL = "categorical"
    MANUAL = "manual"

class LayerType(str, Enum):

    POINT = "point"
    LINE = "line"
    POLYGON = "polygon"
    RASTER = "raster"

# ========== Column Info ==========

class ColumnInfo(BaseModel):

    column_name: str
    data_type: str
    is_nullable: bool = True
    is_numeric: bool = False
    is_categorical: bool = False

# ========== Classification Results ==========

class ClassificationResult(BaseModel):

    method: ClassificationMethod
    breaks: List[float] = []  # For numeric: class boundaries
    categories: List[str] = []  # For categorical: distinct values
    colors: List[str] = []
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    num_classes: int = 5

# ========== MBStyle Output ==========

class MBStyleLayer(BaseModel):

    id: str
    type: str  # fill, line, circle, symbol
    source: Optional[str] = None
    source_layer: Optional[str] = None
    paint: Dict[str, Any] = {}
    layout: Dict[str, Any] = {}
    filter: Optional[List[Any]] = None


class MBStyleOutput(BaseModel):

    version: int = 8
    name: str
    layers: List[MBStyleLayer] = []
    sources: Dict[str, Any] = {}
    sprite: Optional[str] = None
    glyphs: Optional[str] = None

# ========== Style Metadata CRUD ==========

class StyleMetadataBase(BaseModel):

    layer_table_name: str = Field(..., description="PostGIS table name")
    workspace: str = Field(..., description="GeoServer workspace")
    layer_name: Optional[str] = Field(None, description="Display name for the layer")
    color_by: str = Field(..., description="Column to classify by")
    layer_type: LayerType = Field(LayerType.POLYGON, description="Geometry type")
    classification_method: ClassificationMethod = Field(
        ClassificationMethod.EQUAL_INTERVAL,
        description="Classification method",
    )
    num_classes: int = Field(
        5, ge=2, le=12, description="Number of classes (2-12)"
    )
    color_palette: str = Field("YlOrRd", description="ColorBrewer palette name")
    custom_colors: Optional[List[str]] = Field(
        None, description="Custom color list"
    )
    fill_opacity: float = Field(
        0.7, ge=0, le=1, description="Fill opacity (0-1)"
    )
    stroke_color: str = Field("#333333", description="Stroke color")
    stroke_width: float = Field(
        1.0, ge=0, description="Stroke width in pixels"
    )
    manual_breaks: Optional[List[float]] = Field(
        None, description="Manual class breaks"
    )

    @validator("layer_table_name", "workspace", "color_by")
    def validate_identifier(cls, v: str) -> str:
        if not re.match(r"^[a-zA-Z0-9_]+$", v):
            raise ValueError(
                "Must contain only letters, numbers, and underscores"
            )
        return v

    @validator("custom_colors")
    def validate_colors(cls, v: Optional[List[str]]) -> Optional[List[str]]:
        if v is not None:
            for color in v:
                if not re.match(r"^#[0-9A-Fa-f]{6}$", color):
                    raise ValueError(f"Invalid hex color: {color}")
        return v


class StyleMetadataCreate(StyleMetadataBase):

    pass


class StyleMetadataOut(StyleMetadataBase):

    id: int
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    distinct_values: Optional[List[str]] = None
    data_type: Optional[str] = None
    generated_style_name: Optional[str] = None
    is_active: bool = True
    last_generated: Optional[datetime] = None
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


# ========== Audit Log ==========

class AuditLogOut(BaseModel):

    id: int
    action: str
    user_id: Optional[str]
    user_email: Optional[str]
    version: int
    status: str
    error_message: Optional[str]
    created_at: datetime

    class Config:
        from_attributes = True


# ========== Style Generation API ==========

class DataSource(str, Enum):
    """Data source type for style generation."""
    POSTGIS = "postgis"  # Data is in PostGIS database
    GEOSERVER = "geoserver"  # Data is in GeoServer (shapefile or other source, not in DB)


class StyleGenerateRequest(BaseModel):

    layer_table_name: str = Field(..., description="PostGIS table name or GeoServer layer name")
    workspace: str = Field(..., description="GeoServer workspace")
    color_by: str = Field(..., description="Column to classify by")
    data_source: DataSource = Field(
        DataSource.POSTGIS, 
        description="Data source type: 'postgis' for database tables, 'geoserver' for shapefiles/layers not in DB"
    )

    # Optional overrides
    layer_type: Optional[LayerType] = None
    classification_method: Optional[ClassificationMethod] = None
    num_classes: Optional[int] = Field(None, ge=2, le=12)
    color_palette: Optional[str] = None
    custom_colors: Optional[List[str]] = None
    manual_breaks: Optional[List[float]] = None

    # Publishing options
    publish_to_geoserver: bool = Field(
        True, description="Publish style to GeoServer"
    )
    attach_to_layer: bool = Field(
        True, description="Set as default style for layer"
    )

    # User info for audit
    user_id: Optional[str] = None
    user_email: Optional[str] = None


class StyleGenerateResponse(BaseModel):

    success: bool
    message: str
    style_name: str
    mbstyle: Optional[MBStyleOutput] = None
    classification: Optional[ClassificationResult] = None
    published_to_geoserver: bool = False
    attached_to_layer: bool = False
    geoserver_style_url: Optional[str] = None



