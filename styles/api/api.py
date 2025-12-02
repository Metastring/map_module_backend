"""
FastAPI endpoints for style management and generation.
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional
import logging

from database.database import get_db
from geoserver.dao import GeoServerDAO
from utils.config import (
    geoserver_host, 
    geoserver_port, 
    geoserver_username, 
    geoserver_password
)

from ..service.style_service import StyleService
from ..service.color_palettes import get_available_palettes, get_palette_preview
from ..models.schema import (
    StyleMetadataCreate,
    StyleMetadataUpdate,
    StyleMetadataOut,
    StyleGenerateRequest,
    StyleGenerateResponse,
    StyleListResponse,
    ClassificationMethod,
    LayerType,
    LegendResponse,
    ClassificationResult,
    AuditLogOut,
    ColumnInfo,
)

logger = logging.getLogger(__name__)

# Initialize router
router = APIRouter(prefix="/styles", tags=["styles"])

# Initialize GeoServer DAO
geoserver_dao = GeoServerDAO(
    base_url=f"http://{geoserver_host}:{geoserver_port}/geoserver/rest",
    username=geoserver_username,
    password=geoserver_password
)


def get_style_service(db: Session = Depends(get_db)) -> StyleService:
    """Dependency to get StyleService instance."""
    return StyleService(db, geoserver_dao)


# ==================== Style Generation ====================

@router.post("/generate", response_model=StyleGenerateResponse)
async def generate_style(
    request: StyleGenerateRequest,
    schema: str = Query("public", description="Database schema"),
    service: StyleService = Depends(get_style_service)
):
    """
    Generate a style for a layer.
    
    This endpoint performs the complete style generation pipeline:
    1. Reads column information from PostGIS
    2. Computes color classes based on classification method
    3. Builds MBStyle JSON
    4. Optionally publishes to GeoServer
    5. Optionally attaches to layer as default style
    """
    result = service.generate_style(request, schema)
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.message)
    
    return result


@router.post("/preview", response_model=ClassificationResult)
async def preview_classification(
    table_name: str = Query(..., description="PostGIS table name"),
    column_name: str = Query(..., description="Column to classify"),
    method: ClassificationMethod = Query(ClassificationMethod.EQUAL_INTERVAL),
    num_classes: int = Query(5, ge=2, le=12),
    palette: str = Query("YlOrRd"),
    schema: str = Query("public"),
    service: StyleService = Depends(get_style_service)
):
    """
    Preview classification without saving.
    Useful for UI to show preview before applying.
    """
    try:
        result = service.preview_classification(
            table_name, column_name, method, num_classes, palette, schema
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# ==================== Style Metadata CRUD ====================

@router.post("/metadata", response_model=StyleMetadataOut)
async def create_style_metadata(
    data: StyleMetadataCreate,
    service: StyleService = Depends(get_style_service)
):
    """Create new style metadata configuration."""
    try:
        style = service.create_style_metadata(data)
        return StyleMetadataOut.from_orm(style)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/metadata", response_model=StyleListResponse)
async def list_style_metadata(
    workspace: Optional[str] = Query(None, description="Filter by workspace"),
    is_active: Optional[bool] = Query(True, description="Filter by active status"),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    service: StyleService = Depends(get_style_service)
):
    """List all style metadata with optional filtering."""
    items, total = service.list_styles(workspace, is_active, skip, limit)
    return StyleListResponse(
        total=total,
        items=[StyleMetadataOut.from_orm(item) for item in items]
    )


@router.get("/metadata/{style_id}", response_model=StyleMetadataOut)
async def get_style_metadata(
    style_id: int,
    service: StyleService = Depends(get_style_service)
):
    """Get style metadata by ID."""
    style = service.get_style_metadata(style_id)
    if not style:
        raise HTTPException(status_code=404, detail="Style not found")
    return StyleMetadataOut.from_orm(style)


@router.get("/metadata/by-table/{table_name}", response_model=StyleMetadataOut)
async def get_style_by_table(
    table_name: str,
    service: StyleService = Depends(get_style_service)
):
    """Get style metadata by table name."""
    style = service.get_style_by_table(table_name)
    if not style:
        raise HTTPException(status_code=404, detail="Style not found for this table")
    return StyleMetadataOut.from_orm(style)


@router.put("/metadata/{style_id}", response_model=StyleMetadataOut)
async def update_style_metadata(
    style_id: int,
    data: StyleMetadataUpdate,
    service: StyleService = Depends(get_style_service)
):
    """Update style metadata."""
    style = service.update_style_metadata(style_id, data)
    if not style:
        raise HTTPException(status_code=404, detail="Style not found")
    return StyleMetadataOut.from_orm(style)


@router.delete("/metadata/{style_id}")
async def delete_style_metadata(
    style_id: int,
    service: StyleService = Depends(get_style_service)
):
    """Delete style metadata."""
    if not service.delete_style_metadata(style_id):
        raise HTTPException(status_code=404, detail="Style not found")
    return {"message": "Style deleted successfully"}


# ==================== Regenerate Style ====================

@router.post("/regenerate/{style_id}", response_model=StyleGenerateResponse)
async def regenerate_style(
    style_id: int,
    publish_to_geoserver: bool = Query(True),
    attach_to_layer: bool = Query(True),
    user_id: Optional[str] = Query(None),
    user_email: Optional[str] = Query(None),
    schema: str = Query("public"),
    service: StyleService = Depends(get_style_service)
):
    """
    Regenerate style from existing metadata.
    Useful when data has changed and style needs to be updated.
    """
    style = service.get_style_metadata(style_id)
    if not style:
        raise HTTPException(status_code=404, detail="Style not found")
    
    request = StyleGenerateRequest(
        layer_table_name=style.layer_table_name,
        workspace=style.workspace,
        color_by=style.color_by,
        layer_type=LayerType(style.layer_type.value) if style.layer_type else None,
        classification_method=ClassificationMethod(style.classification_method.value) if style.classification_method else None,
        num_classes=style.num_classes,
        color_palette=style.color_palette,
        custom_colors=style.custom_colors,
        manual_breaks=style.manual_breaks,
        publish_to_geoserver=publish_to_geoserver,
        attach_to_layer=attach_to_layer,
        user_id=user_id,
        user_email=user_email
    )
    
    result = service.generate_style(request, schema)
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.message)
    
    return result


# ==================== Legend ====================

@router.get("/legend/{style_id}", response_model=LegendResponse)
async def get_legend(
    style_id: int,
    service: StyleService = Depends(get_style_service)
):
    """Get legend for a style."""
    legend = service.get_legend(style_id)
    if not legend:
        raise HTTPException(status_code=404, detail="Legend not found")
    return legend


# ==================== Column Information ====================

@router.get("/columns/{table_name}", response_model=List[ColumnInfo])
async def get_layer_columns(
    table_name: str,
    schema: str = Query("public"),
    service: StyleService = Depends(get_style_service)
):
    """
    Get styleable columns for a layer.
    Returns column names with their data types.
    """
    try:
        columns = service.get_layer_columns(table_name, schema)
        return columns
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# ==================== Color Palettes ====================

@router.get("/palettes")
async def list_palettes():
    """Get list of available ColorBrewer palettes."""
    palettes = get_available_palettes()
    
    # Add preview colors for each palette
    result = {}
    for name, classes in palettes.items():
        result[name] = {
            "supported_classes": classes,
            "preview": get_palette_preview(name)
        }
    
    return result


@router.get("/palettes/{palette_name}")
async def get_palette(
    palette_name: str,
    num_classes: int = Query(5, ge=2, le=12)
):
    """Get colors for a specific palette."""
    from ..service.color_palettes import get_colors
    
    try:
        colors = get_colors(palette_name, num_classes)
        return {
            "palette": palette_name,
            "num_classes": num_classes,
            "colors": colors
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# ==================== Cache Management ====================

@router.post("/cache/invalidate")
async def invalidate_cache(
    table_name: str = Query(..., description="Table name to invalidate cache for"),
    column_name: Optional[str] = Query(None, description="Specific column (optional)"),
    service: StyleService = Depends(get_style_service)
):
    """
    Invalidate cached classification data.
    Use when underlying data has changed.
    """
    service.invalidate_cache(table_name, column_name)
    return {"message": f"Cache invalidated for {table_name}" + (f".{column_name}" if column_name else "")}


# ==================== MBStyle JSON ====================

@router.get("/mbstyle/{style_id}")
async def get_mbstyle_json(
    style_id: int,
    service: StyleService = Depends(get_style_service)
):
    """Get the raw MBStyle JSON for a style."""
    style = service.get_style_metadata(style_id)
    if not style:
        raise HTTPException(status_code=404, detail="Style not found")
    
    if not style.mbstyle_json:
        raise HTTPException(status_code=404, detail="Style has not been generated yet")
    
    return style.mbstyle_json


# ==================== Audit Logs ====================

@router.get("/audit/{style_id}", response_model=List[AuditLogOut])
async def get_audit_logs(
    style_id: int,
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=100),
    service: StyleService = Depends(get_style_service)
):
    """Get audit logs for a style."""
    logs = service.dao.get_audit_logs(style_id, skip, limit)
    return [AuditLogOut.from_orm(log) for log in logs]
