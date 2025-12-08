from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional, Any
import logging
from urllib.parse import quote
import uuid
from database.database import get_db
from geoserver.dao import GeoServerDAO
from utils.config import (geoserver_host, geoserver_port, geoserver_username, geoserver_password)
from metadata.models.schema import Metadata
from ..service.style_service import StyleService
from ..models.model import (StyleMetadataOut, StyleGenerateRequest, StyleGenerateResponse, AuditLogOut)

logger = logging.getLogger(__name__)

# Note: prefix is added in main.py when including the router
router = APIRouter(tags=["styles"])

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


# ==================== Style Metadata ====================

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


# ==================== Legend ====================

@router.get("/legend/{style_name}")
async def get_legend(
    style_name: str,
    service: StyleService = Depends(get_style_service)
):
    """
    Get legend for a style in Mapbox GL style format.
    Returns complete MBStyle JSON with sources (TMS tiles) included.
    """
    try:
        style = service.get_style_metadata_by_name(style_name)
        if not style:
            raise HTTPException(status_code=404, detail="Style not found")
        
        if not style.mbstyle_json:
            raise HTTPException(status_code=404, detail="Style has not been generated yet")
        
        # Construct layer name from workspace and table name
        layer_name = f"{style.workspace}:{style.layer_table_name}"
        
        # Build TMS source URL (relative path for frontend)
        # Format: /geoserver/gwc/service/tms/1.0.0/{workspace}:{layer}@EPSG%3A900913@pbf/{z}/{x}/{y}.pbf
        
        
        encoded_layer = quote(layer_name, safe='')
        tms_url = f"/geoserver/gwc/service/tms/1.0.0/{encoded_layer}@EPSG%3A900913@pbf/{{z}}/{{x}}/{{y}}.pbf"
        
        # Clone the MBStyle JSON
        mbstyle = style.mbstyle_json.copy()
        
        # Ensure version is set
        mbstyle["version"] = mbstyle.get("version", 8)
        
        # Add sources
        source_name = style.layer_table_name  # Use table name as source identifier
        mbstyle["sources"] = {
            source_name: {
                "type": "vector",
                "scheme": "tms",
                "tiles": [tms_url]
            }
        }
        
        # Get style name for layers
        style_name_for_layers = style.generated_style_name or f"{style.layer_table_name}_{style.color_by}_style"
        
        # Update layers to use the source and ensure source-layer is set
        # Also transform match/step expressions to stops format for frontend
        if "layers" in mbstyle:
            for layer in mbstyle["layers"]:
                # Replace 'id' with 'styleName' and use the style name (remove any suffix like -circle, -fill, etc.)
                if "id" in layer:
                    del layer["id"]
                layer["styleName"] = style_name_for_layers
                
                layer["source"] = source_name
                layer["source-layer"] = style.layer_table_name
                
                # Transform color expressions to stops format
                paint = layer.get("paint", {})
                for paint_key in ["fill-color", "circle-color", "line-color"]:
                    if paint_key in paint:
                        paint[paint_key] = _transform_color_expression(
                            paint[paint_key], 
                            style.color_by,
                            style.classification_method.value if style.classification_method else None
                        )
        
        return mbstyle
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting legend for style {style_name}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ==================== Frontend Integration APIs ====================

@router.get("/by-layer/{layer_id}")
async def get_styles_by_layer(
    layer_id: str,
    workspace: Optional[str] = Query(None, description="Workspace name (optional)"),
    service: StyleService = Depends(get_style_service),
    db: Session = Depends(get_db)
):
    try:
        # Parse UUID from string
        try:
            layer_uuid = uuid.UUID(layer_id)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid layer ID format")
        
        # Query metadata by ID to get geoserver_name
        metadata = db.query(Metadata).filter(Metadata.id == layer_uuid).first()
        if not metadata:
            raise HTTPException(status_code=404, detail="Layer not found")
        
        # Get layer name from metadata
        layer_name = metadata.geoserver_name
        
        # Extract workspace and table name from layer name (e.g., "metastring:gbif" -> workspace="metastring", table="gbif")
        table_name = layer_name
        extracted_workspace = None
        
        if ":" in layer_name:
            parts = layer_name.split(":", 1)  # Split only on first colon
            extracted_workspace = parts[0]
            table_name = parts[1]
        
        # Use workspace from query parameter if provided, otherwise use extracted workspace from layer_name
        filter_workspace = workspace if workspace else extracted_workspace
        
        # Get styles filtered by workspace (if specified) and active status
        styles, _ = service.dao.list_styles(workspace=filter_workspace, is_active=True, skip=0, limit=1000)
        
        # Filter by table name
        layer_styles = [s for s in styles if s.layer_table_name == table_name]
        
        if not layer_styles:
            return {
                "layerName": layer_name,
                "titleColumn": None,
                "summaryColumn": [],
                "styles": []
            }
        
        # Get column info for title column and summary columns
        # Use the first style's table to get columns
        first_style = layer_styles[0]
        schema = "public"  # Default schema, could be made configurable
        columns = service.dao.get_column_info(first_style.layer_table_name, schema)
        
        # Find title column (first categorical or first column)
        title_column = None
        summary_columns = []
        for col in columns:
            if col.is_categorical and not title_column:
                title_column = col.column_name
            summary_columns.append(col.column_name)
        
        if not title_column and columns:
            title_column = columns[0].column_name
        
        # Build response
        style_list = []
        for style in layer_styles:
            # Get column data type
            col_type = service.dao.get_column_data_type(
                style.layer_table_name, 
                style.color_by, 
                schema
            ) or "unknown"
            
            # Generate human-readable title from column name
            style_title = _format_column_name(style.color_by)
            
            style_list.append({
                "styleName": style.generated_style_name or f"{style.layer_table_name}_{style.color_by}_style",
                "styleTitle": style_title,
                "styleType": col_type,
                "styleId": style.id,
                "colorBy": style.color_by
            })
        
        return {
            "layerName": layer_name,
            "titleColumn": title_column,
            "summaryColumn": summary_columns[:10],  # Limit to 10 columns
            "styles": style_list
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting styles for layer ID {layer_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{style_id}/mbstyle")
async def get_mbstyle_with_sources(
    style_id: int,
    layer_name: Optional[str] = Query(None, description="Layer name for source (e.g., metastring:gbif)"),
    service: StyleService = Depends(get_style_service)
):
    try:
        style = service.get_style_metadata(style_id)
        if not style:
            raise HTTPException(status_code=404, detail="Style not found")
        
        if not style.mbstyle_json:
            raise HTTPException(status_code=404, detail="Style has not been generated yet")
        
        # Get layer name from style or parameter
        if not layer_name:
            # Try to construct from workspace and table name
            layer_name = f"{style.workspace}:{style.layer_table_name}"
        
        # Build TMS source URL
        # Format: /geoserver/gwc/service/tms/1.0.0/{workspace}:{layer}@EPSG%3A900913@pbf/{z}/{x}/{y}.pbf
        from utils.config import geoserver_host, geoserver_port
        base_url = f"http://{geoserver_host}:{geoserver_port}"
        
        # URL encode the layer name
        from urllib.parse import quote
        encoded_layer = quote(layer_name, safe='')
        tms_url = f"{base_url}/geoserver/gwc/service/tms/1.0.0/{encoded_layer}@EPSG%3A900913@pbf/{{z}}/{{x}}/{{y}}.pbf"
        
        # Clone the MBStyle JSON
        mbstyle = style.mbstyle_json.copy()
        
        # Add sources
        mbstyle["sources"] = {
            layer_name: {
                "type": "vector",
                "scheme": "tms",
                "tiles": [tms_url]
            }
        }
        
        # Update layers to use the source
        if "layers" in mbstyle:
            for layer in mbstyle["layers"]:
                layer["source"] = layer_name
                layer["source-layer"] = style.layer_table_name
        
        return mbstyle
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting MBStyle for style {style_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


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


def _transform_color_expression(
    expression: Any,
    property_name: str,
    classification_method: Optional[str] = None
) -> Any:
    if not isinstance(expression, list) or len(expression) < 3:
        # Not an expression, return as-is (single color)
        return expression
    
    expr_type = expression[0]
    
    if expr_type == "match":
        # Extract property name from ["get", "property"]
        extracted_property = property_name  # Default to parameter
        if len(expression) > 1 and isinstance(expression[1], list) and len(expression[1]) == 2:
            if expression[1][0] == "get":
                extracted_property = expression[1][1]
        
        # Extract value-color pairs (skip ["match", ["get", "prop"]] and default color at end)
        stops = []
        i = 2  # Start after ["match", ["get", "prop"]]
        while i < len(expression) - 1:  # -1 to skip default color
            if i + 1 < len(expression):
                value = expression[i]
                color = expression[i + 1]
                stops.append([value, color])
            i += 2
        
        return {
            "property": extracted_property,
            "type": "categorical",
            "stops": stops
        }
    
    elif expr_type == "step":
        # Extract property name from ["get", "property"]
        extracted_property = property_name  # Default to parameter
        if len(expression) > 1 and isinstance(expression[1], list) and len(expression[1]) == 2:
            if expression[1][0] == "get":
                extracted_property = expression[1][1]
        
        # Extract break-color pairs
        # Format: ["step", ["get", "prop"], color0, break1, color1, break2, color2, ...]
        stops = []
        first_color = expression[2] if len(expression) > 2 else "#cccccc"
        
        # For step expressions, we need to include the first color with a break
        # But since we don't know the min value here, we'll start from the first break
        i = 3  # Start after ["step", ["get", "prop"], first_color]
        while i < len(expression):
            if i + 1 < len(expression):
                break_val = expression[i]
                color = expression[i + 1]
                stops.append([break_val, color])
            i += 2
        
        # If no breaks, use first color as single stop
        if not stops:
            stops = [[0, first_color]]
        
        return {
            "property": extracted_property,
            "type": "interval",
            "stops": stops
        }
    
    # Not a match or step expression, return as-is
    return expression


def _format_column_name(column_name: str) -> str:
    # Replace underscores with spaces
    title = column_name.replace("_", " ")
    # Capitalize first letter of each word
    title = " ".join(word.capitalize() for word in title.split())
    return title
