"""
Main Style Service.
Orchestrates the complete style generation workflow:
Metadata → DB Queries → Classification → MBStyle JSON → Publish to GeoServer → Attach to Layer
"""
from typing import Optional, List, Dict, Any, Tuple
from sqlalchemy.orm import Session
import logging
import json
from datetime import datetime

from ..dao.dao import StyleDAO
from ..models.schema import (
    StyleMetadataCreate,
    StyleMetadataUpdate,
    StyleMetadataOut,
    StyleGenerateRequest,
    StyleGenerateResponse,
    ClassificationMethod,
    LayerType,
    ClassificationResult,
    MBStyleOutput,
    LegendItem,
    LegendResponse,
)
from ..models.model import StyleMetadata
from .classification import ClassificationService
from .mbstyle_builder import MBStyleBuilder
from .color_palettes import get_colors, get_available_palettes

logger = logging.getLogger(__name__)


class StyleService:
    """
    Main service for automated style generation and GeoServer publishing.
    Implements the complete pipeline:
    Metadata → DB Queries → Classification → MBStyle JSON → Publish → Attach
    """

    def __init__(self, db: Session, geoserver_dao=None):
        """
        Initialize the style service.
        
        Args:
            db: SQLAlchemy database session
            geoserver_dao: GeoServerDAO instance for publishing styles
        """
        self.db = db
        self.dao = StyleDAO(db)
        self.classification_service = ClassificationService()
        self.mbstyle_builder = MBStyleBuilder()
        self.geoserver_dao = geoserver_dao

    # ==================== Main Generation Pipeline ====================

    def generate_style(
        self,
        request: StyleGenerateRequest,
        schema: str = "public"
    ) -> StyleGenerateResponse:
        """
        Main method to generate a complete style for a layer.
        
        Pipeline:
        1. Get or create style metadata
        2. Query database for column info and statistics
        3. Compute classification
        4. Build MBStyle JSON
        5. Optionally publish to GeoServer
        6. Optionally attach to layer
        
        Args:
            request: Style generation request
            schema: Database schema name
        
        Returns:
            StyleGenerateResponse with results
        """
        try:
            # Step 1: Get or create style metadata
            style_metadata = self.dao.get_style_by_workspace_table(
                request.workspace, 
                request.layer_table_name
            )
            
            if not style_metadata:
                # Create new metadata
                create_data = StyleMetadataCreate(
                    layer_table_name=request.layer_table_name,
                    workspace=request.workspace,
                    color_by=request.color_by,
                    layer_type=request.layer_type or LayerType.POLYGON,
                    classification_method=request.classification_method or ClassificationMethod.EQUAL_INTERVAL,
                    num_classes=request.num_classes or 5,
                    color_palette=request.color_palette or "YlOrRd",
                    custom_colors=request.custom_colors,
                    manual_breaks=request.manual_breaks,
                )
                style_metadata = self.dao.create_style_metadata(create_data)
            
            # Step 2: Get layer type from geometry if not specified
            layer_type = request.layer_type
            if not layer_type:
                detected_type = self.dao.get_geometry_type(request.layer_table_name, schema)
                if detected_type:
                    layer_type = LayerType(detected_type)
                else:
                    layer_type = LayerType.POLYGON
            
            # Step 3: Query database for column statistics
            column_data_type = self.dao.get_column_data_type(
                request.layer_table_name, 
                request.color_by, 
                schema
            )
            
            is_numeric = column_data_type in [
                'integer', 'bigint', 'smallint', 'numeric', 
                'real', 'double precision', 'decimal'
            ]
            
            # Step 4: Get classification data
            classification_method = request.classification_method or style_metadata.classification_method
            if isinstance(classification_method, str):
                classification_method = ClassificationMethod(classification_method)
            
            num_classes = request.num_classes or style_metadata.num_classes
            palette = request.color_palette or style_metadata.color_palette
            custom_colors = request.custom_colors or style_metadata.custom_colors
            
            if is_numeric:
                classification = self._classify_numeric(
                    request.layer_table_name,
                    request.color_by,
                    classification_method,
                    num_classes,
                    palette,
                    custom_colors,
                    request.manual_breaks,
                    schema
                )
                data_type = "numeric"
                distinct_values = None
            else:
                # Categorical
                classification = self._classify_categorical(
                    request.layer_table_name,
                    request.color_by,
                    palette,
                    custom_colors,
                    schema
                )
                data_type = "categorical"
                distinct_values = classification.categories
            
            # Step 5: Build MBStyle JSON
            style_name = f"{request.layer_table_name}_{request.color_by}_style"
            
            mbstyle = self.mbstyle_builder.build_style(
                style_name=style_name,
                layer_name=request.layer_table_name,
                color_by=request.color_by,
                classification=classification,
                layer_type=layer_type,
                fill_opacity=style_metadata.fill_opacity,
                stroke_color=style_metadata.stroke_color,
                stroke_width=style_metadata.stroke_width,
            )
            
            mbstyle_dict = self.mbstyle_builder.to_dict(mbstyle)
            
            # Step 6: Update metadata with generated info
            self.dao.update_style_generated_info(
                style_id=style_metadata.id,
                style_name=style_name,
                mbstyle_json=mbstyle_dict,
                min_value=classification.min_value,
                max_value=classification.max_value,
                distinct_values=distinct_values,
                data_type=data_type
            )
            
            # Step 7: Create audit log
            self.dao.create_audit_log(
                style_metadata_id=style_metadata.id,
                action="generated",
                user_id=request.user_id,
                user_email=request.user_email,
                new_style=mbstyle_dict,
                status="success"
            )
            
            # Step 8: Optionally publish to GeoServer
            published = False
            attached = False
            geoserver_url = None
            
            if request.publish_to_geoserver and self.geoserver_dao:
                try:
                    published = self._publish_to_geoserver(
                        request.workspace,
                        style_name,
                        mbstyle_dict
                    )
                    if published:
                        geoserver_url = f"/geoserver/rest/workspaces/{request.workspace}/styles/{style_name}"
                        
                        # Step 9: Attach to layer
                        if request.attach_to_layer:
                            attached = self._attach_style_to_layer(
                                request.workspace,
                                request.layer_table_name,
                                style_name
                            )
                except Exception as e:
                    logger.error(f"Failed to publish style to GeoServer: {e}")
            
            return StyleGenerateResponse(
                success=True,
                message="Style generated successfully",
                style_name=style_name,
                mbstyle=mbstyle,
                classification=classification,
                published_to_geoserver=published,
                attached_to_layer=attached,
                geoserver_style_url=geoserver_url
            )
            
        except Exception as e:
            logger.error(f"Style generation failed: {e}", exc_info=True)
            
            # Log failure
            if style_metadata:
                self.dao.create_audit_log(
                    style_metadata_id=style_metadata.id,
                    action="generation_failed",
                    user_id=request.user_id,
                    user_email=request.user_email,
                    status="failed",
                    error_message=str(e)
                )
            
            return StyleGenerateResponse(
                success=False,
                message=f"Style generation failed: {str(e)}",
                style_name="",
            )

    def _classify_numeric(
        self,
        table_name: str,
        column_name: str,
        method: ClassificationMethod,
        num_classes: int,
        palette: str,
        custom_colors: Optional[List[str]],
        manual_breaks: Optional[List[float]],
        schema: str
    ) -> ClassificationResult:
        """Classify numeric column."""
        # Check cache first
        cache_key = f"{method.value}_{num_classes}"
        cached = self.dao.get_cached_data(table_name, column_name, cache_key)
        
        if cached:
            return ClassificationResult(**cached)
        
        # Get min/max
        min_val, max_val, count = self.dao.get_numeric_stats(
            table_name, column_name, schema
        )
        
        # Get additional data based on method
        values = None
        quantile_breaks = None
        
        if method == ClassificationMethod.QUANTILE:
            quantile_breaks = self.dao.get_quantile_breaks(
                table_name, column_name, num_classes, schema
            )
        elif method == ClassificationMethod.JENKS:
            values = self.dao.get_all_values_for_jenks(
                table_name, column_name, schema
            )
        
        # Compute classification
        result = self.classification_service.classify(
            method=method,
            num_classes=num_classes,
            values=values,
            min_value=min_val,
            max_value=max_val,
            quantile_breaks=quantile_breaks,
            palette_name=palette,
            custom_colors=custom_colors,
            manual_breaks=manual_breaks
        )
        
        # Cache result
        self.dao.set_cached_data(
            table_name, column_name, cache_key,
            result.dict(),
            row_count=count,
            ttl_hours=24
        )
        
        return result

    def _classify_categorical(
        self,
        table_name: str,
        column_name: str,
        palette: str,
        custom_colors: Optional[List[str]],
        schema: str
    ) -> ClassificationResult:
        """Classify categorical column."""
        # Check cache
        cached = self.dao.get_cached_data(table_name, column_name, "categorical")
        
        if cached:
            return ClassificationResult(**cached)
        
        # Get distinct values
        categories = self.dao.get_distinct_values(
            table_name, column_name, schema
        )
        
        # Compute classification
        result = self.classification_service.classify(
            method=ClassificationMethod.CATEGORICAL,
            num_classes=len(categories),
            categories=categories,
            palette_name=palette,
            custom_colors=custom_colors
        )
        
        # Cache result
        self.dao.set_cached_data(
            table_name, column_name, "categorical",
            result.dict(),
            ttl_hours=24
        )
        
        return result

    # ==================== GeoServer Integration ====================

    def _publish_to_geoserver(
        self,
        workspace: str,
        style_name: str,
        mbstyle_json: Dict[str, Any]
    ) -> bool:
        """
        Publish MBStyle to GeoServer via REST API.
        """
        if not self.geoserver_dao:
            logger.warning("GeoServer DAO not configured")
            return False
        
        try:
            # Convert to JSON string
            style_content = json.dumps(mbstyle_json, indent=2)
            
            # Create style via REST API
            response = self.geoserver_dao.create_mbstyle(
                workspace=workspace,
                style_name=style_name,
                style_content=style_content
            )
            
            return response.status_code in [200, 201]
            
        except Exception as e:
            logger.error(f"Failed to publish style to GeoServer: {e}")
            return False

    def _attach_style_to_layer(
        self,
        workspace: str,
        layer_name: str,
        style_name: str
    ) -> bool:
        """
        Set style as default for a layer.
        """
        if not self.geoserver_dao:
            return False
        
        try:
            response = self.geoserver_dao.set_layer_default_style(
                workspace=workspace,
                layer_name=layer_name,
                style_name=style_name
            )
            return response.status_code in [200, 201]
        except Exception as e:
            logger.error(f"Failed to attach style to layer: {e}")
            return False

    # ==================== Style Metadata CRUD ====================

    def create_style_metadata(self, data: StyleMetadataCreate) -> StyleMetadata:
        """Create new style metadata."""
        return self.dao.create_style_metadata(data)

    def get_style_metadata(self, style_id: int) -> Optional[StyleMetadata]:
        """Get style metadata by ID."""
        return self.dao.get_style_metadata(style_id)

    def get_style_by_table(self, table_name: str) -> Optional[StyleMetadata]:
        """Get style metadata by table name."""
        return self.dao.get_style_by_table(table_name)

    def list_styles(
        self,
        workspace: Optional[str] = None,
        is_active: Optional[bool] = True,
        skip: int = 0,
        limit: int = 100
    ) -> Tuple[List[StyleMetadata], int]:
        """List style metadata."""
        return self.dao.list_styles(workspace, is_active, skip, limit)

    def update_style_metadata(
        self, 
        style_id: int, 
        data: StyleMetadataUpdate
    ) -> Optional[StyleMetadata]:
        """Update style metadata."""
        return self.dao.update_style_metadata(style_id, data)

    def delete_style_metadata(self, style_id: int) -> bool:
        """Delete style metadata."""
        return self.dao.delete_style_metadata(style_id)

    # ==================== Column Information ====================

    def get_layer_columns(
        self, 
        table_name: str, 
        schema: str = "public"
    ) -> List[Dict[str, Any]]:
        """Get styleable columns for a layer."""
        columns = self.dao.get_column_info(table_name, schema)
        return [c.dict() for c in columns]

    # ==================== Legend ====================

    def get_legend(self, style_id: int) -> Optional[LegendResponse]:
        """Get legend for a style."""
        style = self.dao.get_style_metadata(style_id)
        if not style or not style.mbstyle_json:
            return None
        
        items = []
        
        if style.data_type == "categorical" and style.distinct_values:
            # Categorical legend
            colors = style.mbstyle_json.get("layers", [{}])[0].get("paint", {}).get("fill-color", [])
            
            # Extract colors from match expression
            if isinstance(colors, list) and len(colors) > 2:
                color_map = {}
                i = 2  # Skip ["match", ["get", "column"]]
                while i < len(colors) - 1:
                    if i + 1 < len(colors):
                        color_map[colors[i]] = colors[i + 1]
                    i += 2
                
                for cat in style.distinct_values:
                    items.append(LegendItem(
                        label=str(cat),
                        color=color_map.get(cat, "#999999")
                    ))
        else:
            # Numeric legend
            if style.min_value is not None and style.max_value is not None:
                colors = []
                breaks = []
                
                # Extract from mbstyle
                paint = style.mbstyle_json.get("layers", [{}])[0].get("paint", {})
                fill_color = paint.get("fill-color", paint.get("circle-color", []))
                
                if isinstance(fill_color, list) and len(fill_color) > 2:
                    # Parse step expression: ["step", ["get", "col"], color0, break1, color1, ...]
                    colors = [fill_color[2]]  # First color
                    i = 3
                    while i < len(fill_color):
                        if i + 1 < len(fill_color):
                            breaks.append(fill_color[i])
                            colors.append(fill_color[i + 1])
                        i += 2
                
                # Build legend items
                all_breaks = [style.min_value] + breaks + [style.max_value]
                for i, color in enumerate(colors):
                    min_v = all_breaks[i] if i < len(all_breaks) else None
                    max_v = all_breaks[i + 1] if i + 1 < len(all_breaks) else None
                    
                    if min_v is not None and max_v is not None:
                        label = f"{min_v:.2f} - {max_v:.2f}"
                    else:
                        label = f"Class {i + 1}"
                    
                    items.append(LegendItem(
                        label=label,
                        color=color,
                        min_value=min_v,
                        max_value=max_v
                    ))
        
        return LegendResponse(
            style_name=style.generated_style_name or "",
            layer_name=style.layer_table_name,
            color_by=style.color_by,
            classification_method=ClassificationMethod(style.classification_method.value),
            items=items
        )

    # ==================== Utility Methods ====================

    def get_available_palettes(self) -> Dict[str, List[int]]:
        """Get available color palettes."""
        return get_available_palettes()

    def preview_classification(
        self,
        table_name: str,
        column_name: str,
        method: ClassificationMethod,
        num_classes: int,
        palette: str = "YlOrRd",
        schema: str = "public"
    ) -> ClassificationResult:
        """
        Preview classification without saving.
        Useful for UI to show preview before applying.
        """
        column_type = self.dao.get_column_data_type(table_name, column_name, schema)
        
        is_numeric = column_type in [
            'integer', 'bigint', 'smallint', 'numeric',
            'real', 'double precision', 'decimal'
        ]
        
        if is_numeric:
            return self._classify_numeric(
                table_name, column_name, method, num_classes,
                palette, None, None, schema
            )
        else:
            return self._classify_categorical(
                table_name, column_name, palette, None, schema
            )

    def invalidate_cache(self, table_name: str, column_name: Optional[str] = None):
        """Invalidate cached classification data."""
        self.dao.invalidate_cache(table_name, column_name)
