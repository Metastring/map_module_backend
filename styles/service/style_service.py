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
from ..models.model import (
    StyleMetadataCreate,
    StyleGenerateRequest,
    StyleGenerateResponse,
    ClassificationMethod,
    LayerType,
    ClassificationResult,
    MBStyleOutput,
)
from ..models.schema import StyleMetadata
from .classification import ClassificationService
from .mbstyle_builder import MBStyleBuilder
from .color_palettes import get_colors

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
            # Check for existing style with same workspace, table, and color_by
            style_metadata = self.dao.get_style_by_workspace_table_color(
                request.workspace, 
                request.layer_table_name,
                request.color_by
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

    # ==================== Style Metadata ====================

    def get_style_metadata(self, style_id: int) -> Optional[StyleMetadata]:
        """Get style metadata by ID."""
        return self.dao.get_style_metadata(style_id)

    def get_style_metadata_by_name(self, style_name: str) -> Optional[StyleMetadata]:
        """Get style metadata by generated_style_name."""
        return self.dao.get_style_by_name(style_name)
