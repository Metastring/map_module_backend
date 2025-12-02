"""
MBStyle Builder Service.
Generates Mapbox Style JSON for GeoServer from classification results.
"""
from typing import Dict, Any, List, Optional
import logging

from ..models.schema import (
    ClassificationResult,
    ClassificationMethod,
    LayerType,
    MBStyleOutput,
    MBStyleLayer,
)

logger = logging.getLogger(__name__)


class MBStyleBuilder:
    """
    Builds MBStyle (Mapbox Style) JSON for GeoServer.
    Supports fill, line, and circle (point) styles.
    """

    def build_style(
        self,
        style_name: str,
        layer_name: str,
        color_by: str,
        classification: ClassificationResult,
        layer_type: LayerType = LayerType.POLYGON,
        fill_opacity: float = 0.7,
        stroke_color: str = "#333333",
        stroke_width: float = 1.0,
        source_layer: Optional[str] = None,
    ) -> MBStyleOutput:
        """
        Build complete MBStyle JSON from classification result.
        
        Args:
            style_name: Name for the style
            layer_name: Name of the layer being styled
            color_by: Column name used for classification
            classification: Classification result with breaks and colors
            layer_type: Geometry type (polygon, line, point)
            fill_opacity: Opacity for fill (0-1)
            stroke_color: Color for outlines
            stroke_width: Width for outlines/lines
            source_layer: Source layer name for vector tiles
        
        Returns:
            MBStyleOutput containing the complete style
        """
        if layer_type == LayerType.POLYGON:
            layers = self._build_polygon_layers(
                style_name, color_by, classification, 
                fill_opacity, stroke_color, stroke_width, source_layer
            )
        elif layer_type == LayerType.LINE:
            layers = self._build_line_layers(
                style_name, color_by, classification, 
                stroke_width, source_layer
            )
        elif layer_type == LayerType.POINT:
            layers = self._build_point_layers(
                style_name, color_by, classification, 
                fill_opacity, stroke_color, stroke_width, source_layer
            )
        else:
            # Default to polygon
            layers = self._build_polygon_layers(
                style_name, color_by, classification, 
                fill_opacity, stroke_color, stroke_width, source_layer
            )
        
        return MBStyleOutput(
            version=8,
            name=style_name,
            layers=layers
        )

    def _build_polygon_layers(
        self,
        style_name: str,
        color_by: str,
        classification: ClassificationResult,
        fill_opacity: float,
        stroke_color: str,
        stroke_width: float,
        source_layer: Optional[str]
    ) -> List[MBStyleLayer]:
        """Build polygon (fill) style layers."""
        layers = []
        
        # Build fill color expression
        fill_color = self._build_color_expression(color_by, classification)
        
        # Fill layer
        fill_layer = MBStyleLayer(
            id=f"{style_name}-fill",
            type="fill",
            source_layer=source_layer,
            paint={
                "fill-color": fill_color,
                "fill-opacity": fill_opacity
            }
        )
        layers.append(fill_layer)
        
        # Outline layer
        outline_layer = MBStyleLayer(
            id=f"{style_name}-outline",
            type="line",
            source_layer=source_layer,
            paint={
                "line-color": stroke_color,
                "line-width": stroke_width
            }
        )
        layers.append(outline_layer)
        
        return layers

    def _build_line_layers(
        self,
        style_name: str,
        color_by: str,
        classification: ClassificationResult,
        stroke_width: float,
        source_layer: Optional[str]
    ) -> List[MBStyleLayer]:
        """Build line style layers."""
        line_color = self._build_color_expression(color_by, classification)
        
        line_layer = MBStyleLayer(
            id=f"{style_name}-line",
            type="line",
            source_layer=source_layer,
            paint={
                "line-color": line_color,
                "line-width": stroke_width
            }
        )
        
        return [line_layer]

    def _build_point_layers(
        self,
        style_name: str,
        color_by: str,
        classification: ClassificationResult,
        fill_opacity: float,
        stroke_color: str,
        stroke_width: float,
        source_layer: Optional[str]
    ) -> List[MBStyleLayer]:
        """Build point (circle) style layers."""
        fill_color = self._build_color_expression(color_by, classification)
        
        circle_layer = MBStyleLayer(
            id=f"{style_name}-circle",
            type="circle",
            source_layer=source_layer,
            paint={
                "circle-color": fill_color,
                "circle-opacity": fill_opacity,
                "circle-radius": 6,
                "circle-stroke-color": stroke_color,
                "circle-stroke-width": stroke_width
            }
        )
        
        return [circle_layer]

    def _build_color_expression(
        self,
        color_by: str,
        classification: ClassificationResult
    ) -> Any:
        """
        Build Mapbox GL expression for data-driven coloring.
        
        For numeric data: Uses "step" expression
        For categorical data: Uses "match" expression
        """
        if classification.method == ClassificationMethod.CATEGORICAL:
            return self._build_match_expression(color_by, classification)
        else:
            return self._build_step_expression(color_by, classification)

    def _build_step_expression(
        self,
        color_by: str,
        classification: ClassificationResult
    ) -> List[Any]:
        """
        Build a "step" expression for numeric classification.
        
        Format: ["step", ["get", "property"], color0, break1, color1, break2, color2, ...]
        """
        if not classification.breaks or not classification.colors:
            # Single color fallback
            return classification.colors[0] if classification.colors else "#cccccc"
        
        expression = ["step", ["get", color_by]]
        
        # First color (below first break)
        expression.append(classification.colors[0])
        
        # Add breaks and colors
        for i, break_val in enumerate(classification.breaks):
            expression.append(break_val)
            if i + 1 < len(classification.colors):
                expression.append(classification.colors[i + 1])
            else:
                expression.append(classification.colors[-1])
        
        return expression

    def _build_match_expression(
        self,
        color_by: str,
        classification: ClassificationResult
    ) -> List[Any]:
        """
        Build a "match" expression for categorical classification.
        
        Format: ["match", ["get", "property"], value1, color1, value2, color2, ..., defaultColor]
        """
        if not classification.categories or not classification.colors:
            return "#cccccc"
        
        expression = ["match", ["get", color_by]]
        
        # Add category-color pairs
        for i, category in enumerate(classification.categories):
            expression.append(category)
            if i < len(classification.colors):
                expression.append(classification.colors[i])
            else:
                expression.append(classification.colors[-1])
        
        # Default color (for unmatched values)
        expression.append("#999999")
        
        return expression

    def to_dict(self, style: MBStyleOutput) -> Dict[str, Any]:
        """
        Convert MBStyleOutput to a dictionary for JSON serialization.
        """
        result = {
            "version": style.version,
            "name": style.name,
            "layers": []
        }
        
        for layer in style.layers:
            layer_dict = {
                "id": layer.id,
                "type": layer.type,
            }
            
            if layer.source:
                layer_dict["source"] = layer.source
            if layer.source_layer:
                layer_dict["source-layer"] = layer.source_layer
            if layer.paint:
                layer_dict["paint"] = layer.paint
            if layer.layout:
                layer_dict["layout"] = layer.layout
            if layer.filter:
                layer_dict["filter"] = layer.filter
            
            result["layers"].append(layer_dict)
        
        if style.sources:
            result["sources"] = style.sources
        if style.sprite:
            result["sprite"] = style.sprite
        if style.glyphs:
            result["glyphs"] = style.glyphs
        
        return result


def build_simple_style(
    style_name: str,
    fill_color: str = "#3388ff",
    fill_opacity: float = 0.7,
    stroke_color: str = "#333333",
    stroke_width: float = 1.0,
    layer_type: LayerType = LayerType.POLYGON
) -> Dict[str, Any]:
    """
    Build a simple single-color style.
    Useful for layers without classification.
    """
    layers = []
    
    if layer_type == LayerType.POLYGON:
        layers = [
            {
                "id": f"{style_name}-fill",
                "type": "fill",
                "paint": {
                    "fill-color": fill_color,
                    "fill-opacity": fill_opacity
                }
            },
            {
                "id": f"{style_name}-outline",
                "type": "line",
                "paint": {
                    "line-color": stroke_color,
                    "line-width": stroke_width
                }
            }
        ]
    elif layer_type == LayerType.LINE:
        layers = [
            {
                "id": f"{style_name}-line",
                "type": "line",
                "paint": {
                    "line-color": fill_color,
                    "line-width": stroke_width
                }
            }
        ]
    elif layer_type == LayerType.POINT:
        layers = [
            {
                "id": f"{style_name}-circle",
                "type": "circle",
                "paint": {
                    "circle-color": fill_color,
                    "circle-opacity": fill_opacity,
                    "circle-radius": 6,
                    "circle-stroke-color": stroke_color,
                    "circle-stroke-width": stroke_width
                }
            }
        ]
    
    return {
        "version": 8,
        "name": style_name,
        "layers": layers
    }
