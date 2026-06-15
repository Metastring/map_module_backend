from typing import List, Optional
import strawberry
from typing import Any

JSON = strawberry.scalar(
    Any,
    name="JSON",
    description="Arbitrary JSON-compatible value"
)


@strawberry.input
class GeometryInput:
    type: str
    coordinates: List[List[List[float]]]  # Supports basic Polygon geometry

@strawberry.input
class PolygonDetailInput:
    geometry: GeometryInput

@strawberry.input
class SpatialQueryInput:
    dataset: List[str]
    polygon_detail: Optional[List[PolygonDetailInput]] = None  # When null/omitted, returns all data for selected dataset(s)
    limit: Optional[int] = 1000
    offset: Optional[int] = 0
    category: Optional[str] = None
    display_fields_by_dataset: Optional[JSON] = strawberry.field(default=None, description="Per-dataset display field overrides, e.g. {\"kew\": [\"scientificname\", \"region\"]}")

@strawberry.type
class SpatialQueryType:
    results: JSON

@strawberry.input
class ScientificNameInput:
    scientificName: str 