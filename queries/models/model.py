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
    polygon_detail: List[PolygonDetailInput]
    limit: Optional[int] = 1000
    offset: Optional[int] = 0
    category: Optional[str] = None  # New field for frontend input

@strawberry.type
class SpatialQueryType:
    data: List[JSON] 