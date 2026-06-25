# queries/models/model.py
from typing import List, Optional
import strawberry
from strawberry.scalars import JSON


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
   category: Optional[str] = None  # New field for frontend input
   # Keys must match entries in `dataset`. Values: column names for display_fields and row projection; null/[] per dataset yields display_fields null and full rows. {} applies that to every dataset with data.
   display_fields_by_dataset: Optional[JSON] = None
   filters: Optional[JSON] = None

@strawberry.type
class SpatialQueryType:
   results: JSON

@strawberry.input
class DatasetColumnsInput:
   dataset: List[str]

@strawberry.input
class ScientificNameInput:
   scientificName: str

