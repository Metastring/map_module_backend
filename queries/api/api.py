# queries/api/api.py
import strawberry
from fastapi import APIRouter, HTTPException
from strawberry.fastapi import GraphQLRouter
from queries.service.service import fetch_polygon_query, fetch_multi_polygon_query, fetch_scientific_name_matches, fetch_multi_polygon_query_with_display_fields, fetch_dataset_columns
from queries.models.model import SpatialQueryInput, SpatialQueryType, ScientificNameInput, DatasetColumnsInput

class SpatialQueryAPI1:
   version = "/v1"
   router = APIRouter()

# GraphQL Query Class
@strawberry.type(description="GraphQL queries for spatial data retrieval")
class Query:
   @strawberry.field(description="Query spatial data within a single polygon boundary. Returns data points and features that intersect with the provided polygon geometry from specified datasets. When polygonDetail is omitted or empty, returns all data for the selected dataset(s).")
   def getPolygonData(self, input: SpatialQueryInput) -> SpatialQueryType:
       try:
           result = fetch_polygon_query(
               dataset=input.dataset,
               polygon_detail=input.polygon_detail,
               limit=input.limit,
               offset=input.offset
           )
           return SpatialQueryType(results=result.get("results", {}))
       except Exception as e:
           raise HTTPException(status_code=500, detail=str(e))

   @strawberry.field(description="Query spatial data within multiple polygon boundaries. Accepts an array of polygons and returns data points and features that intersect with any of the provided polygons from specified datasets. When polygonDetail is omitted or empty, returns all data for the selected dataset(s).")
   def getMultiPolygonData(self, input: SpatialQueryInput) -> SpatialQueryType:
       try:
           result = fetch_multi_polygon_query(
               dataset=input.dataset,
               polygon_detail=input.polygon_detail,
               limit=input.limit,
               offset=input.offset
           )
           return SpatialQueryType(results=result.get("results", {}))
       except HTTPException:
           raise
       except Exception as e:
           raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

   @strawberry.field(description="Search for spatial data by scientific name. Performs a case-insensitive partial match search across datasets and returns all matching records with their associated geographic data.")
   def getScientificNameMatches(self, input: ScientificNameInput) -> SpatialQueryType:
       try:
           result = fetch_scientific_name_matches(
               scientific_name=input.scientificName
           )
           return SpatialQueryType(results=result.get("results", {}))
       except Exception as e:
           raise HTTPException(status_code=500, detail=str(e))

   @strawberry.field(description="Query spatial data within multiple polygon boundaries with display fields. Input also accepts `category` (currently not used for filtering). Pass displayFieldsByDataset (map of dataset name -> column names) to control display_fields only (data payload remains unchanged); use {} or per-dataset null/[] for display_fields null. Omit displayFieldsByDataset to auto-derive display_fields from schema and row keys. When polygonDetail is omitted or empty, returns all data for the selected dataset(s).")
   def getMultiPolygonDataWithDisplayFields(self, input: SpatialQueryInput) -> SpatialQueryType:
       try:
           result = fetch_multi_polygon_query_with_display_fields(
               dataset=input.dataset,
               polygon_detail=input.polygon_detail,
               limit=input.limit,
               offset=input.offset,
               display_fields_by_dataset=input.display_fields_by_dataset,
               filters=input.filters,
           )
           return SpatialQueryType(results=result.get("results", {}))
       except ValueError as e:
           raise HTTPException(
               status_code=400,
               detail=str(e)
               )
       except HTTPException:
           raise
       except Exception as e:
           raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

   @strawberry.field(description="Get all column names for the provided dataset tables. Returns a per-dataset list of columns (or null if a dataset table does not exist).")
   def getDatasetColumns(self, input: DatasetColumnsInput) -> SpatialQueryType:
       try:
           result = fetch_dataset_columns(dataset=input.dataset)
           return SpatialQueryType(results=result.get("results", {}))
       except HTTPException:
           raise
       except Exception as e:
           raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

# Create GraphQL schema and router
schema = strawberry.Schema(query=Query)
spatial_graphql_app = GraphQLRouter(schema)
SpatialQueryAPI1.router.include_router(spatial_graphql_app, prefix="/spatial_search")


