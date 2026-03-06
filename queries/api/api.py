import strawberry
from fastapi import APIRouter, HTTPException
from strawberry.fastapi import GraphQLRouter
from queries.service.service import fetch_polygon_query, fetch_multi_polygon_query, fetch_scientific_name_matches, fetch_multi_polygon_query_with_display_fields
from queries.models.model import SpatialQueryInput, SpatialQueryType, ScientificNameInput

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

    @strawberry.field(description="Query spatial data within multiple polygon boundaries with display fields. Accepts an array of polygons and returns data points and features that intersect with any of the provided polygons from specified datasets. Response includes display_fields for each dataset. When polygonDetail is omitted or empty, returns all data for the selected dataset(s).")
    def getMultiPolygonDataWithDisplayFields(self, input: SpatialQueryInput) -> SpatialQueryType:
        try:
            result = fetch_multi_polygon_query_with_display_fields(
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

# Create GraphQL schema and router
schema = strawberry.Schema(query=Query)
spatial_graphql_app = GraphQLRouter(schema)
SpatialQueryAPI1.router.include_router(spatial_graphql_app, prefix="/spatial_search")
