import strawberry
from fastapi import APIRouter, HTTPException
from strawberry.fastapi import GraphQLRouter
from queries.service.service import fetch_polygon_query, fetch_multi_polygon_query, fetch_scientific_name_matches
from queries.models.model import SpatialQueryInput, SpatialQueryType, ScientificNameInput

class SpatialQueryAPI1:
    version = "/v1"
    router = APIRouter()

# GraphQL Query Class
@strawberry.type(description="GraphQL queries for spatial data retrieval")
class Query:
    @strawberry.field(description="Query spatial data within a single polygon boundary. Returns data points and features that intersect with the provided polygon geometry from specified datasets.")
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

    @strawberry.field(description="Query spatial data within multiple polygon boundaries. Accepts an array of polygons and returns data points and features that intersect with any of the provided polygons from specified datasets. Useful for querying non-contiguous regions.")
    def getMultiPolygonData(self, input: SpatialQueryInput) -> SpatialQueryType:
        try:
            # Validate that at least one polygon is provided
            if not input.polygon_detail or len(input.polygon_detail) == 0:
                raise HTTPException(status_code=400, detail="At least one polygon must be provided")
            
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

# Create GraphQL schema and router
schema = strawberry.Schema(query=Query)
spatial_graphql_app = GraphQLRouter(schema)
SpatialQueryAPI1.router.include_router(spatial_graphql_app, prefix="/spatial_search")
