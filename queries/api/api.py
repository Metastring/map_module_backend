import strawberry
from fastapi import APIRouter, HTTPException
from strawberry.fastapi import GraphQLRouter
from queries.service.service import fetch_polygon_query, fetch_multi_polygon_query, fetch_scientific_name_matches
from queries.models.model import SpatialQueryInput, SpatialQueryType, ScientificNameInput

class SpatialQueryAPI1:
    version = "/v1"
    router = APIRouter()

# GraphQL Query Class
@strawberry.type
class Query:
    @strawberry.field
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

    @strawberry.field
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

    @strawberry.field
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
SpatialQueryAPI1.router.include_router(spatial_graphql_app, prefix="/graphql_data_method")
