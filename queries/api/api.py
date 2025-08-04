import strawberry
from fastapi import APIRouter, HTTPException
from strawberry.fastapi import GraphQLRouter
from queries.service.service import fetch_polygon_query  # New logic
from queries.models.model import SpatialQueryInput, SpatialQueryType

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
            return SpatialQueryType(**result)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

# Create GraphQL schema and router
schema = strawberry.Schema(query=Query)
spatial_graphql_app = GraphQLRouter(schema)
SpatialQueryAPI1.router.include_router(spatial_graphql_app, prefix="/graphql_data_method")
