from typing import List

import strawberry
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from strawberry.fastapi import GraphQLRouter
from queries.service.service import fetch_polygon_query, fetch_multi_polygon_query, fetch_scientific_name_matches, fetch_multi_polygon_query_with_display_fields, fetch_scientific_name_matches_by_dataset, fetch_dataset_columns
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


class SpatialQueryAPI2:
    version = "/v2"
    router = APIRouter()


class ScientificNameSearchRequest(BaseModel):
    """REST-friendly input matching the /federated-search contract used
    elsewhere in the platform. search_text drives the match (passed through as
    the GraphQL scientificName variable). When dataset is provided (a single
    dataset_master.title, e.g. "Traded Medicinal Plants of India (TMPI)"), the
    search is scoped to that dataset's registered physical table and result
    columns are renamed to their dataset_mapping ontology_mapping names. Every
    result record always includes all of that dataset's mapped fields (plus
    geometry keys needed for map rendering); fields is accepted for contract
    compatibility but no longer filters anything -- callers pick what to
    display client-side from displayFields/the full record. category is
    accepted for contract compatibility but unused, since no category-based
    routing exists in dataset_mapping."""
    category: List[str] = []
    dataset: List[str] = []
    fields: List[str] = []
    search_text: str


@SpatialQueryAPI2.router.post(
    "/spatial_search",
    description="REST wrapper around getScientificNameMatches. Accepts search_text "
    "(mapped to scientificName) and, optionally, a single dataset name to scope the "
    "search to that dataset's registered table with columns renamed per "
    "dataset_mapping. Every result record includes all of the dataset's mapped "
    "fields; fields/category are accepted for contract compatibility but unused. "
    "Returns {displayFields: {ontology_name: label}, results: [...]} directly "
    "(no GraphQL-style envelope); results is a flat list of records for the "
    "single resolved dataset/table.",
)
def scientific_name_search_v2(input: ScientificNameSearchRequest):
    try:
        if input.dataset:
            return fetch_scientific_name_matches_by_dataset(
                scientific_name=input.search_text,
                dataset_title=input.dataset[0],
                fields=input.fields,
            )
        # Legacy (no-dataset) path shares fetch_scientific_name_matches with the v1
        # GraphQL API, which still wants geom_geojson for map rendering, so it's
        # stripped here rather than in the shared function. No dataset_mapping to
        # draw labels from, so displayFields is empty.
        legacy = fetch_scientific_name_matches(scientific_name=input.search_text)
        rows = []
        for table_rows in legacy.get("results", {}).values():
            for row in table_rows:
                row.pop("geom_geojson", None)
            rows.extend(table_rows)
        return {"displayFields": {}, "results": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
