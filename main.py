from fastapi import FastAPI
from geoserver.api import router as geoserver_router  # Import router directly
from geoserver.admin.api import router as geoserver_admin_router  # Import admin router
from upload_log.api.api import router as upload_log_router

# Import the new spatial queries API
from queries.api.api import SpatialQueryAPI1  # Import the original API for comparison

# Import metadata GraphQL API
from metadata.api.api import metadata_app

# Import styles API for layer styling
from styles.api.api import router as styles_router

# Import register dataset API
from register_dataset.api.api import router as register_dataset_router

# Allow CORS (if needed)
from fastapi.middleware.cors import CORSMiddleware
origins = ["*"]
app = FastAPI(
    openapi_tags=[
        {
            "name": "spatial-search",
            "description": "GraphQL APIs for spatial data queries. Supports polygon-based searches (single and multi-polygon) to find data points and features within specified geographic boundaries, as well as scientific name-based searches to locate species data across datasets."
        },
        {
            "name": "metadata-graphql",
            "description": "GraphQL APIs for metadata management. Provides create and read operations for saving and retrieving metadata details in the metadata table of the database."
        }
    ]
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(geoserver_router, tags=["geoserver"])  # Add the GeoServer API router
app.include_router(geoserver_admin_router, prefix="/admin", tags=["geoserver-admin"])  # Add the GeoServer Admin API router
app.include_router(SpatialQueryAPI1.router, prefix=SpatialQueryAPI1.version, tags=["spatial-search"])  # GraphQL APIs for spatial data queries including polygon-based and scientific name searches
app.include_router(upload_log_router, prefix="/upload_log", tags=["upload-log"])
app.include_router(metadata_app, prefix="/metadata", tags=["metadata-graphql"])  # Add metadata GraphQL endpoint
app.include_router(styles_router, prefix="/styles", tags=["styles"])
app.include_router(register_dataset_router, prefix="/register_dataset", tags=["register-dataset"]) 

# Add health check endpoint for GraphQL service
@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "cmlapis-with-graphql"}

# Add GraphQL playground info endpoint
@app.get("/")
async def root():
    return {
        "message": "CML APIs with GraphQL Spatial Queries",
        "endpoints": {
            "spatial_graphql": "/v1/graphql",
            "upload_log": {
                "upload": "/upload_log/upload",
                "list": "/upload_log/",
                "detail": "/upload_log/{id}"
            },
            "geoserver": "/geoserver",
            "metadata_graphql": "/metadata",
            "health_check": "/health"
        }
    }
