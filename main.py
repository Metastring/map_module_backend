from fastapi import FastAPI
from geoserver.api import router as geoserver_router  # Import router directly
from upload_log.api.api import router as upload_log_router

# Import the new spatial queries API
from queries.api.api import SpatialQueryAPI1  # Import the original API for comparison

# Import metadata GraphQL API
from metadata.api.api import metadata_app

# Allow CORS (if needed)
from fastapi.middleware.cors import CORSMiddleware
origins = ["*"]
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(geoserver_router, tags=["geoserver"])  # Add the GeoServer API router
app.include_router(SpatialQueryAPI1.router, prefix=SpatialQueryAPI1.version, tags=["spatial-graphql-copy"])  # Add copy spatial queries GraphQL endpoint
app.include_router(upload_log_router, prefix="/upload_log", tags=["upload-log"])
app.include_router(metadata_app, prefix="/metadata", tags=["metadata-graphql"])  # Add metadata GraphQL endpoint

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
