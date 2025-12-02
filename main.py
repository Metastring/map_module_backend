from fastapi import FastAPI
from geoserver.api import router as geoserver_router  # Import router directly
from upload_log.api.api import router as upload_log_router

# Import the new spatial queries API
from queries.api.api import SpatialQueryAPI1  # Import the original API for comparison

# Import metadata GraphQL API
from metadata.api.api import metadata_app

# Import styles API for layer styling
from styles.api.api import router as styles_router

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
app.include_router(styles_router, prefix="/styles", tags=["styles"])  # Add styles API router

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
            "styles": {
                "generate": "/styles/generate",
                "preview": "/styles/preview",
                "metadata": "/styles/metadata",
                "legend": "/styles/legend/{style_id}",
                "palettes": "/styles/palettes",
                "regenerate": "/styles/regenerate/{style_id}"
            },
            "health_check": "/health"
        }
    }
