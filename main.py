from fastapi import FastAPI
from geoserver.api import router as geoserver_router  # Import router directly
from upload_log.api.api import router as upload_log_router
from unified_data.api.api import router as unified_data_router  # New unified data management

# Import the new spatial queries API
from queries.api.api import SpatialQueryAPI1  # Import the original API for comparison

# Allow CORS (if needed)
from fastapi.middleware.cors import CORSMiddleware
origins = ["*"]
app = FastAPI(
    title="CML APIs - Map Module Backend",
    description="Comprehensive backend for biodiversity and geospatial data management",
    version="2.0.0"
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
app.include_router(SpatialQueryAPI1.router, prefix=SpatialQueryAPI1.version, tags=["spatial-graphql-copy"])  # Add copy spatial queries GraphQL endpoint
app.include_router(upload_log_router, prefix="/upload_log", tags=["upload-log"])
app.include_router(unified_data_router, tags=["unified-data"])  # New unified data management system

# Add health check endpoint for GraphQL service
@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "cmlapis-with-graphql"}

# Add GraphQL playground info endpoint
@app.get("/")
async def root():
    return {
        "message": "CML APIs - Unified Biodiversity and Geospatial Data Management",
        "version": "2.0.0",
        "capabilities": [
            "Multi-format data upload (Vector, Raster, Shapefile, CSV, GeoJSON)",
            "Dynamic attribute storage with JSONB",
            "Advanced spatial and attribute querying", 
            "Automatic GeoServer publishing",
            "GraphQL spatial queries",
            "Dataset categorization and management"
        ],
        "endpoints": {
            "unified_data_management": {
                "base": "/unified-data",
                "upload": "/unified-data/upload",
                "datasets": "/unified-data/datasets", 
                "query_features": "/unified-data/query/features",
                "categories": "/unified-data/categories",
                "statistics": "/unified-data/statistics"
            },
            "spatial_graphql": "/v1/graphql",
            "legacy_upload_log": {
                "upload": "/upload_log/upload",
                "list": "/upload_log/",
                "detail": "/upload_log/{id}"
            },
            "geoserver": "/geoserver",
            "health_checks": {
                "main": "/health",
                "unified_data": "/unified-data/health"
            }
        },
        "documentation": {
            "swagger_ui": "/docs",
            "openapi_json": "/openapi.json",
            "redoc": "/redoc"
        }
    }
