from fastapi import FastAPI
from geoserver.api import router as geoserver_router  # Import router directly

# Import the new spatial queries API
from queries.api.api import SpatialQueryAPI1  # Import the original API for comparison

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
            "geoserver": "/geoserver",
            "health_check": "/health"
        }
    }
