# CML Map Module Backend - Complete API Documentation

## Table of Contents
1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Project Structure](#project-structure)
4. [Module Documentation](#module-documentation)
5. [Complete API Reference](#complete-api-reference)
6. [Configuration](#configuration)
7. [Installation & Setup](#installation--setup)
8. [Usage Examples](#usage-examples)
9. [Database Schema](#database-schema)
10. [Dependencies](#dependencies)

---

## Overview

The CML Map Module Backend is a comprehensive FastAPI-based service that provides spatial data management, visualization, and querying capabilities. It integrates with GeoServer for map rendering, PostgreSQL/PostGIS for spatial data storage, and provides both REST and GraphQL APIs for flexible data access.

### Key Features
- **Spatial Data Management**: Upload, store, and manage spatial datasets (shapefiles, XLSX files)
- **GeoServer Integration**: Publish spatial data as map layers with automatic styling
- **Spatial Queries**: GraphQL-based queries for polygon-based and scientific name searches
- **Metadata Management**: Store and retrieve dataset metadata
- **Style Generation**: Automatic map style generation based on data attributes
- **Upload Logging**: Track all data uploads with comprehensive metadata

---

## Architecture

### Technology Stack
- **Framework**: FastAPI (Python web framework)
- **Database**: PostgreSQL with PostGIS extension
- **Map Server**: GeoServer
- **Query Language**: GraphQL (Strawberry GraphQL)
- **ORM**: SQLAlchemy
- **File Processing**: Fiona, Rasterio, Pandas, OpenPyXL

### Architecture Pattern
The application follows a layered architecture:
- **API Layer**: FastAPI routers handling HTTP requests
- **Service Layer**: Business logic and orchestration
- **DAO Layer**: Data access objects for database operations
- **Model Layer**: Pydantic models for request/response validation

---

## Project Structure

```
map_module_backend/
├── main.py                 # FastAPI application entry point
├── database/
│   ├── database.py        # Database connection and session management
├── geoserver/
│   ├── api.py             # GeoServer REST API endpoints
│   ├── admin/
│   │   └── api.py        # GeoServer admin operations
│   ├── dao.py            # GeoServer data access
│   ├── service.py        # GeoServer business logic
│   └── model.py          # GeoServer request/response models
├── metadata/
│   ├── api/
│   │   └── api.py        # Metadata GraphQL API
│   ├── dao/
│   │   └── dao.py        # Metadata data access
│   ├── models/
│   │   ├── model.py      # GraphQL types
│   │   └── schema.py     # SQLAlchemy models
│   └── service/
│       └── service.py    # Metadata business logic
├── queries/
│   ├── api/
│   │   └── api.py        # Spatial query GraphQL API
│   ├── dao/
│   │   └── dao.py        # Query data access
│   ├── models/
│   │   ├── model.py      # GraphQL types
│   │   └── schema.py     # Data models
│   └── service/
│       └── service.py    # Query business logic
├── styles/
│   ├── api/
│   │   └── api.py        # Style management REST API
│   ├── dao/
│   │   └── dao.py        # Style data access
│   ├── models/
│   │   ├── model.py      # Style request/response models
│   │   └── schema.py     # Style database models
│   └── service/
│       ├── style_service.py      # Style business logic
│       ├── classification.py     # Data classification algorithms
│       ├── color_palettes.py     # Color palette definitions
│       └── mbstyle_builder.py    # Mapbox style builder
├── upload_log/
│   ├── api/
│   │   └── api.py        # Upload log REST API
│   ├── dao/
│   │   └── dao.py        # Upload log data access
│   ├── models/
│   │   ├── model.py      # Upload log request/response models
│   │   └── schema.py     # Upload log database models
│   └── service/
│       ├── service.py    # Upload log business logic
│       └── metadata.py   # File metadata extraction
└── utils/
    └── config.py         # Configuration management
```

---

## Module Documentation

### 1. GeoServer Module (`geoserver/`)

Manages integration with GeoServer for publishing and managing spatial data layers.

**Key Components:**
- **API (`api.py`)**: REST endpoints for layer operations
- **Admin API (`admin/api.py`)**: Administrative operations (workspaces, datastores, layers, styles)
- **DAO (`dao.py`)**: GeoServer REST API client
- **Service (`service.py`)**: Business logic for GeoServer operations

**Responsibilities:**
- Upload spatial resources (shapefiles)
- Create PostGIS datastore connections
- List and manage layers
- Generate WMS tile URLs
- Retrieve layer schema and data via WFS

### 2. Metadata Module (`metadata/`)

Manages dataset metadata using GraphQL API.

**Key Components:**
- **GraphQL API (`api/api.py`)**: GraphQL schema for metadata operations
- **Models (`models/`)**: GraphQL types and SQLAlchemy models
- **Service (`service/service.py`)**: Metadata business logic

**Responsibilities:**
- Create metadata records
- Query metadata by geoserver name
- Filter metadata by various criteria

### 3. Spatial Queries Module (`queries/`)

Provides GraphQL-based spatial query capabilities.

**Key Components:**
- **GraphQL API (`api/api.py`)**: Spatial query GraphQL schema
- **Service (`service/service.py`)**: Query execution logic

**Responsibilities:**
- Polygon-based spatial queries (single and multi-polygon)
- Scientific name-based searches
- Dataset mapping and translation

### 4. Styles Module (`styles/`)

Manages map layer styling and visualization.

**Key Components:**
- **API (`api/api.py`)**: Style management REST endpoints
- **Service (`service/style_service.py`)**: Style generation and management
- **Classification (`service/classification.py`)**: Data classification algorithms
- **MBStyle Builder (`service/mbstyle_builder.py`)**: Mapbox style generation

**Responsibilities:**
- Generate styles based on column data
- Apply classification methods (equal interval, quantile, etc.)
- Create Mapbox GL styles (MBStyle JSON)
- Publish styles to GeoServer
- Retrieve style metadata and legends

### 5. Upload Log Module (`upload_log/`)

Tracks and manages spatial data file uploads.

**Key Components:**
- **API (`api/api.py`)**: Upload log REST endpoints
- **Service (`service/service.py`)**: Upload processing logic
- **Metadata (`service/metadata.py`)**: File metadata extraction

**Responsibilities:**
- Accept file uploads (shapefiles, XLSX)
- Extract spatial metadata (CRS, bounding box, data type)
- Log uploads in database
- Publish to GeoServer automatically
- Create PostGIS tables from XLSX files

---

## Complete API Reference

### Base URL
All endpoints are relative to the base URL where the FastAPI application is running (default: `http://localhost:8000`).

### Authentication
Currently, the API does not require authentication. CORS is enabled for all origins.

---

## 1. Health & Information Endpoints

### GET `/`
**Description**: Root endpoint providing API information and available endpoints.

**Response:**
```json
{
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
```

### GET `/health`
**Description**: Health check endpoint.

**Response:**
```json
{
  "status": "healthy",
  "service": "cmlapis-with-graphql"
}
```

---

## 2. GeoServer APIs (`/geoserver`)

### 2.1 Resource Upload

#### POST `/geoserver/upload`
**Description**: Upload a shapefile or other resource to GeoServer (used for internal API calls).

**Request:**
- **Content-Type**: `multipart/form-data`
- **Parameters:**
  - `workspace` (string, required): Target workspace name (e.g., 'metastring')
  - `store_name` (string, required): Name of the datastore
  - `resource_type` (string, required): Type of resource (e.g., 'shapefile')
  - `file` (file, required): ZIP file containing shapefile components

**Response:**
```json
{
  "message": "Resource uploaded successfully!",
  "status_code": 200
}
```

### 2.2 PostGIS Datastore

#### POST `/geoserver/upload-postgis`
**Description**: Create a PostGIS datastore connection in GeoServer (used for internal API calls).

**Request Body:**
```json
{
  "workspace": "metastring",
  "store_name": "postgis_store",
  "host": "localhost",
  "port": 5432,
  "database": "CML_test",
  "username": "postgres",
  "password": "password",
  "schema": "public"
}
```

**Response:**
```json
{
  "message": "PostGIS datastore 'postgis_store' created successfully!",
  "status_code": 200,
  "workspace": "metastring",
  "store_name": "postgis_store",
  "database": "CML_test",
  "host": "localhost"
}
```

### 2.3 Layer Management

#### GET `/geoserver/layers`
**Description**: List all layers in GeoServer with metadata and styles (used for frontend API calls).

**Response:**
```json
{
  "layers": {
    "layer": [
      {
        "name": "metastring:gbif",
        "href": "http://localhost:8080/geoserver/rest/layers/metastring:gbif.json",
        "id": "uuid",
        "geoserverName": "metastring:gbif",
        "nameOfDataset": "GBIF Occurrence Data",
        "theme": "Biodiversity",
        "styles": [
          {
            "styleName": "gbif_species_count_style",
            "styleTitle": "Species Count",
            "styleType": "numeric",
            "styleId": 1,
            "colorBy": "species_count"
          }
        ],
        "wms_link": "http://localhost:8080/geoserver/wms?..."
      }
    ]
  }
}
```

#### GET `/geoserver/layers/{layer}/tile_url`
**Description**: Get WMS tile URL for a specific layer (used for frontend API calls).

**Parameters:**
- `layer` (path): Layer name (e.g., 'metastring:gbif')

**Response:**
```json
{
  "tile_url": "http://localhost:8080/geoserver/wms?..."
}
```

#### POST `/geoserver/layers/tile_urls`
**Description**: Get tile URLs for multiple datasets at once.

**Request Body:**
```json
{
  "datasets": ["metastring:gbif", "metastring:kew"]
}
```

**Response:**
```json
{
  "metastring:gbif": "http://localhost:8080/geoserver/wms?...",
  "metastring:kew": "http://localhost:8080/geoserver/wms?..."
}
```

### 2.4 Layer Schema & Data

#### GET `/geoserver/layer/columns`
**Description**: Get layer schema/column definitions.

**Query Parameters:**
- `layer` (required): Layer name (e.g., 'metastring:gbif')

**Response:**
```json
{
  "columns": [
    {
      "name": "id",
      "type": "integer",
      "nullable": false
    },
    {
      "name": "geom",
      "type": "geometry",
      "nullable": false
    }
  ]
}
```

#### GET `/geoserver/layer/data`
**Description**: Get layer feature data via WFS with optional filtering.

**Query Parameters:**
- `layer` (required): Layer name
- `maxFeatures` (optional, default: 100): Maximum features to return
- `bbox` (optional): Bounding box filter (format: 'minx,miny,maxx,maxy')
- `filter` (optional): CQL filter expression
- `properties` (optional): Comma-separated property names

**Response:** GeoJSON format with features

### 2.5 Upload Log Publishing

#### POST `/geoserver/upload_logs/{log_id}/publish`
**Description**: Publish a previously uploaded file to GeoServer (used for internal API calls).

**Path Parameters:**
- `log_id` (integer): Upload log ID

**Request Body:**
```json
{
  "workspace": "metastring",
  "store_name": "uploaded_data",
  "layer_name": "my_layer"
}
```

**Response:**
```json
{
  "message": "Layer published successfully",
  "layer_name": "my_layer",
  "workspace": "metastring"
}
```

---

## 3. GeoServer Admin APIs (`/admin`)

### 3.1 Workspace Management

#### GET `/admin/workspaces`
**Description**: List all workspaces in GeoServer.

**Response:**
```json
{
  "workspaces": {
    "workspace": [
      {
        "name": "metastring",
        "href": "http://localhost:8080/geoserver/rest/workspaces/metastring.json"
      }
    ]
  }
}
```

#### POST `/admin/workspaces`
**Description**: Create a new workspace.

**Query Parameters:**
- `workspace_name` (string, required): Name of the workspace

**Response:**
```json
{
  "message": "Workspace 'metastring' created successfully!",
  "status_code": 201
}
```

#### GET `/admin/workspaces/{workspace}`
**Description**: Get workspace details.

**Response:** Workspace configuration JSON

#### PUT `/admin/workspaces/{workspace}`
**Description**: Update workspace configuration.

**Request Body:**
```json
{
  "name": "new_workspace_name"
}
```

#### DELETE `/admin/workspaces/{workspace}`
**Description**: Delete a workspace (removes all associated data stores and layers).

### 3.2 Datastore Management

#### GET `/admin/workspaces/{workspace}/datastores`
**Description**: List all datastores in a workspace.

#### GET `/admin/workspaces/{workspace}/datastores/{datastore}`
**Description**: Get datastore details.

#### PUT `/admin/workspaces/{workspace}/datastores/{datastore}`
**Description**: Update datastore configuration.

#### DELETE `/admin/workspaces/{workspace}/datastores/{datastore}`
**Description**: Delete a datastore.

### 3.3 Table Management

#### GET `/admin/workspaces/{workspace}/datastores/{datastore}/tables`
**Description**: List all tables in a PostGIS datastore.

#### GET `/admin/workspaces/{workspace}/datastores/{datastore}/schema/{schema}/tables`
**Description**: List tables in a specific PostGIS schema.

#### GET `/admin/workspaces/{workspace}/datastores/{datastore}/tables-direct`
**Description**: List tables using direct database query.

**Query Parameters:**
- `schema` (optional, default: "public"): Schema name

#### GET `/admin/workspaces/{workspace}/datastores/{datastore}/tables/{table}`
**Description**: Get table details including column definitions.

### 3.4 Layer Management

#### GET `/admin/layers/{layer}`
**Description**: Get detailed layer information.

#### PUT `/admin/layers/{layer}`
**Description**: Update layer configuration.

**Request Body:**
```json
{
  "defaultStyle": "style_name",
  "enabled": true
}
```

#### DELETE `/admin/layers/{layer}`
**Description**: Delete a layer.

#### POST `/admin/create-layer`
**Description**: Create a layer from a PostGIS table.

**Request Body:**
```json
{
  "workspace": "metastring",
  "store_name": "postgis_store",
  "table_name": "gbif",
  "layer_name": "gbif_layer",
  "default_style": "point"
}
```

**Response:**
```json
{
  "message": "Layer 'gbif_layer' created successfully from table 'gbif'!",
  "status_code": 201,
  "workspace": "metastring",
  "store_name": "postgis_store",
  "table_name": "gbif",
  "layer_name": "gbif_layer"
}
```

### 3.5 Style Management

#### GET `/admin/styles`
**Description**: List all styles in GeoServer.

#### GET `/admin/styles/{style}`
**Description**: Get style details.

#### PUT `/admin/styles/{style}`
**Description**: Update style configuration.

#### DELETE `/admin/styles/{style}`
**Description**: Delete a style.

---

## 4. Upload Log APIs (`/upload_log`)

### 4.1 File Upload

#### POST `/upload_log/upload`
**Description**: Upload a spatial data file (shapefile) and log the upload (used for frontend API calls).

**Request:**
- **Content-Type**: `multipart/form-data`
- **Parameters:**
  - `file` (file, required): Spatial data file (shapefile ZIP)
  - `uploaded_by` (string, required): User identifier
  - `layer_name` (string, optional): Custom layer name
  - `geoserver_layer` (string, optional): GeoServer layer name
  - `tags` (array of strings, optional): Tags for categorization

**Response:**
```json
{
  "id": "uuid",
  "layer_name": "my_layer",
  "file_format": "shp",
  "data_type": "VECTOR",
  "crs": "EPSG:4326",
  "bbox": [-180, -90, 180, 90],
  "source_path": "/path/to/file",
  "geoserver_layer": "my_layer",
  "tags": ["biodiversity", "species"],
  "uploaded_by": "user123",
  "uploaded_on": "2024-01-01T00:00:00"
}
```

#### POST `/upload_log/create-table-and-insert1/`
**Description**: Upload XLSX file, create PostGIS table, and publish to GeoServer (used for frontend API calls).

**Request:**
- **Content-Type**: `multipart/form-data`
- **Parameters:**
  - `table_name` (string, required): Name for the database table
  - `schema` (string, required): Database schema name
  - `file` (file, required): XLSX file
  - `uploaded_by` (string, optional): User identifier (for logging)
  - `layer_name` (string, optional): Layer name
  - `tags` (array of strings, optional): Tags
  - `workspace` (string, default: "metastring"): GeoServer workspace
  - `store_name` (string, optional): GeoServer store name

**Response:**
```json
{
  "message": "Table created and data inserted successfully",
  "upload_log_id": "uuid"
}
```

### 4.2 Upload Log Queries

#### GET `/upload_log/`
**Description**: List upload logs with optional filtering.

**Query Parameters:**
- `id` (UUID, optional): Filter by ID
- `layer_name` (string, optional): Filter by layer name
- `file_format` (string, optional): Filter by file format
- `data_type` (enum, optional): Filter by data type (VECTOR, RASTER, UNKNOWN)
- `crs` (string, optional): Filter by CRS
- `source_path` (string, optional): Filter by source path
- `geoserver_layer` (string, optional): Filter by GeoServer layer
- `tags` (array of strings, optional): Filter by tags
- `uploaded_by` (string, optional): Filter by uploader
- `uploaded_on` (string, optional): Filter by upload date (ISO format)

**Response:**
```json
[
  {
    "id": "uuid",
    "layer_name": "my_layer",
    "file_format": "shp",
    "data_type": "VECTOR",
    "uploaded_by": "user123",
    "uploaded_on": "2024-01-01T00:00:00"
  }
]
```

#### GET `/upload_log/{log_id}`
**Description**: Get detailed upload log by ID.

**Path Parameters:**
- `log_id` (UUID): Upload log ID

**Response:** Complete upload log object

---

## 5. Metadata GraphQL API (`/metadata`)

### GraphQL Endpoint
**URL**: `/metadata`

### Schema

#### Queries

##### `get(geoserver_name: String!): MetadataType`
**Description**: Get metadata by GeoServer layer name.

**Example Query:**
```graphql
query {
  get(geoserverName: "metastring:gbif") {
    id
    geoserverName
    nameOfDataset
    theme
    keywords
    dataType
    contactPerson
    organization
    contactEmail
    country
    createdOn
    updatedOn
  }
}
```

**Response:**
```json
{
  "data": {
    "get": {
      "id": "uuid",
      "geoserverName": "metastring:gbif",
      "nameOfDataset": "GBIF Occurrence Data",
      "theme": "Biodiversity",
      "keywords": ["species", "occurrence"],
      "dataType": "Point",
      "contactPerson": "John Doe",
      "organization": "Research Institute",
      "contactEmail": "john@example.com",
      "country": "USA",
      "createdOn": "2024-01-01T00:00:00",
      "updatedOn": "2024-01-01T00:00:00"
    }
  }
}
```

##### `getAny(filters: MetadataFilterInput): [MetadataType]`
**Description**: Get metadata with optional filters.

**Input Type:**
```graphql
input MetadataFilterInput {
  nameOfDataset: String
  theme: String
  keywords: [String]
  dataType: String
  country: String
  organization: String
}
```

**Example Query:**
```graphql
query {
  getAny(filters: {
    theme: "Biodiversity"
    country: "USA"
  }) {
    id
    geoserverName
    nameOfDataset
    theme
  }
}
```

#### Mutations

##### `create(metadataData: MetadataInput!): MetadataType`
**Description**: Create new metadata record.

**Input Type:**
```graphql
input MetadataInput {
  geoserverName: String!
  nameOfDataset: String
  theme: String
  keywords: [String]
  purposeOfCreatingData: String
  dataType: String
  contactPerson: String
  organization: String
  contactEmail: String
  country: String
  accessConstraints: String
  useConstraints: String
  mailingAddress: String
  cityLocalityCountry: String
}
```

**Example Mutation:**
```graphql
mutation {
  create(metadataData: {
    geoserverName: "metastring:new_layer"
    nameOfDataset: "New Dataset"
    theme: "Ecology"
    keywords: ["forest", "trees"]
    dataType: "Polygon"
    contactPerson: "Jane Doe"
    organization: "Ecology Lab"
    contactEmail: "jane@example.com"
    country: "Canada"
  }) {
    id
    geoserverName
    nameOfDataset
  }
}
```

---

## 6. Spatial Query GraphQL API (`/v1/spatial_search`)

### GraphQL Endpoint
**URL**: `/v1/spatial_search`

### Schema

#### Queries

##### `getPolygonData(input: SpatialQueryInput!): SpatialQueryType`
**Description**: Query spatial data within a single polygon boundary.

**Input Type:**
```graphql
input SpatialQueryInput {
  dataset: String!
  polygonDetail: [[[Float!]!]!]!  # Array of coordinate arrays
  limit: Int
  offset: Int
}
```

**Example Query:**
```graphql
query {
  getPolygonData(input: {
    dataset: "gbif"
    polygonDetail: [[
      [[-122.5, 37.7], [-122.4, 37.7], [-122.4, 37.8], [-122.5, 37.8], [-122.5, 37.7]]
    ]]
    limit: 100
    offset: 0
  }) {
    results {
      gbif {
        count
        features {
          geometry
          properties
        }
      }
    }
  }
}
```

**Response:**
```json
{
  "data": {
    "getPolygonData": {
      "results": {
        "gbif": {
          "count": 150,
          "features": [
            {
              "geometry": {
                "type": "Point",
                "coordinates": [-122.45, 37.75]
              },
              "properties": {
                "species": "Quercus agrifolia",
                "year": 2020
              }
            }
          ]
        }
      }
    }
  }
}
```

##### `getMultiPolygonData(input: SpatialQueryInput!): SpatialQueryType`
**Description**: Query spatial data within multiple polygon boundaries.

**Input Type:** Same as `getPolygonData`

**Example Query:**
```graphql
query {
  getMultiPolygonData(input: {
    dataset: "gbif"
    polygonDetail: [
      [[[-122.5, 37.7], [-122.4, 37.7], [-122.4, 37.8], [-122.5, 37.8], [-122.5, 37.7]]],
      [[[-122.3, 37.6], [-122.2, 37.6], [-122.2, 37.7], [-122.3, 37.7], [-122.3, 37.6]]]
    ]
    limit: 100
  }) {
    results {
      gbif {
        count
        features {
          geometry
          properties
        }
      }
    }
  }
}
```

##### `getScientificNameMatches(input: ScientificNameInput!): SpatialQueryType`
**Description**: Search for spatial data by scientific name.

**Input Type:**
```graphql
input ScientificNameInput {
  scientificName: String!
}
```

**Example Query:**
```graphql
query {
  getScientificNameMatches(input: {
    scientificName: "Quercus"
  }) {
    results {
      gbif {
        count
        features {
          geometry
          properties
        }
      }
      kew {
        count
        features {
          geometry
          properties
        }
      }
    }
  }
}
```

---

## 7. Styles API (`/styles`)

### 7.1 Style Generation

#### POST `/styles/generate`
**Description**: Generate a map style for a layer based on column data and classification method.

**Query Parameters:**
- `schema` (string, default: "public"): Database schema

**Request Body:**
```json
{
  "workspace": "metastring",
  "layer_table_name": "gbif",
  "color_by": "species_count",
  "classification_method": "quantile",
  "num_classes": 5,
  "color_palette": "viridis",
  "publish_to_geoserver": true,
  "attach_as_default": true
}
```

**Response:**
```json
{
  "success": true,
  "message": "Style generated successfully",
  "style_id": 1,
  "style_name": "gbif_species_count_style",
  "mbstyle_json": {
    "version": 8,
    "layers": [...]
  }
}
```

### 7.2 Style Metadata

#### GET `/styles/metadata/{style_id}`
**Description**: Get style metadata by ID.

**Path Parameters:**
- `style_id` (integer): Style ID

**Response:**
```json
{
  "id": 1,
  "workspace": "metastring",
  "layer_table_name": "gbif",
  "color_by": "species_count",
  "classification_method": "quantile",
  "num_classes": 5,
  "color_palette": "viridis",
  "generated_style_name": "gbif_species_count_style",
  "is_active": true,
  "created_at": "2024-01-01T00:00:00"
}
```

### 7.3 Style Legend

#### GET `/styles/legend/{style_name}`
**Description**: Get complete Mapbox GL style JSON with TMS sources.

**Path Parameters:**
- `style_name` (string): Style name

**Response:** Complete MBStyle JSON with sources configured

### 7.4 Frontend Integration

#### GET `/styles/by-layer/{layer_id}`
**Description**: Get all active styles for a specific layer.

**Path Parameters:**
- `layer_id` (UUID): Layer ID (from metadata table)

**Query Parameters:**
- `workspace` (string, optional): Workspace name

**Response:**
```json
{
  "layerName": "metastring:gbif",
  "titleColumn": "species",
  "summaryColumn": ["species", "year", "count"],
  "styles": [
    {
      "styleName": "gbif_species_count_style",
      "styleTitle": "Species Count",
      "styleType": "numeric",
      "styleId": 1,
      "colorBy": "species_count"
    }
  ]
}
```

#### GET `/styles/{style_id}/mbstyle`
**Description**: Get MBStyle JSON with sources for a style ID.

**Path Parameters:**
- `style_id` (integer): Style ID

**Query Parameters:**
- `layer_name` (string, optional): Layer name for source

**Response:** Complete MBStyle JSON with TMS sources

### 7.5 Audit Logs

#### GET `/styles/audit/{style_id}`
**Description**: Get audit logs for a style.

**Path Parameters:**
- `style_id` (integer): Style ID

**Query Parameters:**
- `skip` (integer, default: 0): Pagination offset
- `limit` (integer, default: 50, max: 100): Number of records

**Response:**
```json
[
  {
    "id": 1,
    "style_id": 1,
    "action": "created",
    "details": "Style generated",
    "created_at": "2024-01-01T00:00:00"
  }
]
```

---

## Configuration

### Database Configuration (`utils/config.py`)

```python
# PostgreSQL Configuration
host = "localhost"
port = 5432
username = "postgres"
password = "password"
database = "CML_test"
database_url = f"postgresql://{username}:{password}@{host}:{port}/{database}"

# GeoServer Configuration
geoserver_host = "localhost"
geoserver_port = "8080"
geoserver_username = "admin"
geoserver_password = "geoserver"

# Dataset Mapping
DATASET_MAPPING = {
    "gbif": "gbif",
    "kew": "kew_with_geom"
}
```

### Environment Variables
The application supports loading configuration from `.env` file using `python-dotenv`.

### Secure Configuration (`secure.ini`)
For production, use `secure.ini` file for sensitive configuration (currently not implemented but structure exists).

---

## Installation & Setup

### Prerequisites
- Python 3.8 or higher
- PostgreSQL 12+ with PostGIS extension
- GeoServer 2.19+ (running and accessible)
- Virtual environment (recommended)

### Installation Steps

1. **Clone the repository** (if applicable)

2. **Create and activate virtual environment:**
```bash
python -m venv env
source env/bin/activate  # On Windows: env\Scripts\activate
```

3. **Install dependencies:**
```bash
pip install -r requirements.txt
```

4. **Configure database:**
   - Create PostgreSQL database with PostGIS extension:
   ```sql
   CREATE DATABASE CML_test;
   \c CML_test
   CREATE EXTENSION postgis;
   ```

5. **Update configuration:**
   - Edit `utils/config.py` with your database and GeoServer credentials

6. **Run database migrations** (if applicable):
   - Ensure all required tables are created (metadata, upload_logs, styles, etc.)

7. **Start the application:**
```bash
uvicorn main:app --reload
```

The API will be available at `http://localhost:8000`

### Accessing API Documentation
- **Swagger UI**: `http://localhost:8000/docs`
- **ReDoc**: `http://localhost:8000/redoc`
- **GraphQL Playground**: 
  - Spatial Queries: `http://localhost:8000/v1/spatial_search`
  - Metadata: `http://localhost:8000/metadata`

---

## Usage Examples

### Example 1: Upload and Publish Shapefile

```python
import requests

# Upload shapefile
with open('data.zip', 'rb') as f:
    files = {'file': f}
    data = {
        'uploaded_by': 'user123',
        'layer_name': 'my_species_data',
        'tags': ['biodiversity', 'species']
    }
    response = requests.post(
        'http://localhost:8000/upload_log/upload',
        files=files,
        data=data
    )
    upload_log = response.json()
    print(f"Uploaded: {upload_log['id']}")
```

### Example 2: Query Spatial Data with GraphQL

```python
import requests

query = """
query {
  getPolygonData(input: {
    dataset: "gbif"
    polygonDetail: [[
      [[-122.5, 37.7], [-122.4, 37.7], [-122.4, 37.8], [-122.5, 37.8], [-122.5, 37.7]]
    ]]
    limit: 10
  }) {
    results {
      gbif {
        count
        features {
          geometry
          properties
        }
      }
    }
  }
}
"""

response = requests.post(
    'http://localhost:8000/v1/spatial_search',
    json={'query': query}
)
data = response.json()
print(f"Found {data['data']['getPolygonData']['results']['gbif']['count']} features")
```

### Example 3: Generate Style for Layer

```python
import requests

style_request = {
    "workspace": "metastring",
    "layer_table_name": "gbif",
    "color_by": "species_count",
    "classification_method": "quantile",
    "num_classes": 5,
    "color_palette": "viridis",
    "publish_to_geoserver": True,
    "attach_as_default": True
}

response = requests.post(
    'http://localhost:8000/styles/generate?schema=public',
    json=style_request
)
style = response.json()
print(f"Style created: {style['style_name']}")
```

### Example 4: Create Metadata

```python
import requests

mutation = """
mutation {
  create(metadataData: {
    geoserverName: "metastring:my_layer"
    nameOfDataset: "My Research Data"
    theme: "Ecology"
    keywords: ["forest", "trees"]
    dataType: "Point"
    contactPerson: "John Doe"
    organization: "Research Lab"
    contactEmail: "john@example.com"
    country: "USA"
  }) {
    id
    geoserverName
    nameOfDataset
  }
}
"""

response = requests.post(
    'http://localhost:8000/metadata',
    json={'query': mutation}
)
result = response.json()
print(f"Metadata created: {result['data']['create']['id']}")
```

---

## Database Schema

### Metadata Table
Stores dataset metadata information.

**Key Fields:**
- `id` (UUID): Primary key
- `geoserver_name` (String): Layer name in GeoServer
- `name_of_dataset` (String): Human-readable dataset name
- `theme` (String): Dataset theme/category
- `keywords` (Array): Search keywords
- `data_type` (String): Data type (Point, Polygon, etc.)
- `contact_person`, `organization`, `contact_email`: Contact information
- `country` (String): Country of origin
- `created_on`, `updated_on` (Timestamp): Timestamps

### Upload Logs Table
Tracks file uploads.

**Key Fields:**
- `id` (UUID): Primary key
- `layer_name` (String): Layer identifier
- `file_format` (String): File format (shp, xlsx, etc.)
- `data_type` (Enum): VECTOR, RASTER, UNKNOWN
- `crs` (String): Coordinate reference system
- `bbox` (Geometry): Bounding box
- `source_path` (String): File system path
- `geoserver_layer` (String): Published layer name
- `tags` (Array): Categorization tags
- `uploaded_by` (String): User identifier
- `uploaded_on` (Timestamp): Upload timestamp

### Styles Table
Stores style configurations.

**Key Fields:**
- `id` (Integer): Primary key
- `workspace` (String): GeoServer workspace
- `layer_table_name` (String): Database table name
- `color_by` (String): Column used for coloring
- `classification_method` (Enum): Classification algorithm
- `num_classes` (Integer): Number of color classes
- `color_palette` (String): Color palette name
- `generated_style_name` (String): Style name in GeoServer
- `mbstyle_json` (JSON): Mapbox style definition
- `is_active` (Boolean): Active status
- `created_at`, `updated_at` (Timestamp): Timestamps

---

## Dependencies

### Core Dependencies
- **fastapi**: Web framework
- **uvicorn**: ASGI server
- **strawberry-graphql[fastapi]**: GraphQL implementation
- **sqlalchemy**: ORM
- **psycopg2-binary**: PostgreSQL driver
- **pydantic[email]**: Data validation

### Spatial Processing
- **shapely**: Geometric operations
- **fiona**: Vector data I/O
- **rasterio**: Raster data I/O
- **pyproj**: Coordinate transformations

### Data Processing
- **pandas**: Data manipulation
- **openpyxl**: Excel file processing

### Utilities
- **requests**: HTTP client
- **httpx**: Async HTTP client
- **aiofiles**: Async file operations
- **python-dotenv**: Environment variable management
- **boto3**: AWS SDK (if needed)
- **jinja2**: Template engine

---

## API Endpoint Summary

### REST Endpoints

| Method | Endpoint | Description | Module |
|--------|----------|-------------|--------|
| GET | `/` | API information | Main |
| GET | `/health` | Health check | Main |
| POST | `/geoserver/upload` | Upload resource | GeoServer |
| POST | `/geoserver/upload-postgis` | Create PostGIS store | GeoServer |
| GET | `/geoserver/layers` | List layers | GeoServer |
| GET | `/geoserver/layers/{layer}/tile_url` | Get tile URL | GeoServer |
| POST | `/geoserver/layers/tile_urls` | Get multiple tile URLs | GeoServer |
| GET | `/geoserver/layer/columns` | Get layer schema | GeoServer |
| GET | `/geoserver/layer/data` | Get layer data | GeoServer |
| POST | `/geoserver/upload_logs/{id}/publish` | Publish upload log | GeoServer |
| GET | `/admin/workspaces` | List workspaces | GeoServer Admin |
| POST | `/admin/workspaces` | Create workspace | GeoServer Admin |
| GET | `/admin/workspaces/{workspace}` | Get workspace | GeoServer Admin |
| PUT | `/admin/workspaces/{workspace}` | Update workspace | GeoServer Admin |
| DELETE | `/admin/workspaces/{workspace}` | Delete workspace | GeoServer Admin |
| GET | `/admin/workspaces/{workspace}/datastores` | List datastores | GeoServer Admin |
| GET | `/admin/workspaces/{workspace}/datastores/{datastore}` | Get datastore | GeoServer Admin |
| PUT | `/admin/workspaces/{workspace}/datastores/{datastore}` | Update datastore | GeoServer Admin |
| DELETE | `/admin/workspaces/{workspace}/datastores/{datastore}` | Delete datastore | GeoServer Admin |
| GET | `/admin/workspaces/{workspace}/datastores/{datastore}/tables` | List tables | GeoServer Admin |
| GET | `/admin/workspaces/{workspace}/datastores/{datastore}/tables/{table}` | Get table | GeoServer Admin |
| GET | `/admin/layers/{layer}` | Get layer details | GeoServer Admin |
| PUT | `/admin/layers/{layer}` | Update layer | GeoServer Admin |
| DELETE | `/admin/layers/{layer}` | Delete layer | GeoServer Admin |
| POST | `/admin/create-layer` | Create layer | GeoServer Admin |
| GET | `/admin/styles` | List styles | GeoServer Admin |
| GET | `/admin/styles/{style}` | Get style | GeoServer Admin |
| PUT | `/admin/styles/{style}` | Update style | GeoServer Admin |
| DELETE | `/admin/styles/{style}` | Delete style | GeoServer Admin |
| POST | `/upload_log/upload` | Upload file | Upload Log |
| POST | `/upload_log/create-table-and-insert1/` | Upload XLSX | Upload Log |
| GET | `/upload_log/` | List upload logs | Upload Log |
| GET | `/upload_log/{id}` | Get upload log | Upload Log |
| POST | `/styles/generate` | Generate style | Styles |
| GET | `/styles/metadata/{id}` | Get style metadata | Styles |
| GET | `/styles/legend/{name}` | Get style legend | Styles |
| GET | `/styles/by-layer/{layer_id}` | Get styles for layer | Styles |
| GET | `/styles/{id}/mbstyle` | Get MBStyle JSON | Styles |
| GET | `/styles/audit/{id}` | Get audit logs | Styles |

### GraphQL Endpoints

| Endpoint | Description | Module |
|----------|-------------|--------|
| `/v1/spatial_search` | Spatial query GraphQL | Queries |
| `/metadata` | Metadata GraphQL | Metadata |

---

## Error Handling

All endpoints follow standard HTTP status codes:
- **200**: Success
- **201**: Created
- **400**: Bad Request (validation errors)
- **404**: Not Found
- **500**: Internal Server Error
- **502**: Bad Gateway (GeoServer errors)

Error responses follow this format:
```json
{
  "detail": "Error message description"
}
```

---

## Notes

- All file uploads are stored in the `uploads/` directory (created automatically)
- GeoServer workspace "metastring" is used by default for uploads
- Dataset names are mapped using `DATASET_MAPPING` in config
- CORS is enabled for all origins (configure in `main.py` for production)
- GraphQL endpoints support introspection and GraphQL Playground

---

## Support & Maintenance

For issues, questions, or contributions, please refer to the project repository or contact the development team.

**Last Updated**: 2024

