# GeoServer API Testing Guide

This document provides Postman-ready examples for testing all GeoServer API endpoints (excluding admin endpoints).

## Base URL

```
http://localhost:8000
```

**Note:** Replace `localhost:8000` with your actual server host and port if different.

---

## Table of Contents

1. [Upload Resources](#1-upload-resources)
2. [PostGIS Datastore Management](#2-postgis-datastore-management)
3. [Layer Operations](#3-layer-operations)
4. [Style Operations](#4-style-operations)
5. [Tile Layer URLs](#5-tile-layer-urls)
6. [Layer Data Queries](#6-layer-data-queries)
7. [Upload Log Publishing](#7-upload-log-publishing)

---

## 1. Upload Resources

### 1.1 Upload Shapefile/Resource

**Endpoint:** `POST /upload`

**Description:** Upload a resource (shapefile/style) to GeoServer.

**Request Type:** `multipart/form-data`

**Body Parameters:**
- `workspace` (string, required): Target workspace name (e.g., "metastring")
- `store_name` (string, required): Name of the datastore
- `resource_type` (string, required): Type of resource (e.g., "shapefile")
- `file` (file, required): The file to upload (must be a .zip file for shapefiles)

**Postman Setup:**
1. Method: `POST`
2. URL: `http://localhost:8000/upload`
3. Body → form-data:
   - Key: `workspace`, Value: `metastring`, Type: Text
   - Key: `store_name`, Value: `my_store`, Type: Text
   - Key: `resource_type`, Value: `shapefile`, Type: Text
   - Key: `file`, Value: [Select File], Type: File

**Example cURL:**
```bash
curl -X POST "http://localhost:8000/upload" \
  -F "workspace=metastring" \
  -F "store_name=my_store" \
  -F "resource_type=shapefile" \
  -F "file=@/path/to/your/shapefile.zip"
```

**Expected Response:**
```json
{
  "message": "Resource uploaded successfully!",
  "status_code": 200
}
```

---

## 2. PostGIS Datastore Management

### 2.1 Create PostGIS Datastore

**Endpoint:** `POST /upload-postgis`

**Description:** Upload PostGIS database connection to GeoServer.

**Request Type:** `application/json`

**Request Body:**
```json
{
  "workspace": "metastring",
  "store_name": "postgis_store",
  "database": "your_database",
  "host": "localhost",
  "port": 5432,
  "username": "postgres",
  "password": "your_password",
  "db_schema": "public",
  "description": "PostGIS datastore for spatial data",
  "enabled": true
}
```

**Postman Setup:**
1. Method: `POST`
2. URL: `http://localhost:8000/upload-postgis`
3. Headers:
   - `Content-Type: application/json`
4. Body → raw → JSON:
   ```json
   {
     "workspace": "metastring",
     "store_name": "postgis_store",
     "database": "your_database",
     "host": "localhost",
     "port": 5432,
     "username": "postgres",
     "password": "your_password",
     "db_schema": "public",
     "description": "PostGIS datastore for spatial data",
     "enabled": true
   }
   ```

**Example cURL:**
```bash
curl -X POST "http://localhost:8000/upload-postgis" \
  -H "Content-Type: application/json" \
  -d '{
    "workspace": "metastring",
    "store_name": "postgis_store",
    "database": "your_database",
    "host": "localhost",
    "port": 5432,
    "username": "postgres",
    "password": "your_password",
    "db_schema": "public",
    "description": "PostGIS datastore for spatial data",
    "enabled": true
  }'
```

**Expected Response:**
```json
{
  "message": "PostGIS datastore 'postgis_store' created successfully!",
  "status_code": 201,
  "workspace": "metastring",
  "store_name": "postgis_store",
  "database": "your_database",
  "host": "localhost"
}
```

---

## 3. Layer Operations

### 3.1 List All Layers

**Endpoint:** `GET /layers`

**Description:** List all layers in GeoServer with metadata and styles if available.

**Postman Setup:**
1. Method: `GET`
2. URL: `http://localhost:8000/layers`

**Example cURL:**
```bash
curl -X GET "http://localhost:8000/layers"
```

**Expected Response:**
```json
{
  "layers": {
    "layer": [
      {
        "name": "metastring:gbif",
        "href": "http://localhost:8080/geoserver/rest/layers/gbif.json",
        "geoserverName": "gbif",
        "nameOfDataset": "GBIF Dataset",
        "styles": [
          {
            "styleName": "gbif_species_count_style",
            "styleTitle": "Species Count",
            "styleType": "integer",
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

### 3.2 Get Layer Details

**Endpoint:** `GET /admin/layers/{layer}`

**Description:** Get details of a specific layer.

**Path Parameters:**
- `layer` (string, required): Layer name (e.g., "metastring:gbif" or "gbif")

**Postman Setup:**
1. Method: `GET`
2. URL: `http://localhost:8000/admin/layers/metastring:gbif`

**Example cURL:**
```bash
curl -X GET "http://localhost:8000/admin/layers/metastring:gbif"
```

**Expected Response:**
```json
{
  "layer": {
    "name": "gbif",
    "path": "/metastring/gbif",
    "type": "VECTOR",
    "defaultStyle": {
      "name": "point",
      "href": "http://localhost:8080/geoserver/rest/styles/point.json"
    }
  }
}
```

### 3.3 Get Layer Tile URL

**Endpoint:** `GET /layers/{layer}/tile_url`

**Description:** Get the GeoServer WMS tile layer URL for frontend rendering.

**Path Parameters:**
- `layer` (string, required): Layer name (e.g., "metastring:gbif")

**Postman Setup:**
1. Method: `GET`
2. URL: `http://localhost:8000/layers/metastring:gbif/tile_url`

**Example cURL:**
```bash
curl -X GET "http://localhost:8000/layers/metastring:gbif/tile_url"
```

**Expected Response:**
```json
{
  "tile_url": "http://localhost:8080/geoserver/wms?service=WMS&version=1.1.1&request=GetMap&layers=metastring:gbif&styles=&bbox=-180,-90,180,90&width=256&height=256&srs=EPSG:4326&format=image/png&transparent=true"
}
```

### 3.4 Get Tile URLs for Multiple Datasets

**Endpoint:** `POST /layers/tile_urls`

**Description:** Get WMS tile URLs for multiple datasets at once.

**Request Type:** `application/json`

**Request Body:**
```json
["gbif", "kew_with_geom", "metastring:another_layer"]
```

**Postman Setup:**
1. Method: `POST`
2. URL: `http://localhost:8000/layers/tile_urls`
3. Headers:
   - `Content-Type: application/json`
4. Body → raw → JSON:
   ```json
   ["gbif", "kew_with_geom", "metastring:another_layer"]
   ```

**Example cURL:**
```bash
curl -X POST "http://localhost:8000/layers/tile_urls" \
  -H "Content-Type: application/json" \
  -d '["gbif", "kew_with_geom", "metastring:another_layer"]'
```

**Expected Response:**
```json
{
  "gbif": "http://localhost:8080/geoserver/wms?service=WMS&version=1.1.1&request=GetMap&layers=metastring:gbif&...",
  "kew_with_geom": "http://localhost:8080/geoserver/wms?service=WMS&version=1.1.1&request=GetMap&layers=metastring:kew_with_geom&...",
  "metastring:another_layer": "http://localhost:8080/geoserver/wms?service=WMS&version=1.1.1&request=GetMap&layers=metastring:another_layer&..."
}
```

### 3.5 Query Layer Features

**Endpoint:** `GET /layers/{layer}/features`

**Description:** Fetch features from GeoServer based on query parameters.

**Path Parameters:**
- `layer` (string, required): Layer name (e.g., "metastring:gbif")

**Query Parameters:**
- `bbox` (string, optional): Bounding box in format "minx,miny,maxx,maxy" (e.g., "-180,-90,180,90")
- `filter_query` (string, optional): CQL filter query (e.g., "species_count > 100")

**Postman Setup:**
1. Method: `GET`
2. URL: `http://localhost:8000/layers/metastring:gbif/features?bbox=-180,-90,180,90&filter_query=species_count%3E100`
   - Note: URL encode special characters (e.g., `>` becomes `%3E`)

**Example cURL:**
```bash
# Without filters
curl -X GET "http://localhost:8000/layers/metastring:gbif/features"

# With bounding box
curl -X GET "http://localhost:8000/layers/metastring:gbif/features?bbox=-180,-90,180,90"

# With CQL filter
curl -X GET "http://localhost:8000/layers/metastring:gbif/features?filter_query=species_count%3E100"

# With both
curl -X GET "http://localhost:8000/layers/metastring:gbif/features?bbox=-10,-10,10,10&filter_query=species_count%3E50"
```

**Expected Response:**
```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "id": "gbif.1",
      "geometry": {
        "type": "Point",
        "coordinates": [0.0, 0.0]
      },
      "properties": {
        "species_count": 150,
        "name": "Example Species"
      }
    }
  ]
}
```

### 3.6 Get Layer Columns

**Endpoint:** `GET /layer/columns`

**Description:** Get a simplified schema (columns) for the given layer.

**Query Parameters:**
- `layer` (string, required): Layer name (e.g., "metastring:gbif")

**Postman Setup:**
1. Method: `GET`
2. URL: `http://localhost:8000/layer/columns?layer=metastring:gbif`

**Example cURL:**
```bash
curl -X GET "http://localhost:8000/layer/columns?layer=metastring:gbif"
```

**Expected Response:**
```json
{
  "columns": [
    {
      "name": "species_count",
      "type": "java.lang.Integer",
      "nillable": true,
      "minOccurs": 0,
      "maxOccurs": 1
    },
    {
      "name": "species_name",
      "type": "java.lang.String",
      "nillable": true,
      "minOccurs": 0,
      "maxOccurs": 1
    }
  ]
}
```

### 3.7 Get Layer Data

**Endpoint:** `GET /layer/data`

**Description:** Return feature data for a layer via WFS with optional filters.

**Query Parameters:**
- `layer` (string, required): Layer name (e.g., "metastring:gbif")
- `maxFeatures` (integer, optional): Maximum number of features to return (default: 100)
- `bbox` (string, optional): Bounding box in format "minx,miny,maxx,maxy"
- `filter` (string, optional): CQL filter query
- `properties` (string, optional): Comma-separated list of property names to return

**Postman Setup:**
1. Method: `GET`
2. URL: `http://localhost:8000/layer/data?layer=metastring:gbif&maxFeatures=50&bbox=-10,-10,10,10&filter=species_count%3E100&properties=species_count,species_name`

**Example cURL:**
```bash
# Basic request
curl -X GET "http://localhost:8000/layer/data?layer=metastring:gbif"

# With all parameters
curl -X GET "http://localhost:8000/layer/data?layer=metastring:gbif&maxFeatures=50&bbox=-10,-10,10,10&filter=species_count%3E100&properties=species_count,species_name"
```

**Expected Response:**
```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "id": "gbif.1",
      "geometry": {
        "type": "Point",
        "coordinates": [0.0, 0.0]
      },
      "properties": {
        "species_count": 150,
        "species_name": "Example Species"
      }
    }
  ]
}
```

---

## 4. Style Operations

### 4.1 List All Styles

**Endpoint:** `GET /admin/styles`

**Description:** List all styles in GeoServer.

**Postman Setup:**
1. Method: `GET`
2. URL: `http://localhost:8000/admin/styles`

**Example cURL:**
```bash
curl -X GET "http://localhost:8000/admin/styles"
```

**Expected Response:**
```json
{
  "styles": {
    "style": [
      {
        "name": "point",
        "href": "http://localhost:8080/geoserver/rest/styles/point.json"
      },
      {
        "name": "polygon",
        "href": "http://localhost:8080/geoserver/rest/styles/polygon.json"
      }
    ]
  }
}
```

### 4.2 Get Style Details

**Endpoint:** `GET /admin/styles/{style}`

**Description:** Get details of a specific style.

**Path Parameters:**
- `style` (string, required): Style name (e.g., "point")

**Postman Setup:**
1. Method: `GET`
2. URL: `http://localhost:8000/admin/styles/point`

**Example cURL:**
```bash
curl -X GET "http://localhost:8000/admin/styles/point"
```

**Expected Response:**
```json
{
  "style": {
    "name": "point",
    "filename": "point.sld",
    "format": "sld",
    "languageVersion": {
      "version": "1.0.0"
    }
  }
}
```

---

## 5. Upload Log Publishing

### 5.1 Publish Upload Log

**Endpoint:** `POST /upload_logs/{log_id}/publish`

**Description:** Publish a stored upload log to GeoServer.

**Path Parameters:**
- `log_id` (integer, required): ID of the upload log to publish

**Request Type:** `application/json`

**Request Body:**
```json
{
  "workspace": "metastring",
  "store_name": "my_store",
  "layer_name": "my_layer"
}
```

**Postman Setup:**
1. Method: `POST`
2. URL: `http://localhost:8000/upload_logs/1/publish`
3. Headers:
   - `Content-Type: application/json`
4. Body → raw → JSON:
   ```json
   {
     "workspace": "metastring",
     "store_name": "my_store",
     "layer_name": "my_layer"
   }
   ```

**Example cURL:**
```bash
curl -X POST "http://localhost:8000/upload_logs/1/publish" \
  -H "Content-Type: application/json" \
  -d '{
    "workspace": "metastring",
    "store_name": "my_store",
    "layer_name": "my_layer"
  }'
```

**Expected Response:**
```json
{
  "message": "Uploaded to GeoServer workspace 'metastring' store 'my_store'",
  "status_code": 200,
  "upload_log": {
    "id": "123e4567-e89b-12d3-a456-426614174000",
    "layer_name": "my_layer",
    "file_format": "shapefile",
    "data_type": "VECTOR",
    "crs": "EPSG:4326",
    "bbox": "-180,-90,180,90",
    "source_path": "/path/to/file.shp",
    "geoserver_layer": "my_layer",
    "tags": ["tag1", "tag2"],
    "uploaded_by": "user@example.com",
    "uploaded_on": "2024-01-01T00:00:00"
  }
}
```

---

## Error Responses

All endpoints may return the following error responses:

### 400 Bad Request
```json
{
  "detail": "Error message describing what went wrong"
}
```

### 404 Not Found
```json
{
  "detail": "Resource not found"
}
```

### 500 Internal Server Error
```json
{
  "detail": "Internal server error message"
}
```

---

## Notes

1. **Layer Naming Convention:** Layers can be referenced in two ways:
   - With workspace prefix: `metastring:gbif`
   - Without workspace prefix: `gbif` (if the layer name is unique)

2. **CQL Filter Examples:**
   - `species_count > 100`
   - `name = 'Example'`
   - `date BETWEEN '2024-01-01' AND '2024-12-31'`
   - `geometry INTERSECTS POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))`

3. **Bounding Box Format:** Always use `minx,miny,maxx,maxy` format (e.g., `-180,-90,180,90`)

4. **File Upload:** For shapefile uploads, ensure the file is a valid ZIP archive containing all required shapefile components (.shp, .shx, .dbf, etc.)

5. **Authentication:** If your GeoServer requires authentication, ensure the credentials are configured in your `utils/config.py` file.

---

## Postman Collection Import

You can create a Postman collection using the following structure:

1. Create a new collection named "GeoServer APIs"
2. Set collection variable `base_url` to `http://localhost:8000`
3. Create folders for each section (Upload, Layers, Styles, etc.)
4. Add each endpoint as a request with the appropriate method, URL, headers, and body

---

## Testing Checklist

- [ ] Upload shapefile resource
- [ ] Create PostGIS datastore
- [ ] List all layers
- [ ] Get layer details
- [ ] Get layer tile URL
- [ ] Get tile URLs for multiple datasets
- [ ] Query layer features (with and without filters)
- [ ] Get layer columns
- [ ] Get layer data (with various parameters)
- [ ] List all styles
- [ ] Get style details
- [ ] Publish upload log

---

**Last Updated:** 2024-01-01
**API Version:** 1.0

