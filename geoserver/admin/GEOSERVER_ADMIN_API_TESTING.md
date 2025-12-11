# GeoServer Admin API Testing Guide

This document provides Postman-ready examples for testing all GeoServer Admin API endpoints.

## Base URL

```
http://localhost:8000/admin
```

**Note:** Replace `localhost:8000` with your actual server host and port if different. All admin endpoints are prefixed with `/admin`.

---

## Table of Contents

1. [Workspace Management](#1-workspace-management)
2. [Datastore Management](#2-datastore-management)
3. [Layer Management](#3-layer-management)
4. [Style Management](#4-style-management)
5. [Table Management](#5-table-management)
6. [Layer Creation](#6-layer-creation)

---

## 1. Workspace Management

### 1.1 List All Workspaces

**Endpoint:** `GET /admin/workspaces`

**Description:** Retrieve a list of all workspaces in GeoServer. Workspaces are logical groupings of data stores and layers.

**Postman Setup:**
1. Method: `GET`
2. URL: `http://localhost:8000/admin/workspaces`

**Example cURL:**
```bash
curl -X GET "http://localhost:8000/admin/workspaces"
```

**Expected Response:**
```json
{
  "workspaces": {
    "workspace": [
      {
        "name": "metastring",
        "href": "http://localhost:8080/geoserver/rest/workspaces/metastring.json"
      },
      {
        "name": "topp",
        "href": "http://localhost:8080/geoserver/rest/workspaces/topp.json"
      }
    ]
  }
}
```

---

### 1.2 Create Workspace

**Endpoint:** `POST /admin/workspaces`

**Description:** Create a new workspace in GeoServer. Workspaces organize data stores and layers logically.

**Query Parameters:**
- `workspace_name` (string, required): Name of the workspace to create

**Postman Setup:**
1. Method: `POST`
2. URL: `http://localhost:8000/admin/workspaces?workspace_name=my_workspace`

**Example cURL:**
```bash
curl -X POST "http://localhost:8000/admin/workspaces?workspace_name=my_workspace"
```

**Expected Response:**
```json
{
  "message": "Workspace 'my_workspace' created successfully!",
  "status_code": 201
}
```

---

### 1.3 Get Workspace Details

**Endpoint:** `GET /admin/workspaces/{workspace}`

**Description:** Retrieve detailed information about a specific workspace, including its configuration and properties.

**Path Parameters:**
- `workspace` (string, required): Workspace name (e.g., "metastring")

**Postman Setup:**
1. Method: `GET`
2. URL: `http://localhost:8000/admin/workspaces/metastring`

**Example cURL:**
```bash
curl -X GET "http://localhost:8000/admin/workspaces/metastring"
```

**Expected Response:**
```json
{
  "workspace": {
    "name": "metastring",
    "isolated": false
  }
}
```

---

### 1.4 Update Workspace

**Endpoint:** `PUT /admin/workspaces/{workspace}`

**Description:** Update the configuration and properties of a specific workspace in GeoServer.

**Path Parameters:**
- `workspace` (string, required): Workspace name to update

**Request Type:** `application/json`

**Request Body:**
```json
{
  "new_name": "updated_workspace_name"
}
```

**Postman Setup:**
1. Method: `PUT`
2. URL: `http://localhost:8000/admin/workspaces/metastring`
3. Headers:
   - `Content-Type: application/json`
4. Body → raw → JSON:
   ```json
   {
     "new_name": "updated_workspace_name"
   }
   ```

**Example cURL:**
```bash
curl -X PUT "http://localhost:8000/admin/workspaces/metastring" \
  -H "Content-Type: application/json" \
  -d '{
    "new_name": "updated_workspace_name"
  }'
```

**Expected Response:**
```json
{
  "message": "Workspace 'metastring' updated successfully!"
}
```

---

### 1.5 Delete Workspace

**Endpoint:** `DELETE /admin/workspaces/{workspace}`

**Description:** Delete a specific workspace from GeoServer. This operation will also remove all associated data stores and layers.

**Path Parameters:**
- `workspace` (string, required): Workspace name to delete

**Postman Setup:**
1. Method: `DELETE`
2. URL: `http://localhost:8000/admin/workspaces/my_workspace`

**Example cURL:**
```bash
curl -X DELETE "http://localhost:8000/admin/workspaces/my_workspace"
```

**Expected Response:**
```json
{
  "message": "Workspace 'my_workspace' deleted successfully!"
}
```

---

## 2. Datastore Management

### 2.1 List Datastores

**Endpoint:** `GET /admin/workspaces/{workspace}/datastores`

**Description:** Retrieve a list of all data stores in a specific workspace. Data stores are connections to spatial data sources.

**Path Parameters:**
- `workspace` (string, required): Workspace name

**Postman Setup:**
1. Method: `GET`
2. URL: `http://localhost:8000/admin/workspaces/metastring/datastores`

**Example cURL:**
```bash
curl -X GET "http://localhost:8000/admin/workspaces/metastring/datastores"
```

**Expected Response:**
```json
{
  "dataStores": {
    "dataStore": [
      {
        "name": "postgis_store",
        "href": "http://localhost:8080/geoserver/rest/workspaces/metastring/datastores/postgis_store.json"
      }
    ]
  }
}
```

---

### 2.2 Get Datastore Details

**Endpoint:** `GET /admin/workspaces/{workspace}/datastores/{datastore}`

**Description:** Retrieve detailed information about a specific data store, including connection parameters and configuration.

**Path Parameters:**
- `workspace` (string, required): Workspace name
- `datastore` (string, required): Datastore name

**Postman Setup:**
1. Method: `GET`
2. URL: `http://localhost:8000/admin/workspaces/metastring/datastores/postgis_store`

**Example cURL:**
```bash
curl -X GET "http://localhost:8000/admin/workspaces/metastring/datastores/postgis_store"
```

**Expected Response:**
```json
{
  "dataStore": {
    "name": "postgis_store",
    "type": "PostGIS",
    "enabled": true,
    "connectionParameters": {
      "entry": [
        {
          "@key": "host",
          "$": "localhost"
        },
        {
          "@key": "port",
          "$": "5432"
        }
      ]
    }
  }
}
```

---

### 2.3 Update Datastore

**Endpoint:** `PUT /admin/workspaces/{workspace}/datastores/{datastore}`

**Description:** Update the configuration and connection parameters of a specific data store in a workspace.

**Path Parameters:**
- `workspace` (string, required): Workspace name
- `datastore` (string, required): Datastore name

**Request Type:** `application/json`

**Request Body:**
```json
{
  "new_name": "updated_datastore_name"
}
```

**Postman Setup:**
1. Method: `PUT`
2. URL: `http://localhost:8000/admin/workspaces/metastring/datastores/postgis_store`
3. Headers:
   - `Content-Type: application/json`
4. Body → raw → JSON:
   ```json
   {
     "new_name": "updated_datastore_name"
   }
   ```

**Example cURL:**
```bash
curl -X PUT "http://localhost:8000/admin/workspaces/metastring/datastores/postgis_store" \
  -H "Content-Type: application/json" \
  -d '{
    "new_name": "updated_datastore_name"
  }'
```

**Expected Response:**
```json
{
  "message": "Datastore 'postgis_store' in workspace 'metastring' updated successfully!"
}
```

---

### 2.4 Delete Datastore

**Endpoint:** `DELETE /admin/workspaces/{workspace}/datastores/{datastore}`

**Description:** Delete a specific data store from a workspace. This will remove the connection but not the underlying data source.

**Path Parameters:**
- `workspace` (string, required): Workspace name
- `datastore` (string, required): Datastore name

**Postman Setup:**
1. Method: `DELETE`
2. URL: `http://localhost:8000/admin/workspaces/metastring/datastores/postgis_store`

**Example cURL:**
```bash
curl -X DELETE "http://localhost:8000/admin/workspaces/metastring/datastores/postgis_store"
```

**Expected Response:**
```json
{
  "message": "Datastore 'postgis_store' in workspace 'metastring' deleted successfully!"
}
```

---

## 3. Layer Management

### 3.1 Get Layer Details

**Endpoint:** `GET /admin/layers/{layer}`

**Description:** Retrieve detailed information about a specific layer in GeoServer. This includes layer configuration, default style, resource information, and other layer properties.

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
    },
    "resource": {
      "href": "http://localhost:8080/geoserver/rest/workspaces/metastring/datastores/postgis_store/featuretypes/gbif.json"
    }
  }
}
```

---

### 3.2 Update Layer

**Endpoint:** `PUT /admin/layers/{layer}`

**Description:** Update the configuration and properties of a specific layer, including style settings and default parameters.

**Path Parameters:**
- `layer` (string, required): Layer name (e.g., "metastring:gbif")

**Request Type:** `application/json`

**Request Body:**
```json
{
  "new_name": "updated_layer_name"
}
```

**Postman Setup:**
1. Method: `PUT`
2. URL: `http://localhost:8000/admin/layers/metastring:gbif`
3. Headers:
   - `Content-Type: application/json`
4. Body → raw → JSON:
   ```json
   {
     "new_name": "updated_layer_name"
   }
   ```

**Example cURL:**
```bash
curl -X PUT "http://localhost:8000/admin/layers/metastring:gbif" \
  -H "Content-Type: application/json" \
  -d '{
    "new_name": "updated_layer_name"
  }'
```

**Expected Response:**
```json
{
  "message": "Layer 'metastring:gbif' updated successfully!"
}
```

---

### 3.3 Delete Layer

**Endpoint:** `DELETE /admin/layers/{layer}`

**Description:** Delete a specific layer from GeoServer. This removes the layer configuration but does not delete the underlying data.

**Path Parameters:**
- `layer` (string, required): Layer name (e.g., "metastring:gbif")

**Postman Setup:**
1. Method: `DELETE`
2. URL: `http://localhost:8000/admin/layers/metastring:gbif`

**Example cURL:**
```bash
curl -X DELETE "http://localhost:8000/admin/layers/metastring:gbif"
```

**Expected Response:**
```json
{
  "message": "Layer 'metastring:gbif' deleted successfully!"
}
```

---

## 4. Style Management

### 4.1 List All Styles

**Endpoint:** `GET /admin/styles`

**Description:** Retrieve a list of all styles available in GeoServer. Styles define how layers are rendered on maps, including colors, symbols, and other visual properties.

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
      },
      {
        "name": "line",
        "href": "http://localhost:8080/geoserver/rest/styles/line.json"
      }
    ]
  }
}
```

---

### 4.2 Get Style Details

**Endpoint:** `GET /admin/styles/{style}`

**Description:** Retrieve detailed information about a specific style in GeoServer, including style format, filename, and language version.

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

### 4.3 Update Style

**Endpoint:** `PUT /admin/styles/{style}`

**Description:** Update the configuration and properties of a specific style, including style format and resource location.

**Path Parameters:**
- `style` (string, required): Style name (e.g., "point")

**Request Type:** `application/json`

**Request Body:**
```json
{
  "new_name": "updated_style_name"
}
```

**Postman Setup:**
1. Method: `PUT`
2. URL: `http://localhost:8000/admin/styles/point`
3. Headers:
   - `Content-Type: application/json`
4. Body → raw → JSON:
   ```json
   {
     "new_name": "updated_style_name"
   }
   ```

**Example cURL:**
```bash
curl -X PUT "http://localhost:8000/admin/styles/point" \
  -H "Content-Type: application/json" \
  -d '{
    "new_name": "updated_style_name"
  }'
```

**Expected Response:**
```json
{
  "message": "Style 'point' updated successfully!"
}
```

---

### 4.4 Delete Style

**Endpoint:** `DELETE /admin/styles/{style}`

**Description:** Delete a specific style from GeoServer. Styles define how geographic features are rendered on maps.

**Path Parameters:**
- `style` (string, required): Style name (e.g., "point")

**Postman Setup:**
1. Method: `DELETE`
2. URL: `http://localhost:8000/admin/styles/point`

**Example cURL:**
```bash
curl -X DELETE "http://localhost:8000/admin/styles/point"
```

**Expected Response:**
```json
{
  "message": "Style 'point' deleted successfully!"
}
```

---

## 5. Table Management

### 5.1 List Datastore Tables

**Endpoint:** `GET /admin/workspaces/{workspace}/datastores/{datastore}/tables`

**Description:** List all available tables in a PostGIS data store. Tables represent spatial data that can be published as layers.

**Path Parameters:**
- `workspace` (string, required): Workspace name
- `datastore` (string, required): Datastore name

**Postman Setup:**
1. Method: `GET`
2. URL: `http://localhost:8000/admin/workspaces/metastring/datastores/postgis_store/tables`

**Example cURL:**
```bash
curl -X GET "http://localhost:8000/admin/workspaces/metastring/datastores/postgis_store/tables"
```

**Expected Response:**
```json
{
  "featureTypes": {
    "featureType": [
      {
        "name": "gbif",
        "href": "http://localhost:8080/geoserver/rest/workspaces/metastring/datastores/postgis_store/featuretypes/gbif.json"
      },
      {
        "name": "kew_with_geom",
        "href": "http://localhost:8080/geoserver/rest/workspaces/metastring/datastores/postgis_store/featuretypes/kew_with_geom.json"
      }
    ]
  }
}
```

---

### 5.2 List Schema Tables

**Endpoint:** `GET /admin/workspaces/{workspace}/datastores/{datastore}/schema/{schema}/tables`

**Description:** List all tables in a specific PostGIS schema by querying the database directly. This provides direct access to schema-level tables.

**Path Parameters:**
- `workspace` (string, required): Workspace name
- `datastore` (string, required): Datastore name
- `schema` (string, required): Schema name (e.g., "public", "gis")

**Postman Setup:**
1. Method: `GET`
2. URL: `http://localhost:8000/admin/workspaces/metastring/datastores/postgis_store/schema/public/tables`

**Example cURL:**
```bash
curl -X GET "http://localhost:8000/admin/workspaces/metastring/datastores/postgis_store/schema/public/tables"
```

**Expected Response:**
```json
{
  "tables": [
    {
      "table_name": "gbif",
      "schema_name": "public"
    },
    {
      "table_name": "kew_with_geom",
      "schema_name": "public"
    }
  ]
}
```

---

### 5.3 List Tables Direct

**Endpoint:** `GET /admin/workspaces/{workspace}/datastores/{datastore}/tables-direct`

**Description:** List all tables in a PostGIS schema using direct database query. Allows specifying a custom schema, defaulting to 'public'.

**Path Parameters:**
- `workspace` (string, required): Workspace name
- `datastore` (string, required): Datastore name

**Query Parameters:**
- `schema` (string, optional): Schema name (default: "public")

**Postman Setup:**
1. Method: `GET`
2. URL: `http://localhost:8000/admin/workspaces/metastring/datastores/postgis_store/tables-direct?schema=public`

**Example cURL:**
```bash
# With default schema (public)
curl -X GET "http://localhost:8000/admin/workspaces/metastring/datastores/postgis_store/tables-direct"

# With custom schema
curl -X GET "http://localhost:8000/admin/workspaces/metastring/datastores/postgis_store/tables-direct?schema=gis"
```

**Expected Response:**
```json
{
  "tables": [
    {
      "table_name": "gbif",
      "schema_name": "public",
      "geometry_column": "geom",
      "geometry_type": "POINT"
    },
    {
      "table_name": "kew_with_geom",
      "schema_name": "public",
      "geometry_column": "geom",
      "geometry_type": "POLYGON"
    }
  ]
}
```

---

### 5.4 Get Table Details

**Endpoint:** `GET /admin/workspaces/{workspace}/datastores/{datastore}/tables/{table}`

**Description:** Retrieve detailed information about a specific table in a data store, including column definitions and spatial properties.

**Path Parameters:**
- `workspace` (string, required): Workspace name
- `datastore` (string, required): Datastore name
- `table` (string, required): Table name

**Postman Setup:**
1. Method: `GET`
2. URL: `http://localhost:8000/admin/workspaces/metastring/datastores/postgis_store/tables/gbif`

**Example cURL:**
```bash
curl -X GET "http://localhost:8000/admin/workspaces/metastring/datastores/postgis_store/tables/gbif"
```

**Expected Response:**
```json
{
  "featureType": {
    "name": "gbif",
    "nativeName": "gbif",
    "namespace": {
      "name": "metastring",
      "href": "http://localhost:8080/geoserver/rest/namespaces/metastring.json"
    },
    "title": "GBIF Dataset",
    "keywords": {
      "string": ["features", "gbif"]
    },
    "nativeBoundingBox": {
      "minx": -180.0,
      "maxx": 180.0,
      "miny": -90.0,
      "maxy": 90.0
    },
    "attributes": {
      "attribute": [
        {
          "name": "geom",
          "minOccurs": 0,
          "maxOccurs": 1,
          "nillable": true,
          "binding": "com.vividsolutions.jts.geom.Point"
        },
        {
          "name": "species_count",
          "minOccurs": 0,
          "maxOccurs": 1,
          "nillable": true,
          "binding": "java.lang.Integer"
        }
      ]
    }
  }
}
```

---

## 6. Layer Creation

### 6.1 Create Layer from Table

**Endpoint:** `POST /admin/create-layer`

**Description:** Create a new GeoServer layer from an existing PostGIS table. This publishes a database table as a map layer with specified style and configuration.

**Request Type:** `application/json`

**Request Body:**
```json
{
  "workspace": "metastring",
  "store_name": "postgis_store",
  "table_name": "gbif",
  "layer_name": "gbif_layer",
  "title": "GBIF Dataset Layer",
  "description": "A layer displaying GBIF species occurrence data",
  "enabled": true,
  "default_style": "point"
}
```

**Postman Setup:**
1. Method: `POST`
2. URL: `http://localhost:8000/admin/create-layer`
3. Headers:
   - `Content-Type: application/json`
4. Body → raw → JSON:
   ```json
   {
     "workspace": "metastring",
     "store_name": "postgis_store",
     "table_name": "gbif",
     "layer_name": "gbif_layer",
     "title": "GBIF Dataset Layer",
     "description": "A layer displaying GBIF species occurrence data",
     "enabled": true,
     "default_style": "point"
   }
   ```

**Example cURL:**
```bash
curl -X POST "http://localhost:8000/admin/create-layer" \
  -H "Content-Type: application/json" \
  -d '{
    "workspace": "metastring",
    "store_name": "postgis_store",
    "table_name": "gbif",
    "layer_name": "gbif_layer",
    "title": "GBIF Dataset Layer",
    "description": "A layer displaying GBIF species occurrence data",
    "enabled": true,
    "default_style": "point"
  }'
```

**Expected Response:**
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

**Note:** The `layer_name` field is optional. If not provided, it will default to `table_name`. Similarly, `title`, `description`, `enabled`, and `default_style` are all optional fields.

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

2. **UpdateRequest Model:** When updating resources (workspaces, datastores, layers, styles), the request body should contain:
   ```json
   {
     "new_name": "optional_new_name",
     "new_file_path": "optional_new_file_path"
   }
   ```
   Both fields are optional, but at least one should be provided.

3. **CreateLayerRequest Model:** When creating a layer from a table, the required fields are:
   - `workspace` (string): Target workspace name
   - `store_name` (string): Datastore name
   - `table_name` (string): Table name in the database
   
   Optional fields:
   - `layer_name` (string): Name for the layer (defaults to `table_name`)
   - `title` (string): Display title for the layer
   - `description` (string): Description for the layer
   - `enabled` (boolean): Whether the layer is enabled (default: true)
   - `default_style` (string): Default style for the layer

4. **Naming Restrictions:** Names for workspaces, datastores, layers, styles, and tables must contain only letters, numbers, underscores, and hyphens (e.g., `my_resource`, `resource-123`, `resource_123`).

5. **Authentication:** If your GeoServer requires authentication, ensure the credentials are configured in your `utils/config.py` file.

---

## Postman Collection Import

You can create a Postman collection using the following structure:

1. Create a new collection named "GeoServer Admin APIs"
2. Set collection variable `base_url` to `http://localhost:8000/admin`
3. Create folders for each section:
   - Workspace Management
   - Datastore Management
   - Layer Management
   - Style Management
   - Table Management
   - Layer Creation
4. Add each endpoint as a request with the appropriate method, URL, headers, and body
5. Use variables in URLs like: `{{base_url}}/workspaces/{{workspace_name}}`

---

## Testing Checklist

### Workspace Management
- [ ] List all workspaces
- [ ] Create a new workspace
- [ ] Get workspace details
- [ ] Update workspace
- [ ] Delete workspace

### Datastore Management
- [ ] List datastores in a workspace
- [ ] Get datastore details
- [ ] Update datastore
- [ ] Delete datastore

### Layer Management
- [ ] Get layer details
- [ ] Update layer
- [ ] Delete layer

### Style Management
- [ ] List all styles
- [ ] Get style details
- [ ] Update style
- [ ] Delete style

### Table Management
- [ ] List datastore tables
- [ ] List schema tables
- [ ] List tables direct (with default schema)
- [ ] List tables direct (with custom schema)
- [ ] Get table details

### Layer Creation
- [ ] Create layer from table (with all fields)
- [ ] Create layer from table (with minimal fields)

---

**Last Updated:** 2024-01-01
**API Version:** 1.0

