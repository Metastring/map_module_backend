# Styles API Testing Guide

This document provides Postman-ready examples for testing all Styles API endpoints.

## Base URL

```
http://localhost:8000
```

**Note:** Replace `localhost:8000` with your actual server host and port if different. All styles endpoints are prefixed with `/styles`.

---

## Table of Contents

1. [Style Generation](#1-style-generation)
2. [Style Metadata](#2-style-metadata)
3. [Legend Retrieval](#3-legend-retrieval)
4. [Frontend Integration](#4-frontend-integration)
5. [Audit Logs](#5-audit-logs)
6. [Error Responses](#6-error-responses)

---

## 1. Style Generation

### 1.1 Generate Style for Layer

**Endpoint:** `POST /styles/generate`

**Description:** Generate a map style for a layer based on column data and classification method. This endpoint reads column information from PostGIS, computes color classes, builds MBStyle JSON, and optionally publishes to GeoServer and attaches it as the default style.

**Request Type:** `application/json`

**Request Body:**
```json
{
  "layer_table_name": "gbif",
  "workspace": "metastring",
  "color_by": "species_count",
  "layer_type": "point",
  "classification_method": "quantile",
  "num_classes": 5,
  "color_palette": "YlOrRd",
  "fill_opacity": 0.7,
  "stroke_color": "#333333",
  "stroke_width": 1.0,
  "publish_to_geoserver": true,
  "attach_to_layer": true,
  "user_id": "user123",
  "user_email": "user@example.com"
}
```

**Query Parameters:**
- `schema` (string, optional): Database schema name (default: "public")

**Postman Setup:**
1. Method: `POST`
2. URL: `http://localhost:8000/styles/generate?schema=public`
3. Headers:
   - `Content-Type: application/json`
4. Body → raw → JSON:
   ```json
   {
     "layer_table_name": "gbif",
     "workspace": "metastring",
     "color_by": "species_count",
     "layer_type": "point",
     "classification_method": "quantile",
     "num_classes": 5,
     "color_palette": "YlOrRd",
     "publish_to_geoserver": true,
     "attach_to_layer": true
   }
   ```

**Example cURL:**
```bash
curl -X POST "http://localhost:8000/styles/generate?schema=public" \
  -H "Content-Type: application/json" \
  -d '{
    "layer_table_name": "gbif",
    "workspace": "metastring",
    "color_by": "species_count",
    "layer_type": "point",
    "classification_method": "quantile",
    "num_classes": 5,
    "color_palette": "YlOrRd",
    "publish_to_geoserver": true,
    "attach_to_layer": true
  }'
```

**Expected Response:**
```json
{
  "success": true,
  "message": "Style generated successfully",
  "style_name": "gbif_species_count_style",
  "mbstyle": {
    "version": 8,
    "name": "gbif_species_count_style",
    "layers": [
      {
        "id": "gbif-species_count-style-circle",
        "type": "circle",
        "paint": {
          "circle-radius": 5,
          "circle-color": ["step", ["get", "species_count"], "#ffffcc", 10, "#ffeda0", 50, "#fed976", 100, "#feb24c", 200, "#fd8d3c"]
        }
      }
    ]
  },
  "classification": {
    "method": "quantile",
    "breaks": [10, 50, 100, 200, 500],
    "colors": ["#ffffcc", "#ffeda0", "#fed976", "#feb24c", "#fd8d3c"],
    "min_value": 0,
    "max_value": 1000,
    "num_classes": 5
  },
  "published_to_geoserver": true,
  "attached_to_layer": true,
  "geoserver_style_url": "http://localhost:8080/geoserver/rest/styles/gbif_species_count_style.json"
}
```

**Required Fields:**
- `layer_table_name` (string): PostGIS table name
- `workspace` (string): GeoServer workspace
- `color_by` (string): Column name to classify by

**Optional Fields:**
- `layer_type` (enum: "point", "line", "polygon", "raster"): Geometry type (default: inferred)
- `classification_method` (enum: "equal_interval", "quantile", "jenks", "categorical", "manual"): Classification method (default: "equal_interval")
- `num_classes` (integer, 2-12): Number of classes (default: 5)
- `color_palette` (string): ColorBrewer palette name (default: "YlOrRd")
- `custom_colors` (array of hex colors): Custom color list (overrides palette)
- `manual_breaks` (array of floats): Manual class breaks (for manual method)
- `fill_opacity` (float, 0-1): Fill opacity (default: 0.7)
- `stroke_color` (string): Stroke color (default: "#333333")
- `stroke_width` (float): Stroke width (default: 1.0)
- `publish_to_geoserver` (boolean): Publish style to GeoServer (default: true)
- `attach_to_layer` (boolean): Set as default style for layer (default: true)
- `user_id` (string): User ID for audit log
- `user_email` (string): User email for audit log

**Classification Methods:**
- `equal_interval`: Divides data into equal-sized ranges
- `quantile`: Divides data into equal-count groups
- `jenks`: Natural breaks optimization
- `categorical`: For categorical/nominal data
- `manual`: Uses provided manual_breaks

---

## 2. Style Metadata

### 2.1 Get Style Metadata by ID

**Endpoint:** `GET /styles/metadata/{style_id}`

**Description:** Retrieve metadata information for a specific style by its ID. Returns style configuration details including layer information, color classification settings, and generation parameters.

**Path Parameters:**
- `style_id` (integer, required): Style ID

**Postman Setup:**
1. Method: `GET`
2. URL: `http://localhost:8000/styles/metadata/1`

**Example cURL:**
```bash
curl -X GET "http://localhost:8000/styles/metadata/1"
```

**Expected Response:**
```json
{
  "id": 1,
  "layer_table_name": "gbif",
  "workspace": "metastring",
  "layer_name": "GBIF Dataset",
  "color_by": "species_count",
  "layer_type": "point",
  "classification_method": "quantile",
  "num_classes": 5,
  "color_palette": "YlOrRd",
  "custom_colors": null,
  "fill_opacity": 0.7,
  "stroke_color": "#333333",
  "stroke_width": 1.0,
  "manual_breaks": null,
  "min_value": 0,
  "max_value": 1000,
  "distinct_values": null,
  "data_type": "integer",
  "generated_style_name": "gbif_species_count_style",
  "is_active": true,
  "last_generated": "2024-01-01T12:00:00",
  "created_at": "2024-01-01T10:00:00",
  "updated_at": "2024-01-01T12:00:00"
}
```

---

## 3. Legend Retrieval

### 3.1 Get Legend with TMS Sources

**Endpoint:** `GET /styles/legend/{style_name}`

**Description:** Retrieve a complete Mapbox GL style (MBStyle) JSON for a style, including TMS tile sources. This endpoint returns the full style definition with vector tile sources configured, ready for use in frontend mapping applications.

**Path Parameters:**
- `style_name` (string, required): Style name (e.g., "gbif_species_count_style")

**Postman Setup:**
1. Method: `GET`
2. URL: `http://localhost:8000/styles/legend/gbif_species_count_style`

**Example cURL:**
```bash
curl -X GET "http://localhost:8000/styles/legend/gbif_species_count_style"
```

**Expected Response:**
```json
{
  "version": 8,
  "name": "gbif_species_count_style",
  "sources": {
    "gbif": {
      "type": "vector",
      "scheme": "tms",
      "tiles": [
        "/geoserver/gwc/service/tms/1.0.0/metastring%3Agbif@EPSG%3A900913@pbf/{z}/{x}/{y}.pbf"
      ]
    }
  },
  "layers": [
    {
      "styleName": "gbif_species_count_style",
      "source": "gbif",
      "source-layer": "gbif",
      "type": "circle",
      "paint": {
        "circle-radius": 5,
        "circle-color": {
          "property": "species_count",
          "type": "interval",
          "stops": [
            [10, "#ffffcc"],
            [50, "#ffeda0"],
            [100, "#fed976"],
            [200, "#feb24c"]
          ]
        }
      }
    }
  ]
}
```

---

## 4. Frontend Integration

### 4.1 Get Styles for Layer

**Endpoint:** `GET /styles/by-layer/{layer_id}`

**Description:** Retrieve all active styles associated with a specific layer by layer ID. Returns style information along with layer metadata including title column, summary columns, and available style configurations for the layer.

**Path Parameters:**
- `layer_id` (UUID, required): Layer ID (metadata ID)

**Query Parameters:**
- `workspace` (string, optional): Workspace name (optional)

**Postman Setup:**
1. Method: `GET`
2. URL: `http://localhost:8000/styles/by-layer/123e4567-e89b-12d3-a456-426614174000?workspace=metastring`

**Example cURL:**
```bash
curl -X GET "http://localhost:8000/styles/by-layer/123e4567-e89b-12d3-a456-426614174000?workspace=metastring"
```

**Expected Response:**
```json
{
  "layerName": "metastring:gbif",
  "titleColumn": "scientific_name",
  "summaryColumn": [
    "scientific_name",
    "species_count",
    "latitude",
    "longitude"
  ],
  "styles": [
    {
      "styleName": "gbif_species_count_style",
      "styleTitle": "Species Count",
      "styleType": "integer",
      "styleId": 1,
      "colorBy": "species_count"
    },
    {
      "styleName": "gbif_scientific_name_style",
      "styleTitle": "Scientific Name",
      "styleType": "string",
      "styleId": 2,
      "colorBy": "scientific_name"
    }
  ]
}
```

---

### 4.2 Get MBStyle with Sources

**Endpoint:** `GET /styles/{style_id}/mbstyle`

**Description:** Retrieve the complete Mapbox GL style JSON for a style ID with TMS tile sources included. This endpoint provides the full style definition configured with vector tile URLs, ready for integration with Mapbox GL or compatible mapping libraries.

**Path Parameters:**
- `style_id` (integer, required): Style ID

**Query Parameters:**
- `layer_name` (string, optional): Layer name for source (e.g., "metastring:gbif")

**Postman Setup:**
1. Method: `GET`
2. URL: `http://localhost:8000/styles/1/mbstyle?layer_name=metastring:gbif`

**Example cURL:**
```bash
curl -X GET "http://localhost:8000/styles/1/mbstyle?layer_name=metastring:gbif"
```

**Expected Response:**
```json
{
  "version": 8,
  "name": "gbif_species_count_style",
  "sources": {
    "metastring:gbif": {
      "type": "vector",
      "scheme": "tms",
      "tiles": [
        "http://localhost:8080/geoserver/gwc/service/tms/1.0.0/metastring%3Agbif@EPSG%3A900913@pbf/{z}/{x}/{y}.pbf"
      ]
    }
  },
  "layers": [
    {
      "id": "gbif-species_count-style-circle",
      "source": "metastring:gbif",
      "source-layer": "gbif",
      "type": "circle",
      "paint": {
        "circle-radius": 5,
        "circle-color": ["step", ["get", "species_count"], "#ffffcc", 10, "#ffeda0", 50, "#fed976", 100, "#feb24c"]
      }
    }
  ]
}
```

---

## 5. Audit Logs

### 5.1 Get Style Audit Logs

**Endpoint:** `GET /styles/audit/{style_id}`

**Description:** Retrieve audit logs for a specific style, showing the history of changes and operations performed on the style. Returns paginated results with timestamps and action details.

**Path Parameters:**
- `style_id` (integer, required): Style ID

**Query Parameters:**
- `skip` (integer, optional): Number of records to skip (default: 0, minimum: 0)
- `limit` (integer, optional): Maximum number of records to return (default: 50, minimum: 1, maximum: 100)

**Postman Setup:**
1. Method: `GET`
2. URL: `http://localhost:8000/styles/audit/1?skip=0&limit=50`

**Example cURL:**
```bash
curl -X GET "http://localhost:8000/styles/audit/1?skip=0&limit=50"
```

**Expected Response:**
```json
[
  {
    "id": 1,
    "action": "generate",
    "user_id": "user123",
    "user_email": "user@example.com",
    "version": 1,
    "status": "success",
    "error_message": null,
    "created_at": "2024-01-01T12:00:00"
  },
  {
    "id": 2,
    "action": "update",
    "user_id": "user123",
    "user_email": "user@example.com",
    "version": 2,
    "status": "success",
    "error_message": null,
    "created_at": "2024-01-02T10:00:00"
  }
]
```

---

## 6. Error Responses

All endpoints may return the following error responses:

### 400 Bad Request
```json
{
  "detail": "Error message describing what went wrong"
}
```

**Example (Style Generation Failed):**
```json
{
  "detail": "Column 'invalid_column' not found in table 'gbif'"
}
```

### 404 Not Found
```json
{
  "detail": "Style not found"
}
```

**Example (Style Metadata):**
```json
{
  "detail": "Style not found"
}
```

**Example (Legend - Style Not Generated):**
```json
{
  "detail": "Style has not been generated yet"
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

1. **Style Generation:** The style generation process reads column information from PostGIS, performs classification, generates MBStyle JSON, and optionally publishes to GeoServer.

2. **Classification Methods:**
   - Use `quantile` for data with skewed distributions
   - Use `equal_interval` for uniform distributions
   - Use `jenks` for natural breaks
   - Use `categorical` for nominal/categorical data
   - Use `manual` with `manual_breaks` for custom classifications

3. **Color Palettes:** Available ColorBrewer palettes include: YlOrRd, YlGnBu, Greens, Blues, Purples, Oranges, Reds, RdYlGn, RdYlBu, Spectral, etc.

4. **Layer Types:** The `layer_type` parameter affects how styles are rendered:
   - `point`: Circle markers
   - `line`: Line styles
   - `polygon`: Fill styles
   - `raster`: Raster overlay styles

5. **TMS Sources:** Vector tile sources use TMS (Tile Map Service) format with GeoServer's GWC (GeoWebCache) tile service.

6. **Style Names:** Generated style names follow the pattern: `{table_name}_{color_by}_style`

7. **Audit Logs:** All style generation and modification operations are logged with user information, timestamps, and status.

8. **Active Styles:** Only active styles are returned by default. Inactive styles are not included in layer style lists.

---

## Postman Collection Import

You can create a Postman collection using the following structure:

1. Create a new collection named "Styles APIs"
2. Set collection variable `base_url` to `http://localhost:8000`
3. Create folders for each section:
   - Style Generation
   - Style Metadata
   - Legend Retrieval
   - Frontend Integration
   - Audit Logs
4. Add each endpoint as a request with the appropriate method, URL, headers, and body

---

## Testing Checklist

- [ ] Generate style with default parameters
- [ ] Generate style with custom classification method
- [ ] Generate style with custom colors
- [ ] Generate style with manual breaks
- [ ] Generate style for point layer
- [ ] Generate style for polygon layer
- [ ] Get style metadata by ID
- [ ] Get style metadata for non-existent ID (should return 404)
- [ ] Get legend by style name
- [ ] Get legend for non-existent style (should return 404)
- [ ] Get styles for layer by ID
- [ ] Get MBStyle with sources
- [ ] Get audit logs for style
- [ ] Get audit logs with pagination

---

**Last Updated:** 2024-01-01
**API Version:** 1.0

