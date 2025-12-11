# Upload Log API Testing Guide

This document provides Postman-ready examples for testing all Upload Log API endpoints.

## Base URL

```
http://localhost:8000
```

**Note:** Replace `localhost:8000` with your actual server host and port if different. All upload log endpoints are prefixed with `/upload_logs`.

---

## Table of Contents

1. [Upload Shapefile](#1-upload-shapefile)
2. [List Upload Logs](#2-list-upload-logs)
3. [Get Upload Log by ID](#3-get-upload-log-by-id)
4. [Upload XLSX File](#4-upload-xlsx-file)
5. [Error Responses](#5-error-responses)

---

## 1. Upload Shapefile

### 1.1 Upload Shapefile and Log

**Endpoint:** `POST /upload_logs/upload`

**Description:** Upload a spatial data file (e.g., shapefile) to the system. This endpoint accepts spatial data uploads, extracts metadata automatically, stores the file, logs the upload in the database, and optionally publishes shapefiles to GeoServer. Used for frontend API calls.

**Request Type:** `multipart/form-data`

**Body Parameters:**
- `file` (file, required): The spatial data file to upload (e.g., .shp, .zip containing shapefile)
- `uploaded_by` (string, required): Identifier for the user performing the upload
- `layer_name` (string, optional): Canonical name for the uploaded layer
- `geoserver_layer` (string, optional): GeoServer layer name if different from layer_name
- `tags` (array of strings, optional): Arbitrary labels for grouping

**Postman Setup:**
1. Method: `POST`
2. URL: `http://localhost:8000/upload_logs/upload`
3. Body → form-data:
   - Key: `file`, Value: [Select File], Type: File
   - Key: `uploaded_by`, Value: `user@example.com`, Type: Text
   - Key: `layer_name`, Value: `my_layer`, Type: Text
   - Key: `geoserver_layer`, Value: `my_geoserver_layer`, Type: Text
   - Key: `tags`, Value: `tag1,tag2,tag3`, Type: Text

**Example cURL:**
```bash
curl -X POST "http://localhost:8000/upload_logs/upload" \
  -F "file=@/path/to/your/shapefile.zip" \
  -F "uploaded_by=user@example.com" \
  -F "layer_name=my_layer" \
  -F "geoserver_layer=my_geoserver_layer" \
  -F "tags=tag1,tag2,tag3"
```

**Expected Response:**
```json
{
  "id": "123e4567-e89b-12d3-a456-426614174000",
  "layer_name": "my_layer",
  "file_format": "shp",
  "data_type": "vector",
  "crs": "EPSG:4326",
  "bbox": {
    "minx": -122.5,
    "miny": 37.7,
    "maxx": -122.3,
    "maxy": 37.9
  },
  "source_path": "/path/to/uploads/abc123.zip",
  "geoserver_layer": "my_layer",
  "tags": ["tag1", "tag2", "tag3"],
  "uploaded_by": "user@example.com",
  "uploaded_on": "2024-01-01T12:00:00"
}
```

**Notes:**
- Shapefiles are automatically published to GeoServer if the file format is recognized as a shapefile
- The system automatically extracts spatial metadata (CRS, bounding box) from the uploaded file
- If `layer_name` is not provided, it will be derived from the filename
- Tags can be provided as comma-separated string or will be parsed from comma-separated input

---

## 2. List Upload Logs

### 2.1 List Upload Logs with Filters

**Endpoint:** `GET /upload_logs/`

**Description:** Retrieve a list of upload logs with optional filtering. This endpoint allows you to query upload logs by various criteria such as layer name, file format, data type, CRS, source path, GeoServer layer, tags, uploaded by user, and upload date.

**Query Parameters:**
- `id` (UUID, optional): Upload log ID
- `layer_name` (string, optional): Layer name filter
- `file_format` (string, optional): File format filter (e.g., "shp", "xlsx")
- `data_type` (enum, optional): Data type filter ("vector", "raster", "unknown")
- `crs` (string, optional): Coordinate reference system filter
- `source_path` (string, optional): Source path filter
- `geoserver_layer` (string, optional): GeoServer layer name filter
- `tags` (array of strings, optional): Tags filter
- `uploaded_by` (string, optional): User identifier filter
- `uploaded_on` (string, optional): Upload date filter (ISO format)

**Postman Setup:**
1. Method: `GET`
2. URL: `http://localhost:8000/upload_logs/?layer_name=my_layer&data_type=vector`

**Example cURL:**
```bash
# List all upload logs
curl -X GET "http://localhost:8000/upload_logs/"

# List with filters
curl -X GET "http://localhost:8000/upload_logs/?layer_name=my_layer&data_type=vector&uploaded_by=user@example.com"

# List by date
curl -X GET "http://localhost:8000/upload_logs/?uploaded_on=2024-01-01T00:00:00"
```

**Expected Response:**
```json
[
  {
    "id": "123e4567-e89b-12d3-a456-426614174000",
    "layer_name": "my_layer",
    "file_format": "shp",
    "data_type": "vector",
    "crs": "EPSG:4326",
    "bbox": {
      "minx": -122.5,
      "miny": 37.7,
      "maxx": -122.3,
      "maxy": 37.9
    },
    "source_path": "/path/to/uploads/abc123.zip",
    "geoserver_layer": "my_layer",
    "tags": ["tag1", "tag2"],
    "uploaded_by": "user@example.com",
    "uploaded_on": "2024-01-01T12:00:00"
  },
  {
    "id": "223e4567-e89b-12d3-a456-426614174001",
    "layer_name": "another_layer",
    "file_format": "xlsx",
    "data_type": "unknown",
    "crs": "UNKNOWN",
    "bbox": null,
    "source_path": "/path/to/uploads/def456.xlsx",
    "geoserver_layer": "another_layer",
    "tags": ["tag3"],
    "uploaded_by": "user@example.com",
    "uploaded_on": "2024-01-02T10:00:00"
  }
]
```

---

## 3. Get Upload Log by ID

### 3.1 Get Upload Log Details

**Endpoint:** `GET /upload_logs/{log_id}`

**Description:** Retrieve detailed information about a specific upload log by its unique identifier. Returns complete upload log metadata including file details, spatial information, and GeoServer publication status.

**Path Parameters:**
- `log_id` (UUID, required): Upload log ID

**Postman Setup:**
1. Method: `GET`
2. URL: `http://localhost:8000/upload_logs/123e4567-e89b-12d3-a456-426614174000`

**Example cURL:**
```bash
curl -X GET "http://localhost:8000/upload_logs/123e4567-e89b-12d3-a456-426614174000"
```

**Expected Response:**
```json
{
  "id": "123e4567-e89b-12d3-a456-426614174000",
  "layer_name": "my_layer",
  "file_format": "shp",
  "data_type": "vector",
  "crs": "EPSG:4326",
  "bbox": {
    "minx": -122.5,
    "miny": 37.7,
    "maxx": -122.3,
    "maxy": 37.9
  },
  "source_path": "/path/to/uploads/abc123.zip",
  "geoserver_layer": "my_layer",
  "tags": ["tag1", "tag2", "tag3"],
  "uploaded_by": "user@example.com",
  "uploaded_on": "2024-01-01T12:00:00"
}
```

---

## 4. Upload XLSX File

### 4.1 Upload XLSX and Create PostGIS Table

**Endpoint:** `POST /upload_logs/create-table-and-insert1/`

**Description:** Upload an XLSX file and automatically create a PostGIS table with the data. This endpoint processes Excel files, creates a database table in the specified schema, inserts the data, publishes it to GeoServer as a layer, and optionally logs the upload if uploaded_by is provided. Used for frontend API calls.

**Request Type:** `multipart/form-data`

**Body Parameters:**
- `table_name` (string, required): Name for the PostGIS table to create
- `schema` (string, required): Database schema name (e.g., "public")
- `file` (file, required): XLSX file to upload
- `uploaded_by` (string, optional): User identifier for logging (if provided, creates upload log entry)
- `layer_name` (string, optional): Layer name for the upload log
- `tags` (array of strings, optional): Tags for the upload log
- `workspace` (string, optional): GeoServer workspace (default: "metastring")
- `store_name` (string, optional): GeoServer datastore name

**Postman Setup:**
1. Method: `POST`
2. URL: `http://localhost:8000/upload_logs/create-table-and-insert1/`
3. Body → form-data:
   - Key: `table_name`, Value: `my_table`, Type: Text
   - Key: `schema`, Value: `public`, Type: Text
   - Key: `file`, Value: [Select XLSX File], Type: File
   - Key: `uploaded_by`, Value: `user@example.com`, Type: Text
   - Key: `layer_name`, Value: `my_layer`, Type: Text
   - Key: `tags`, Value: `tag1,tag2`, Type: Text
   - Key: `workspace`, Value: `metastring`, Type: Text
   - Key: `store_name`, Value: `my_store`, Type: Text

**Example cURL:**
```bash
curl -X POST "http://localhost:8000/upload_logs/create-table-and-insert1/" \
  -F "table_name=my_table" \
  -F "schema=public" \
  -F "file=@/path/to/your/file.xlsx" \
  -F "uploaded_by=user@example.com" \
  -F "layer_name=my_layer" \
  -F "tags=tag1,tag2" \
  -F "workspace=metastring" \
  -F "store_name=my_store"
```

**Expected Response:**
```json
{
  "message": "Table 'my_table' created successfully in schema 'public' and published to GeoServer workspace 'metastring' store 'my_store'",
  "upload_log_id": "323e4567-e89b-12d3-a456-426614174002"
}
```

**Response without upload logging (uploaded_by not provided):**
```json
{
  "message": "Table 'my_table' created successfully in schema 'public' and published to GeoServer workspace 'metastring' store 'my_store'"
}
```

**Notes:**
- Only XLSX files are accepted
- If `uploaded_by` is provided and not empty, an upload log entry is created with the generated dataset_id
- The table is automatically created in PostGIS with columns based on the Excel file structure
- The table is automatically published to GeoServer as a layer
- The `upload_log_id` is only included in the response if `uploaded_by` was provided
- If upload logging is enabled, the `geoserver_layer` field in the upload log is updated with the table name after successful GeoServer publication

---

## 5. Error Responses

All endpoints may return the following error responses:

### 400 Bad Request
```json
{
  "detail": "Error message describing what went wrong"
}
```

**Example (File Name Required):**
```json
{
  "detail": "File name is required"
}
```

**Example (Invalid File Format):**
```json
{
  "detail": "Only XLSX files are allowed"
}
```

**Example (Unable to Read Metadata):**
```json
{
  "detail": "Unable to read spatial metadata"
}
```

**Example (Invalid Date Format):**
```json
{
  "detail": "uploaded_on must be ISO formatted"
}
```

### 404 Not Found
```json
{
  "detail": "Upload log not found"
}
```

### 500 Internal Server Error
```json
{
  "detail": "Internal server error message"
}
```

**Example (File Persistence Failed):**
```json
{
  "detail": "Failed to persist file"
}
```

**Example (GeoServer Upload Failed):**
```json
{
  "detail": "Stored upload file is missing; cannot publish to GeoServer."
}
```

---

## Notes

1. **File Formats:** Supported file formats include:
   - Shapefiles (.shp, .zip containing shapefile components)
   - Excel files (.xlsx)

2. **Automatic Metadata Extraction:** For spatial files, the system automatically extracts:
   - Coordinate Reference System (CRS)
   - Bounding box (bbox)
   - Data type (vector/raster)
   - File format

3. **GeoServer Integration:** 
   - Shapefiles are automatically published to GeoServer
   - XLSX files create PostGIS tables that are then published to GeoServer
   - The `geoserver_layer` field is updated after successful publication

4. **Upload Logging:** 
   - Upload logs are created when `uploaded_by` is provided
   - For XLSX uploads, logging is optional (backward compatible)
   - Upload logs track the full lifecycle of uploaded files

5. **Tags:** Tags can be provided as:
   - Comma-separated string: `"tag1,tag2,tag3"`
   - Array of strings: `["tag1", "tag2", "tag3"]`

6. **Data Types:** Possible values for `data_type`:
   - `"vector"`: Vector spatial data
   - `"raster"`: Raster spatial data
   - `"unknown"`: Unknown or non-spatial data

7. **Bounding Box Format:** The bounding box is stored as a JSON object with keys:
   - `minx`: Minimum X coordinate (longitude)
   - `miny`: Minimum Y coordinate (latitude)
   - `maxx`: Maximum X coordinate (longitude)
   - `maxy`: Maximum Y coordinate (latitude)

8. **UUID Format:** Upload log IDs are UUIDs in standard format (e.g., `123e4567-e89b-12d3-a456-426614174000`).

9. **Date Format:** The `uploaded_on` parameter in queries must be in ISO format (e.g., `2024-01-01T00:00:00`).

10. **File Storage:** Uploaded files are stored in the server's uploads directory with unique filenames to prevent conflicts.

---

## Postman Collection Import

You can create a Postman collection using the following structure:

1. Create a new collection named "Upload Log APIs"
2. Set collection variable `base_url` to `http://localhost:8000`
3. Create folders for each section:
   - Upload Operations
   - Query Operations
4. Add each endpoint as a request with the appropriate method, URL, headers, and body
5. For file uploads, use form-data body type

---

## Testing Checklist

- [ ] Upload shapefile with all parameters
- [ ] Upload shapefile with minimal parameters
- [ ] Upload shapefile (verify automatic GeoServer publication)
- [ ] Upload XLSX file and create table
- [ ] Upload XLSX file with upload logging enabled
- [ ] Upload XLSX file without upload logging (backward compatibility)
- [ ] List all upload logs
- [ ] List upload logs with single filter
- [ ] List upload logs with multiple filters
- [ ] List upload logs by date
- [ ] Get upload log by ID (existing)
- [ ] Get upload log by ID (non-existent - should return 404)
- [ ] Upload invalid file format (should return 400)
- [ ] Upload file without required parameters (should return 400)

---

**Last Updated:** 2024-01-01
**API Version:** 1.0

