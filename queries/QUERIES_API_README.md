# Spatial Queries API Documentation

## Overview

The Spatial Queries API provides GraphQL endpoints for querying spatial biodiversity data from multiple datasets. This API allows you to:

- Query data within single or multiple polygon boundaries
- Search for species by scientific name
- Retrieve point-based and polygon-based spatial data
- Support pagination for large result sets

## Base Configuration

- **Base URL**: `http://127.0.0.1:8001` (or your server URL)
- **GraphQL Endpoint**: `/v1/spatial_search`
- **Method**: POST
- **Content-Type**: `application/json`

**Note**: To verify the exact endpoint, check the root endpoint (`GET /`) which returns all available endpoints, or check your server's OpenAPI documentation at `/docs`.

## Supported Datasets

- **gbif**: Global Biodiversity Information Facility - Point-based occurrence data
- **kew**: Royal Botanic Gardens Kew - Polygon-based species distribution data

## API Endpoints

### GraphQL Endpoint

**URL**: `POST /v1/spatial_search`

All queries are sent as POST requests to this endpoint with a GraphQL query in the request body.

## GraphQL Queries

### 1. getPolygonData

Query spatial data within a single polygon boundary. Returns all data points and features that intersect with the provided polygon geometry.

**Query Structure:**
```graphql
query GetPolygonData($input: SpatialQueryInput!) {
  getPolygonData(input: $input) {
    results
  }
}
```

**Variables:**
```json
{
  "input": {
    "dataset": ["gbif", "kew"],
    "polygon_detail": [
      {
        "geometry": {
          "type": "Polygon",
          "coordinates": [[[-122.4, 37.8], [-122.3, 37.8], [-122.3, 37.9], [-122.4, 37.9], [-122.4, 37.8]]]
        }
      }
    ],
    "limit": 100,
    "offset": 0
  }
}
```

**Parameters:**
- `dataset` (required): Array of dataset names to query (e.g., `["gbif", "kew"]`)
- `polygon_detail` (required): Array containing exactly one polygon geometry object
- `limit` (optional): Maximum number of results per dataset (default: 1000)
- `offset` (optional): Number of results to skip for pagination (default: 0)
- `category` (optional): Category filter (currently not used in queries)

**Response Format:**
```json
{
  "data": {
    "getPolygonData": {
      "results": {
        "gbif": [
          {
            "scientificName": "Species name",
            "longitude": -122.35,
            "latitude": 37.85,
            ...
          }
        ],
        "kew": [
          {
            "scientificName": "Species name",
            "geom_geojson": {...},
            ...
          }
        ]
      }
    }
  }
}
```

### 2. getMultiPolygonData

Query spatial data within multiple polygon boundaries. Useful for querying non-contiguous regions or multiple areas at once.

**Query Structure:**
```graphql
query GetMultiPolygonData($input: SpatialQueryInput!) {
  getMultiPolygonData(input: $input) {
    results
  }
}
```

**Variables:**
```json
{
  "input": {
    "dataset": ["gbif", "kew"],
    "polygon_detail": [
      {
        "geometry": {
          "type": "Polygon",
          "coordinates": [[[-122.4, 37.8], [-122.3, 37.8], [-122.3, 37.9], [-122.4, 37.9], [-122.4, 37.8]]]
        }
      },
      {
        "geometry": {
          "type": "Polygon",
          "coordinates": [[[-122.2, 37.7], [-122.1, 37.7], [-122.1, 37.8], [-122.2, 37.8], [-122.2, 37.7]]]
        }
      }
    ],
    "limit": 200
  }
}
```

**Parameters:**
- `dataset` (required): Array of dataset names to query
- `polygon_detail` (required): Array containing one or more polygon geometry objects (at least one required)
- `limit` (optional): Maximum number of results per dataset (default: 1000)
- `offset` (optional): Number of results to skip for pagination (default: 0)
- `category` (optional): Category filter

**Error Handling:**
- Returns HTTP 400 if no polygons are provided
- Returns HTTP 500 for internal server errors

### 3. getScientificNameMatches

Search for spatial data by scientific name. Performs case-insensitive partial match search across all datasets.

**Query Structure:**
```graphql
query GetScientificNameMatches($input: ScientificNameInput!) {
  getScientificNameMatches(input: $input) {
    results
  }
}
```

**Variables:**
```json
{
  "input": {
    "scientificName": "Quercus"
  }
}
```

**Parameters:**
- `scientificName` (required): Scientific name to search for (case-insensitive, supports partial matching)

**Response Format:**
Returns all matching records from all datasets with their associated geographic data.

## Data Types

### Polygon Geometry Format

Polygons must be provided in GeoJSON format:

```json
{
  "type": "Polygon",
  "coordinates": [
    [
      [longitude1, latitude1],
      [longitude2, latitude2],
      [longitude3, latitude3],
      [longitude1, latitude1]  // Must close the polygon
    ]
  ]
}
```

**Important Notes:**
- Coordinates are in WGS84 (EPSG:4326) format: `[longitude, latitude]`
- First and last coordinates must be the same (closed polygon)
- Coordinates are ordered as `[longitude, latitude]` (not lat/lon)

## Postman Collection

### Setup

1. Create a new request in Postman
2. Set method to **POST**
3. Set URL to: `http://127.0.0.1:8001/v1/spatial_search`
4. Add header: `Content-Type: application/json`
5. Use the Body tab:
   - Select **GraphQL** body type (recommended), which provides separate Query and Variables tabs
   - OR select **raw** with **JSON** format, including both `query` and `variables` fields

### Postman Request Examples

#### Example 1: Single Polygon Query (getPolygonData)

**Query:**
```graphql
query GetPolygonData($input: SpatialQueryInput!) {
  getPolygonData(input: $input) {
    results
  }
}
```

**Variables:**
```json
{
  "input": {
    "dataset": ["gbif", "kew"],
    "polygon_detail": [
      {
        "geometry": {
          "type": "Polygon",
          "coordinates": [[[-122.4, 37.8], [-122.3, 37.8], [-122.3, 37.9], [-122.4, 37.9], [-122.4, 37.8]]]
        }
      }
    ],
    "limit": 100,
    "offset": 0
  }
}
```

**Postman Setup:**
- Use **GraphQL** body type with Query and Variables tabs, or use raw JSON format as shown below:

```json
{
  "query": "query GetPolygonData($input: SpatialQueryInput!) { getPolygonData(input: $input) { results } }",
  "variables": {
    "input": {
      "dataset": ["gbif", "kew"],
      "polygon_detail": [
        {
          "geometry": {
            "type": "Polygon",
            "coordinates": [[[-122.4, 37.8], [-122.3, 37.8], [-122.3, 37.9], [-122.4, 37.9], [-122.4, 37.8]]]
          }
        }
      ],
      "limit": 100,
      "offset": 0
    }
  }
}
```

#### Example 2: Single Polygon Query (GBIF Only)

**Query:**
```graphql
query GetPolygonData($input: SpatialQueryInput!) {
  getPolygonData(input: $input) {
    results
  }
}
```

**Variables:**
```json
{
  "input": {
    "dataset": ["gbif"],
    "polygon_detail": [
      {
        "geometry": {
          "type": "Polygon",
          "coordinates": [[[-122.4, 37.8], [-122.3, 37.8], [-122.3, 37.9], [-122.4, 37.9], [-122.4, 37.8]]]
        }
      }
    ],
    "limit": 50
  }
}
```

#### Example 3: Multi-Polygon Query (getMultiPolygonData)

**Query:**
```graphql
query GetMultiPolygonData($input: SpatialQueryInput!) {
  getMultiPolygonData(input: $input) {
    results
  }
}
```

**Variables:**
```json
{
  "input": {
    "dataset": ["gbif", "kew"],
    "polygon_detail": [
      {
        "geometry": {
          "type": "Polygon",
          "coordinates": [[[-122.4, 37.8], [-122.3, 37.8], [-122.3, 37.9], [-122.4, 37.9], [-122.4, 37.8]]]
        }
      },
      {
        "geometry": {
          "type": "Polygon",
          "coordinates": [[[-122.2, 37.7], [-122.1, 37.7], [-122.1, 37.8], [-122.2, 37.8], [-122.2, 37.7]]]
        }
      }
    ],
    "limit": 200
  }
}
```

#### Example 4: Scientific Name Search (getScientificNameMatches)

**Query:**
```graphql
query GetScientificNameMatches($input: ScientificNameInput!) {
  getScientificNameMatches(input: $input) {
    results
  }
}
```

**Variables:**
```json
{
  "input": {
    "scientificName": "Quercus"
  }
}
```

#### Example 5: Scientific Name Search (Specific Species)

**Query:**
```graphql
query GetScientificNameMatches($input: ScientificNameInput!) {
  getScientificNameMatches(input: $input) {
    results
  }
}
```

**Variables:**
```json
{
  "input": {
    "scientificName": "Quercus alba"
  }
}
```

#### Example 6: Large Area Query with Pagination

**Query:**
```graphql
query GetPolygonData($input: SpatialQueryInput!) {
  getPolygonData(input: $input) {
    results
  }
}
```

**Variables (First Page):**
```json
{
  "input": {
    "dataset": ["gbif"],
    "polygon_detail": [
      {
        "geometry": {
          "type": "Polygon",
          "coordinates": [[[-124.5, 32.5], [-114.0, 32.5], [-114.0, 42.0], [-124.5, 42.0], [-124.5, 32.5]]]
        }
      }
    ],
    "limit": 1000,
    "offset": 0
  }
}
```

**Variables (Second Page):**
```json
{
  "input": {
    "dataset": ["gbif"],
    "polygon_detail": [
      {
        "geometry": {
          "type": "Polygon",
          "coordinates": [[[-124.5, 32.5], [-114.0, 32.5], [-114.0, 42.0], [-124.5, 42.0], [-124.5, 32.5]]]
        }
      }
    ],
    "limit": 1000,
    "offset": 1000
  }
}
```

#### Example 7: Query Only GBIF Dataset

**Query:**
```graphql
query GetPolygonData($input: SpatialQueryInput!) {
  getPolygonData(input: $input) {
    results
  }
}
```

**Variables:**
```json
{
  "input": {
    "dataset": ["gbif"],
    "polygon_detail": [
      {
        "geometry": {
          "type": "Polygon",
          "coordinates": [[[-122.4, 37.8], [-122.3, 37.8], [-122.3, 37.9], [-122.4, 37.9], [-122.4, 37.8]]]
        }
      }
    ],
    "limit": 10
  }
}
```

#### Example 8: Query Only Kew Dataset

**Query:**
```graphql
query GetPolygonData($input: SpatialQueryInput!) {
  getPolygonData(input: $input) {
    results
  }
}
```

**Variables:**
```json
{
  "input": {
    "dataset": ["kew"],
    "polygon_detail": [
      {
        "geometry": {
          "type": "Polygon",
          "coordinates": [[[-122.4, 37.8], [-122.3, 37.8], [-122.3, 37.9], [-122.4, 37.9], [-122.4, 37.8]]]
        }
      }
    ],
    "limit": 10
  }
}
```

## Postman Environment Variables

For easier testing, create a Postman environment with these variables:

```
base_url: http://127.0.0.1:8001
graphql_endpoint: /v1/spatial_search
```

Then use in your requests:
- URL: `{{base_url}}{{graphql_endpoint}}`

## cURL Examples

### Single Polygon Query
```bash
curl -X POST http://127.0.0.1:8001/v1/spatial_search \
  -H "Content-Type: application/json" \
  -d '{
    "query": "query GetPolygonData($input: SpatialQueryInput!) { getPolygonData(input: $input) { results } }",
    "variables": {
      "input": {
        "dataset": ["gbif"],
        "polygon_detail": [
          {
            "geometry": {
              "type": "Polygon",
              "coordinates": [[[-122.4, 37.8], [-122.3, 37.8], [-122.3, 37.9], [-122.4, 37.9], [-122.4, 37.8]]]
            }
          }
        ],
        "limit": 10
      }
    }
  }'
```

### Multi-Polygon Query
```bash
curl -X POST http://127.0.0.1:8001/v1/spatial_search \
  -H "Content-Type: application/json" \
  -d '{
    "query": "query GetMultiPolygonData($input: SpatialQueryInput!) { getMultiPolygonData(input: $input) { results } }",
    "variables": {
      "input": {
        "dataset": ["gbif", "kew"],
        "polygon_detail": [
          {
            "geometry": {
              "type": "Polygon",
              "coordinates": [[[-122.4, 37.8], [-122.3, 37.8], [-122.3, 37.9], [-122.4, 37.9], [-122.4, 37.8]]]
            }
          },
          {
            "geometry": {
              "type": "Polygon",
              "coordinates": [[[-122.2, 37.7], [-122.1, 37.7], [-122.1, 37.8], [-122.2, 37.8], [-122.2, 37.7]]]
            }
          }
        ],
        "limit": 100
      }
    }
  }'
```

### Scientific Name Search
```bash
curl -X POST http://127.0.0.1:8001/v1/spatial_search \
  -H "Content-Type: application/json" \
  -d '{
    "query": "query GetScientificNameMatches($input: ScientificNameInput!) { getScientificNameMatches(input: $input) { results } }",
    "variables": {
      "input": {
        "scientificName": "Quercus"
      }
    }
  }'
```

## Response Format Details

### GBIF Dataset Response
Point-based data returns:
- `longitude`: Extracted X coordinate
- `latitude`: Extracted Y coordinate
- All other fields from the GBIF table

### Kew Dataset Response
Polygon-based data returns:
- `geom_geojson`: Full geometry as GeoJSON object
- All other fields from the Kew table

### Common Response Structure
```json
{
  "data": {
    "getPolygonData": {
      "results": {
        "gbif": [
          {
            "scientificName": "Species name",
            "longitude": -122.35,
            "latitude": 37.85,
            "field1": "value1",
            "field2": "value2"
          }
        ],
        "kew": [
          {
            "scientificName": "Species name",
            "geom_geojson": {
              "type": "Polygon",
              "coordinates": [[...]]
            },
            "field1": "value1",
            "field2": "value2"
          }
        ]
      }
    }
  }
}
```

## Error Responses

### Validation Error (400)
```json
{
  "errors": [
    {
      "message": "At least one polygon must be provided",
      "extensions": {
        "status_code": 400
      }
    }
  ]
}
```

### Server Error (500)
```json
{
  "errors": [
    {
      "message": "Internal server error: [error details]",
      "extensions": {
        "status_code": 500
      }
    }
  ]
}
```

## Testing Tips

1. **Start Small**: Begin with small polygons and low limits to test the API
2. **Check Coordinates**: Ensure coordinates are in `[longitude, latitude]` format
3. **Close Polygons**: Always ensure the first and last coordinates are identical
4. **Use Pagination**: For large areas, use limit/offset for pagination
5. **Test Each Dataset**: Test with individual datasets first, then combine
6. **Scientific Names**: Try both full names and partial matches

## Coordinate Examples

### San Francisco Bay Area
```json
"coordinates": [[
  [-122.5, 37.7],
  [-122.3, 37.7],
  [-122.3, 37.9],
  [-122.5, 37.9],
  [-122.5, 37.7]
]]
```

### California State (Approximate)
```json
"coordinates": [[
  [-124.5, 32.5],
  [-114.0, 32.5],
  [-114.0, 42.0],
  [-124.5, 42.0],
  [-124.5, 32.5]
]]
```

### London Area
```json
"coordinates": [[
  [-0.5, 51.4],
  [-0.1, 51.4],
  [-0.1, 51.6],
  [-0.5, 51.6],
  [-0.5, 51.4]
]]
```

## Notes

- All spatial operations use WGS84 (EPSG:4326) coordinate system
- Dataset names are automatically mapped (e.g., "kew" → "kew_with_geom")
- NaN values are automatically converted to `null` in responses
- Scientific name searches are case-insensitive and support partial matching
- The API uses PostGIS for efficient spatial queries
- Results are organized by dataset name in the response

## Quick Reference

| Query | Purpose | Key Parameter |
|-------|---------|---------------|
| `getPolygonData` | Single area search | `polygon_detail` (1 polygon) |
| `getMultiPolygonData` | Multiple areas search | `polygon_detail` (multiple polygons) |
| `getScientificNameMatches` | Species name search | `scientificName` |

## Support

For issues or questions:
1. Check the main README.md for architecture details
2. Verify database connectivity
3. Ensure PostGIS is installed and enabled
4. Check server logs for detailed error messages

