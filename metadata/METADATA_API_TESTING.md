# Metadata API Testing Guide

This document provides Postman-ready examples for testing all Metadata API endpoints.

## Base URL

```
http://localhost:8000
```

**Note:** Replace `localhost:8000` with your actual server host and port if different. All metadata endpoints are GraphQL-based.

---

## Table of Contents

1. [GraphQL Endpoint](#1-graphql-endpoint)
2. [Queries](#2-queries)
3. [Mutations](#3-mutations)
4. [Error Responses](#4-error-responses)

---

## 1. GraphQL Endpoint

**Endpoint:** `POST /v1/metadata`

**Description:** All metadata operations are performed through a single GraphQL endpoint. Queries are used to retrieve metadata, and mutations are used to create or modify metadata.

**Request Type:** `application/json`

**Content-Type:** `application/json`

---

## 2. Queries

### 2.1 Get Metadata by GeoServer Name

**Description:** Retrieve metadata information for a specific dataset by its GeoServer layer name.

**GraphQL Query:**
```graphql
query GetMetadata($geoserverName: String!) {
  get(geoserverName: $geoserverName) {
    id
    geoserverName
    nameOfDataset
    theme
    keywords
    purposeOfCreatingData
    dataType
    contactPerson
    organization
    contactEmail
    country
    createdOn
    updatedOn
    accessConstraints
    useConstraints
    mailingAddress
    cityLocalityCountry
  }
}
```

**Variables:**
```json
{
  "geoserverName": "gbif"
}
```

**Postman Setup:**
1. Method: `POST`
2. URL: `http://localhost:8000/v1/metadata`
3. Headers:
   - `Content-Type: application/json`
4. Body → GraphQL (or raw → JSON):
   - **Query** tab:
     ```graphql
     query GetMetadata($geoserverName: String!) {
       get(geoserverName: $geoserverName) {
         id
         geoserverName
         nameOfDataset
         theme
         keywords
         purposeOfCreatingData
         dataType
         contactPerson
         organization
         contactEmail
         country
         createdOn
         updatedOn
         accessConstraints
         useConstraints
         mailingAddress
         cityLocalityCountry
       }
     }
     ```
   - **Variables** tab:
     ```json
     {
       "geoserverName": "gbif"
     }
     ```
   - If using raw JSON format:
     ```json
     {
       "query": "query GetMetadata($geoserverName: String!) { get(geoserverName: $geoserverName) { id geoserverName nameOfDataset theme keywords purposeOfCreatingData dataType contactPerson organization contactEmail country createdOn updatedOn accessConstraints useConstraints mailingAddress cityLocalityCountry } }",
       "variables": {
         "geoserverName": "gbif"
       }
     }
     ```

**Example cURL:**
```bash
curl -X POST "http://localhost:8000/v1/metadata" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "query GetMetadata($geoserverName: String!) { get(geoserverName: $geoserverName) { id geoserverName nameOfDataset theme keywords } }",
    "variables": {
      "geoserverName": "gbif"
    }
  }'
```

**Expected Response:**
```json
{
  "data": {
    "get": {
      "id": "123e4567-e89b-12d3-a456-426614174000",
      "geoserverName": "gbif",
      "nameOfDataset": "GBIF Dataset",
      "theme": "Biodiversity",
      "keywords": ["species", "occurrence", "biodiversity"],
      "purposeOfCreatingData": "Research",
      "dataType": "VECTOR",
      "contactPerson": "John Doe",
      "organization": "Research Organization",
      "contactEmail": "john@example.com",
      "country": "USA",
      "createdOn": "2024-01-01T00:00:00",
      "updatedOn": "2024-01-01T00:00:00",
      "accessConstraints": "None",
      "useConstraints": "CC-BY",
      "mailingAddress": "123 Research St",
      "cityLocalityCountry": "New York, USA"
    }
  }
}
```

---

### 2.2 Get Metadata with Filters

**Description:** Retrieve metadata records matching specified filter criteria. All filter fields are optional and can be combined.

**GraphQL Query:**
```graphql
query GetAny($filters: MetadataFilterInput) {
  getAny(filters: $filters) {
    id
    geoserverName
    nameOfDataset
    theme
    keywords
    country
    contactPerson
    organization
  }
}
```

**Variables:**
```json
{
  "filters": {
    "theme": "Biodiversity",
    "country": "USA",
    "dataType": "VECTOR"
  }
}
```

**Postman Setup:**
1. Method: `POST`
2. URL: `http://localhost:8000/v1/metadata`
3. Headers:
   - `Content-Type: application/json`
4. Body → GraphQL (or raw → JSON):
   - **Query** tab:
     ```graphql
     query GetAny($filters: MetadataFilterInput) {
       getAny(filters: $filters) {
         id
         geoserverName
         nameOfDataset
         theme
         keywords
         country
         contactPerson
         organization
       }
     }
     ```
   - **Variables** tab:
     ```json
     {
       "filters": {
         "theme": "Biodiversity",
         "country": "USA",
         "dataType": "VECTOR"
       }
     }
     ```
   - If using raw JSON format:
     ```json
     {
       "query": "query GetAny($filters: MetadataFilterInput) { getAny(filters: $filters) { id geoserverName nameOfDataset theme keywords country contactPerson organization } }",
       "variables": {
         "filters": {
           "theme": "Biodiversity",
           "country": "USA",
           "dataType": "VECTOR"
         }
       }
     }
     ```

**Example cURL:**
```bash
curl -X POST "http://localhost:8000/v1/metadata" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "query GetAny($filters: MetadataFilterInput) { getAny(filters: $filters) { id geoserverName nameOfDataset theme country } }",
    "variables": {
      "filters": {
        "theme": "Biodiversity",
        "country": "USA"
      }
    }
  }'
```

**Expected Response:**
```json
{
  "data": {
    "getAny": [
      {
        "id": "123e4567-e89b-12d3-a456-426614174000",
        "geoserverName": "gbif",
        "nameOfDataset": "GBIF Dataset",
        "theme": "Biodiversity",
        "keywords": ["species", "occurrence"],
        "country": "USA",
        "contactPerson": "John Doe",
        "organization": "Research Organization"
      },
      {
        "id": "223e4567-e89b-12d3-a456-426614174001",
        "geoserverName": "kew",
        "nameOfDataset": "Kew Gardens Dataset",
        "theme": "Biodiversity",
        "keywords": ["plants", "distribution"],
        "country": "USA",
        "contactPerson": "Jane Smith",
        "organization": "Kew Gardens"
      }
    ]
  }
}
```

**Available Filter Fields:**
- `id` (UUID, optional)
- `dataset_id` (UUID, optional)
- `geoserver_name` (string, optional)
- `name_of_dataset` (string, optional)
- `theme` (string, optional)
- `keywords` (array of strings, optional)
- `purpose_of_creating_data` (string, optional)
- `access_constraints` (string, optional)
- `use_constraints` (string, optional)
- `data_type` (string, optional)
- `contact_person` (string, optional)
- `organization` (string, optional)
- `mailing_address` (string, optional)
- `city_locality_country` (string, optional)
- `country` (string, optional)
- `contact_email` (string, optional)
- `created_on` (datetime, optional)
- `updated_on` (datetime, optional)

---

## 3. Mutations

### 3.1 Create Metadata

**Description:** Create a new metadata record for a dataset. This associates metadata information with a GeoServer layer.

**GraphQL Mutation:**
```graphql
mutation CreateMetadata($metadataData: MetadataInput!) {
  create(metadataData: $metadataData) {
    id
    geoserverName
    nameOfDataset
    theme
    keywords
    createdOn
  }
}
```

**Variables:**
```json
{
  "metadataData": {
    "geoserverName": "new_layer",
    "nameOfDataset": "New Dataset",
    "theme": "Ecology",
    "keywords": ["ecology", "habitat"],
    "purposeOfCreatingData": "Research and analysis",
    "dataType": "VECTOR",
    "contactPerson": "John Doe",
    "organization": "Research Org",
    "contactEmail": "john@example.com",
    "country": "USA",
    "accessConstraints": "None",
    "useConstraints": "CC-BY",
    "mailingAddress": "123 Main St",
    "cityLocalityCountry": "New York, USA"
  }
}
```

**Postman Setup:**
1. Method: `POST`
2. URL: `http://localhost:8000/v1/metadata`
3. Headers:
   - `Content-Type: application/json`
4. Body → GraphQL (or raw → JSON):
   - **Query** tab:
     ```graphql
     mutation CreateMetadata($metadataData: MetadataInput!) {
       create(metadataData: $metadataData) {
         id
         geoserverName
         nameOfDataset
         theme
         keywords
         createdOn
       }
     }
     ```
   - **Variables** tab:
     ```json
     {
       "metadataData": {
         "geoserverName": "new_layer",
         "nameOfDataset": "New Dataset",
         "theme": "Ecology",
         "keywords": ["ecology", "habitat"],
         "purposeOfCreatingData": "Research and analysis",
         "dataType": "VECTOR",
         "contactPerson": "John Doe",
         "organization": "Research Org",
         "contactEmail": "john@example.com",
         "country": "USA",
         "accessConstraints": "None",
         "useConstraints": "CC-BY",
         "mailingAddress": "123 Main St",
         "cityLocalityCountry": "New York, USA"
       }
     }
     ```
   - If using raw JSON format:
     ```json
     {
       "query": "mutation CreateMetadata($metadataData: MetadataInput!) { create(metadataData: $metadataData) { id geoserverName nameOfDataset theme keywords createdOn } }",
       "variables": {
         "metadataData": {
           "geoserverName": "new_layer",
           "nameOfDataset": "New Dataset",
           "theme": "Ecology",
           "keywords": ["ecology", "habitat"],
           "purposeOfCreatingData": "Research and analysis",
           "dataType": "VECTOR",
           "contactPerson": "John Doe",
           "organization": "Research Org",
           "contactEmail": "john@example.com",
           "country": "USA",
           "accessConstraints": "None",
           "useConstraints": "CC-BY",
           "mailingAddress": "123 Main St",
           "cityLocalityCountry": "New York, USA"
         }
       }
     }
     ```

**Example cURL:**
```bash
curl -X POST "http://localhost:8000/v1/metadata" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "mutation CreateMetadata($metadataData: MetadataInput!) { create(metadataData: $metadataData) { id geoserverName nameOfDataset theme createdOn } }",
    "variables": {
      "metadataData": {
        "geoserverName": "new_layer",
        "nameOfDataset": "New Dataset",
        "theme": "Ecology",
        "keywords": ["ecology", "habitat"],
        "dataType": "VECTOR",
        "contactPerson": "John Doe",
        "organization": "Research Org",
        "contactEmail": "john@example.com",
        "country": "USA"
      }
    }
  }'
```

**Expected Response:**
```json
{
  "data": {
    "create": {
      "id": "323e4567-e89b-12d3-a456-426614174002",
      "geoserverName": "new_layer",
      "nameOfDataset": "New Dataset",
      "theme": "Ecology",
      "keywords": ["ecology", "habitat"],
      "createdOn": "2024-01-01T12:00:00"
    }
  }
}
```

**Required Fields:**
- `geoserverName` (string, required): The GeoServer layer name
- `nameOfDataset` (string, required): Display name of the dataset

**Optional Fields:**
- `dataset_id` (UUID, optional)
- `theme` (string, optional)
- `keywords` (array of strings, optional)
- `purposeOfCreatingData` (string, optional)
- `accessConstraints` (string, optional)
- `useConstraints` (string, optional)
- `dataType` (string, optional)
- `contactPerson` (string, optional)
- `organization` (string, optional)
- `mailingAddress` (string, optional)
- `cityLocalityCountry` (string, optional)
- `country` (string, optional)
- `contactEmail` (string, optional)

---

## 4. Error Responses

### 404 Not Found
```json
{
  "errors": [
    {
      "message": "No metadata found for geoserver_name gbif",
      "locations": [{"line": 2, "column": 3}],
      "path": ["get"]
    }
  ],
  "data": {
    "get": null
  }
}
```

### 500 Internal Server Error
```json
{
  "errors": [
    {
      "message": "Internal Server Error",
      "locations": [{"line": 2, "column": 3}],
      "path": ["create"]
    }
  ],
  "data": {
    "create": null
  }
}
```

---

## Notes

1. **GraphQL Endpoint:** All metadata operations use a single GraphQL endpoint at `/v1/metadata`.

2. **Field Selection:** GraphQL allows you to select only the fields you need in the response. This reduces payload size and improves performance.

3. **Filtering:** The `getAny` query supports multiple filter fields that can be combined for complex searches. All filters are optional.

4. **Keywords:** Keywords are stored as an array of strings. Provide them as a JSON array in the mutation.

5. **Timestamps:** `createdOn` is automatically set when creating metadata. `updatedOn` is set when metadata is modified.

6. **UUID Format:** IDs are UUIDs in standard format (e.g., `123e4567-e89b-12d3-a456-426614174000`).

7. **GeoServer Name:** The `geoserverName` field should match the actual layer name in GeoServer (e.g., "gbif", "metastring:kew").

---

## Postman Collection Import

You can create a Postman collection using the following structure:

1. Create a new collection named "Metadata APIs"
2. Set collection variable `base_url` to `http://localhost:8000`
3. Set collection variable `metadata_endpoint` to `/v1/metadata`
4. Create requests for:
   - Get metadata by GeoServer name
   - Get metadata with filters
   - Create metadata
5. Use variables in requests: `{{base_url}}{{metadata_endpoint}}`

---

## Testing Checklist

- [ ] Get metadata by GeoServer name (existing layer)
- [ ] Get metadata by GeoServer name (non-existent layer - should return 404)
- [ ] Get metadata with filters (single filter)
- [ ] Get metadata with filters (multiple filters)
- [ ] Get metadata with filters (no matches - should return 404)
- [ ] Create metadata (with all fields)
- [ ] Create metadata (with minimal required fields)
- [ ] Create metadata (with duplicate geoserverName - should handle appropriately)

---

**Last Updated:** 2024-01-01
**API Version:** 1.0

