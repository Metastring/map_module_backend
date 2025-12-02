# CML APIs - Map Module Backend

A comprehensive backend service that helps researchers and scientists work with biodiversity and geographical data through interactive maps and spatial queries.

## What This System Does

This backend service acts as a bridge between biodiversity databases and map visualization tools. Think of it as a smart assistant that:

1. **Manages Map Data**: Stores and organizes geographical information about plants, animals, and their locations
2. **Creates Interactive Maps**: Converts raw data into visual maps that can be displayed on websites
3. **Answers Spatial Questions**: Helps find answers like "What species exist in this area?" or "Where can I find this particular plant?"
4. **Connects Different Systems**: Links databases, map servers, and web applications together

## Main Components

### 🗺️ GeoServer Management (geoserver folder)
**What it does**: Manages a map server called GeoServer that creates visual maps from your data.

**Key Features**:
- **Upload Map Data**: Takes your data files (like spreadsheets with location information) and prepares them for mapping
- **Create Workspaces**: Organizes your data into different projects or categories
- **Manage Layers**: Each dataset becomes a "layer" that can be shown or hidden on maps
- **Connect to Databases**: Links to PostgreSQL databases where your biodiversity data is stored
- **Generate Map Tiles**: Creates the actual map images that appear in web browsers

**Real-world example**: If you have a spreadsheet of bird sightings with GPS coordinates, this system can turn that into a visual map showing bird locations as dots or areas.

### 🔍 Spatial Queries (queries folder)
**What it does**: Provides powerful search capabilities for geographical and biodiversity data using GraphQL technology.

**Key Features**:
- **Area-Based Search**: Draw a shape on a map and find all species within that area
- **Multi-Area Search**: Search across multiple regions at once
- **Species Name Search**: Find all locations where a specific species has been recorded
- **Smart Data Mapping**: Automatically translates between user-friendly names and technical database names

**Real-world example**: A researcher can draw a circle around a forest area and instantly get a list of all plant species recorded in that region, along with their exact locations.

## How the System Works

### Data Flow
1. **Data Storage**: Biodiversity data is stored in PostgreSQL databases
2. **Data Processing**: The system processes location data and species information
3. **Map Creation**: GeoServer converts this data into visual map layers
4. **User Queries**: Users can search and filter data through web interfaces
5. **Results Display**: Search results are returned as both data and visual maps

### Supported Data Types
- **Point Data**: Exact locations (like GBIF species occurrence records)
- **Polygon Data**: Area-based data (like species distribution ranges from Kew Gardens)
- **Scientific Names**: Species identification and matching across datasets

## Technical Architecture

### Core Technologies
- **FastAPI**: Modern web framework for creating APIs
- **GraphQL**: Flexible query language for complex data requests
- **PostgreSQL**: Database system for storing spatial data
- **GeoServer**: Map server for creating visual map layers
- **SQLAlchemy**: Database connection and query management

### Database Integration
The system connects to PostgreSQL databases containing:
- **GBIF Data**: Global Biodiversity Information Facility records (point locations)
- **Kew Gardens Data**: Royal Botanic Gardens species distribution data (polygon areas)
- **Custom Datasets**: Any additional biodiversity or geographical data

## Key Features for Users

### For Researchers
- **Easy Data Upload**: Upload your research data and have it automatically mapped
- **Visual Analysis**: See your data on interactive maps
- **Spatial Queries**: Ask complex geographical questions about your data
- **Data Integration**: Combine your data with global biodiversity databases

### For Developers
- **REST APIs**: Standard web APIs for all GeoServer operations
- **GraphQL Endpoint**: Flexible query interface for spatial data
- **Standardized Responses**: Consistent data formats across all endpoints
- **Error Handling**: Clear error messages and validation

### For System Administrators
- **Workspace Management**: Organize data into logical groups
- **User Access Control**: Manage who can access what data
- **Performance Monitoring**: Track system usage and performance
- **Data Backup**: Ensure data safety and recovery options

## API Endpoints

### GeoServer Management
- **Workspaces**: Create, list, update, and delete data organization spaces
- **Data Stores**: Manage connections to databases and file systems
- **Layers**: Create and manage map layers from your data
- **Styles**: Customize how your data appears on maps
- **Map Tiles**: Generate visual map tiles for web display

### Spatial Queries
- **GraphQL Endpoint**: `/v1/graphql` - Flexible spatial data queries
- **Health Check**: `/health` - System status monitoring
- **Documentation**: `/` - API information and endpoints

### 🎨 Automated Styling System (styles folder)
**What it does**: Creates professional map styles automatically based on your data.

The styling system is a **metadata-driven** solution that:
1. **Reads Column Info**: Queries PostGIS to understand your data columns (numeric vs categorical)
2. **Computes Class Breaks**: Uses classification algorithms to divide data into meaningful ranges
3. **Builds MBStyle JSON**: Generates Mapbox-compatible style definitions
4. **Publishes to GeoServer**: Uploads styles directly to the map server
5. **Attaches to Layers**: Sets the style as default for your layer

**Key Features**:
- **Multiple Classification Methods**:
  - Equal Interval: Divides data range into equal parts
  - Quantile: Equal number of features per class
  - Jenks Natural Breaks: Optimizes class boundaries for natural groupings
  - Categorical: Uses distinct values for categories

- **ColorBrewer Palettes**: Professional color schemes including:
  - Sequential: YlOrRd, Blues, Greens, Purples, Oranges, Greys
  - Diverging: RdYlGn, RdBu, BrBG, PuOr
  - Qualitative: Set1, Set2, Set3, Paired, Pastel1

- **Geometry Support**: Works with Points, Lines, and Polygons

**API Endpoints**:
- `POST /styles/generate` - Generate and publish a new style
- `POST /styles/preview` - Preview style without saving
- `GET /styles/metadata` - List all style configurations
- `GET /styles/metadata/{id}` - Get specific style details
- `GET /styles/legend/{style_id}` - Get legend for a style
- `GET /styles/palettes` - List available color palettes
- `POST /styles/regenerate/{style_id}` - Regenerate style from current data

**Example Request**:
```json
POST /styles/generate
{
  "layer_name": "health_cases",
  "workspace": "my_workspace",
  "table_name": "health_cases",
  "style_column": "cases_count",
  "geometry_type": "polygon",
  "classification_method": "quantile",
  "num_classes": 5,
  "color_palette": "YlOrRd",
  "publish_to_geoserver": true,
  "set_as_default": true
}
```

## Configuration

The system uses configuration files to connect to different environments:
- **Database Settings**: PostgreSQL connection details
- **GeoServer Settings**: Map server connection information
- **Dataset Mapping**: Links user-friendly names to database tables

## Getting Started

### Prerequisites
- Python 3.8 or higher
- PostgreSQL database with spatial extensions (PostGIS)
- GeoServer installation
- Required Python packages (listed in requirements.txt)

### Installation
1. Install Python dependencies: `pip install -r requirements.txt`
2. Configure database connection in `utils/config.py`
3. Set up GeoServer connection details
4. Run the application: `uvicorn main:app --reload`

### First Steps
1. **Create a Workspace**: Organize your data into logical groups
2. **Upload Data**: Connect your databases or upload data files
3. **Create Layers**: Turn your data into visual map layers
4. **Test Queries**: Try spatial searches to explore your data

## Example Use Cases

### Conservation Research
- Map endangered species locations
- Analyze habitat distribution patterns
- Track species migration routes
- Identify biodiversity hotspots

### Environmental Monitoring
- Monitor ecosystem changes over time
- Track invasive species spread
- Analyze climate impact on species distribution
- Plan conservation areas

### Academic Research
- Combine multiple biodiversity datasets
- Perform statistical analysis on spatial data
- Create publication-ready maps
- Share research data with colleagues

## Support and Maintenance

### Regular Tasks
- Monitor database performance
- Update GeoServer configurations
- Backup important data
- Check system health endpoints

### Troubleshooting
- Check database connections
- Verify GeoServer status
- Review error logs
- Test API endpoints

This system provides a powerful foundation for working with biodiversity and geographical data, making complex spatial analysis accessible to researchers and scientists worldwide.
