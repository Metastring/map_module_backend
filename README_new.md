# Unified Data Management System

A comprehensive FastAPI backend system for managing multiple types of geospatial and tabular datasets with dynamic attributes, advanced querying capabilities, and automatic GeoServer integration.

## 🌟 Key Features

- **Multi-format Data Support**: Handle Shapefiles, GeoJSON, CSV, GeoTIFF, and raster data
- **Dynamic Attributes**: Store any attributes using JSONB for flexible schema
- **Advanced Querying**: Spatial queries (bbox, buffer, intersection) and attribute-based search
- **GeoServer Integration**: Automatic layer publishing and WMS/WFS services
- **Real-time Processing**: Background task processing with status tracking
- **Category Management**: Organize datasets with hierarchical categories
- **Statistical Analysis**: Built-in aggregation and statistical functions
- **Bulk Operations**: Efficient handling of large dataset operations

## 🚀 Quick Start

### Prerequisites

- Python 3.8+
- PostgreSQL with PostGIS extension
- GeoServer (optional, for map publishing)

### Installation

1. **Clone and setup the project:**
```bash
git clone <repository-url>
cd map_module_backend
```

2. **Run the automated setup:**
```bash
python setup.py
```

3. **Or manual setup:**
```bash
# Install dependencies
pip install -r requirements.txt

# Configure database (update secure.ini)
cp secure.ini.template secure.ini
# Edit secure.ini with your database credentials

# Initialize database
python scripts/init_database.py

# Start the application
uvicorn main:app --reload
```

### Configuration

Update `secure.ini` with your settings:

```ini
[database]
host = localhost
port = 5432
dbname = your_database_name
user = your_username
password = your_password

[geoserver]
url = http://localhost:8080/geoserver
username = admin
password = geoserver
workspace = default
```

## 📊 Supported Data Types

### Vector Data
- **Shapefiles** (.zip with .shp, .shx, .dbf files)
- **GeoJSON** (.geojson, .json)
- **CSV with coordinates** (.csv with lat/lon or x/y columns)

### Raster Data
- **GeoTIFF** (.tif, .tiff)
- **Other raster formats** supported by GDAL

### Tabular Data
- **CSV files** (.csv)
- **Excel files** (.xlsx, .xls)

## 🔧 API Usage Examples

### Upload a Dataset

```python
import requests

# Upload a shapefile
files = {
    'file': open('data.zip', 'rb')
}
data = {
    'name': 'My Dataset',
    'description': 'Sample environmental data',
    'category': 'environment',
    'publish_to_geoserver': True
}

response = requests.post(
    'http://localhost:8000/unified-data/upload',
    files=files,
    data=data
)
```

### Query Features with Spatial Filter

```python
import requests

query = {
    'spatial_filter': {
        'type': 'bbox',
        'coordinates': [-180, -90, 180, 90]
    },
    'limit': 100,
    'include_geometry': True
}

response = requests.post(
    'http://localhost:8000/unified-data/query/spatial',
    json=query
)
```

### Search by Attributes

```python
import requests

query = {
    'filters': [
        {
            'field': 'temperature',
            'operator': 'gt',
            'value': 25
        },
        {
            'field': 'species',
            'operator': 'ilike',
            'value': '%tiger%'
        }
    ],
    'limit': 50
}

response = requests.post(
    'http://localhost:8000/unified-data/query/attributes',
    json=query
)
```

## 📈 API Endpoints Overview

### Dataset Management
- `POST /unified-data/upload` - Upload new datasets
- `GET /unified-data/datasets` - List all datasets
- `GET /unified-data/datasets/{id}` - Get dataset details
- `PUT /unified-data/datasets/{id}` - Update dataset metadata
- `DELETE /unified-data/datasets/{id}` - Delete dataset

### Data Querying
- `GET /unified-data/datasets/{id}/features` - Get dataset features
- `POST /unified-data/query/spatial` - Advanced spatial queries
- `POST /unified-data/query/attributes` - Attribute-based search
- `POST /unified-data/query/combined` - Combined spatial + attribute queries

### Category Management
- `GET /unified-data/categories` - List categories
- `POST /unified-data/categories` - Create category
- `PUT /unified-data/categories/{id}` - Update category

### Analytics & Statistics
- `GET /unified-data/datasets/{id}/statistics` - Dataset statistics
- `POST /unified-data/analytics/aggregate` - Data aggregation
- `GET /unified-data/analytics/summary` - System summary

### System Operations
- `GET /unified-data/health` - Health check
- `POST /unified-data/bulk/delete` - Bulk operations
- `GET /unified-data/tasks/{id}/status` - Task status

## 🗄️ Database Schema

### Core Tables

1. **datasets** - Main dataset metadata
2. **dataset_features** - Individual features/records with JSONB attributes
3. **dataset_categories** - Hierarchical category system

### Key Features

- **JSONB Attributes**: Store any custom attributes dynamically
- **Spatial Indexing**: GIST indexes for efficient spatial queries
- **Full-text Search**: GIN indexes for attribute searching
- **UUID Primary Keys**: Globally unique identifiers

## 🎯 Use Cases

### Environmental Monitoring
```python
# Upload climate station data
# Query by temperature range and location
# Generate statistical summaries
```

### Biodiversity Research
```python
# Upload species occurrence records
# Find species within protected areas
# Analyze distribution patterns
```

### Urban Planning
```python
# Upload building footprints
# Query infrastructure within city bounds
# Calculate density statistics
```

## 🔍 Advanced Features

### Dynamic Attribute Querying

The system supports complex JSONB queries:

```sql
-- Find records where temperature > 25 AND humidity < 60
SELECT * FROM dataset_features 
WHERE attributes->>'temperature'::numeric > 25 
  AND attributes->>'humidity'::numeric < 60;
```

### Spatial Operations

Built-in spatial functions:
- Bounding box queries
- Buffer operations
- Intersection checks
- Distance calculations

### Background Processing

Large file uploads are processed asynchronously:
- Real-time status updates
- Progress tracking
- Error handling and retry logic

## 📚 Documentation

- **API Documentation**: Visit `/docs` for interactive Swagger UI
- **GraphQL**: Available at `/graphql` for complex queries
- **Health Check**: Monitor system status at `/unified-data/health`

## 🧪 Testing

```bash
# Run tests
pytest tests/

# Run with coverage
pytest --cov=. tests/
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For issues and questions:
1. Check the API documentation at `/docs`
2. Review the health endpoint at `/unified-data/health`
3. Check application logs for detailed error messages
4. Ensure PostgreSQL and PostGIS are properly configured

## 🔄 Version History

- **v2.0** - Unified Data Management System with multi-format support
- **v1.0** - Initial GeoServer integration and basic queries