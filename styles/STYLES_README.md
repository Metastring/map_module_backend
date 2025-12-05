# Automated Layer Styling System

## Overview

This module provides a fully automated, metadata-driven styling system for GeoServer layers. It reads column information from PostGIS, computes color classification classes, builds MBStyle JSON, publishes to GeoServer, and attaches the style to your layer.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              API Layer                                   │
│  POST /styles/generate  │  GET /styles/legend  │  GET /styles/palettes  │
└────────────────────────────────────┬────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                           StyleService                                   │
│   • Orchestrates the complete workflow                                   │
│   • Coordinates DAO, Classification, MBStyle Builder                     │
│   • Manages GeoServer publication                                        │
└────────────────────────────────────┬────────────────────────────────────┘
                                     │
        ┌────────────────────────────┼────────────────────────────┐
        ▼                            ▼                            ▼
┌───────────────────┐    ┌───────────────────┐    ┌───────────────────┐
│      StyleDAO      │    │ ClassificationSvc │    │  MBStyleBuilder   │
│  • Get column info │    │  • Equal Interval │    │  • Build layers   │
│  • Get statistics  │    │  • Quantile       │    │  • Step/Match exp │
│  • Query breaks    │    │  • Jenks Natural  │    │  • Point/Line/Poly│
│  • CRUD metadata   │    │  • Categorical    │    │  • MapBox Spec v8 │
└───────────────────┘    └───────────────────┘    └───────────────────┘
                                     │
                                     ▼
                         ┌───────────────────┐
                         │   GeoServerDAO    │
                         │  • Create MBStyle │
                         │  • Set default    │
                         │  • List styles    │
                         └───────────────────┘
```

## Files Structure

```
styles/
├── __init__.py
├── api/
│   ├── __init__.py
│   └── api.py              # FastAPI endpoints
├── dao/
│   ├── __init__.py
│   └── dao.py              # Database access (PostGIS queries)
├── models/
│   ├── __init__.py
│   ├── model.py            # SQLAlchemy models
│   └── schema.py           # Pydantic schemas
└── service/
    ├── __init__.py
    ├── classification.py   # Classification algorithms
    ├── color_palettes.py   # ColorBrewer palettes
    ├── mbstyle_builder.py  # MBStyle JSON builder
    └── style_service.py    # Main orchestration service
```

## Database Tables

Run `scripts/create_style_tables.sql` to create required tables:

- **style_metadata**: Stores style configurations and MBStyle JSON
- **style_audit_log**: Tracks all style changes for audit
- **style_cache**: Caches expensive classification computations

## API Endpoints

### Generate a New Style
```http
POST /styles/generate
Content-Type: application/json

{
  "layer_name": "health_cases",
  "workspace": "my_workspace",
  "table_name": "health_cases",
  "schema_name": "public",
  "style_column": "cases_count",
  "geometry_type": "polygon",
  "classification_method": "quantile",
  "num_classes": 5,
  "color_palette": "YlOrRd",
  "fill_opacity": 0.7,
  "stroke_color": "#333333",
  "stroke_width": 1.0,
  "publish_to_geoserver": true,
  "set_as_default": true
}
```

### Preview Style Without Saving
```http
POST /styles/preview
Content-Type: application/json

{
  "table_name": "health_cases",
  "schema_name": "public",
  "style_column": "cases_count",
  "geometry_type": "polygon",
  "classification_method": "equal_interval",
  "num_classes": 7,
  "color_palette": "Blues"
}
```

### Get Legend
```http
GET /styles/legend/{style_id}
```

Response:
```json
{
  "style_id": "uuid-here",
  "style_name": "health_cases_cases_count_quantile",
  "legend_items": [
    {"color": "#ffffb2", "label": "0 - 50", "min_value": 0, "max_value": 50},
    {"color": "#fecc5c", "label": "50 - 100", "min_value": 50, "max_value": 100},
    {"color": "#fd8d3c", "label": "100 - 200", "min_value": 100, "max_value": 200},
    {"color": "#f03b20", "label": "200 - 400", "min_value": 200, "max_value": 400},
    {"color": "#bd0026", "label": "400 - 800", "min_value": 400, "max_value": 800}
  ]
}
```

### List Available Palettes
```http
GET /styles/palettes
```

### List All Style Metadata
```http
GET /styles/metadata?workspace=my_workspace
```

### Regenerate Style
```http
POST /styles/regenerate/{style_id}
```

## Classification Methods

| Method | Description | Best For |
|--------|-------------|----------|
| `equal_interval` | Divides data range into equal parts | Evenly distributed data |
| `quantile` | Equal number of features per class | Skewed distributions |
| `jenks` | Natural breaks optimization | Complex distributions |
| `categorical` | Distinct values as classes | Text/category columns |

## Color Palettes

### Sequential (for ordered data)
- `YlOrRd` - Yellow-Orange-Red (heat maps)
- `Blues` - Light to dark blue
- `Greens` - Light to dark green
- `Reds` - Light to dark red
- `Purples` - Light to dark purple
- `Oranges` - Light to dark orange
- `Greys` - Light to dark grey

### Diverging (for data with meaningful center)
- `RdYlGn` - Red-Yellow-Green
- `RdBu` - Red-Blue
- `BrBG` - Brown-Blue-Green
- `PuOr` - Purple-Orange

### Qualitative (for categorical data)
- `Set1`, `Set2`, `Set3` - Distinct colors
- `Paired` - Paired colors
- `Pastel1`, `Pastel2` - Soft colors

## Usage Example

```python
from styles.service.style_service import StyleService
from database.database import get_db
from geoserver.dao import GeoServerDAO

# Initialize
db = next(get_db())
geoserver_dao = GeoServerDAO(host, port, user, password)
style_service = StyleService(db, geoserver_dao)

# Generate and publish style
result = style_service.generate_style(
    layer_name="biodiversity_distribution",
    workspace="my_workspace",
    table_name="biodiversity_distribution",
    schema_name="public",
    style_column="species_count",
    geometry_type="polygon",
    classification_method="quantile",
    num_classes=5,
    color_palette="Greens",
    publish_to_geoserver=True,
    set_as_default=True
)

print(f"Style created: {result.style_name}")
print(f"Published: {result.published}")
```

## MBStyle Output Example

```json
{
  "version": 8,
  "name": "biodiversity_distribution_species_count_quantile",
  "layers": [
    {
      "id": "biodiversity_distribution_species_count_quantile",
      "type": "fill",
      "paint": {
        "fill-color": [
          "step",
          ["get", "species_count"],
          "#f7fcf5",
          10, "#c7e9c0",
          25, "#74c476",
          50, "#31a354",
          100, "#006d2c"
        ],
        "fill-opacity": 0.7,
        "fill-outline-color": "#333333"
      }
    }
  ]
}
```

## Integration with Frontend

The frontend can use the legend endpoint to display a map legend:

```typescript
// Fetch legend data
const response = await fetch(`/styles/legend/${styleId}`);
const legend = await response.json();

// Render legend items
legend.legend_items.forEach(item => {
  console.log(`${item.color}: ${item.label}`);
});
```

## Troubleshooting

### Style not appearing in GeoServer
1. Check GeoServer credentials in `utils/config.py`
2. Verify workspace exists in GeoServer
3. Check GeoServer logs for errors

### Classification fails
1. Verify column exists in table
2. Check column has numeric data (for numeric classification)
3. Ensure table has data (not empty)

### Cache issues
1. Run `SELECT clean_expired_style_cache()` in database
2. Or invalidate specific table cache: `SELECT invalidate_style_cache('table_name')`
