from typing import List
from shapely.geometry import Polygon, MultiPolygon
from queries.dao.dao import get_polygon_data_from_datasets, get_multi_polygon_data_from_datasets
from utils.config import DATASET_MAPPING


def map_dataset_names(frontend_datasets: List[str]) -> List[str]:
    mapped_datasets = []
    for dataset in frontend_datasets:
        if dataset in DATASET_MAPPING:
            mapped_datasets.append(DATASET_MAPPING[dataset])
        else:
            # If no mapping found, use the original name (fallback)
            mapped_datasets.append(dataset)
    return mapped_datasets


def fetch_polygon_query(dataset: List[str], polygon_detail: List[dict], limit: int = 1000, offset: int = 0):
    if not polygon_detail:
        return {"data": []}

    # Map frontend dataset names to database table names
    mapped_datasets = map_dataset_names(dataset)

    # Extract polygon coordinates (first polygon only)
    coordinates = polygon_detail[0].geometry.coordinates[0]  # Exterior ring only
    polygon = Polygon(coordinates)

    results = get_polygon_data_from_datasets(mapped_datasets, polygon, limit, offset)
    return {"data": results}


def fetch_multi_polygon_query(dataset: List[str], polygon_detail: List[dict], limit: int = 1000, offset: int = 0):
    if not polygon_detail:
        return {"data": []}

    # Map frontend dataset names to database table names
    mapped_datasets = map_dataset_names(dataset)

    # Handle multiple polygons
    polygons = []
    for detail in polygon_detail:
        if hasattr(detail, 'geometry') and hasattr(detail.geometry, 'coordinates'):
            # Extract polygon coordinates (exterior ring only)
            coordinates = detail.geometry.coordinates[0]
            try:
                polygon = Polygon(coordinates)
                if polygon.is_valid:
                    polygons.append(polygon)
            except Exception as e:
                print(f"Warning: Invalid polygon coordinates: {e}")
                continue
    
    if not polygons:
        return {"data": []}
    
    # Create a MultiPolygon from all valid polygons
    if len(polygons) == 1:
        geometry = polygons[0]
    else:
        geometry = MultiPolygon(polygons)

    results = get_multi_polygon_data_from_datasets(mapped_datasets, geometry, limit, offset)
    return {"data": results}


######## This logic because this way we won't have to define the model individually it wll give response for any number of datasets but it will return entire data ################