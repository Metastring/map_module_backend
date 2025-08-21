from typing import List
import math
from shapely.geometry import Polygon, MultiPolygon
from queries.dao.dao import get_polygon_data_from_datasets, get_multi_polygon_data_from_datasets, get_scientific_name_matches_from_datasets
from utils.config import DATASET_MAPPING, REVERSE_DATASET_MAPPING


def map_dataset_names(frontend_datasets: List[str]) -> List[str]:
	mapped_datasets = []
	for dataset in frontend_datasets:
		if dataset in DATASET_MAPPING:
			mapped_datasets.append(DATASET_MAPPING[dataset])
		else:
			# If no mapping found, use the original name (fallback)
			mapped_datasets.append(dataset)
	return mapped_datasets

def clean_nan_values(obj):
	if isinstance(obj, dict):
		return {k: clean_nan_values(v) for k, v in obj.items()}
	elif isinstance(obj, list):
		return [clean_nan_values(i) for i in obj]
	elif isinstance(obj, float) and math.isnan(obj):
		return None
	return obj


def fetch_polygon_query(dataset: List[str], polygon_detail: List[dict], limit: int = 1000, offset: int = 0):
	if not polygon_detail:
		return {"results": {}}

	# Map frontend dataset names to database table names
	mapped_datasets = map_dataset_names(dataset)

	# Extract polygon coordinates (first polygon only)
	coordinates = polygon_detail[0].geometry.coordinates[0]  # Exterior ring only
	polygon = Polygon(coordinates)

	raw_results_by_table = get_polygon_data_from_datasets(mapped_datasets, polygon, limit, offset)

	# Normalize keys back to frontend names and clean values
	results_by_frontend: dict = {}
	for table_name, rows in raw_results_by_table.items():
		frontend_name = REVERSE_DATASET_MAPPING.get(table_name, table_name)
		results_by_frontend[frontend_name] = clean_nan_values(rows)

	return {"results": results_by_frontend}



def fetch_multi_polygon_query(dataset: List[str], polygon_detail: List[dict], limit: int = 1000, offset: int = 0):
	if not polygon_detail:
		return {"results": {}}

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
		return {"results": {}}
	
	# Create a MultiPolygon from all valid polygons
	if len(polygons) == 1:
		geometry = polygons[0]
	else:
		geometry = MultiPolygon(polygons)

	raw_results_by_table = get_multi_polygon_data_from_datasets(mapped_datasets, geometry, limit, offset)

	# Normalize keys back to frontend names and clean values
	results_by_frontend: dict = {}
	for table_name, rows in raw_results_by_table.items():
		frontend_name = REVERSE_DATASET_MAPPING.get(table_name, table_name)
		results_by_frontend[frontend_name] = clean_nan_values(rows)

	return {"results": results_by_frontend}



def fetch_scientific_name_matches(scientific_name: str):
	raw = get_scientific_name_matches_from_datasets(scientific_name)
	results_by_frontend: dict = {}
	for table_name, rows in raw.items():
		frontend_name = REVERSE_DATASET_MAPPING.get(table_name, table_name)
		results_by_frontend[frontend_name] = clean_nan_values(rows)
	return {"results": results_by_frontend}


######## This logic because this way we won't have to define the model individually it wll give response for any number of datasets but it will return entire data ################