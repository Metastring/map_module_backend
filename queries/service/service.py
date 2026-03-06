from typing import List
import math
import uuid
from shapely.geometry import Polygon, MultiPolygon
from queries.dao.dao import get_polygon_data_from_datasets, get_multi_polygon_data_from_datasets, get_all_data_from_datasets, get_scientific_name_matches_from_datasets, get_table_column_names
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
	elif isinstance(obj, uuid.UUID):
		return str(obj)
	return obj


def fetch_polygon_query(dataset: List[str], polygon_detail: List[dict], limit: int = 1000, offset: int = 0):
	# Map frontend dataset names to database table names
	mapped_datasets = map_dataset_names(dataset)

	# When no polygon provided: return all data for the selected dataset(s)
	if not polygon_detail:
		raw_results_by_table = get_all_data_from_datasets(mapped_datasets, limit, offset)
		results_by_frontend: dict = {}
		for table_name, rows in raw_results_by_table.items():
			frontend_name = REVERSE_DATASET_MAPPING.get(table_name, table_name)
			results_by_frontend[frontend_name] = clean_nan_values(rows)
		return {"results": results_by_frontend}

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
	# Use dataset names directly without mapping
	datasets_to_query = dataset

	# When no polygon(s) provided: return all data for the selected dataset(s)
	if not polygon_detail:
		raw_results_by_table = get_all_data_from_datasets(datasets_to_query, limit, offset)
		results_by_frontend = {table_name: clean_nan_values(rows) for table_name, rows in raw_results_by_table.items()}
		return {"results": results_by_frontend}

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

	raw_results_by_table = get_multi_polygon_data_from_datasets(datasets_to_query, geometry, limit, offset)

	# Use dataset names directly without reverse mapping
	results_by_frontend: dict = {}
	for table_name, rows in raw_results_by_table.items():
		results_by_frontend[table_name] = clean_nan_values(rows)

	return {"results": results_by_frontend}



def fetch_scientific_name_matches(scientific_name: str):
	raw = get_scientific_name_matches_from_datasets(scientific_name)
	results_by_frontend: dict = {}
	for table_name, rows in raw.items():
		frontend_name = REVERSE_DATASET_MAPPING.get(table_name, table_name)
		results_by_frontend[frontend_name] = clean_nan_values(rows)
	return {"results": results_by_frontend}


######## This logic because this way we won't have to define the model individually it wll give response for any number of datasets but it will return entire data ################

def transform_results_with_display_fields(results_by_frontend: dict, mapped_datasets: List[str], frontend_datasets: List[str]) -> dict:
	"""
	Transform results to include display_fields for each dataset.
	Fetches column names from database schema, so display_fields are available even if no data is returned.
	Format: {
		"dataset_name": {
			"display_fields": [...],
			"data": [...]
		}
	}
	"""
	transformed_results = {}
	
	# Create mapping from frontend names to database table names
	# mapped_datasets[i] corresponds to frontend_datasets[i]
	frontend_to_table = {}
	for i, frontend_name in enumerate(frontend_datasets):
		if i < len(mapped_datasets):
			frontend_to_table[frontend_name] = mapped_datasets[i]
		else:
			# Fallback: use frontend name as table name
			frontend_to_table[frontend_name] = frontend_name
	
	# Process all requested datasets, even if they have no data
	for frontend_name in frontend_datasets:
		# Get data if it exists, otherwise use empty list
		data = results_by_frontend.get(frontend_name, [])
		
		# Get column names from database schema
		table_name = frontend_to_table.get(frontend_name, frontend_name)
		display_fields = get_table_column_names(table_name)
		
		# If we have data, verify we have all columns (in case of computed columns)
		if isinstance(data, list) and len(data) > 0:
			# Get all unique keys from actual data
			all_keys = set()
			for record in data:
				if isinstance(record, dict):
					all_keys.update(record.keys())
			# Merge with schema columns to ensure we have all columns
			display_fields = sorted(list(set(display_fields) | all_keys))
		elif isinstance(data, dict) and len(data) > 0:
			# If data is a single dict, merge with schema columns
			display_fields = sorted(list(set(display_fields) | set(data.keys())))
		
		transformed_results[frontend_name] = {
			"display_fields": display_fields,
			"data": data
		}
	
	return transformed_results


def fetch_multi_polygon_query_with_display_fields(dataset: List[str], polygon_detail: List[dict], limit: int = 1000, offset: int = 0):
	"""
	Same as fetch_multi_polygon_query but returns data in format with display_fields.
	When polygon_detail is missing or empty, returns all data for the selected dataset(s).
	"""
	# Map frontend dataset names to database table names
	mapped_datasets = map_dataset_names(dataset)

	# When no polygon(s) provided: return all data for the selected dataset(s)
	if not polygon_detail:
		raw_results_by_table = get_all_data_from_datasets(mapped_datasets, limit, offset)
		results_by_frontend: dict = {}
		for table_name, rows in raw_results_by_table.items():
			frontend_name = REVERSE_DATASET_MAPPING.get(table_name, table_name)
			results_by_frontend[frontend_name] = clean_nan_values(rows)
		transformed_results = transform_results_with_display_fields(results_by_frontend, mapped_datasets, dataset)
		return {"results": transformed_results}

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

	# Transform to include display_fields (pass mapped_datasets and frontend datasets to get table names for schema queries)
	transformed_results = transform_results_with_display_fields(results_by_frontend, mapped_datasets, dataset)

	return {"results": transformed_results}