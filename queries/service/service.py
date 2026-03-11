from typing import List
import math
import uuid
from shapely.geometry import Polygon, MultiPolygon
from queries.dao.dao import get_polygon_data_from_datasets, get_multi_polygon_data_from_datasets, get_all_data_from_datasets, get_scientific_name_matches_from_datasets, get_table_column_names, filter_existing_tables
from utils.config import DATASET_MAPPING, REVERSE_DATASET_MAPPING

# Curated display_fields per dataset (only these columns appear under display_fields in the API response)
DISPLAY_FIELDS_BY_DATASET = {
	"gbif": ["scientificname", "eventdate", "basisofrecord"],
	"kew": ["scientificname", "continent", "region", "area"],
	"cpmp": ["family", "genus", "species", "author", "state"],
}


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

		# If there is no data for this dataset, return null instead of an
		# object with empty data/display_fields to match API requirements.
		if (isinstance(data, list) and len(data) == 0) or (isinstance(data, dict) and len(data) == 0):
			transformed_results[frontend_name] = None
			continue

		# Use curated display_fields for known datasets; otherwise derive from schema/data
		if frontend_name in DISPLAY_FIELDS_BY_DATASET:
			display_fields = list(DISPLAY_FIELDS_BY_DATASET[frontend_name])
		else:
			table_name = frontend_to_table.get(frontend_name, frontend_name)
			display_fields = get_table_column_names(table_name)
			# If we have data, verify we have all columns (in case of computed columns)
			if isinstance(data, list) and len(data) > 0:
				all_keys = set()
				for record in data:
					if isinstance(record, dict):
						all_keys.update(record.keys())
				display_fields = sorted(list(set(display_fields) | all_keys))
			elif isinstance(data, dict) and len(data) > 0:
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
	Uses dataset names directly as table names (no mapping) so response column names match getMultiPolygonData.
	"""
	# Use dataset names directly as table names, same as fetch_multi_polygon_query (no kew -> kew_with_geom mapping)
	datasets_to_query = dataset
	# If at least one requested dataset exists, ignore the unknown ones.
	# If none exist (e.g. ["xyz"]), keep original behavior (DB error is acceptable per requirements).
	existing = filter_existing_tables(datasets_to_query)
	if existing:
		datasets_to_query = existing

	# When no polygon(s) provided: return all data for the selected dataset(s)
	if not polygon_detail:
		raw_results_by_table = get_all_data_from_datasets(datasets_to_query, limit, offset)
		results_by_frontend = {table_name: clean_nan_values(rows) for table_name, rows in raw_results_by_table.items()}
		transformed_results = transform_results_with_display_fields(results_by_frontend, datasets_to_query, dataset)
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

	raw_results_by_table = get_multi_polygon_data_from_datasets(datasets_to_query, geometry, limit, offset)

	# Use table names directly (no reverse mapping), same as fetch_multi_polygon_query
	results_by_frontend = {table_name: clean_nan_values(rows) for table_name, rows in raw_results_by_table.items()}

	transformed_results = transform_results_with_display_fields(results_by_frontend, datasets_to_query, dataset)

	return {"results": transformed_results}