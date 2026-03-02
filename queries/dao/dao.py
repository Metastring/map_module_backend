from shapely.geometry import Polygon, MultiPolygon
from shapely.geometry.base import BaseGeometry
from sqlalchemy.sql import text
from database.database import engine
from typing import List, Dict, Union
from utils.config import db_schema

SCHEMA = db_schema

# Accepts a Polygon object and returns results from datasets

def get_polygon_data_from_datasets(dataset: List[str], polygon: Polygon, limit: int = 1000, offset: int = 0) -> Dict[str, List[Dict]]:
	wkt = polygon.wkt
	results_by_dataset: Dict[str, List[Dict]] = {}
	with engine.connect() as conn:
		for table in dataset:
			# Treat 'gbif' as point dataset; others (e.g., 'kew_with_geom') as polygon/distribution datasets
			if table == "gbif":
				query = text(f"""
					SELECT t.*, 
					       ST_X(t.geom) AS longitude,
					       ST_Y(t.geom) AS latitude
					FROM {SCHEMA}.{table} t
					WHERE ST_Intersects(
						t.geom,
						ST_SetSRID(ST_GeomFromText(:wkt), 4326)
					)
					LIMIT :limit OFFSET :offset
				""")
			else:
				# Distribution polygons: return full features without centroid reduction
				query = text(f"""
					SELECT t.*, 
					       ST_AsGeoJSON(t.geom) AS geom_geojson
					FROM {SCHEMA}.{table} t
					WHERE ST_Intersects(
						t.geom,
						ST_SetSRID(ST_GeomFromText(:wkt), 4326)
					)
					LIMIT :limit OFFSET :offset
				""")
			res = conn.execute(query, {"wkt": wkt, "limit": limit, "offset": offset})
			results_by_dataset[table] = [dict(row._mapping) for row in res]
	return results_by_dataset

# Accepts a Polygon or MultiPolygon object and returns results from datasets

def get_multi_polygon_data_from_datasets(dataset: List[str], polygon: Union[Polygon, MultiPolygon], limit: int = 1000, offset: int = 0) -> Dict[str, List[Dict]]:
	wkt = polygon.wkt
	results_by_dataset: Dict[str, List[Dict]] = {}
	with engine.connect() as conn:
		for table in dataset:
			if table == "gbif":
				query = text(f"""
					SELECT t.*, 
					       ST_X(t.geom) AS longitude,
					       ST_Y(t.geom) AS latitude
					FROM {SCHEMA}.{table} t
					WHERE ST_Intersects(
						t.geom,
						ST_SetSRID(ST_GeomFromText(:wkt), 4326)
					)
					LIMIT :limit OFFSET :offset
				""")
			else:
				query = text(f"""
					SELECT t.*, 
					       ST_AsGeoJSON(t.geom) AS geom_geojson
					FROM {SCHEMA}.{table} t
					WHERE ST_Intersects(
						t.geom,
						ST_SetSRID(ST_GeomFromText(:wkt), 4326)
					)
					LIMIT :limit OFFSET :offset
				""")
			res = conn.execute(query, {"wkt": wkt, "limit": limit, "offset": offset})
			results_by_dataset[table] = [dict(row._mapping) for row in res]
	return results_by_dataset

# Accepts a scientific name and returns matching names with longitude and latitude from both datasets

def get_scientific_name_matches_from_datasets(scientific_name: str, dataset: list = ["gbif", "kew_with_geom"]) -> Dict[str, List[Dict]]:
	results_by_dataset: Dict[str, List[Dict]] = {}
	with engine.connect() as conn:
		for table in dataset:
			if table == "gbif":
				query = text(f''' 
					SELECT t.*,
					       ST_X(t.geom) AS longitude,
					       ST_Y(t.geom) AS latitude
					FROM {SCHEMA}.{table} t
					WHERE LOWER(t."scientificName") LIKE :name
				''')
			else:
				query = text(f''' 
					SELECT t.*,
					       ST_AsGeoJSON(t.geom) AS geom_geojson
					FROM {SCHEMA}.{table} t
					WHERE LOWER(t."scientificName") LIKE :name
				''')
			res = conn.execute(query, {"name": f"%{scientific_name.lower()}%"})
			results_by_dataset[table] = [dict(row._mapping) for row in res]
	return results_by_dataset

# Get column names from database schema for a given table
def get_table_column_names(table_name: str) -> List[str]:
	"""
	Get column names from database schema for a given table.
	Returns list of column names including computed columns based on table type.
	Returns empty list if table doesn't exist.
	"""
	column_names = []
	try:
		with engine.connect() as conn:
			# Get base table columns
			query = text(f"""
				SELECT column_name
				FROM information_schema.columns
				WHERE table_schema = :schema
				AND table_name = :table_name
				ORDER BY ordinal_position
			""")
			res = conn.execute(query, {"schema": SCHEMA, "table_name": table_name})
			base_columns = [row[0] for row in res]
			column_names.extend(base_columns)
			
			# Add computed columns based on table type
			if table_name == "gbif":
				# For gbif, add longitude and latitude (computed from geom)
				if "longitude" not in column_names:
					column_names.append("longitude")
				if "latitude" not in column_names:
					column_names.append("latitude")
			else:
				# For other tables, add geom_geojson (computed from geom)
				if "geom_geojson" not in column_names:
					column_names.append("geom_geojson")
	except Exception as e:
		# If table doesn't exist or query fails, return empty list
		print(f"Warning: Could not fetch column names for table {table_name}: {e}")
		return []
	
	return column_names