from shapely.geometry import Polygon, MultiPolygon
from shapely.geometry.base import BaseGeometry
from sqlalchemy.sql import text
from database.database import engine
from typing import List, Dict, Union
import configparser

config = configparser.ConfigParser()
encodings_to_try = ['utf-8-sig', 'utf-8', 'latin-1', 'cp1252']
read_success = False
for encoding in encodings_to_try:
    try:
        if config.read("secure.ini", encoding=encoding):
            read_success = True
            break
    except (UnicodeDecodeError, UnicodeError):
        continue
    except Exception:
        continue

if not read_success:
    raise ValueError("Error reading secure.ini: Could not decode file with any supported encoding. Please ensure the file is saved as UTF-8.")

SCHEMA = config.get("DB_SCHEMA", "schema")

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