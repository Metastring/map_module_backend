# queries/dao/dao.py
from shapely.geometry import Polygon, MultiPolygon
from shapely.geometry.base import BaseGeometry
from sqlalchemy.sql import text
from database.database import engine
from typing import List, Dict, Union, Optional
from utils.config import db_schema

SCHEMA = db_schema

SEMANTIC_FILTERS = {
    "scientific_name": {
        "cpmp": "scientific_name",
        "cpmp_v3": "scientific_name",
        "gbif": "scientificname",
    }
}

# Filter dataset names to only tables that exist in the configured schema.
# If none exist, caller can decide whether to raise or let existing logic error.
def filter_existing_tables(dataset: List[str]) -> List[str]:
   if not dataset:
       return []
   try:
       with engine.connect() as conn:
           query = text("""
               SELECT table_name
               FROM information_schema.tables
               WHERE table_schema = :schema
                 AND table_name = ANY(:table_names)
           """)
           res = conn.execute(query, {"schema": SCHEMA, "table_names": dataset})
           existing = {row[0] for row in res}
           return [t for t in dataset if t in existing]
   except Exception as e:
       # If the check itself fails, be conservative and return original dataset list
       print(f"Warning: Could not validate dataset tables: {e}")
       return dataset


def strict_build_filter_clause(filters):
    if not filters:
        return "", {}

    clauses = []
    params = {}

    idx = 0

    for column, values in filters.items():

        if not values:
            continue

        #
        # scientific name search
        #
        if column in {"scientific_name", "scientificName"}:

            like_clauses = []

            for value in values:

                param_name = f"filter_{idx}"

                like_clauses.append(
                    f'LOWER(t."{column}") LIKE LOWER(:{param_name})'
                )

                params[param_name] = f"%{value}%"

                idx += 1

            clauses.append(
                "(" + " OR ".join(like_clauses) + ")"
            )

        #
        # normal equality filters
        #
        else:

            param_name = f"filter_{idx}"

            clauses.append(
                f't."{column}" = ANY(:{param_name})'
            )

            params[param_name] = values

            idx += 1

    if not clauses:
        return "", {}

    return " AND " + " AND ".join(clauses), params

def build_filter_clause(filters):
    if not filters:
        return "", {}

    clauses = []
    params = {}

    idx = 0

    for column, values in filters.items():

        if not values:
            continue

        value_clauses = []

        for value in values:

            param_name = f"filter_{idx}"

            value_clauses.append(
                f'LOWER(CAST(t."{column}" AS TEXT)) LIKE LOWER(:{param_name})'
            )

            params[param_name] = f"%{value}%"

            idx += 1

        clauses.append(
            "(" + " OR ".join(value_clauses) + ")"
        )

    if not clauses:
        return "", {}

    return " AND " + " AND ".join(clauses), params

def validate_filters_for_table(
    table: str,
    filters: Optional[dict]
):
    if not filters:
        return {}

    allowed_columns = set(get_table_column_names(table))

    validated_filters = {}

    for frontend_column, values in filters.items():

        db_column = frontend_column

        if frontend_column in SEMANTIC_FILTERS:
            db_column = SEMANTIC_FILTERS[frontend_column].get(table)

        #
        # ignore unsupported semantic filters
        #
        if not db_column:
            continue

        #
        # ignore unknown columns
        #
        if db_column not in allowed_columns:
            continue

        validated_filters[db_column] = values

    return validated_filters


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

def get_multi_polygon_data_from_datasets(dataset: List[str], polygon: Union[Polygon, MultiPolygon], limit: int = 1000, offset: int = 0, filters=None) -> Dict[str, List[Dict]]:

   wkt = polygon.wkt
   results_by_dataset: Dict[str, List[Dict]] = {}
   with engine.connect() as conn:
       for table in dataset:
           validated_filters = validate_filters_for_table(
               table,
               filters
               )
           filter_sql, filter_params = build_filter_clause(
               validated_filters
               )
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
                   {filter_sql}
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
                   {filter_sql}
                   LIMIT :limit OFFSET :offset
               """)
           res = conn.execute(query, {"wkt": wkt, "limit": limit, "offset": offset, **filter_params,})
           results_by_dataset[table] = [dict(row._mapping) for row in res]
   return results_by_dataset


# Returns all data from datasets (no geometry filter). Used when no polygon is provided.
def get_all_data_from_datasets(dataset: List[str], limit: int = 1000, offset: int = 0, filters=None) -> Dict[str, List[Dict]]:
  
   results_by_dataset: Dict[str, List[Dict]] = {}
   with engine.connect() as conn:
       for table in dataset:
           validated_filters = validate_filters_for_table(
               table,
               filters
               )
           filter_sql, filter_params = build_filter_clause(
               validated_filters
               )
          
           if table == "gbif":
               query = text(f"""
                   SELECT t.*,
                          ST_X(t.geom) AS longitude,
                          ST_Y(t.geom) AS latitude
                   FROM {SCHEMA}.{table} t
                   WHERE 1=1
                   {filter_sql}
                   LIMIT :limit OFFSET :offset
               """)
           else:
               query = text(f"""
                   SELECT t.*,
                          ST_AsGeoJSON(t.geom) AS geom_geojson
                   FROM {SCHEMA}.{table} t
                   WHERE 1=1
                   {filter_sql}
                   LIMIT :limit OFFSET :offset
               """)
           res = conn.execute(query, {"limit": limit, "offset": offset, **filter_params,})
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


