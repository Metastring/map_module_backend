from shapely.geometry import Polygon, MultiPolygon
from shapely.geometry.base import BaseGeometry
from sqlalchemy.sql import text
from database.database import engine
from typing import List, Dict, Union, Optional
from utils.config import db_schema
import json

SCHEMA = db_schema

SEMANTIC_FILTERS = {
    "scientific_name": {
        "cpmp": "scientific_name",
        "cpmp_v2": "scientific_name",
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

def get_multi_polygon_data_from_datasets(
    dataset: List[str],
    polygon: Union[Polygon, MultiPolygon],
    limit: int = 1000,
    offset: int = 0,
    filters=None
) -> Dict[str, Dict]:

    wkt = polygon.wkt
    results_by_dataset: Dict[str, Dict] = {}

    with engine.connect() as conn:

        for table in dataset:

            # ---------------------------------------------------------
            # 1. Validate filters against this table
            # ---------------------------------------------------------
            validated_filters = validate_filters_for_table(
                table,
                filters
            )

            # ---------------------------------------------------------
            # 2. Full filters
            #
            # Used for:
            #   - total
            #   - data
            #
            # Includes state filter if provided.
            # ---------------------------------------------------------
            filter_sql, filter_params = build_filter_clause(
                validated_filters
            )

            # ---------------------------------------------------------
            # 3. Aggregation filters
            #
            # Used for state aggregation.
            #
            # IMPORTANT:
            # Exclude state itself so aggregation shows the
            # distribution across all states intersecting the polygon.
            # ---------------------------------------------------------
            aggregation_filters = {
                column: values
                for column, values in validated_filters.items()
                if column != "state"
            }

            aggregation_filter_sql, aggregation_filter_params = (
                build_filter_clause(
                    aggregation_filters
                )
            )

            # ---------------------------------------------------------
            # 4. Selected filters
            #
            # Tell frontend which state(s) user explicitly selected.
            # ---------------------------------------------------------
            selected_filters = {}

            if "state" in validated_filters:
                selected_filters["state"] = validated_filters["state"]

            # ---------------------------------------------------------
            # 5. Spatial condition
            #
            # This condition is applied to total, aggregation and data.
            # ---------------------------------------------------------
            spatial_sql = """
                ST_Intersects(
                    t.geom,
                    ST_SetSRID(
                        ST_GeomFromText(:wkt),
                        4326
                    )
                )
            """

            # ---------------------------------------------------------
            # 6. TOTAL
            #
            # Uses:
            #   polygon + ALL filters
            #
            # Example:
            #   polygon intersects 6205 records
            #   => total = 6205
            # ---------------------------------------------------------
            count_query = text(f"""
                SELECT COUNT(*)
                FROM {SCHEMA}.{table} t
                WHERE {spatial_sql}
                {filter_sql}
            """)

            total = conn.execute(
                count_query,
                {
                    "wkt": wkt,
                    **filter_params
                }
            ).scalar() or 0

            # ---------------------------------------------------------
            # 7. STATE AGGREGATION
            #
            # Uses:
            #   polygon + ALL filters EXCEPT state
            #
            # So for the current polygon:
            #
            #   Maharashtra  -> 2630
            #   Uttarakhand  -> 2342
            #   Rajasthan     -> 1233
            #
            # total          -> 6205
            # ---------------------------------------------------------
            aggregation = {}

            if "state" in validated_filters or table == "cpmp_v2":

                aggregation_query = text(f"""
                    WITH state_counts AS (
                        SELECT
                            t."state",
                            COUNT(*) AS count
                        FROM {SCHEMA}.{table} t
                        WHERE {spatial_sql}
                        {aggregation_filter_sql}
                        GROUP BY t."state"
                    ),
                    state_geometries AS (
                        SELECT DISTINCT ON (t."state")
                            t."state",
                            ST_AsGeoJSON(t.geom) AS geom_geojson
                        FROM {SCHEMA}.{table} t
                        WHERE {spatial_sql}
                          AND t."state" IS NOT NULL
                        ORDER BY t."state"
                    )
                    SELECT
                        sc."state",
                        sc.count,
                        sg.geom_geojson
                    FROM state_counts sc
                    LEFT JOIN state_geometries sg
                      ON sg."state" = sc."state"
                    ORDER BY sc.count DESC
                """)

                aggregation_rows = conn.execute(
                    aggregation_query,
                    {
                        "wkt": wkt,
                        **aggregation_filter_params
                    }
                )

                aggregation["state"] = [
                    {
                        "value": row[0],
                        "count": row[1],
                        "geom": json.loads(row[2]) if row[2] else None,
                    }
                    for row in aggregation_rows
                    if row[0] is not None
                ]

            # ---------------------------------------------------------
            # 8. DATA
            #
            # Uses:
            #   polygon + ALL filters
            #
            # Pagination applies ONLY here.
            # ---------------------------------------------------------
            if table == "gbif":

                query = text(f"""
                    SELECT
                        t.*,
                        ST_X(t.geom) AS longitude,
                        ST_Y(t.geom) AS latitude
                    FROM {SCHEMA}.{table} t
                    WHERE {spatial_sql}
                    {filter_sql}
                    LIMIT :limit
                    OFFSET :offset
                """)

            else:

                query = text(f"""
                    SELECT
                        t.*,
                        ST_AsGeoJSON(t.geom) AS geom_geojson
                    FROM {SCHEMA}.{table} t
                    WHERE {spatial_sql}
                    {filter_sql}
                    LIMIT :limit
                    OFFSET :offset
                """)

            res = conn.execute(
                query,
                {
                    "wkt": wkt,
                    "limit": limit,
                    "offset": offset,
                    **filter_params
                }
            )

            rows = [
                dict(row._mapping)
                for row in res
            ]

            # ---------------------------------------------------------
            # 9. FINAL RESULT
            # ---------------------------------------------------------
            results_by_dataset[table] = {
                "total": total,
                "aggregation": aggregation,
                "selected_filters": selected_filters,
                "data": rows
            }

    return results_by_dataset

# Returns all data from datasets (no geometry filter). Used when no polygon is provided.
def get_all_data_from_datasets(
    dataset: List[str],
    limit: int = 1000,
    offset: int = 0,
    filters=None
) -> Dict[str, Dict]:

    results_by_dataset: Dict[str, Dict] = {}

    with engine.connect() as conn:

        for table in dataset:

            # ---------------------------------------------------------
            # 1. Validate filters against this table
            # ---------------------------------------------------------
            validated_filters = validate_filters_for_table(
                table,
                filters
            )

            # ---------------------------------------------------------
            # 2. Full filters
            #
            # Used for:
            #   - total
            #   - data
            #
            # Includes state filter if provided.
            # ---------------------------------------------------------
            filter_sql, filter_params = build_filter_clause(
                validated_filters
            )

            # ---------------------------------------------------------
            # 3. Aggregation filters
            #
            # Used for state aggregation.
            #
            # IMPORTANT:
            # Exclude state itself so aggregation always shows the
            # distribution across ALL states.
            # ---------------------------------------------------------
            aggregation_filters = {
                column: values
                for column, values in validated_filters.items()
                if column != "state"
            }

            aggregation_filter_sql, aggregation_filter_params = (
                build_filter_clause(
                    aggregation_filters
                )
            )

            # ---------------------------------------------------------
            # 4. Selected filters
            #
            # Tell frontend which state(s) user explicitly selected.
            # ---------------------------------------------------------
            selected_filters = {}

            if "state" in validated_filters:
                selected_filters["state"] = validated_filters["state"]

            # ---------------------------------------------------------
            # 5. TOTAL
            #
            # Uses ALL filters, including state.
            # ---------------------------------------------------------
            count_query = text(f"""
                SELECT COUNT(*)
                FROM {SCHEMA}.{table} t
                WHERE 1=1
                {filter_sql}
            """)

            total = conn.execute(
                count_query,
                filter_params
            ).scalar() or 0

            # ---------------------------------------------------------
            # 6. STATE AGGREGATION
            #
            # Uses ALL filters EXCEPT state.
            # ---------------------------------------------------------
            aggregation = {}

            if "state" in validated_filters or table == "cpmp_v2":

                aggregation_query = text(f"""
                                         WITH state_counts AS (
                                         SELECT
                                         t."state",
                                         COUNT(*) AS count
                                         FROM {SCHEMA}.{table} t
                                         WHERE 1=1
                                         {aggregation_filter_sql}
                                         GROUP BY t."state"
                                         ),
                                         state_geometries AS (
                                         SELECT DISTINCT ON (t."state")
                                         t."state",
                                         ST_AsGeoJSON(t.geom) AS geom_geojson
                                         FROM {SCHEMA}.{table} t
                                         WHERE t."state" IS NOT NULL
                                         ORDER BY t."state"
                                         )
                                         SELECT
                                         sc."state",
                                         sc.count,
                                         sg.geom_geojson
                                         FROM state_counts sc
                                         LEFT JOIN state_geometries sg
                                         ON sg."state" = sc."state"
                                         ORDER BY sc.count DESC
                                         """)

                aggregation_rows = conn.execute(
                    aggregation_query,
                    aggregation_filter_params
                )

                aggregation["state"] = [
                    {
                        "value": row[0],
                        "count": row[1],
                        "geom": json.loads(row[2]) if row[2] else None,
                        }
                        for row in aggregation_rows
                        if row[0] is not None
                        ]

            # ---------------------------------------------------------
            # 7. DATA
            #
            # Uses ALL filters, including state.
            # ---------------------------------------------------------
            if table == "gbif":

                query = text(f"""
                    SELECT
                        t.*,
                        ST_X(t.geom) AS longitude,
                        ST_Y(t.geom) AS latitude
                    FROM {SCHEMA}.{table} t
                    WHERE 1=1
                    {filter_sql}
                    LIMIT :limit
                    OFFSET :offset
                """)

            else:

                query = text(f"""
                    SELECT
                        t.*,
                        ST_AsGeoJSON(t.geom) AS geom_geojson
                    FROM {SCHEMA}.{table} t
                    WHERE 1=1
                    {filter_sql}
                    LIMIT :limit
                    OFFSET :offset
                """)

            res = conn.execute(
                query,
                {
                    "limit": limit,
                    "offset": offset,
                    **filter_params
                }
            )

            rows = [
                dict(row._mapping)
                for row in res
            ]

            # ---------------------------------------------------------
            # 8. FINAL RESULT
            # ---------------------------------------------------------
            results_by_dataset[table] = {
                "total": total,
                "aggregation": aggregation,
                "selected_filters": selected_filters,
                "data": rows
            }

    return results_by_dataset

# Accepts a scientific name and returns matching names with longitude and latitude from both datasets

def get_scientific_name_matches_from_datasets(scientific_name: str, dataset: list = ["gbif", "kew_with_geom", "cpmp"]) -> Dict[str, List[Dict]]:
	results_by_dataset: Dict[str, List[Dict]] = {}
	with engine.connect() as conn:
		for table in dataset:
			if table == "gbif":
				query = text(f'''
					SELECT t.*,
					       ST_X(t.geom) AS longitude,
					       ST_Y(t.geom) AS latitude
					FROM {SCHEMA}.{table} t
					WHERE LOWER(t.scientificname) LIKE :name
				''')
			elif table == "cpmp":
				# cpmp has no scientificname column; match against genus+species and genus+species+author
				query = text(f'''
					SELECT t.*,
					       ST_AsGeoJSON(t.geom) AS geom_geojson
					FROM {SCHEMA}.{table} t
					WHERE LOWER(COALESCE(t.genus, '') || ' ' || COALESCE(t.species, '')) LIKE :name
					   OR LOWER(COALESCE(t.genus, '') || ' ' || COALESCE(t.species, '') || ' ' || COALESCE(t.author, '')) LIKE :name
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

# Returns the dataset_mapping rows (raw field_name -> canonical ontology_mapping,
# plus its human-readable display label) registered for the dataset whose
# dataset_master.title matches dataset_title. Comparison is case- and
# surrounding-whitespace-insensitive, since callers (e.g. REST clients) may pass
# titles with incidental leading/trailing spaces.
def get_dataset_field_mappings(dataset_title: str) -> List[Dict]:
	query = text(f"""
		SELECT dm.field_name, dm.ontology_mapping, dm.ontology_mapping_to_display
		FROM {SCHEMA}.dataset_mapping dm
		JOIN {SCHEMA}.dataset_master dmas ON dmas.dataset_id = dm.dataset_id
		WHERE LOWER(TRIM(dmas.title)) = LOWER(TRIM(:title))
	""")
	with engine.connect() as conn:
		res = conn.execute(query, {"title": dataset_title})
		return [dict(row._mapping) for row in res]

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


