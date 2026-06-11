"""
Data-access layer for attribute-based map search.

Given an attribute (a `field_name` registered in the `dataset_mapping` table) and a
value, this layer:
  1. resolves which dataset(s) own that attribute,
  2. locates the dataset's physical table,
  3. fetches the matching rows, returning geometry as GeoJSON when the table has a
     `geom` column and the raw rows (including a location key such as `shrid2`)
     otherwise.

All user-supplied values are passed as bound parameters. Identifiers (table/column
names) are validated against `dataset_mapping` / `information_schema` before being
interpolated, so they act as a whitelist and are never taken raw from the request.
"""

from typing import Dict, List, Optional

from sqlalchemy.sql import text

from database.database import engine
from utils.config import db_schema

SCHEMA = db_schema

# Postgres types we treat as numeric for exact-match casting.
_NUMERIC_TYPES = {
    "smallint", "integer", "bigint", "decimal", "numeric",
    "real", "double precision",
}

# Columns that act as a "join key" to a boundary layer the frontend already renders.
# Used when a dataset has no geometry of its own (e.g. PMGSY roads, SECC, Econ Census).
_LOCATION_KEYS = ["shrid2", "shrid", "shrug_id", "lgd_code", "district_code", "state_code"]


def resolve_attribute(attribute: str) -> List[Dict]:
    """Return the dataset_mapping entries that own this attribute (field_name).

    Each entry: {dataset_id, dataset_title, data_type, ontology_mapping_to_display}.
    Empty list if the attribute is not registered for any dataset.
    """
    query = text(f"""
        SELECT dm.dataset_id,
               dmas.title AS dataset_title,
               dm.data_type,
               dm.ontology_mapping_to_display
        FROM {SCHEMA}.dataset_mapping dm
        JOIN {SCHEMA}.dataset_master dmas ON dmas.dataset_id = dm.dataset_id
        WHERE dm.field_name = :attribute
    """)
    with engine.connect() as conn:
        res = conn.execute(query, {"attribute": attribute})
        return [dict(row._mapping) for row in res]


def get_table_info(attribute: str, dataset_id: int) -> Optional[Dict]:
    """Locate the physical table that owns `attribute` for the given dataset.

    A column name (the attribute) usually belongs to exactly one table. When more
    than one table carries the same column, we prefer the one that also has a
    `dataset_id` column whose value matches, so the resolution stays correct even
    if two datasets happen to share a column name.

    Returns {table, column_type, has_geom, geom_type, location_key, has_dataset_id}
    or None when no physical table can be found.
    """
    candidates_q = text("""
        SELECT table_name, data_type
        FROM information_schema.columns
        WHERE table_schema = :schema AND column_name = :attribute
    """)
    with engine.connect() as conn:
        candidates = [dict(r._mapping) for r in conn.execute(
            candidates_q, {"schema": SCHEMA, "attribute": attribute})]
        if not candidates:
            return None

        chosen = None
        # Prefer a table that carries the matching dataset_id.
        for cand in candidates:
            info = _describe_table(conn, cand["table_name"])
            if info["has_dataset_id"] and _table_has_dataset_id(conn, cand["table_name"], dataset_id):
                chosen = (cand, info)
                break
        if chosen is None:
            # Fall back to the first candidate (single-table case is the norm).
            cand = candidates[0]
            chosen = (cand, _describe_table(conn, cand["table_name"]))

        cand, info = chosen
        return {
            "table": cand["table_name"],
            "column_type": cand["data_type"],
            **info,
        }


def _describe_table(conn, table_name: str) -> Dict:
    """Inspect a table for geometry, a dataset_id column, and a known location key."""
    cols_q = text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = :schema AND table_name = :table_name
    """)
    columns = {r[0] for r in conn.execute(cols_q, {"schema": SCHEMA, "table_name": table_name})}

    geom_type = None
    has_geom = "geom" in columns
    if has_geom:
        gtype_q = text("""
            SELECT type FROM geometry_columns
            WHERE f_table_schema = :schema AND f_table_name = :table_name
            LIMIT 1
        """)
        row = conn.execute(gtype_q, {"schema": SCHEMA, "table_name": table_name}).first()
        geom_type = row[0] if row else None

    location_key = next((k for k in _LOCATION_KEYS if k in columns), None)

    return {
        "has_geom": has_geom,
        "geom_type": geom_type,
        "location_key": location_key,
        "has_dataset_id": "dataset_id" in columns,
    }


def _table_has_dataset_id(conn, table_name: str, dataset_id: int) -> bool:
    q = text(f'SELECT 1 FROM {SCHEMA}."{table_name}" WHERE dataset_id = :dsid LIMIT 1')
    return conn.execute(q, {"dsid": dataset_id}).first() is not None


def query_by_attribute(
    table: str,
    attribute: str,
    value: str,
    table_info: Dict,
    dataset_id: Optional[int] = None,
    limit: int = 1000,
    offset: int = 0,
) -> List[Dict]:
    """Fetch rows where `attribute` exactly equals `value`.

    When the table has geometry, geometry is returned as `geom_geojson` (and, for
    point layers, `longitude`/`latitude`). Identifiers are pre-validated; the value
    is always bound as a parameter.
    """
    is_numeric = (table_info.get("column_type") in _NUMERIC_TYPES)

    select_extra = ""
    if table_info.get("has_geom"):
        if table_info.get("geom_type") and "POINT" in table_info["geom_type"].upper():
            select_extra = (
                ', ST_X(t.geom) AS longitude, ST_Y(t.geom) AS latitude'
                ', ST_AsGeoJSON(t.geom) AS geom_geojson'
            )
        else:
            select_extra = ', ST_AsGeoJSON(t.geom) AS geom_geojson'

    # Exact match. Cast both sides to text for non-numeric columns so the bound
    # string parameter always compares cleanly; cast the value to numeric otherwise.
    if is_numeric:
        where_clause = f't."{attribute}" = CAST(:value AS double precision)'
    else:
        where_clause = f't."{attribute}"::text = :value'

    params = {"value": value, "limit": limit, "offset": offset}
    dataset_filter = ""
    if dataset_id is not None and table_info.get("has_dataset_id"):
        dataset_filter = " AND t.dataset_id = :dsid"
        params["dsid"] = dataset_id

    query = text(f"""
        SELECT t.*{select_extra}
        FROM {SCHEMA}."{table}" t
        WHERE {where_clause}{dataset_filter}
        LIMIT :limit OFFSET :offset
    """)
    with engine.connect() as conn:
        res = conn.execute(query, params)
        return [dict(row._mapping) for row in res]
