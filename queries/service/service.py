from typing import List, Optional, Any, Dict
import math
import uuid
from shapely.geometry import Polygon, MultiPolygon
from queries.dao.dao import get_polygon_data_from_datasets, get_multi_polygon_data_from_datasets, get_all_data_from_datasets, get_scientific_name_matches_from_datasets, get_table_column_names, filter_existing_tables, get_dataset_field_mappings
from utils.config import DATASET_MAPPING, REVERSE_DATASET_MAPPING

# Candidate physical tables that back a scientific-name search, in tie-break order.
SCIENTIFIC_NAME_SEARCH_TABLES = ("gbif", "kew_with_geom", "cpmp")

# Raw columns that carry geometry/position data for map rendering. These have no
# ontology_mapping entry of their own, so they're preserved on every dataset-scoped
# result. Raw WKB "geom" and the geom_geojson polygon blob are deliberately
# excluded -- the v2 REST search response is record data, not map geometry.
GEOMETRY_KEEP_KEYS = {"longitude", "latitude", "decimallatitude", "decimallongitude"}

# Ontology names never returned in result rows at all (not useful as record data).
RESULT_EXCLUDE_ONTOLOGY_NAMES = {"geom"}

# Ontology names excluded from displayFields only -- still present in each result row.
DISPLAY_FIELDS_EXCLUDE_ONTOLOGY_NAMES = {"id", "geom"}


def _normalize(s: str) -> str:
	"""Loosely normalize a field/column name for comparison (case, spaces, underscores)."""
	return s.strip().lower().replace(" ", "").replace("_", "")


def resolve_table_for_field_names(field_names: List[str], candidates=SCIENTIFIC_NAME_SEARCH_TABLES) -> Optional[str]:
	"""Pick whichever candidate table's columns best overlap the given field_names.

	field_names typically come from dataset_mapping.field_name, which may not match
	real column names exactly (extra spaces, casing) -> compared via _normalize.
	Returns None if no candidate has any overlap.
	"""
	normalized_fields = {_normalize(f) for f in field_names}
	best_table = None
	best_overlap = 0
	for table in candidates:
		columns = {_normalize(c) for c in get_table_column_names(table)}
		overlap = len(normalized_fields & columns)
		if overlap > best_overlap:
			best_overlap = overlap
			best_table = table
	return best_table

# Curated display_fields per dataset (only these columns appear under display_fields in the API response)
DISPLAY_FIELDS_BY_DATASET = {
	"gbif": [
		"basisofrecord",
		"countrycode",
		"decimallatitude",
		"decimallongitude",
		"eventdate",
		"latitude",
		"longitude",
		"scientificname",
	],
	"kew": ["scientificname", "continent", "region", "area"],
	"cpmp": ["family", "genus", "species", "author", "state"],
}
# When subsetting rows by client-provided display fields, always retain these if present (map clients).
_GEOM_OUTPUT_KEYS = frozenset({"geom", "geom_geojson", "longitude", "latitude"})


def _normalize_display_fields_map(raw: Any) -> Dict[str, List[str]]:
   if not raw or not isinstance(raw, dict):
       return {}
   out: Dict[str, List[str]] = {}
   for key, val in raw.items():
       if not isinstance(key, str):
           continue
       if isinstance(val, list):
           fields = [str(x) for x in val if x is not None and str(x).strip() != ""]
           if fields:
               out[key] = fields
   return out


def _project_row(record: dict, display_fields: List[str]) -> dict:
   allowed = set(display_fields) | _GEOM_OUTPUT_KEYS
   return {k: v for k, v in record.items() if k in allowed}

# Columns omitted from display_fields when derived from schema/data (e.g. gbif)
DISPLAY_FIELDS_EXCLUDE_BY_DATASET = {
	"gbif": {"dataset_id", "geom"},
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

def normalize_filters(filters):
   if not filters:
       return {}

   normalized = {}

   for column, value in filters.items():
       if value is None:
           continue

       if isinstance(value, list):
           normalized[column] = value
       else:
           normalized[column] = [value]

   return normalized



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

def fetch_dataset_columns(dataset: List[str]):
   """
   Return column names for each requested dataset table.
   Format: {"results": {"kew": ["col1", ...], "gbif": ["col1", ...], "unknown": None}}
   """
   results: dict = {}
   # Columns that should not be exposed by the "columns-only" API
   hidden_cols = {"dataset_id", "geom", "geom_geojson", "longitude", "latitude"}

   # Validate which tables exist (keeps input order)
   existing = set(filter_existing_tables(dataset))
   for table_name in dataset:
       if table_name not in existing:
           results[table_name] = None
           continue
       cols = get_table_column_names(table_name)
       results[table_name] = [c for c in cols if c not in hidden_cols]

   return {"results": results}


def _legacy_matches_flat(scientific_name: str) -> dict:
	"""Fallback shape for dataset-scoped search when dataset_title isn't registered
	or resolves to no known table: flattens the legacy multi-table (gbif+kew+cpmp)
	search into the same {displayFields, results} contract used by
	fetch_scientific_name_matches_by_dataset, with an empty label map since there's
	no dataset_mapping to draw labels from.
	"""
	legacy = fetch_scientific_name_matches(scientific_name)
	rows = []
	for table_rows in legacy.get("results", {}).values():
		rows.extend(table_rows)
	return {"displayFields": {}, "results": rows}


def fetch_scientific_name_matches_by_dataset(scientific_name: str, dataset_title: str, fields: List[str] = None):
	"""Scientific-name search scoped to a single registered dataset.

	Resolves dataset_title -> its dataset_mapping rows -> the physical table those
	raw field_names best match, then narrows those rows to the ones whose field_name
	is an actual column on that table (dataset_mapping can register more fields --
	e.g. trade_name -- than the resolved physical table has, in which case they're
	dropped rather than advertised with no data behind them). Queries only that
	table and renames matched columns to their ontology_mapping name. Response is
	{"displayFields": {ontology_name: label, ...}, "results": [...]}: every
	table-backed ontology-mapped field is always included in each result row
	(minus RESULT_EXCLUDE_ONTOLOGY_NAMES, e.g. raw "geom"), regardless of the
	incoming `fields` -- it's accepted for contract compatibility but no longer
	acts as a whitelist/display hint; callers decide what to display from the
	full result object. displayFields lists every returned field except
	DISPLAY_FIELDS_EXCLUDE_ONTOLOGY_NAMES ("id", "geom"); labels come from
	dataset_mapping.ontology_mapping_to_display (falling back to the ontology
	name itself if a mapping row has no label set).

	Falls back to the legacy fetch_scientific_name_matches (gbif+kew+cpmp, unrenamed,
	empty displayFields) when dataset_title isn't registered, resolves to no known
	table, or resolves to a table with no field_name overlap at all.
	"""
	mappings = get_dataset_field_mappings(dataset_title)
	if not mappings:
		return _legacy_matches_flat(scientific_name)

	field_names = [m["field_name"] for m in mappings]
	table = resolve_table_for_field_names(field_names)
	if table is None:
		return _legacy_matches_flat(scientific_name)

	table_columns = {_normalize(c) for c in get_table_column_names(table)}
	mappings = [m for m in mappings if _normalize(m["field_name"]) in table_columns]
	if not mappings:
		return _legacy_matches_flat(scientific_name)

	raw = get_scientific_name_matches_from_datasets(scientific_name, dataset=[table])
	rows = clean_nan_values(raw.get(table, []))

	rename_map = {_normalize(m["field_name"]): m["ontology_mapping"] for m in mappings}
	label_map = {}
	available_ontology_names = []
	seen = set()
	for m in mappings:
		name = m["ontology_mapping"]
		if name not in label_map and m.get("ontology_mapping_to_display"):
			label_map[name] = m["ontology_mapping_to_display"]
		if name not in seen:
			seen.add(name)
			available_ontology_names.append(name)

	result_exclude_normalized = {_normalize(n) for n in RESULT_EXCLUDE_ONTOLOGY_NAMES}
	ontology_names = [n for n in available_ontology_names if _normalize(n) not in result_exclude_normalized]

	transformed_rows = []
	for row in rows:
		new_row = {}
		for key, value in row.items():
			normalized_key = _normalize(key)
			if normalized_key in rename_map:
				out_key = rename_map[normalized_key]
				if _normalize(out_key) not in result_exclude_normalized:
					new_row[out_key] = value
			elif key in GEOMETRY_KEEP_KEYS:
				new_row[key] = value
		transformed_rows.append(new_row)

	display_exclude_normalized = {_normalize(n) for n in DISPLAY_FIELDS_EXCLUDE_ONTOLOGY_NAMES}
	display_fields = {
		name: label_map.get(name, name)
		for name in ontology_names
		if _normalize(name) not in display_exclude_normalized
	}

	return {"displayFields": display_fields, "results": transformed_rows}


######## This logic because this way we won't have to define the model individually it wll give response for any number of datasets but it will return entire data ################

def _raw_display_fields_empty_for_dataset(raw: Any, frontend_name: str) -> bool:
   """True if client sent null or [] for this dataset's field list."""
   if not isinstance(raw, dict) or frontend_name not in raw:
       return False
   v = raw[frontend_name]
   return v is None or (isinstance(v, list) and len(v) == 0)


def transform_results_with_display_fields(
   results_by_frontend: dict,
   mapped_datasets: List[str],
   frontend_datasets: List[str],
   fields_map: Optional[Dict[str, List[str]]] = None,
   raw_display_fields_by_dataset: Any = None,
) -> dict:
   """
   Transform results to include display_fields, total and data for each dataset.
   The input for each dataset is expected to contain:
   - total: total number of matching records
   - data: paginated dataset records
   
   If fields_map provides a non-empty list for a dataset,
   that list is returned as display_fields and data is projected
   to only those fields plus geometry fields.
   
   If displayFieldsByDataset is sent as {} (empty object),
   or a dataset is sent with null/[],
   display_fields is null and data is returned unfiltered.
   
   When displayFieldsByDataset is omitted (None),
   display_fields are derived from the DB schema merged with
   keys present in returned rows.
   
   """
   
   transformed_results = {}
   requested = fields_map or {}

   global_empty_object = (
       isinstance(raw_display_fields_by_dataset, dict)
       and len(raw_display_fields_by_dataset) == 0
   )

   frontend_to_table = {}

   for i, frontend_name in enumerate(frontend_datasets):
       if i < len(mapped_datasets):
           frontend_to_table[frontend_name] = mapped_datasets[i]
       else:
           frontend_to_table[frontend_name] = frontend_name

   for frontend_name in frontend_datasets:

       result = results_by_frontend.get(frontend_name)

       # Dataset not returned / not found
       if result is None:
           transformed_results[frontend_name] = None
           continue

       # New DAO structure
       if isinstance(result, dict) and "data" in result:
            total = result.get("total", 0)
            aggregation = result.get("aggregation", {})
            data = result.get("data", [])
            selected_filters = result.get("selected_filters", {})
       else:
            total = None
            aggregation = {}
            selected_filters = {}
            data = result

       if (
           global_empty_object
           or
           _raw_display_fields_empty_for_dataset(
               raw_display_fields_by_dataset,
               frontend_name
           )
       ):
           transformed_results[frontend_name] = {
                "display_fields": None,
                "total": total,
                "aggregation": aggregation,
                "selected_filters": selected_filters,
                "data": data,
            }
           continue

       client_fields = requested.get(frontend_name)

       if client_fields:

           display_fields = list(client_fields)

           if isinstance(data, list):
               data = [
                   _project_row(record, client_fields)
                   if isinstance(record, dict)
                   else record
                   for record in data
               ]

           elif isinstance(data, dict):
               data = _project_row(data, client_fields)

       else:

           table_name = frontend_to_table.get(
               frontend_name,
               frontend_name
           )

           display_fields = get_table_column_names(table_name)

           if isinstance(data, list) and len(data) > 0:

               all_keys = set()

               for record in data:
                   if isinstance(record, dict):
                       all_keys.update(record.keys())

               display_fields = sorted(
                   list(set(display_fields) | all_keys)
               )

           elif isinstance(data, dict) and len(data) > 0:

               display_fields = sorted(
                   list(set(display_fields) | set(data.keys()))
               )

       transformed_results[frontend_name] = {
            "display_fields": display_fields,
            "total": total,
            "aggregation": aggregation,
            "selected_filters": selected_filters,
            "data": data,
        }

   return transformed_results

def fetch_multi_polygon_query_with_display_fields(
   dataset: List[str],
   polygon_detail: List[dict],
   limit: int = 1000,
   offset: int = 0,
   display_fields_by_dataset: Any = None,
   filters=None,
):
   """
   Same as fetch_multi_polygon_query but returns data in format with display_fields.
   When polygon_detail is missing or empty, returns all data for the selected dataset(s).
   Uses dataset names directly as table names (no mapping) so response column names match getMultiPolygonData.
   Optional display_fields_by_dataset: dict mapping each dataset name (as in `dataset`) to a list of column names.
   Sending `{}` yields display_fields null per dataset while returning full rows. A dataset entry of null or [] does the same for that dataset only.
   """
   fields_map = _normalize_display_fields_map(display_fields_by_dataset)
   raw_fields = display_fields_by_dataset
   filters = normalize_filters(filters)

   # Use dataset names directly as table names, same as fetch_multi_polygon_query (no kew -> kew_with_geom mapping)
   datasets_to_query = dataset
   # If at least one requested dataset exists, ignore the unknown ones.
   # If none exist (e.g. ["xyz"]), keep original behavior (DB error is acceptable per requirements).
   existing = filter_existing_tables(datasets_to_query)
   if existing:
       datasets_to_query = existing

   # When no polygon(s) provided: return all data for the selected dataset(s)
   if not polygon_detail:
       raw_results_by_table = get_all_data_from_datasets(datasets_to_query, limit, offset, filters=filters,)
       results_by_frontend = {table_name: clean_nan_values(rows) for table_name, rows in raw_results_by_table.items()}
       transformed_results = transform_results_with_display_fields(
           results_by_frontend, datasets_to_query, dataset, fields_map, raw_fields
       )
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

   raw_results_by_table = get_multi_polygon_data_from_datasets(datasets_to_query, geometry, limit, offset, filters=filters,)

   # Use table names directly (no reverse mapping), same as fetch_multi_polygon_query
   results_by_frontend = {table_name: clean_nan_values(rows) for table_name, rows in raw_results_by_table.items()}

   transformed_results = transform_results_with_display_fields(
       results_by_frontend, datasets_to_query, dataset, fields_map, raw_fields
   )

   return {"results": transformed_results}



