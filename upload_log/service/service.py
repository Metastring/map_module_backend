from typing import List, Optional, Dict, Any, Tuple
import logging
import io
import os
import shutil
import subprocess
import tempfile
import zipfile
import json
from pathlib import Path
from uuid import UUID, uuid4

import aiofiles
import fiona
import pandas as pd
from fastapi import UploadFile, HTTPException, status
from pyproj import CRS
from sqlalchemy.orm import Session

from upload_log.models.model import DataType, UploadLogCreate, UploadLogFilter, UploadLogOut
from upload_log.models.schema import UploadLog as UploadLogTable
from upload_log.dao.dao import UploadLogDAO
from upload_log.service.metadata import derive_file_metadata
from geoserver.model import PostGISRequest, CreateLayerRequest
from geoserver.service import GeoServerService
from geoserver.admin.service import GeoServerAdminService
from geoserver.admin.dao import GeoServerAdminDAO
from geoserver.dao import GeoServerDAO
from utils.config import (
    host, port, username, password, database,
    geoserver_host, geoserver_port, geoserver_username, geoserver_password,
    sudo_password, geoserver_data_dir
)

logger = logging.getLogger(__name__)

# Initialize GeoServer services for helper functions
_geo_dao = GeoServerDAO(
    base_url=f"http://{geoserver_host}:{geoserver_port}/geoserver/rest",
    username=geoserver_username,
    password=geoserver_password,
)


def run_sudo_command(command: list, timeout: int = 10) -> subprocess.CompletedProcess:
    """Run a sudo command with password authentication."""
    if command[0] == 'sudo':
        command = ['sudo', '-S'] + command[1:]
    else:
        command = ['sudo', '-S'] + command
    
    try:
        result = subprocess.run(
            command,
            input=sudo_password + '\n',
            capture_output=True,
            text=True,
            timeout=timeout
        )
        if result.returncode != 0:
            logger.warning(f"Sudo command failed (exit code {result.returncode}): {' '.join(command)}")
        return result
    except subprocess.TimeoutExpired:
        logger.error(f"Sudo command timed out: {' '.join(command)}")
        raise
    except Exception as e:
        logger.error(f"Error running sudo command {' '.join(command)}: {e}")
        raise


def extract_shapefile_name_from_zip(zip_path: Path) -> Optional[str]:
    """Extract the shapefile name from a zip archive."""
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            shp_files = [f for f in zip_ref.namelist() if f.lower().endswith('.shp')]
            if shp_files:
                return Path(shp_files[0]).stem
    except Exception as exc:
        logger.error("Could not extract shapefile name from zip %s: %s", zip_path, exc, exc_info=True)
    return None


def get_shapefile_schema(shapefile_path: Path) -> Optional[Tuple[List[Dict[str, Any]], Optional[str], Optional[Dict[str, float]]]]:
    """Read the shapefile schema using fiona and convert to GeoServer attribute format."""
    try:
        with fiona.open(shapefile_path) as src:
            schema = src.schema
            properties = schema.get('properties', {})
            geometry_type = schema.get('geometry', 'Unknown')
            
            crs = None
            if src.crs:
                try:
                    crs = normalize_crs_to_epsg(str(src.crs))
                except Exception:
                    pass
            
            bbox = None
            if src.bounds:
                try:
                    minx, miny, maxx, maxy = src.bounds
                    bbox = {"minx": float(minx), "miny": float(miny), "maxx": float(maxx), "maxy": float(maxy)}
                except Exception:
                    pass
            
            attributes = []
            geometry_binding_map = {
                'Point': 'org.locationtech.jts.geom.Point',
                'LineString': 'org.locationtech.jts.geom.LineString',
                'Polygon': 'org.locationtech.jts.geom.Polygon',
                'MultiPoint': 'org.locationtech.jts.geom.MultiPoint',
                'MultiLineString': 'org.locationtech.jts.geom.MultiLineString',
                'MultiPolygon': 'org.locationtech.jts.geom.MultiPolygon',
                'GeometryCollection': 'org.locationtech.jts.geom.GeometryCollection',
            }
            geometry_binding = geometry_binding_map.get(geometry_type, 'org.locationtech.jts.geom.Geometry')
            
            attributes.append({
                "name": "the_geom",
                "minOccurs": 0,
                "maxOccurs": 1,
                "nillable": True,
                "binding": geometry_binding
            })
            
            type_mapping = {
                'str': 'java.lang.String',
                'int': 'java.lang.Integer',
                'float': 'java.lang.Double',
                'date': 'java.util.Date',
                'bool': 'java.lang.Boolean',
                'datetime': 'java.util.Date',
            }
            
            for prop_name, prop_type in properties.items():
                if prop_name.lower() in ['geometry', 'geom', 'the_geom', 'shape']:
                    continue
                java_type = type_mapping.get(prop_type, 'java.lang.String')
                attributes.append({
                    "name": prop_name,
                    "minOccurs": 0,
                    "maxOccurs": 1,
                    "nillable": True,
                    "binding": java_type
                })
            
            logger.info("Read schema: geometry type=%s, CRS=%s, bbox=%s, total attributes=%d",
                       geometry_type, crs, bbox, len(attributes))
            return (attributes, crs, bbox)
    except Exception as exc:
        logger.error("Failed to read shapefile schema from %s: %s", shapefile_path, exc, exc_info=True)
        return None


def extract_shapefile_from_zip_for_schema(zip_path: Path) -> Optional[Path]:
    """Extract the shapefile from zip to a temp location so we can read its schema."""
    try:
        temp_dir = tempfile.mkdtemp()
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
            shp_files = list(Path(temp_dir).rglob("*.shp"))
            if shp_files:
                return shp_files[0]
    except Exception as exc:
        logger.error("Failed to extract shapefile from zip %s: %s", zip_path, exc, exc_info=True)
    return None


def normalize_crs_to_epsg(crs_string: Optional[str]) -> Optional[str]:
    """Normalize CRS string to EPSG format (e.g., 'EPSG:4326')."""
    if not crs_string:
        return None
    try:
        crs = CRS.from_user_input(crs_string)
        epsg_code = crs.to_epsg()
        if epsg_code:
            return f"EPSG:{epsg_code}"
        if crs.to_authority():
            auth_name, code = crs.to_authority()
            if auth_name and code:
                return f"{auth_name.upper()}:{code}"
        return crs_string
    except Exception as exc:
        logger.warning("Failed to normalize CRS '%s': %s", crs_string, exc)
        if isinstance(crs_string, str) and crs_string.upper().startswith("EPSG:"):
            return crs_string.upper()
        return None


async def cleanup_datastore_directory(datastore_path: str) -> None:
    """Clean up old files from datastore directory."""
    if not os.path.exists(datastore_path):
        return
    try:
        deleted_count = 0
        failed_files = []
        for filename in os.listdir(datastore_path):
            file_item_path = os.path.join(datastore_path, filename)
            try:
                if os.path.isfile(file_item_path):
                    os.remove(file_item_path)
                    deleted_count += 1
                elif os.path.isdir(file_item_path):
                    shutil.rmtree(file_item_path)
                    deleted_count += 1
            except PermissionError:
                failed_files.append(file_item_path)
            except Exception:
                failed_files.append(file_item_path)
        
        if failed_files:
            for file_item_path in failed_files:
                try:
                    result = run_sudo_command(['rm', '-rf', file_item_path], timeout=5)
                    if result.returncode == 0:
                        deleted_count += 1
                except Exception:
                    pass
            
            if deleted_count == 0:
                try:
                    result = run_sudo_command(['rm', '-rf', datastore_path], timeout=10)
                    if result.returncode == 0:
                        os.makedirs(datastore_path, exist_ok=True)
                        try:
                            run_sudo_command(['chown', '-R', 'tomcat:tomcat', datastore_path], timeout=5)
                        except Exception:
                            pass
                except Exception:
                    pass
    except Exception as exc:
        logger.debug("Exception cleaning up directory: %s", exc)


def resolve_feature_type_name(file_path: Path, store_name: str) -> str:
    """Resolve the expected feature type name from zip or use store_name."""
    if file_path.suffix.lower() == '.zip':
        zip_filename_extracted = extract_shapefile_name_from_zip(file_path)
        if zip_filename_extracted:
            return zip_filename_extracted
        try:
            metadata = derive_file_metadata(file_path)
            metadata_layer_name = metadata.get("layer_name")
            if metadata_layer_name:
                return metadata_layer_name
        except Exception:
            pass
    return store_name


def get_feature_type_from_response(response) -> Optional[str]:
    """Extract feature type name from GeoServer response."""
    if hasattr(response, 'headers') and 'Location' in response.headers:
        location = response.headers['Location']
        if '/featuretypes/' in location:
            return location.split('/featuretypes/')[-1].split('.')[0]
    
    if response.text:
        try:
            response_data = json.loads(response.text)
            if isinstance(response_data, dict):
                feature_type = response_data.get("featureType", {})
                if isinstance(feature_type, dict):
                    return feature_type.get("name")
        except (json.JSONDecodeError, AttributeError):
            pass
    return None


async def wait_for_geoserver_processing(status_code: int) -> None:
    """Wait for GeoServer to process based on response status."""
    import asyncio
    if status_code == 202:
        await asyncio.sleep(5)
    elif status_code in (200, 201):
        await asyncio.sleep(3)


def fix_subdirectory_files(datastore_path: str, expected_feature_type_name: str) -> bool:
    """Fix files if they're in subdirectory instead of root. Returns True if fixed."""
    subdirectory_path = os.path.join(datastore_path, expected_feature_type_name)
    root_shp_path = os.path.join(datastore_path, expected_feature_type_name + ".shp")
    subdir_shp_path = os.path.join(subdirectory_path, expected_feature_type_name + ".shp")
    
    if not (os.path.isdir(subdirectory_path) and os.path.exists(subdir_shp_path)):
        return False
    
    root_shp_size = os.path.getsize(root_shp_path) if os.path.exists(root_shp_path) else 0
    subdir_shp_size = os.path.getsize(subdir_shp_path) if os.path.exists(subdir_shp_path) else 0
    
    if subdir_shp_size <= root_shp_size * 10:
        return False
    
    try:
        required_exts = ['.shp', '.shx', '.dbf', '.prj', '.cpg', '.qmd', '.qix']
        copied_count = 0
        for ext in required_exts:
            subdir_file = os.path.join(subdirectory_path, expected_feature_type_name + ext)
            root_file = os.path.join(datastore_path, expected_feature_type_name + ext)
            if os.path.exists(subdir_file):
                try:
                    shutil.copy2(subdir_file, root_file)
                    copied_count += 1
                except PermissionError:
                    try:
                        result = run_sudo_command(['cp', subdir_file, root_file], timeout=5)
                        if result.returncode == 0:
                            copied_count += 1
                    except Exception:
                        pass
                except Exception:
                    pass
        
        if copied_count == 0:
            try:
                copy_cmd = f"cp -r {subdirectory_path}/* {datastore_path}/"
                result = run_sudo_command(['sh', '-c', copy_cmd], timeout=10)
                if result.returncode == 0:
                    copied_count = len(required_exts)
            except Exception:
                pass
        
        if copied_count > 0:
            try:
                run_sudo_command(['chown', '-R', 'tomcat:tomcat', datastore_path], timeout=5)
            except Exception:
                pass
            return True
    except Exception as exc:
        logger.debug("Exception fixing subdirectory files: %s", exc)
    
    return False


def verify_layer_features(geoserver_layer_name: str) -> Tuple[bool, Optional[int]]:
    """Verify layer exists and has features. Returns (success, feature_count)."""
    try:
        wfs_response = _geo_dao.query_features(layer=geoserver_layer_name, max_features=1)
        if wfs_response.status_code != 200:
            return False, None
        
        import xml.etree.ElementTree as ET
        try:
            root = ET.fromstring(wfs_response.text)
            num_features = root.get("numberOfFeatures", "0")
            feature_count = int(num_features) if num_features.isdigit() else 0
            return feature_count > 0, feature_count
        except ET.ParseError:
            try:
                json_data = json.loads(wfs_response.text)
                if isinstance(json_data, dict):
                    features = json_data.get("features", [])
                    feature_count = len(features) if isinstance(features, list) else 0
                    return feature_count > 0, feature_count
            except json.JSONDecodeError:
                if "numberOfFeatures=\"0\"" in wfs_response.text or "numberOfFeatures='0'" in wfs_response.text:
                    return False, 0
    except Exception:
        pass
    return False, None


async def persist_upload(file: UploadFile, uploads_dir: Path) -> Path:
    """Persist uploaded file to disk."""
    if not file.filename:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="File name is required")
    
    uploads_dir.mkdir(parents=True, exist_ok=True)
    file_suffix = Path(file.filename).suffix
    unique_name = f"{uuid4().hex}{file_suffix}"
    destination = uploads_dir / unique_name
    
    try:
        async with aiofiles.open(destination, "wb") as out_file:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                await out_file.write(chunk)
    except Exception as exc:
        if destination.exists():
            destination.unlink(missing_ok=True)
        logger.error("Failed to persist upload %s: %s", file.filename, exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to persist file") from exc
    finally:
        await file.close()
    
    return destination


class UploadLogService:
    @staticmethod
    def create(upload_log_input: UploadLogCreate, db: Session) -> UploadLogOut:
        """Create a new upload log record from REST input."""
        try:
            crs_value = upload_log_input.crs or "UNKNOWN"
            bbox_value = upload_log_input.bbox if upload_log_input.bbox else None

            db_record = UploadLogTable(
                layer_name=upload_log_input.layer_name,
                file_format=upload_log_input.file_format,
                data_type=upload_log_input.data_type.value,
                crs=crs_value,
                bbox=bbox_value,
                source_path=upload_log_input.source_path,
                geoserver_layer=upload_log_input.geoserver_layer,
                tags=upload_log_input.tags,
                uploaded_by=upload_log_input.uploaded_by,
            )

            if upload_log_input.uploaded_on:
                db_record.uploaded_on = upload_log_input.uploaded_on

            created_record = UploadLogDAO.create(db_record, db)
            return UploadLogService._convert_to_model(created_record)
        except Exception as exc:
            logger.error("Error creating upload log record: %s", exc)
            raise

    @staticmethod
    def create_with_id(upload_log_input: UploadLogCreate, db: Session, log_id: UUID) -> UploadLogOut:
        """Create a new upload log record with a specific ID (used for dataset_id)."""
        try:
            logger.info(f"Attempting to create upload log with id {log_id}")
            logger.debug(f"Upload log input: layer_name={upload_log_input.layer_name}, "
                        f"file_format={upload_log_input.file_format}, "
                        f"uploaded_by={upload_log_input.uploaded_by}, "
                        f"source_path={upload_log_input.source_path}")
            
            crs_value = upload_log_input.crs or "UNKNOWN"
            bbox_value = upload_log_input.bbox if upload_log_input.bbox else None

            db_record = UploadLogTable(
                id=log_id,  # Set the specific ID
                layer_name=upload_log_input.layer_name,
                file_format=upload_log_input.file_format,
                data_type=upload_log_input.data_type.value,
                crs=crs_value,
                bbox=bbox_value,
                source_path=upload_log_input.source_path,
                geoserver_layer=upload_log_input.geoserver_layer,
                tags=upload_log_input.tags,
                uploaded_by=upload_log_input.uploaded_by,
            )

            if upload_log_input.uploaded_on:
                db_record.uploaded_on = upload_log_input.uploaded_on

            logger.info(f"Calling UploadLogDAO.create for id {log_id}")
            created_record = UploadLogDAO.create(db_record, db)
            logger.info(f"Successfully created upload log with id {log_id} (dataset_id)")
            return UploadLogService._convert_to_model(created_record)
        except Exception as exc:
            logger.error("Error creating upload log record with id %s: %s", log_id, exc, exc_info=True)
            raise

    @staticmethod
    def get_by_id(log_id: UUID, db: Session) -> Optional[UploadLogOut]:
        """Get upload log record by ID."""
        try:
            record = UploadLogDAO.get_by_id(log_id, db)
            if record:
                return UploadLogService._convert_to_model(record)
            return None
        except Exception as exc:
            logger.error("Error getting upload log by id %s: %s", log_id, exc)
            raise

    @staticmethod
    def get_filtered(filters: Optional[UploadLogFilter], db: Session) -> List[UploadLogOut]:
        """Get filtered upload log records."""
        try:
            db_filters = filters if filters else None
            records = UploadLogDAO.get_filtered(db_filters, db)
            return [UploadLogService._convert_to_model(record) for record in records]
        except Exception as exc:
            logger.error("Error getting filtered upload logs: %s", exc)
            raise

    @staticmethod
    def _convert_to_model(db_record: UploadLogTable) -> UploadLogOut:
        """Convert database record to API model."""
        data_type_value = db_record.data_type or DataType.UNKNOWN.value
        try:
            data_type = DataType(data_type_value)
        except ValueError:
            data_type = DataType.UNKNOWN

        return UploadLogOut(
            id=db_record.id,
            layer_name=db_record.layer_name,
            file_format=db_record.file_format,
            data_type=data_type,
            crs=db_record.crs,
            bbox=db_record.bbox,
            source_path=db_record.source_path,
            geoserver_layer=db_record.geoserver_layer,
            tags=db_record.tags,
            uploaded_by=db_record.uploaded_by,
            uploaded_on=db_record.uploaded_on,
        )

    @staticmethod
    async def create_table_and_insert1(
        table_name: str, 
        schema: str, 
        file,  # Can be UploadFile or file-like object
        db: Session,
        geo_service: Optional[GeoServerService] = None,
        workspace: str = "metastring",
        store_name: Optional[str] = None,
        dataset_id: Optional[UUID] = None,
        upload_log_id: Optional[UUID] = None
    ) -> str:
        # Read the file contents
        # Handle different file input types
        filename = None
        if hasattr(file, 'file') and hasattr(file, 'filename'):
            # It's a SimpleNamespace with file attribute (from API)
            contents = file.file.read()
            filename = file.filename
        elif hasattr(file, 'read'):
            # It's an UploadFile or file-like object
            try:
                # Try async read first
                contents = await file.read()
                filename = getattr(file, 'filename', None)
            except (TypeError, AttributeError):
                # Fallback for sync file objects
                contents = file.read()
                filename = getattr(file, 'filename', None)
        else:
            raise ValueError("Invalid file object provided")

        try:
            # Use provided dataset_id or generate a new one
            if dataset_id is None:
                dataset_id = uuid.uuid4()
                logger.info(f"Generated dataset_id: {dataset_id} for table {schema}.{table_name}")
            else:
                logger.info(f"Using provided dataset_id: {dataset_id} for table {schema}.{table_name}")

            is_excel_file = False
            if len(contents) >= 2:
                if contents[:2] == b'PK':
                    is_excel_file = True
                elif len(contents) >= 8 and contents[:8] == b'\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1':
                    is_excel_file = True
            
            has_csv_extension = filename and filename.lower().endswith('.csv')
            is_csv = has_csv_extension and not is_excel_file
            
            if is_csv:
                try:
                    sample = contents[:1024] if len(contents) > 1024 else contents
                    text_sample = sample.decode('utf-8', errors='strict')
                    non_printable = sum(1 for c in text_sample if ord(c) < 32 and c not in '\n\r\t')
                    if len(text_sample) > 0 and non_printable / len(text_sample) > 0.3:
                        is_csv = False
                        is_excel_file = True
                except UnicodeDecodeError:
                    if contents[:2] == b'PK':
                        is_csv = False
                        is_excel_file = True
            
            if is_csv:
                encodings_to_try = ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252', 'iso-8859-1']
                df = None
                last_error = None
                
                for encoding in encodings_to_try:
                    try:
                        parsing_strategies = [
                            {
                                'engine': 'python',
                                'sep': None,
                                'on_bad_lines': 'skip',
                                'skipinitialspace': True,
                                'quotechar': '"',
                                'doublequote': True,
                                'skip_blank_lines': True
                            },
                            {
                                'engine': 'python',
                                'sep': ',',
                                'on_bad_lines': 'skip',
                                'skipinitialspace': True,
                                'quotechar': '"',
                                'doublequote': True
                            },
                            {
                                'engine': 'c',
                                'sep': ',',
                                'on_bad_lines': 'skip',
                                'skipinitialspace': True
                            },
                            {
                                'engine': 'python',
                                'sep': None,
                                'error_bad_lines': False,
                                'warn_bad_lines': True,
                                'skipinitialspace': True
                            },
                            {
                                'engine': 'c',
                                'sep': ',',
                                'error_bad_lines': False,
                                'warn_bad_lines': True,
                                'skipinitialspace': True
                            },
                            {
                                'engine': 'python',
                                'sep': None,
                                'skipinitialspace': True
                            }
                        ]
                        
                        df = None
                        for strategy in parsing_strategies:
                            try:
                                read_params = {'encoding': encoding, **strategy}
                                df = pd.read_csv(io.BytesIO(contents), **read_params)
                                break
                            except (TypeError, ValueError):
                                continue
                            except Exception:
                                continue
                        
                        if df is not None and not df.empty:
                            break
                        else:
                            raise ValueError(f"All parsing strategies failed for encoding {encoding}")
                            
                    except UnicodeDecodeError as e:
                        last_error = e
                        continue
                    except Exception as e:
                        last_error = e
                        continue
                
                if df is None:
                    error_msg = f"Could not read CSV file '{filename}'. Tried encodings: {', '.join(encodings_to_try)}. Last error: {str(last_error)}"
                    raise HTTPException(status_code=400, detail=error_msg)
                
                if df.empty:
                    raise HTTPException(status_code=400, detail=f"CSV file '{filename}' appears to be empty or could not be parsed correctly.")
                
                for col in df.columns:
                    col_str = str(col)
                    if len(col_str) > 200:
                        raise HTTPException(status_code=400, detail=f"File '{filename}' appears to be a binary file (Excel/ZIP) misidentified as CSV. Column names contain binary data. Please ensure the file is actually a CSV file or upload it with .xlsx extension.")
                    non_ascii_count = sum(1 for c in col_str if ord(c) > 127 or (ord(c) < 32 and c not in '\n\r\t'))
                    if len(col_str) > 0 and non_ascii_count / len(col_str) > 0.5:
                        raise HTTPException(status_code=400, detail=f"File '{filename}' appears to be a binary file misidentified as CSV. Column name '{col_str[:50]}...' contains binary data. Please ensure the file is actually a CSV file or upload it with .xlsx extension.")
            else:
                df = pd.read_excel(io.BytesIO(contents))

            # Step 1: Create table dynamically
            logger.info(f"Creating table {schema}.{table_name}")
            UploadLogDAO.create_table1(table_name, schema, df, db)

            # Step 2: Insert data into the newly created table
            logger.info(f"Inserting data into table {schema}.{table_name} with dataset_id: {dataset_id}")
            UploadLogDAO.insert_data_dynamic1(table_name, schema, df, db, dataset_id)

            # Step 3: Add geometry column
            logger.info(f"Adding geometry column to table {schema}.{table_name}")
            UploadLogDAO.add_geometry_column(table_name, schema, db)

            # Step 4: Map geometry - prioritize geometry_wkt over state
            # If geometry_wkt column exists and has data, use it and skip state logic
            # Otherwise, use state logic
            geometry_mapping_message = ""
            has_geometry_wkt = "geometry_wkt" in df.columns
            
            if has_geometry_wkt:
                # Check if geometry_wkt column has any non-null, non-empty values
                has_wkt_data = df["geometry_wkt"].notna().any() and (df["geometry_wkt"].astype(str).str.strip() != "").any()
                
                if has_wkt_data:
                    # Use geometry_wkt column - convert WKT to MULTIPOLYGON
                    # Skip state logic when geometry_wkt is present (as per requirements)
                    logger.info(f"Mapping geometry from geometry_wkt column to table {schema}.{table_name} (skipping state logic)")
                    rows_updated = UploadLogDAO.map_geometry_from_wkt(table_name, schema, db)
                    logger.info(f"Updated {rows_updated} rows with geometry from geometry_wkt")
                    geometry_mapping_message = f"Geometry column populated from geometry_wkt ({rows_updated} rows updated)."
                else:
                    # geometry_wkt column exists but has no data, fall back to state logic
                    logger.info(f"geometry_wkt column exists but has no data, falling back to state-based mapping")
                    rows_updated = UploadLogDAO.map_geometry_from_world_geojson(table_name, schema, db)
                    logger.info(f"Updated {rows_updated} rows with geometry from world_geojson")
                    geometry_mapping_message = f"Geometry column populated from world_geojson using state column ({rows_updated} rows updated)."
            else:
                # No geometry_wkt column, use state logic
                logger.info(f"Mapping geometry from world_geojson to table {schema}.{table_name}")
                rows_updated = UploadLogDAO.map_geometry_from_world_geojson(table_name, schema, db)
                logger.info(f"Updated {rows_updated} rows with geometry data")
                geometry_mapping_message = f"Geometry column populated from world_geojson using state column ({rows_updated} rows updated)."

            # Step 5: Upload to GeoServer if geo_service is provided
            geoserver_message = ""
            if geo_service:
                try:
                    # Use provided store_name or default to table_name
                    final_store_name = store_name or f"{table_name}_store"
                    
                    # Create PostGIS datastore request
                    postgis_request = PostGISRequest(
                        workspace=workspace,
                        store_name=final_store_name,
                        database=database,
                        host=host,
                        port=port,
                        username=username,
                        password=password,
                        db_schema=schema
                    )
                    
                    # Upload PostGIS datastore
                    logger.info(f"Creating PostGIS datastore '{final_store_name}' in workspace '{workspace}'")
                    response = await geo_service.upload_postgis(postgis_request)
                    
                    if response.status_code in [200, 201]:
                        logger.info(f"PostGIS datastore '{final_store_name}' created successfully with status {response.status_code}")
                        # Create layer from table using admin service
                        layer_request = CreateLayerRequest(
                            workspace=workspace,
                            store_name=final_store_name,
                            table_name=table_name,
                            layer_name=table_name
                        )
                        
                        # Initialize admin service for layer creation
                        admin_dao = GeoServerAdminDAO(
                            base_url=f"http://{geoserver_host}:{geoserver_port}/geoserver/rest",
                            username=geoserver_username,
                            password=geoserver_password
                        )
                        admin_service = GeoServerAdminService(admin_dao)
                        
                        logger.info(f"Creating layer '{table_name}' from table '{table_name}' in store '{final_store_name}'")
                        layer_response = await admin_service.create_layer_from_table(layer_request)
                        
                        logger.info(f"Layer creation response status: {layer_response.status_code}, response: {layer_response.text[:500] if layer_response.text else 'No response text'}")
                        
                        if layer_response.status_code in [200, 201]:
                            # Verify the layer was actually created by checking if feature type exists
                            import asyncio
                            await asyncio.sleep(1)  # Give GeoServer a moment to process
                            
                            try:
                                verify_response = admin_dao.get_table_details(
                                    workspace=workspace,
                                    datastore=final_store_name,
                                    table_name=table_name
                                )
                                if verify_response.status_code == 200:
                                    logger.info(f"Verified: Feature type '{table_name}' exists in store '{final_store_name}'")
                                    geoserver_message = f" Successfully uploaded to GeoServer workspace '{workspace}' as layer '{table_name}'."
                                else:
                                    logger.warning(f"Layer creation returned {layer_response.status_code} but verification failed (status {verify_response.status_code}): {verify_response.text[:200]}")
                                    geoserver_message = f" Layer creation returned success but verification failed. Status: {layer_response.status_code}, Verify: {verify_response.status_code}"
                            except Exception as verify_error:
                                logger.warning(f"Could not verify layer creation: {verify_error}")
                                geoserver_message = f" Layer creation returned success (status {layer_response.status_code}) but verification failed: {str(verify_error)}"
                        else:
                            geoserver_message = f" PostGIS datastore created but layer creation failed (status {layer_response.status_code}): {layer_response.text[:200]}"
                            logger.error(f"Layer creation failed: status {layer_response.status_code}, response: {layer_response.text}")
                    else:
                        geoserver_message = f" GeoServer upload failed (status {response.status_code}): {response.text[:200]}"
                        logger.error(f"PostGIS datastore creation failed: status {response.status_code}, response: {response.text}")
                except Exception as geoserver_error:
                    logger.error(f"Error uploading to GeoServer: {geoserver_error}")
                    geoserver_message = f" GeoServer upload encountered an error: {str(geoserver_error)}"

            return (
                f"Table '{table_name}' created in schema '{schema}' and data inserted successfully. "
                + geometry_mapping_message
                + geoserver_message
            )
        except Exception as e:
            logger.error("Error in create_table_and_insert1: %s", e)
            raise e

