import json
import logging
import os
import shutil
import zipfile
import tempfile
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any
from uuid import uuid4, UUID

import aiofiles
import fiona
from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile, status
from sqlalchemy.orm import Session

from database.database import get_db
from upload_log.models.model import DataType, UploadLogCreate, UploadLogFilter, UploadLogOut
from upload_log.service.metadata import derive_file_metadata
from upload_log.service.service import UploadLogService
from geoserver.dao import GeoServerDAO
from geoserver.service import GeoServerService
from geoserver.admin.dao import GeoServerAdminDAO
from geoserver.admin.service import GeoServerAdminService
from upload_log.dao.dao import UploadLogDAO
from utils.config import geoserver_host, geoserver_port, geoserver_username, geoserver_password, sudo_password, geoserver_data_dir
from types import SimpleNamespace
from pyproj import CRS
import subprocess

router = APIRouter()
LOGGER = logging.getLogger(__name__)

def run_sudo_command(command: list, timeout: int = 10) -> subprocess.CompletedProcess:
    """
    Run a sudo command with password authentication.
    Uses sudo -S to read password from stdin.
    
    Args:
        command: List of command arguments (e.g., ['cp', 'src', 'dst'])
        timeout: Command timeout in seconds
    
    Returns:
        subprocess.CompletedProcess result
    """
    # Replace 'sudo' with 'sudo -S' to read password from stdin
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
        # Log the result for debugging
        if result.returncode != 0:
            LOGGER.warning(f"Sudo command failed (exit code {result.returncode}): {' '.join(command)}")
            if result.stderr:
                LOGGER.warning(f"Sudo stderr: {result.stderr[:200]}")
        return result
    except subprocess.TimeoutExpired:
        LOGGER.error(f"Sudo command timed out: {' '.join(command)}")
        raise
    except Exception as e:
        LOGGER.error(f"Error running sudo command {' '.join(command)}: {e}")
        raise
UPLOADS_DIR = Path(__file__).resolve().parents[2] / "uploads"
GEOSERVER_WORKSPACE = "metastring"

geo_dao = GeoServerDAO(
    base_url=f"http://{geoserver_host}:{geoserver_port}/geoserver/rest",
    username=geoserver_username,
    password=geoserver_password,
)
geo_service = GeoServerService(geo_dao)

geo_admin_dao = GeoServerAdminDAO(
    base_url=f"http://{geoserver_host}:{geoserver_port}/geoserver/rest",
    username=geoserver_username,
    password=geoserver_password,
)
geo_admin_service = GeoServerAdminService(geo_admin_dao)


def _extract_shapefile_name_from_zip(zip_path: Path) -> Optional[str]:
    """
    Extract the shapefile name from a zip archive.
    Returns the base name of the first .shp file found (without extension).
    
    IMPORTANT: GeoServer uses the shapefile name from INSIDE the zip, not the zip filename.
    So if your zip is named "agar_soil.zip" but contains "lyr_1_agar_geology.shp",
    GeoServer will use "lyr_1_agar_geology" as the nativeName.
    """
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Get all .shp files in the zip
            shp_files = [f for f in zip_ref.namelist() if f.lower().endswith('.shp')]
            
            LOGGER.debug("ZIP FILE ANALYSIS: %s - Found %d shapefile(s)", zip_path.name, len(shp_files))
            
            if shp_files:
                # Get the first .shp file and extract its base name
                # Handle both flat structure and directory structure
                first_shp = shp_files[0]
                shp_name = Path(first_shp).stem
                LOGGER.debug("Using first shapefile: '%s' (from path: %s)", shp_name, first_shp)
                LOGGER.debug("GeoServer will use shapefile name '%s' as nativeName", shp_name)
                return shp_name
            else:
                LOGGER.warning("No .shp files found in zip: %s", zip_path)
                LOGGER.debug("=" * 80)
    except Exception as exc:
        LOGGER.error("Could not extract shapefile name from zip %s: %s", zip_path, exc, exc_info=True)
    return None


def _get_shapefile_schema(shapefile_path: Path) -> Optional[tuple[List[Dict[str, Any]], Optional[str], Optional[Dict[str, float]]]]:
    """
    Read the shapefile schema using fiona and convert to GeoServer attribute format.
    Returns a tuple of (attributes list, CRS string, bounding box dict).
    CRITICAL: Must include the geometry attribute, otherwise GeoServer will fail with NullPointerException.
    """
    try:
        with fiona.open(shapefile_path) as src:
            schema = src.schema
            properties = schema.get('properties', {})
            geometry_type = schema.get('geometry', 'Unknown')
            
            # Get CRS from the shapefile
            crs = None
            if src.crs:
                try:
                    # Normalize CRS to EPSG format
                    crs = _normalize_crs_to_epsg(str(src.crs))
                except Exception:
                    pass
            
            # Get bounding box from the shapefile
            bbox = None
            if src.bounds:
                try:
                    minx, miny, maxx, maxy = src.bounds
                    bbox = {
                        "minx": float(minx),
                        "miny": float(miny),
                        "maxx": float(maxx),
                        "maxy": float(maxy)
                    }
                except Exception:
                    pass
            
            attributes = []
            
            # CRITICAL: Add geometry attribute first - GeoServer requires this!
            # Map fiona geometry types to GeoServer/Java geometry types
            # Note: GeoServer 2.26+ uses org.locationtech.jts (newer JTS library)
            # Older versions use com.vividsolutions.jts
            geometry_binding_map = {
                'Point': 'org.locationtech.jts.geom.Point',
                'LineString': 'org.locationtech.jts.geom.LineString',
                'Polygon': 'org.locationtech.jts.geom.Polygon',
                'MultiPoint': 'org.locationtech.jts.geom.MultiPoint',
                'MultiLineString': 'org.locationtech.jts.geom.MultiLineString',
                'MultiPolygon': 'org.locationtech.jts.geom.MultiPolygon',
                'GeometryCollection': 'org.locationtech.jts.geom.GeometryCollection',
            }
            
            # Get the geometry binding type
            geometry_binding = geometry_binding_map.get(geometry_type, 'org.locationtech.jts.geom.Geometry')
            
            # Add geometry attribute - GeoServer typically uses 'the_geom' as the default geometry field name
            attributes.append({
                "name": "the_geom",
                "minOccurs": 0,
                "maxOccurs": 1,
                "nillable": True,
                "binding": geometry_binding
            })
            
            # Map fiona types to GeoServer/Java types for regular attributes
            type_mapping = {
                'str': 'java.lang.String',
                'int': 'java.lang.Integer',
                'float': 'java.lang.Double',
                'date': 'java.util.Date',
                'bool': 'java.lang.Boolean',
                'datetime': 'java.util.Date',
            }
            
            # Add all other attributes (excluding geometry)
            for prop_name, prop_type in properties.items():
                # Skip geometry fields (already added above)
                if prop_name.lower() in ['geometry', 'geom', 'the_geom', 'shape']:
                    continue
                
                # Get Java type
                java_type = type_mapping.get(prop_type, 'java.lang.String')
                
                attributes.append({
                    "name": prop_name,
                    "minOccurs": 0,
                    "maxOccurs": 1,
                    "nillable": True,
                    "binding": java_type
                })
            
            LOGGER.info("Read schema: geometry type=%s, CRS=%s, bbox=%s, total attributes=%d (including geometry)", 
                       geometry_type, crs, bbox, len(attributes))
            return (attributes, crs, bbox)
    except Exception as exc:
        LOGGER.error("Failed to read shapefile schema from %s: %s", shapefile_path, exc, exc_info=True)
        return None


def _extract_shapefile_from_zip_for_schema(zip_path: Path) -> Optional[Path]:
    """
    Extract the shapefile from zip to a temp location so we can read its schema.
    Returns the path to the .shp file.
    """
    try:
        temp_dir = tempfile.mkdtemp()
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Extract all files
            zip_ref.extractall(temp_dir)
            
            # Find the .shp file
            shp_files = list(Path(temp_dir).rglob("*.shp"))
            if shp_files:
                return shp_files[0]
    except Exception as exc:
        LOGGER.error("Failed to extract shapefile from zip %s: %s", zip_path, exc, exc_info=True)
    return None


def _normalize_crs_to_epsg(crs_string: Optional[str]) -> Optional[str]:
    """
    Normalize CRS string to EPSG format (e.g., 'EPSG:4326').
    Returns None if CRS cannot be determined.
    """
    if not crs_string:
        return None
    
    try:
        # Try to parse the CRS and get EPSG code
        crs = CRS.from_user_input(crs_string)
        epsg_code = crs.to_epsg()
        if epsg_code:
            return f"EPSG:{epsg_code}"
        # If no EPSG code, try to get authority code
        if crs.to_authority():
            auth_name, code = crs.to_authority()
            if auth_name and code:
                return f"{auth_name.upper()}:{code}"
        # Fallback to string representation
        return crs_string
    except Exception as exc:
        LOGGER.warning("Failed to normalize CRS '%s': %s", crs_string, exc)
        # If it already looks like EPSG:XXXX, return as is
        if isinstance(crs_string, str) and crs_string.upper().startswith("EPSG:"):
            return crs_string.upper()
        return None


async def _persist_upload(file: UploadFile) -> Path:
    if not file.filename:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="File name is required")

    UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
    file_suffix = Path(file.filename).suffix
    unique_name = f"{uuid4().hex}{file_suffix}"
    destination = UPLOADS_DIR / unique_name

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
        LOGGER.error("Failed to persist upload %s: %s", file.filename, exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to persist file") from exc
    finally:
        await file.close()

    return destination


@router.post("/upload", response_model=UploadLogOut, status_code=status.HTTP_200_OK, summary="Upload Shapefile and other spatial data files and log the upload  in the database and publish to GeoServer (Used for frontend api calls)", description="Upload a spatial data file (e.g., shapefile) to the system. This endpoint accepts spatial data uploads, extracts metadata automatically, stores the file, logs the upload in the database, and optionally publishes shapefiles to GeoServer.")
async def upload_dataset(
    file: UploadFile = File(...),
    uploaded_by: str = Form(...),
    store_name: Optional[str] = Form(None),
    geoserver_layer: Optional[str] = Form(None),
    tags: Optional[List[str]] = Form(None),
    db: Session = Depends(get_db),
) -> UploadLogOut:
    """Accept spatial data uploads, extract metadata, and log the upload."""
    stored_path = await _persist_upload(file)

    try:
        metadata = derive_file_metadata(stored_path)
    except Exception as exc:
        LOGGER.error("Failed to derive metadata for %s: %s", stored_path, exc)
        stored_path.unlink(missing_ok=True)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Unable to read spatial metadata") from exc

    # Resolve store_name: use provided store_name, or metadata layer_name, or filename stem
    # This store_name will be used as the GeoServer datastore name
    resolved_store_name = store_name or metadata.get("layer_name") or Path(file.filename).stem
    
    # Extract the actual layer name from the zip file if it's a zip
    # This will be used as the feature type name in GeoServer
    actual_layer_name = None
    if stored_path.suffix.lower() == '.zip':
        actual_layer_name = _extract_shapefile_name_from_zip(stored_path)
        LOGGER.info("Extracted layer name from zip file: '%s' (file: %s)", actual_layer_name, stored_path)
    
    # Also check what metadata says (from fiona, which reads the shapefile's internal name)
    metadata_layer_name = metadata.get("layer_name")
    LOGGER.info("Layer name from metadata (fiona): '%s'", metadata_layer_name)
    
    # If we couldn't extract from zip, use metadata layer_name (which comes from the shapefile)
    # or fall back to resolved_store_name
    if not actual_layer_name:
        actual_layer_name = metadata_layer_name or resolved_store_name
        LOGGER.info("Using layer name: '%s' (from metadata or fallback)", actual_layer_name)
    
    # Log warning if zip extraction and metadata don't match
    if stored_path.suffix.lower() == '.zip' and actual_layer_name and metadata_layer_name:
        if actual_layer_name != metadata_layer_name:
            LOGGER.warning(
                "Shapefile name mismatch: zip filename='%s', fiona layer name='%s'. Using zip filename.",
                actual_layer_name, metadata_layer_name
            )
    
    data_type = metadata.get("data_type") or DataType.UNKNOWN
    file_format = metadata.get("file_format") or stored_path.suffix.lstrip(".")

    upload_log = UploadLogCreate(
        layer_name=resolved_store_name,  # Model field is still called layer_name for backward compatibility (stores the datastore name)
        file_format=file_format,
        data_type=data_type,
        crs=metadata.get("crs"),
        bbox=metadata.get("bbox"),
        source_path=os.fspath(stored_path),
        geoserver_layer=geoserver_layer,
        tags=tags,
        uploaded_by=uploaded_by,
    )

    created_log = UploadLogService.create(upload_log, db)
    await _publish_to_geoserver(created_log, db)
    return created_log


async def _publish_to_geoserver(upload_log: UploadLogOut, db: Session) -> None:
    """
    Publish a shapefile to GeoServer.
    
    IMPORTANT: Understanding store_name vs feature type name:
    - store_name (datastore): The name of the GeoServer datastore (e.g., "agar_soil")
      This is what the user provides via the store_name parameter or what's derived from metadata.
    - feature_type_name (layer): The name of the feature type/layer in GeoServer (e.g., "lyr_3_agar_soil")
      This is extracted from the shapefile name inside the zip file.
    
    When uploading a zip file:
    - The datastore name is set to store_name (e.g., "agar_soil")
    - GeoServer automatically creates a feature type with the name from the shapefile inside the zip (e.g., "lyr_3_agar_soil")
    - We extract the feature type name from the zip and use it as-is (we don't try to rename it)
    - The final GeoServer layer name is: workspace:feature_type_name (e.g., "metastring:lyr_3_agar_soil")
    
    The nativeName in the feature type configuration points to the actual shapefile name,
    while the name is the display name used in GeoServer.
    """
    LOGGER.debug("_publish_to_geoserver called for file_format: %s", upload_log.file_format)
    
    if not upload_log.file_format or upload_log.file_format.lower() != "shp":
        LOGGER.info("Skipping GeoServer publication for file format: %s", upload_log.file_format)
        return

    # Get the file path - it should be in the uploads directory
    source_path_str = upload_log.source_path
    LOGGER.info("Source path from upload_log: %s", source_path_str)
    
    file_path = Path(source_path_str)
    
    # Validate the file path
    if not file_path.exists():
        LOGGER.error("Stored upload file not found: %s", file_path)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Stored upload file is missing: {file_path}. Cannot publish to GeoServer.",
        )
    
    # Ensure it's a file, not a directory
    if file_path.is_dir():
        LOGGER.error("CRITICAL: Source path points to a directory, not a file: %s", file_path)
        LOGGER.error("This indicates the source_path in database is corrupted. Expected a .zip file in uploads directory.")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Source path is a directory, not a file: {file_path}. Database may have corrupted source_path.",
        )
    
    if not file_path.is_file():
        LOGGER.error("Source path is not a file: %s", file_path)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Source path is not a valid file: {file_path}",
        )
    
    # Ensure it's a zip file
    if file_path.suffix.lower() != '.zip':
        LOGGER.warning("File is not a .zip file: %s (suffix: %s)", file_path, file_path.suffix)
    
    LOGGER.info("File path validated: %s (exists: %s, is_file: %s, suffix: %s)", 
                file_path, file_path.exists(), file_path.is_file(), file_path.suffix)

    # The store_name from upload_log.layer_name is used as the store_name (datastore) in GeoServer.
    # However, GeoServer will create a feature type with the name from the shapefile inside the zip.
    # We should use the shapefile name as the feature type name, not try to rename it.
    store_name = upload_log.layer_name  # This is the store_name (datastore) provided by the user
    LOGGER.debug("Store name (datastore): %s", store_name)
    
    # Extract the actual layer/feature type name from the zip file
    # IMPORTANT: GeoServer uses the shapefile filename from INSIDE the zip, not fiona's layer name
    # So we should use the zip filename extraction, not fiona's metadata
    expected_feature_type_name = None
    if file_path.suffix.lower() == '.zip':
        # Extract from zip filename - this is what GeoServer uses (the .shp filename inside the zip)
        zip_filename_extracted = _extract_shapefile_name_from_zip(file_path)
        LOGGER.info("Shapefile name extracted from zip: '%s'", zip_filename_extracted)
        
        # Also get metadata for comparison/logging (fiona reads the shapefile's internal properties)
        metadata_layer_name = None
        try:
            metadata = derive_file_metadata(file_path)
            metadata_layer_name = metadata.get("layer_name")
            LOGGER.info("Metadata layer_name (from fiona/shapefile properties): '%s'", metadata_layer_name)
        except Exception as meta_exc:
            LOGGER.debug("Could not get metadata for comparison: %s", meta_exc)
        
        # GeoServer uses the shapefile FILENAME, not the internal layer name
        # So we use zip_filename_extracted (the .shp filename) as the expected name
        if zip_filename_extracted:
            expected_feature_type_name = zip_filename_extracted
            if metadata_layer_name and zip_filename_extracted != metadata_layer_name:
                LOGGER.info(
                    "Note: Zip filename is '%s' but fiona layer name is '%s'. "
                    "GeoServer will use the filename: '%s'",
                    zip_filename_extracted, metadata_layer_name, zip_filename_extracted
                )
            LOGGER.info("Expected feature type name (from zip filename): '%s'", expected_feature_type_name)
        elif metadata_layer_name:
            # Fallback to metadata if zip extraction failed
            expected_feature_type_name = metadata_layer_name
            LOGGER.warning("Using metadata layer_name '%s' as fallback (zip extraction failed)", expected_feature_type_name)
        else:
            LOGGER.error("Could not determine feature type name from zip file: %s", file_path)
            expected_feature_type_name = store_name
        
        LOGGER.info(
            "Publishing to GeoServer: workspace=%s, store_name (datastore)=%s, expected_feature_type_name=%s, file=%s",
            GEOSERVER_WORKSPACE, store_name, expected_feature_type_name, file_path
        )
    else:
        # For non-zip files, use store_name as feature type name
        expected_feature_type_name = store_name
        LOGGER.info("Publishing to GeoServer: workspace=%s, store_name (datastore)=%s, feature_type_name=%s, file=%s, file_format=%s", 
                    GEOSERVER_WORKSPACE, store_name, expected_feature_type_name, file_path, upload_log.file_format)
    
    # CRITICAL: Delete the entire datastore if it exists to ensure clean upload
    # This is necessary because GeoServer may keep old feature types when uploading to existing datastores
    LOGGER.debug("Checking if datastore '%s' exists in workspace '%s'...", store_name, GEOSERVER_WORKSPACE)
    try:
        datastore_response = geo_admin_service.get_datastore_details(
            workspace=GEOSERVER_WORKSPACE,
            datastore=store_name,
        )
        LOGGER.debug("Datastore check response: status=%s", datastore_response.status_code)
        if datastore_response.status_code == 200:
            LOGGER.debug("Datastore '%s' EXISTS. Deleting it to ensure clean upload...", store_name)
            try:
                delete_ds_response = geo_admin_service.delete_datastore(
                    workspace=GEOSERVER_WORKSPACE,
                    datastore=store_name,
                )
                LOGGER.debug("Delete datastore response: status=%s", delete_ds_response.status_code)
                if delete_ds_response.status_code in (200, 204):
                    LOGGER.debug("Successfully deleted datastore: %s", store_name)
                    # Also delete the physical files to ensure all old .shp files are removed
                    # This is critical because GeoServer may keep old .shp files even after datastore deletion
                    datastore_path = f"{geoserver_data_dir}/{GEOSERVER_WORKSPACE}/{store_name}"
                    try:
                        if os.path.exists(datastore_path):
                            LOGGER.debug("Old files exist in datastore directory. Attempting to delete...")
                            
                            # Try to delete files normally first
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
                                    LOGGER.debug("Permission denied deleting %s. Will try with sudo.", file_item_path)
                                except Exception as file_exc:
                                    failed_files.append(file_item_path)
                                    LOGGER.debug("Could not delete %s: %s", file_item_path, file_exc)
                            
                            # If some files failed due to permissions, try with sudo
                            if failed_files:
                                LOGGER.debug("Attempting to delete %d files with sudo...", len(failed_files))
                                for file_item_path in failed_files:
                                    try:
                                        # Try to delete with sudo
                                        result = run_sudo_command(['rm', '-rf', file_item_path], timeout=5)
                                        if result.returncode == 0:
                                            deleted_count += 1
                                            LOGGER.debug("Deleted with sudo: %s", file_item_path)
                                        else:
                                            error_msg = result.stderr[:200] if result.stderr else "Unknown error"
                                            LOGGER.debug("Sudo delete failed for %s (exit code %d): %s", file_item_path, result.returncode, error_msg)
                                    except (FileNotFoundError, subprocess.TimeoutExpired, Exception):
                                        LOGGER.debug("Could not delete %s with sudo", file_item_path)
                                
                                # If individual file deletion failed, try deleting the entire directory and recreating it
                                if deleted_count == 0 and len(failed_files) > 0:
                                    LOGGER.debug("Individual file deletion failed. Attempting to delete entire directory and recreate...")
                                    try:
                                        # Try to delete the entire directory with sudo
                                        result = run_sudo_command(['rm', '-rf', datastore_path], timeout=10)
                                        if result.returncode == 0:
                                            LOGGER.debug("Successfully deleted entire datastore directory: %s", datastore_path)
                                            # Recreate the directory
                                            try:
                                                os.makedirs(datastore_path, exist_ok=True)
                                                # Set permissions if possible
                                                try:
                                                    run_sudo_command(['chown', '-R', 'tomcat:tomcat', datastore_path], timeout=5)
                                                except Exception:
                                                    pass
                                                LOGGER.debug("Recreated datastore directory: %s", datastore_path)
                                            except Exception as recreate_exc:
                                                LOGGER.debug("Could not recreate directory: %s", recreate_exc)
                                        else:
                                            LOGGER.debug("Failed to delete directory with sudo: %s", result.stderr)
                                    except Exception as dir_delete_exc:
                                        LOGGER.debug("Exception deleting directory: %s", dir_delete_exc)
                            
                            if deleted_count > 0:
                                LOGGER.debug("Deleted %d files from physical datastore directory: %s", deleted_count, datastore_path)
                            else:
                                # Not critical - files will be overwritten during copy operation
                                LOGGER.debug("Could not delete old files from %s. Will attempt to overwrite during upload.", datastore_path)
                        else:
                            LOGGER.info("Physical directory does not exist: %s", datastore_path)
                    except Exception as dir_exc:
                        LOGGER.error("✗ EXCEPTION deleting physical files: %s", dir_exc, exc_info=True)
                    # Wait for deletion to complete
                    import asyncio
                    await asyncio.sleep(3)  # Increased wait time
                else:
                    LOGGER.error(
                        "✗ FAILED to delete datastore %s: status %s, response: %s",
                        store_name, delete_ds_response.status_code, 
                        delete_ds_response.text[:500] if delete_ds_response.text else "No text"
                    )
            except Exception as delete_ds_exc:
                LOGGER.error("✗ EXCEPTION deleting datastore %s: %s", store_name, delete_ds_exc, exc_info=True)
        elif datastore_response.status_code == 404:
            LOGGER.debug("Datastore '%s' does not exist. Checking for leftover files in directory...", store_name)
            # Even if datastore doesn't exist via API, old files might still be in the directory
            # Delete them to ensure clean upload
            datastore_path = f"{geoserver_data_dir}/{GEOSERVER_WORKSPACE}/{store_name}"
            if os.path.exists(datastore_path):
                LOGGER.debug("Found leftover directory with old files: %s", datastore_path)
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
                            LOGGER.debug("Permission denied deleting %s. Will try with sudo.", file_item_path)
                        except Exception as file_exc:
                            failed_files.append(file_item_path)
                            LOGGER.debug("Could not delete %s: %s", file_item_path, file_exc)
                    
                    # If some files failed due to permissions, try with sudo
                    if failed_files:
                        LOGGER.debug("Attempting to delete %d files with sudo...", len(failed_files))
                        import subprocess
                        for file_item_path in failed_files:
                            try:
                                result = run_sudo_command(['rm', '-rf', file_item_path], timeout=5)
                                if result.returncode == 0:
                                    deleted_count += 1
                                    LOGGER.debug("Deleted with sudo: %s", file_item_path)
                                else:
                                    LOGGER.debug("Sudo delete failed for %s", file_item_path)
                            except (FileNotFoundError, subprocess.TimeoutExpired, Exception):
                                LOGGER.debug("Could not delete %s with sudo", file_item_path)
                        
                        # If individual file deletion failed, try deleting the entire directory and recreating it
                        if deleted_count == 0 and len(failed_files) > 0:
                            LOGGER.debug("Individual file deletion failed. Attempting to delete entire directory and recreate...")
                            try:
                                # Try to delete the entire directory with sudo
                                result = run_sudo_command(['rm', '-rf', datastore_path], timeout=10)
                                if result.returncode == 0:
                                    LOGGER.debug("Successfully deleted entire datastore directory: %s", datastore_path)
                                    # Recreate the directory
                                    try:
                                        os.makedirs(datastore_path, exist_ok=True)
                                        # Set permissions if possible
                                        try:
                                            run_sudo_command(['chown', '-R', 'tomcat:tomcat', datastore_path], timeout=5)
                                        except Exception:
                                            pass
                                        LOGGER.debug("Recreated datastore directory: %s", datastore_path)
                                    except Exception as recreate_exc:
                                        LOGGER.debug("Could not recreate directory: %s", recreate_exc)
                                else:
                                    LOGGER.debug("Failed to delete directory with sudo: %s", result.stderr)
                            except Exception as dir_delete_exc:
                                LOGGER.debug("Exception deleting directory: %s", dir_delete_exc)
                    
                    if deleted_count > 0:
                        LOGGER.debug("Cleaned up %d old files from directory", deleted_count)
                    else:
                        # Not critical - files will be overwritten during copy operation
                        LOGGER.debug("Could not delete old files from %s. Will attempt to overwrite during upload.", datastore_path)
                except Exception as cleanup_exc:
                    LOGGER.error("Exception cleaning up directory: %s", cleanup_exc, exc_info=True)
            else:
                LOGGER.debug("No leftover directory found. Clean upload will proceed.")
        else:
            LOGGER.error("✗ Unexpected status when checking datastore: %s, response: %s", 
                         datastore_response.status_code, 
                         datastore_response.text[:500] if datastore_response.text else "No text")
    except Exception as check_exc:
        LOGGER.error("✗ EXCEPTION checking datastore existence: %s", check_exc, exc_info=True)
        # Continue anyway - the upload will create the datastore if it doesn't exist
    # Upload directly using the persisted file path (works for both .zip and .shp files)
    # Ensure file_path is a Path object (defensive check)
    if not isinstance(file_path, Path):
        LOGGER.debug("file_path is not a Path object, converting: %s (type: %s)", file_path, type(file_path))
        file_path = Path(file_path)
    
    created_feature_type_from_response = None
    LOGGER.debug("Uploading to GeoServer: file=%s", file_path)
    
    # CRITICAL: Verify zip file contents before upload
    if file_path.suffix.lower() == '.zip':
        LOGGER.info("=" * 80)
        LOGGER.info("VERIFYING ZIP FILE CONTENTS BEFORE UPLOAD:")
        LOGGER.info("=" * 80)
        try:
            import zipfile
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                all_files = zip_ref.namelist()
                shp_files = [f for f in all_files if f.lower().endswith('.shp')]
                LOGGER.info("Total files in zip: %d", len(all_files))
                LOGGER.info("Shapefile (.shp) files found: %d", len(shp_files))
                for idx, shp_file in enumerate(shp_files, 1):
                    shp_name = Path(shp_file).stem
                    LOGGER.info("  %d. Shapefile: %s (path in zip: %s)", idx, shp_name, shp_file)
                    # List related files for this shapefile
                    base_name = Path(shp_file).stem
                    related_files = [f for f in all_files if Path(f).stem == base_name]
                    LOGGER.info("     Related files: %s", ', '.join([Path(f).name for f in related_files]))
                
                if shp_files:
                    first_shp = shp_files[0]
                    first_shp_name = Path(first_shp).stem
                    LOGGER.debug("=" * 80)
                    LOGGER.info("EXPECTED: GeoServer should create feature type: '%s'", first_shp_name)
                    LOGGER.info("If GeoServer creates a different name, it may be reading old files from the datastore directory.")
                    LOGGER.debug("=" * 80)
                else:
                    LOGGER.error("NO SHAPEFILE FOUND IN ZIP FILE! This will cause upload to fail.")
        except Exception as zip_check_exc:
            LOGGER.error("Failed to verify zip contents: %s", zip_check_exc, exc_info=True)
    
    try:
        file_path_str = str(file_path.resolve())
        LOGGER.debug("Uploading to GeoServer: workspace=%s, store=%s, file=%s", GEOSERVER_WORKSPACE, store_name, file_path_str)
        response = geo_dao.upload_shapefile(
            workspace=GEOSERVER_WORKSPACE,
            store_name=store_name,
            file_path=file_path_str,
        )
        LOGGER.info("=" * 80)
        LOGGER.info("GEOSERVER UPLOAD RESPONSE:")
        LOGGER.info("  Status Code: %s", response.status_code)
        LOGGER.info("  Headers: %s", dict(response.headers) if hasattr(response, 'headers') else None)
        LOGGER.info("  Response Text (first 1000 chars): %s", response.text[:1000] if response.text else "No response text")
        LOGGER.info("=" * 80)
        
        # Check if response contains information about the created feature type
        # GeoServer may return a Location header or response body with feature type info
        if hasattr(response, 'headers') and 'Location' in response.headers:
            location = response.headers['Location']
            # Location might be like: /geoserver/rest/workspaces/metastring/datastores/agar_soil/featuretypes/lyr_3_agar_soil
            if '/featuretypes/' in location:
                created_feature_type_from_response = location.split('/featuretypes/')[-1].split('.')[0]
                LOGGER.info("Extracted feature type name from Location header: %s", created_feature_type_from_response)
        
        # Also try to parse response body for feature type info (if it's JSON)
        if not created_feature_type_from_response and response.text:
            try:
                response_data = json.loads(response.text)
                # Check for featureType name in response
                if isinstance(response_data, dict):
                    feature_type = response_data.get("featureType", {})
                    if isinstance(feature_type, dict):
                        ft_name = feature_type.get("name")
                        if ft_name:
                            created_feature_type_from_response = ft_name
                            LOGGER.info("Extracted feature type name from response body: %s", created_feature_type_from_response)
            except (json.JSONDecodeError, AttributeError):
                # Response is not JSON or doesn't contain feature type info - that's okay
                pass
    except ValueError as exc:
        LOGGER.error("GeoServer rejected upload for layer %s: %s", store_name, exc)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except Exception as exc:
        LOGGER.error("Unexpected error uploading layer %s to GeoServer: %s", store_name, exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Unexpected error occurred while publishing to GeoServer.",
        ) from exc

    if response.status_code not in (200, 201, 202):
        LOGGER.error(
            "GeoServer upload failed for layer %s with status %s: %s",
            store_name,
            response.status_code,
            response.text,
        )
        raise HTTPException(status_code=response.status_code, detail=response.text)
    
    LOGGER.info("GeoServer upload succeeded for layer %s (status %s)", store_name, response.status_code)
    
    # Give GeoServer a moment to process the upload (especially for async 202 responses)
    if response.status_code == 202:
        LOGGER.info("GeoServer returned 202 (Accepted), upload is being processed asynchronously")
        import asyncio
        await asyncio.sleep(5)  # Wait 5 seconds for async processing
    elif response.status_code in (200, 201):
        # For successful uploads, wait a moment for GeoServer to process
        import asyncio
        await asyncio.sleep(3)  # Wait 3 seconds for processing
    
    # CRITICAL: Reload the datastore to force GeoServer to re-read the files
    # This ensures GeoServer picks up the newly uploaded shapefile files
    LOGGER.debug("Reloading datastore '%s' to force GeoServer to re-read shapefile files...", store_name)
    try:
        reload_response = geo_admin_service.reload_datastore(
            workspace=GEOSERVER_WORKSPACE,
            datastore=store_name,
        )
        if reload_response.status_code in (200, 201, 202):
            LOGGER.debug("Datastore reloaded successfully (status %s)", reload_response.status_code)
            # Wait a bit for reload to complete
            await asyncio.sleep(2)
        else:
            LOGGER.debug("Datastore reload returned status %s: %s", 
                         reload_response.status_code,
                         reload_response.text[:200] if reload_response.text else "No response")
    except Exception as reload_exc:
        LOGGER.debug("Exception reloading datastore: %s", reload_exc)
    
    # CRITICAL: After uploading the shapefile, check if feature type was auto-created
    # The upload should have used configure=all to trigger auto-creation
    # If not found, try reloading the datastore to trigger auto-discovery
    if expected_feature_type_name:
        LOGGER.debug("Checking if feature type '%s' was auto-created...", expected_feature_type_name)
        try:
            # Check if feature type already exists
            ft_check_response = geo_admin_service.get_feature_type_details(
                workspace=GEOSERVER_WORKSPACE,
                datastore=store_name,
                feature_type=expected_feature_type_name,
            )
            
            if ft_check_response.status_code == 200:
                LOGGER.info("✓ Feature type '%s' already exists in datastore '%s'", expected_feature_type_name, store_name)
                # Even if it exists, trigger bounding box recalculation to ensure they're correct
                LOGGER.info("Triggering bounding box recalculation for existing feature type '%s'...", expected_feature_type_name)
                try:
                    ft_config = ft_check_response.json()
                    if isinstance(ft_config, dict) and "featureType" in ft_config:
                        native_name = ft_config["featureType"].get("nativeName", expected_feature_type_name)
                        update_config = {
                            "featureType": {
                                "name": expected_feature_type_name,
                                "nativeName": native_name,
                            }
                        }
                        # Use recalculate to fix any invalid bounding boxes
                        recalc_response = geo_admin_service.update_feature_type(
                            workspace=GEOSERVER_WORKSPACE,
                            datastore=store_name,
                            feature_type=expected_feature_type_name,
                            config=update_config,
                            recalculate=True,
                        )
                        if recalc_response.status_code in (200, 201):
                            LOGGER.info("✓ Successfully recalculated bounding boxes for existing feature type")
                except Exception as recalc_exc:
                    LOGGER.debug("Could not recalculate bounding boxes for existing feature type: %s", recalc_exc)
            else:
                # Feature type doesn't exist - explicitly create it with schema from shapefile
                # The files are already uploaded, we need to read the schema and create the feature type
                LOGGER.debug("Feature type '%s' not found (status: %s). Creating it explicitly with schema from shapefile...", 
                             expected_feature_type_name, ft_check_response.status_code)
                
                # Read the shapefile schema from the uploaded zip file
                attributes = None
                shapefile_crs = None
                shapefile_bbox = None
                try:
                    # Extract shapefile from zip to read schema
                    temp_shp_path = _extract_shapefile_from_zip_for_schema(file_path)
                    if temp_shp_path:
                        schema_result = _get_shapefile_schema(temp_shp_path)
                        if schema_result:
                            attributes, shapefile_crs, shapefile_bbox = schema_result
                            if attributes:
                                LOGGER.info("✓ Read %d attributes from shapefile schema (CRS: %s, BBox: %s)", 
                                          len(attributes), shapefile_crs or "unknown", shapefile_bbox)
                            else:
                                LOGGER.warning("⚠ Could not read attributes from shapefile, will try without them")
                        else:
                            LOGGER.warning("⚠ Could not read schema from shapefile")
                        # Clean up temp directory
                        temp_dir = temp_shp_path.parent
                        shutil.rmtree(temp_dir, ignore_errors=True)
                    else:
                        LOGGER.warning("⚠ Could not extract shapefile from zip to read schema")
                except Exception as schema_exc:
                    LOGGER.warning("⚠ Failed to read shapefile schema: %s. Will try creating feature type without attributes.", schema_exc)
                
                # Use CRS from shapefile if available, otherwise from upload_log
                srs = shapefile_crs
                if not srs and upload_log.crs:
                    srs = _normalize_crs_to_epsg(upload_log.crs)
                
                # Use bounding box from shapefile if available, otherwise from upload_log
                native_bbox = shapefile_bbox
                if not native_bbox and upload_log.bbox:
                    # Convert upload_log bbox format to native_bbox format
                    native_bbox = {
                        "minx": upload_log.bbox.get("min_x", 0),
                        "miny": upload_log.bbox.get("min_y", 0),
                        "maxx": upload_log.bbox.get("max_x", 0),
                        "maxy": upload_log.bbox.get("max_y", 0)
                    }
                
                # CRITICAL: Delete existing feature type first to ensure clean state
                # This prevents issues with old configurations that might point to wrong files
                LOGGER.info("Deleting any existing feature type '%s' to ensure clean creation...", expected_feature_type_name)
                try:
                    delete_response = geo_admin_service.delete_feature_type(
                        workspace=GEOSERVER_WORKSPACE,
                        datastore=store_name,
                        feature_type=expected_feature_type_name,
                    )
                    if delete_response.status_code in (200, 404):  # 404 means it didn't exist, which is fine
                        LOGGER.info("✓ Deleted existing feature type (or it didn't exist)")
                        # Wait a bit for deletion to complete
                        import asyncio
                        await asyncio.sleep(2)
                    else:
                        LOGGER.warning("⚠ Could not delete existing feature type: status %s", delete_response.status_code)
                except Exception as delete_exc:
                    LOGGER.warning("⚠ Exception while deleting existing feature type: %s", delete_exc)
                
                # CRITICAL: For shapefiles, we should NOT specify attributes explicitly
                # GeoServer needs to auto-discover them from the actual shapefile to properly read the data
                # However, we got "no attributes specified" error before, so we'll try with minimal config
                # and let GeoServer discover the rest
                LOGGER.info("Creating feature type - GeoServer will auto-discover attributes from shapefile data")
                
                # Create the feature type explicitly with minimal config
                # Let GeoServer discover attributes from the actual shapefile files
                try:
                    # Try creating WITHOUT attributes first - let GeoServer auto-discover
                    # This is the correct way for shapefiles - it forces GeoServer to read the actual data
                    create_ft_response = geo_admin_service.create_feature_type_from_shapefile(
                        workspace=GEOSERVER_WORKSPACE,
                        datastore=store_name,
                        shapefile_name=expected_feature_type_name,  # nativeName (the actual shapefile name)
                        feature_type_name=expected_feature_type_name,  # display name
                        enabled=True,
                        attributes=None,  # Don't specify attributes - let GeoServer auto-discover from shapefile
                        srs=srs,  # Pass the SRS if available
                        native_bbox=native_bbox  # Pass the bounding box explicitly
                    )
                    
                    # If that fails with "no attributes", try with attributes as fallback
                    # BUT then immediately remove them and recalculate to force reading from shapefile
                    if create_ft_response.status_code == 400 and "attributes" in (create_ft_response.text or "").lower():
                        LOGGER.debug("GeoServer requires attributes. Creating with explicit attributes, then will remove them to force data read...")
                        create_ft_response = geo_admin_service.create_feature_type_from_shapefile(
                            workspace=GEOSERVER_WORKSPACE,
                            datastore=store_name,
                            shapefile_name=expected_feature_type_name,
                            feature_type_name=expected_feature_type_name,
                            enabled=True,
                            attributes=attributes,  # Fallback: use attributes we read
                            srs=srs,
                            native_bbox=native_bbox
                        )
                        
                        # If creation succeeded with attributes, we need to remove them and recalculate
                        # to force GeoServer to read from the actual shapefile
                        if create_ft_response.status_code in (200, 201):
                            LOGGER.info("Feature type created with attributes. Now removing attributes to force GeoServer to read from shapefile...")
                            await asyncio.sleep(2)
                            
                            # Get current config and remove attributes
                            ft_details = geo_admin_service.get_feature_type_details(
                                workspace=GEOSERVER_WORKSPACE,
                                datastore=store_name,
                                feature_type=expected_feature_type_name,
                            )
                            
                            if ft_details.status_code == 200:
                                ft_config = ft_details.json()
                                if isinstance(ft_config, dict) and "featureType" in ft_config:
                                    native_name = ft_config["featureType"].get("nativeName", expected_feature_type_name)
                                    
                                    # Remove attributes from config - this forces GeoServer to re-read from shapefile
                                    update_config_no_attrs = {
                                        "featureType": {
                                            "name": expected_feature_type_name,
                                            "nativeName": native_name,
                                        }
                                    }
                                    
                                    if srs:
                                        update_config_no_attrs["featureType"]["srs"] = srs
                                        update_config_no_attrs["featureType"]["nativeSRS"] = srs
                                        update_config_no_attrs["featureType"]["projectionPolicy"] = "FORCE_DECLARED"
                                    
                                    # Update without attributes and recalculate - this forces reading from shapefile
                                    remove_attrs_response = geo_admin_service.update_feature_type(
                                        workspace=GEOSERVER_WORKSPACE,
                                        datastore=store_name,
                                        feature_type=expected_feature_type_name,
                                        config=update_config_no_attrs,
                                        recalculate=True,  # This will force GeoServer to read from shapefile
                                    )
                                    
                                    if remove_attrs_response.status_code in (200, 201):
                                        LOGGER.info("✓ Removed attributes and triggered recalculation - GeoServer will now read from shapefile")
                                        # Wait for recalculation
                                        await asyncio.sleep(3)
                                    else:
                                        LOGGER.warning("⚠ Failed to remove attributes: %s", remove_attrs_response.status_code)
                    
                    # Check if feature type was created successfully (either way)
                    if create_ft_response.status_code in (200, 201):
                        LOGGER.info("✓ Successfully created feature type '%s'", expected_feature_type_name)
                        # Wait longer for GeoServer to fully process the feature type creation
                        import asyncio
                        await asyncio.sleep(5)  # Increased wait time to ensure GeoServer processes everything
                        
                        # CRITICAL: Trigger bounding box recalculation immediately after creation
                        # This fixes the "illegal bbox" error when previewing the layer
                        LOGGER.info("Triggering bounding box recalculation for feature type '%s'...", expected_feature_type_name)
                        try:
                            # Get current feature type config
                            ft_details = geo_admin_service.get_feature_type_details(
                                workspace=GEOSERVER_WORKSPACE,
                                datastore=store_name,
                                feature_type=expected_feature_type_name,
                            )
                            
                            if ft_details.status_code == 200:
                                ft_config = ft_details.json()
                                if isinstance(ft_config, dict) and "featureType" in ft_config:
                                    native_name = ft_config["featureType"].get("nativeName", expected_feature_type_name)
                                    
                                    # CRITICAL: Force GeoServer to read from the actual shapefile data
                                    # Remove attributes from config to force GeoServer to re-read the shapefile
                                    # This ensures it reads from the correct file (in subdirectory) not the empty one
                                    update_config = {
                                        "featureType": {
                                            "name": expected_feature_type_name,
                                            "nativeName": native_name,  # Ensure nativeName points to correct shapefile
                                        }
                                    }
                                    
                                    # Add SRS if we have it
                                    if srs:
                                        update_config["featureType"]["srs"] = srs
                                        update_config["featureType"]["nativeSRS"] = srs
                                        update_config["featureType"]["projectionPolicy"] = "FORCE_DECLARED"
                                    
                                    # CRITICAL: Do NOT include attributes or bounding boxes
                                    # This forces GeoServer to re-read everything from the actual shapefile data
                                    # The recalculate parameter will make GeoServer read the shapefile and calculate bounding boxes
                                    # This should make it read from the correct file in the subdirectory
                                    recalc_response = geo_admin_service.update_feature_type(
                                        workspace=GEOSERVER_WORKSPACE,
                                        datastore=store_name,
                                        feature_type=expected_feature_type_name,
                                        config=update_config,
                                        recalculate=True,  # This triggers full recalculation from actual data
                                    )
                                    
                                    if recalc_response.status_code in (200, 201):
                                        LOGGER.info("✓ Successfully triggered bounding box recalculation for feature type '%s'", 
                                                  expected_feature_type_name)
                                        # Wait for recalculation to complete
                                        await asyncio.sleep(3)
                                        
                                        # Verify that features are now readable
                                        try:
                                            wfs_test = geo_dao.query_features(
                                                layer=f"{GEOSERVER_WORKSPACE}:{expected_feature_type_name}",
                                                max_features=1
                                            )
                                            if wfs_test.status_code == 200:
                                                # Check if we got any features
                                                import xml.etree.ElementTree as ET
                                                try:
                                                    root = ET.fromstring(wfs_test.text)
                                                    # Look for numberOfFeatures attribute
                                                    num_features = root.get("numberOfFeatures", "0")
                                                    if num_features and int(num_features) > 0:
                                                        LOGGER.info("✓ Verified: Feature type is readable with %s features", num_features)
                                                    else:
                                                        LOGGER.error("✗ CRITICAL: Feature type created but numberOfFeatures=0. "
                                                                   "GeoServer is not reading features from the shapefile. "
                                                                   "This may be due to old files in the datastore directory.")
                                                        # Try one more recalculation with a longer wait
                                                        LOGGER.info("Attempting additional recalculation to force GeoServer to read correct file...")
                                                        await asyncio.sleep(2)
                                                        second_recalc = geo_admin_service.update_feature_type(
                                                            workspace=GEOSERVER_WORKSPACE,
                                                            datastore=store_name,
                                                            feature_type=expected_feature_type_name,
                                                            config=update_config,
                                                            recalculate=True,
                                                        )
                                                        if second_recalc.status_code in (200, 201):
                                                            LOGGER.info("✓ Performed second recalculation")
                                                            await asyncio.sleep(3)
                                                except Exception:
                                                    # If XML parsing fails, check text content
                                                    if "numberOfFeatures=\"0\"" in wfs_test.text:
                                                        LOGGER.error("✗ CRITICAL: Feature type created but numberOfFeatures=0. "
                                                                   "GeoServer may be reading wrong shapefile (empty one in root directory).")
                                        except Exception as verify_exc:
                                            LOGGER.debug("Could not verify features: %s", verify_exc)
                                    else:
                                        LOGGER.warning("⚠ Bounding box recalculation returned status %s: %s", 
                                                     recalc_response.status_code,
                                                     recalc_response.text[:200] if recalc_response.text else "No response")
                        except Exception as recalc_exc:
                            LOGGER.warning("⚠ Failed to trigger bounding box recalculation: %s", recalc_exc)
                    else:
                        LOGGER.error("✗ Failed to create feature type '%s': status %s, response: %s", 
                                   expected_feature_type_name, create_ft_response.status_code,
                                   create_ft_response.text[:500] if create_ft_response.text else "No response")
                except Exception as create_exc:
                    LOGGER.error("✗ Exception while creating feature type: %s", create_exc, exc_info=True)
        except Exception as reload_exc:
            LOGGER.error("✗ Exception while checking/reloading datastore: %s", reload_exc, exc_info=True)
    
    # After upload, check what feature types exist now
    LOGGER.info("Checking feature types in datastore '%s' after upload...", store_name)
    try:
        ft_list_after = geo_admin_service.list_datastore_tables(
            workspace=GEOSERVER_WORKSPACE,
            datastore=store_name,
        )
        if ft_list_after.status_code == 200:
            ft_list_data = ft_list_after.json()
            if isinstance(ft_list_data, dict):
                feature_types_obj = ft_list_data.get("featureTypes", {})
                if isinstance(feature_types_obj, dict):
                    feature_types_after = feature_types_obj.get("featureType", [])
                    if isinstance(feature_types_after, dict):
                        feature_types_after = [feature_types_after]
                    LOGGER.info("Feature types after upload: %d", len(feature_types_after) if isinstance(feature_types_after, list) else 1)
                    for ft in (feature_types_after if isinstance(feature_types_after, list) else [feature_types_after]):
                        if isinstance(ft, dict):
                            ft_name = ft.get("name", "unknown")
                            LOGGER.info("  - %s", ft_name)
    except Exception as check_after_exc:
        LOGGER.debug("Could not check feature types after upload: %s", check_after_exc)
    
    # Determine the actual feature type name that was created
    # Priority: 1) From response Location header, 2) Expected name from zip, 3) Store name
    actual_feature_type_name = created_feature_type_from_response or expected_feature_type_name or store_name
    
    # If we got the feature type name from the response, use it directly
    if created_feature_type_from_response:
        LOGGER.info(
            "Using feature type name from GeoServer response: %s (store_name/datastore: %s)",
            actual_feature_type_name, store_name
        )
    # If we have an expected feature type name from the zip, try to verify it exists
    # If it doesn't exist, GeoServer may have auto-renamed it (e.g., due to duplicates)
    elif expected_feature_type_name:
        try:
            # Try to get the feature type by the expected name
            ft_details_response = geo_admin_service.get_feature_type_details(
                workspace=GEOSERVER_WORKSPACE,
                datastore=store_name,
                feature_type=expected_feature_type_name,
            )
            if ft_details_response.status_code == 200:
                # Feature type exists with the expected name - use it
                actual_feature_type_name = expected_feature_type_name
                LOGGER.info(
                    "Found feature type with expected name: %s (store_name/datastore: %s)",
                    actual_feature_type_name, store_name
                )
            else:
                # Feature type doesn't exist with expected name - GeoServer may have renamed it
                # List all feature types to find what was actually created
                LOGGER.warning(
                    "Feature type '%s' not found in datastore '%s'. Listing all feature types to find what was created.",
                    expected_feature_type_name, store_name
                )
                ft_list_response = geo_admin_service.list_datastore_tables(
                    workspace=GEOSERVER_WORKSPACE,
                    datastore=store_name,
                )
                if ft_list_response.status_code == 200:
                    ft_list_data = ft_list_response.json()
                    if isinstance(ft_list_data, dict):
                        feature_types_obj = ft_list_data.get("featureTypes", {})
                        if isinstance(feature_types_obj, dict):
                            feature_types = feature_types_obj.get("featureType", [])
                            if feature_types:
                                # Convert to list if it's a single dict
                                if isinstance(feature_types, dict):
                                    feature_types = [feature_types]
                                
                                if isinstance(feature_types, list) and len(feature_types) > 0:
                                    # Try to find a feature type that matches the expected name
                                    # First, try exact match by name
                                    matching_ft = None
                                    for ft in feature_types:
                                        if isinstance(ft, dict):
                                            ft_name = ft.get("name", "")
                                            if ft_name == expected_feature_type_name:
                                                matching_ft = ft
                                                LOGGER.info("Found exact match by name: %s", ft_name)
                                                break
                                    
                                    # If no exact match, try to match by nativeName (the actual shapefile name)
                                    # This is CRITICAL - nativeName is what GeoServer uses from the zip file
                                    if not matching_ft:
                                        LOGGER.info("No exact name match. Checking nativeName for expected: '%s'", expected_feature_type_name)
                                        LOGGER.info("Checking all %d feature types in datastore for matching nativeName...", len(feature_types))
                                        for ft in feature_types:
                                            if isinstance(ft, dict):
                                                # Get full details to check nativeName
                                                ft_name = ft.get("name", "")
                                                try:
                                                    ft_details = geo_admin_service.get_feature_type_details(
                                                        workspace=GEOSERVER_WORKSPACE,
                                                        datastore=store_name,
                                                        feature_type=ft_name,
                                                    )
                                                    if ft_details.status_code == 200:
                                                        ft_data = ft_details.json()
                                                        if isinstance(ft_data, dict) and "featureType" in ft_data:
                                                            native_name = ft_data["featureType"].get("nativeName", "")
                                                            LOGGER.info("  Feature type '%s' -> nativeName: '%s'", ft_name, native_name)
                                                            # Match by nativeName (exact match)
                                                            if native_name == expected_feature_type_name:
                                                                matching_ft = ft
                                                                LOGGER.info("✓ FOUND MATCH by nativeName: '%s' (feature type name: '%s')", native_name, ft_name)
                                                                break
                                                except Exception as detail_exc:
                                                    LOGGER.debug("Could not get details for feature type %s: %s", ft_name, detail_exc)
                                        
                                        if not matching_ft:
                                            LOGGER.error(
                                                "CRITICAL: No feature type found with nativeName matching expected '%s'. "
                                                "This means GeoServer did not create the feature type we expected, "
                                                "or it was created with a different name. Available feature types:",
                                                expected_feature_type_name
                                            )
                                            for ft in feature_types:
                                                if isinstance(ft, dict):
                                                    ft_name = ft.get("name", "")
                                                    try:
                                                        ft_details = geo_admin_service.get_feature_type_details(
                                                            workspace=GEOSERVER_WORKSPACE,
                                                            datastore=store_name,
                                                            feature_type=ft_name,
                                                        )
                                                        if ft_details.status_code == 200:
                                                            ft_data = ft_details.json()
                                                            if isinstance(ft_data, dict) and "featureType" in ft_data:
                                                                native_name = ft_data["featureType"].get("nativeName", "")
                                                                LOGGER.error("  - Feature type: '%s', nativeName: '%s'", ft_name, native_name)
                                                    except Exception:
                                                        pass
                                    
                                    # If still no match, use the last one in the list (most recently created)
                                    if not matching_ft and len(feature_types) > 0:
                                        matching_ft = feature_types[-1]
                                        LOGGER.warning("No match found by name or nativeName. Using last feature type in list.")
                                    
                                    if matching_ft and isinstance(matching_ft, dict):
                                        actual_feature_type_name = matching_ft.get("name", expected_feature_type_name)
                                        
                                        # Get nativeName for logging
                                        native_name_info = "unknown"
                                        try:
                                            ft_details = geo_admin_service.get_feature_type_details(
                                                workspace=GEOSERVER_WORKSPACE,
                                                datastore=store_name,
                                                feature_type=actual_feature_type_name,
                                            )
                                            if ft_details.status_code == 200:
                                                ft_data = ft_details.json()
                                                if isinstance(ft_data, dict) and "featureType" in ft_data:
                                                    native_name_info = ft_data["featureType"].get("nativeName", "unknown")
                                        except Exception:
                                            pass
                                        
                                        if actual_feature_type_name != expected_feature_type_name:
                                            LOGGER.warning(
                                                "Using feature type '%s' (nativeName: '%s') but expected '%s' from zip. "
                                                "GeoServer may have auto-renamed it due to duplicates, or the zip contains a different shapefile.",
                                                actual_feature_type_name, native_name_info, expected_feature_type_name
                                            )
                                        else:
                                            LOGGER.info(
                                                "Using feature type '%s' (nativeName: '%s') - matches expected name from zip.",
                                                actual_feature_type_name, native_name_info
                                            )
                                    else:
                                        actual_feature_type_name = expected_feature_type_name
                                        LOGGER.warning(
                                            "Could not determine feature type name. Using expected name: %s",
                                            actual_feature_type_name
                                        )
        except Exception as ft_exc:
            LOGGER.warning(
                "Could not verify feature type name: %s. Using expected name '%s' (from zip) or store_name '%s'.",
                ft_exc, expected_feature_type_name, store_name
            )
            actual_feature_type_name = expected_feature_type_name or store_name
    else:
        # No expected feature type name - list all and use the last one (most recent)
        try:
            ft_list_response = geo_admin_service.list_datastore_tables(
                workspace=GEOSERVER_WORKSPACE,
                datastore=store_name,
            )
            if ft_list_response.status_code == 200:
                ft_list_data = ft_list_response.json()
                if isinstance(ft_list_data, dict):
                    feature_types_obj = ft_list_data.get("featureTypes", {})
                    if isinstance(feature_types_obj, dict):
                        feature_types = feature_types_obj.get("featureType", [])
                        if feature_types:
                            if isinstance(feature_types, dict):
                                feature_types = [feature_types]
                            if isinstance(feature_types, list) and len(feature_types) > 0:
                                # Use the last feature type (most recently created)
                                last_ft = feature_types[-1]
                                if isinstance(last_ft, dict):
                                    actual_feature_type_name = last_ft.get("name", store_name)
                                    LOGGER.info(
                                        "Using most recent feature type: %s (store_name/datastore: %s)",
                                        actual_feature_type_name, store_name
                                    )
        except Exception as list_exc:
            LOGGER.warning(
                "Could not list feature types: %s. Using store_name '%s' as feature type name.",
                list_exc, store_name
            )
            actual_feature_type_name = store_name

    # Update feature type with correct SRS from metadata if available
    # Use the actual feature type name (from GeoServer, which should match the shapefile name from zip)
    try:
        normalized_crs = _normalize_crs_to_epsg(upload_log.crs)
        if normalized_crs:
            # Try to update SRS using the actual feature type name
            try:
                # Get the current feature type config to preserve nativeName
                ft_details_response = geo_admin_service.get_feature_type_details(
                    workspace=GEOSERVER_WORKSPACE,
                    datastore=store_name,
                    feature_type=actual_feature_type_name,
                )
                
                if ft_details_response.status_code == 200:
                    ft_config = ft_details_response.json()
                    if isinstance(ft_config, dict) and "featureType" in ft_config:
                        # Preserve the nativeName (points to the actual shapefile name in the datastore)
                        # nativeName is the internal name GeoServer uses to reference the data source
                        # name is the display name used in GeoServer UI and WMS/WFS requests
                        # For shapefiles, nativeName typically matches the shapefile name from the zip
                        native_name = ft_config["featureType"].get("nativeName", actual_feature_type_name)
                        
                        # Update SRS while preserving other settings and bounding boxes
                        # Get existing bounding boxes to preserve them
                        existing_native_bbox = ft_config["featureType"].get("nativeBoundingBox", {})
                        existing_latlon_bbox = ft_config["featureType"].get("latLonBoundingBox", {})
                        
                        update_config = {
                            "featureType": {
                                "name": actual_feature_type_name,
                                "nativeName": native_name,  # Preserve nativeName
                                "srs": normalized_crs,
                                "nativeSRS": normalized_crs,
                                "projectionPolicy": "FORCE_DECLARED",
                            }
                        }
                        
                        # Preserve bounding boxes if they exist and are valid
                        if existing_native_bbox and all(k in existing_native_bbox for k in ["minx", "miny", "maxx", "maxy"]):
                            # Only preserve if they're valid (minx < maxx, miny < maxy)
                            if (existing_native_bbox["minx"] < existing_native_bbox["maxx"] and 
                                existing_native_bbox["miny"] < existing_native_bbox["maxy"]):
                                update_config["featureType"]["nativeBoundingBox"] = existing_native_bbox
                                # Update CRS in native bbox if needed
                                if "crs" in existing_native_bbox:
                                    update_config["featureType"]["nativeBoundingBox"]["crs"] = normalized_crs
                        
                        if existing_latlon_bbox and all(k in existing_latlon_bbox for k in ["minx", "miny", "maxx", "maxy"]):
                            if (existing_latlon_bbox["minx"] < existing_latlon_bbox["maxx"] and 
                                existing_latlon_bbox["miny"] < existing_latlon_bbox["maxy"]):
                                update_config["featureType"]["latLonBoundingBox"] = existing_latlon_bbox
                        
                        # Use recalculate=True to ensure bounding boxes are correct
                        # This will recalculate if bounding boxes are invalid or missing
                        update_response = geo_admin_service.update_feature_type(
                            workspace=GEOSERVER_WORKSPACE,
                            datastore=store_name,
                            feature_type=actual_feature_type_name,
                            config=update_config,
                            recalculate=True,
                        )
                        if update_response.status_code in (200, 201):
                            LOGGER.info("Successfully updated SRS for feature type %s (nativeName: %s) to %s", 
                                      actual_feature_type_name, native_name, normalized_crs)
                        else:
                            LOGGER.debug("SRS update returned status %s for layer %s", update_response.status_code, actual_feature_type_name)
                    else:
                        LOGGER.debug("Could not parse feature type config for SRS update")
                else:
                    LOGGER.debug("Could not get feature type details for SRS update. Status: %s", ft_details_response.status_code)
            except Exception as srs_exc:
                LOGGER.debug("Could not update SRS for layer %s: %s", actual_feature_type_name, srs_exc)
    except Exception as exc:
        LOGGER.debug("Error updating SRS for layer %s: %s. Continuing.", actual_feature_type_name, exc)

    # CRITICAL: Check what files are actually in the datastore directory
    # This helps diagnose why GeoServer might not be reading the shapefile
    datastore_path = f"{geoserver_data_dir}/{GEOSERVER_WORKSPACE}/{store_name}"
    LOGGER.debug("Checking datastore directory contents: %s", datastore_path)
    
    if os.path.exists(datastore_path):
        try:
            # CRITICAL FIX: Check if there's a subdirectory with the same name as the shapefile
            # GeoServer sometimes extracts zips creating both root files AND a subdirectory
            # The root files are often empty/corrupted, while the real data is in the subdirectory
            subdirectory_path = os.path.join(datastore_path, expected_feature_type_name)
            root_shp_path = os.path.join(datastore_path, expected_feature_type_name + ".shp")
            subdir_shp_path = os.path.join(subdirectory_path, expected_feature_type_name + ".shp")
            
            if os.path.isdir(subdirectory_path) and os.path.exists(subdir_shp_path):
                LOGGER.debug("Both root files AND subdirectory exist for '%s'", expected_feature_type_name)
                
                # Check file sizes to determine which has real data
                root_shp_size = os.path.getsize(root_shp_path) if os.path.exists(root_shp_path) else 0
                subdir_shp_size = os.path.getsize(subdir_shp_path) if os.path.exists(subdir_shp_path) else 0
                
                LOGGER.debug("Root .shp file size: %d bytes, Subdirectory .shp file size: %d bytes", root_shp_size, subdir_shp_size)
                
                # If subdirectory files are significantly larger, they contain the real data
                if subdir_shp_size > root_shp_size * 10:  # Subdirectory has at least 10x more data
                    LOGGER.debug("Root files are empty/corrupted (%d bytes), but subdirectory has real data (%d bytes). Copying files...", 
                               root_shp_size, subdir_shp_size)
                    
                    try:
                        # Strategy: Try to copy (overwrite) files from subdirectory to root
                        # This might work even if we can't delete, if the directory is writable
                        required_exts = ['.shp', '.shx', '.dbf', '.prj', '.cpg', '.qmd', '.qix']
                        copied_count = 0
                        failed_exts = []
                        
                        for ext in required_exts:
                            subdir_file = os.path.join(subdirectory_path, expected_feature_type_name + ext)
                            root_file = os.path.join(datastore_path, expected_feature_type_name + ext)
                            
                            if os.path.exists(subdir_file):
                                try:
                                    # Try to copy (overwrite) the file
                                    shutil.copy2(subdir_file, root_file)
                                    copied_count += 1
                                    LOGGER.info("  ✓ Copied %s from subdirectory to root (overwrote empty file)", ext)
                                except PermissionError:
                                    # Can't copy due to permissions - try with sudo
                                    failed_exts.append(ext)
                                    LOGGER.debug("Cannot copy %s due to permissions. Trying with sudo...", ext)
                                    try:
                                        result = run_sudo_command(['cp', subdir_file, root_file], timeout=5)
                                        if result.returncode == 0:
                                            copied_count += 1
                                            failed_exts.remove(ext)  # Remove from failed list since it succeeded
                                            LOGGER.debug("Copied %s with sudo from subdirectory to root", ext)
                                        else:
                                            error_msg = result.stderr[:100] if result.stderr else "Unknown error"
                                            LOGGER.debug("Sudo copy failed for %s (exit code %d): %s", ext, result.returncode, error_msg)
                                    except (FileNotFoundError, subprocess.TimeoutExpired, Exception) as e:
                                        LOGGER.debug("Could not copy %s with sudo: %s", ext, e)
                                except Exception as copy_exc:
                                    failed_exts.append(ext)
                                    LOGGER.warning("  ⚠ Exception copying %s: %s", ext, copy_exc)
                        
                        # If we still have failures, try copying all files at once with sudo
                        if failed_exts and copied_count == 0:
                            LOGGER.warning("⚠ Regular and individual sudo copies failed. Attempting bulk copy with sudo...")
                            try:
                                # Copy all files from subdirectory to root at once using sh -c for proper wildcard expansion
                                copy_cmd = f"cp -r {subdirectory_path}/* {datastore_path}/"
                                result = run_sudo_command(['sh', '-c', copy_cmd], timeout=10)
                                if result.returncode == 0:
                                    LOGGER.debug("Successfully copied all files from subdirectory to root using sudo")
                                    # Verify files were actually copied by checking a few key files
                                    verified_count = 0
                                    for ext in required_exts[:3]:  # Check first 3 required files
                                        root_file = os.path.join(datastore_path, expected_feature_type_name + ext)
                                        if os.path.exists(root_file) and os.path.getsize(root_file) > 100:
                                            verified_count += 1
                                    if verified_count > 0:
                                        copied_count = len(required_exts)  # Assume all were copied if some verified
                                        failed_exts = []  # Clear failed list
                                        LOGGER.debug("Verified %d files were successfully copied", verified_count)
                                    else:
                                        LOGGER.debug("Copy command succeeded but files not found. May need manual intervention.")
                                else:
                                    error_msg = result.stderr[:200] if result.stderr else "Unknown error"
                                    LOGGER.debug("Bulk sudo copy failed (exit code %d): %s", result.returncode, error_msg)
                            except (FileNotFoundError, subprocess.TimeoutExpired, Exception) as e:
                                LOGGER.debug("Bulk sudo copy failed: %s", e)
                        
                        if copied_count > 0:
                            LOGGER.debug("Successfully copied %d files from subdirectory to root", copied_count)
                            # Ensure proper ownership so GeoServer can read the files
                            try:
                                run_sudo_command(['chown', '-R', 'tomcat:tomcat', datastore_path], timeout=5)
                                LOGGER.debug("Set proper ownership on copied files")
                            except Exception as chown_exc:
                                LOGGER.debug("Could not set ownership: %s", chown_exc)
                            LOGGER.debug("Reloading datastore to pick up the fixed files...")
                            try:
                                reload_response = geo_admin_service.reload_datastore(
                                    workspace=GEOSERVER_WORKSPACE,
                                    datastore=store_name,
                                )
                                if reload_response.status_code in (200, 201, 202):
                                    LOGGER.debug("Datastore reloaded after file fix")
                                    import asyncio
                                    await asyncio.sleep(3)
                            except Exception as reload_exc:
                                LOGGER.debug("Could not reload datastore: %s", reload_exc)
                        
                        # If we couldn't copy all files, try updating nativeName to point to subdirectory
                        if failed_exts and copied_count == 0:
                            LOGGER.debug("Could not copy files due to permissions. Attempting to update nativeName to point to subdirectory...")
                            try:
                                # Get current feature type config
                                ft_details = geo_admin_service.get_feature_type_details(
                                    workspace=GEOSERVER_WORKSPACE,
                                    datastore=store_name,
                                    feature_type=expected_feature_type_name,
                                )
                                
                                if ft_details.status_code == 200:
                                    ft_config = ft_details.json()
                                    ft_info = ft_config.get("featureType", {})
                                    
                                    # Update nativeName to include subdirectory path
                                    # GeoServer uses nativeName to find the shapefile
                                    # Format: subdirectory_name/shapefile_name (without extension)
                                    subdirectory_native_name = f"{expected_feature_type_name}/{expected_feature_type_name}"
                                    
                                    update_config = {
                                        "featureType": {
                                            "name": expected_feature_type_name,
                                            "nativeName": subdirectory_native_name,
                                        }
                                    }
                                    
                                    update_response = geo_admin_service.update_feature_type(
                                        workspace=GEOSERVER_WORKSPACE,
                                        datastore=store_name,
                                        feature_type=expected_feature_type_name,
                                        config=update_config,
                                        recalculate=True,
                                    )
                                    
                                    if update_response.status_code in (200, 201):
                                        LOGGER.info("✓ Updated nativeName to '%s' to point to subdirectory", subdirectory_native_name)
                                        await asyncio.sleep(3)
                                    else:
                                        LOGGER.error("✗ Failed to update nativeName: %s", update_response.text[:200])
                                        LOGGER.error("MANUAL FIX REQUIRED:")
                                        LOGGER.error("1. Run: sudo cp -r %s/* %s/", subdirectory_path, datastore_path)
                                        LOGGER.error("2. Or update nativeName in GeoServer UI to: %s", subdirectory_native_name)
                                else:
                                    LOGGER.error("✗ Could not get feature type details to update nativeName")
                            except Exception as native_exc:
                                LOGGER.error("✗ Exception updating nativeName: %s", native_exc)
                                LOGGER.error("MANUAL FIX REQUIRED:")
                                LOGGER.error("Run: sudo cp -r %s/* %s/", subdirectory_path, datastore_path)
                        elif failed_exts:
                            LOGGER.warning("⚠ Some files could not be copied. Layer may still have issues.")
                    except Exception as fix_exc:
                        LOGGER.error("✗ Exception during file fix: %s", fix_exc, exc_info=True)
                        LOGGER.error("MANUAL FIX REQUIRED:")
                        LOGGER.error("Run: sudo cp -r %s/* %s/", subdirectory_path, datastore_path)
                else:
                    LOGGER.info("Root files appear to have data. No fix needed.")
            
            # Continue with normal diagnostic
            all_files = []
            for root, dirs, files in os.walk(datastore_path):
                for file in files:
                    rel_path = os.path.relpath(os.path.join(root, file), datastore_path)
                    all_files.append(rel_path)
            
            shp_files = [f for f in all_files if f.lower().endswith('.shp')]
            LOGGER.info("Total files in datastore directory: %d", len(all_files))
            LOGGER.info("Shapefile (.shp) files found: %d", len(shp_files))
            
            for idx, shp_file in enumerate(shp_files, 1):
                shp_name = Path(shp_file).stem
                shp_full_path = os.path.join(datastore_path, shp_file) if not os.path.isabs(shp_file) else shp_file
                shp_size = os.path.getsize(shp_full_path) if os.path.exists(shp_full_path) else 0
                LOGGER.info("  %d. Shapefile: %s (path: %s, size: %d bytes)", idx, shp_name, shp_file, shp_size)
                
                # Check if related files exist
                base_name = Path(shp_file).stem
                base_dir = os.path.dirname(shp_file) if os.path.dirname(shp_file) != '.' else ''
                required_exts = ['.shp', '.shx', '.dbf']
                found_exts = []
                for ext in required_exts:
                    if base_dir:
                        check_path = os.path.join(datastore_path, base_dir, base_name + ext)
                    else:
                        check_path = os.path.join(datastore_path, base_name + ext)
                    if os.path.exists(check_path):
                        found_exts.append(ext)
                
                LOGGER.info("     Required files: %s (found: %s)", ', '.join(required_exts), ', '.join(found_exts))
                
                # Check if this matches the expected feature type name
                if shp_name == expected_feature_type_name:
                    LOGGER.info("     ✓ This matches expected feature type name: '%s'", expected_feature_type_name)
                    if shp_size < 1000:  # Less than 1KB is suspicious
                        LOGGER.warning("     ⚠ WARNING: Shapefile is very small (%d bytes). May be empty or corrupted!", shp_size)
                else:
                    LOGGER.warning("     ⚠ This does NOT match expected feature type name: '%s' (expected: '%s')", 
                                 shp_name, expected_feature_type_name)
            
            if not shp_files:
                LOGGER.error("✗ CRITICAL: No shapefile (.shp) files found in datastore directory!")
                LOGGER.error("This means the upload did not extract the shapefile correctly.")
        except Exception as diag_exc:
            LOGGER.warning("Could not check datastore directory contents: %s", diag_exc, exc_info=True)
    else:
        LOGGER.error("✗ CRITICAL: Datastore directory does not exist: %s", datastore_path)
        LOGGER.error("This means the upload failed to create the datastore directory.")
    
    # CRITICAL: Final verification - check if the layer exists and has features
    # If the layer is empty, it means GeoServer is not reading the shapefile correctly
    # This usually happens when old files exist in the datastore directory
    geoserver_layer_name = f"{GEOSERVER_WORKSPACE}:{actual_feature_type_name}"
    LOGGER.info("=" * 80)
    LOGGER.info("FINAL VERIFICATION: Checking if layer '%s' exists and has features...", geoserver_layer_name)
    LOGGER.info("=" * 80)
    
    verification_passed = False
    feature_count = None
    
    try:
        # Wait a bit more for GeoServer to fully process
        import asyncio
        await asyncio.sleep(3)
        
        # Method 1: Check if layer exists via REST API
        LOGGER.info("Verification Method 1: Checking layer existence via REST API...")
        try:
            layer_details_response = geo_admin_service.get_layer_details(geoserver_layer_name)
            if layer_details_response.status_code == 200:
                layer_data = layer_details_response.json()
                layer_info = layer_data.get("layer", {})
                is_enabled = layer_info.get("enabled", False)
                layer_type = layer_info.get("type", "UNKNOWN")
                LOGGER.debug("Layer exists in GeoServer (enabled: %s, type: %s)", is_enabled, layer_type)
                
                if not is_enabled:
                    LOGGER.debug("Layer exists but is DISABLED. Attempting to enable it via REST API...")
                    try:
                        # Use REST API directly to enable the layer
                        from geoserver.admin.dao import GeoServerAdminDAO
                        import requests
                        
                        layer_url = f"http://{geoserver_host}:{geoserver_port}/geoserver/rest/layers/{geoserver_layer_name}.json"
                        enable_config = {"layer": {"enabled": True}}
                        enable_response = requests.put(
                            layer_url,
                            json=enable_config,
                            auth=(geoserver_username, geoserver_password),
                            headers={"Content-Type": "application/json"}
                        )
                        if enable_response.status_code in (200, 201):
                            LOGGER.info("✓ Layer enabled successfully")
                        else:
                            LOGGER.warning("Could not enable layer: %s - %s", enable_response.status_code, enable_response.text[:200])
                    except Exception as enable_exc:
                        LOGGER.warning("Exception enabling layer: %s", enable_exc)
            else:
                LOGGER.warning("Layer not found via REST API (status %s): %s", 
                             layer_details_response.status_code,
                             layer_details_response.text[:200] if layer_details_response.text else "No response")
        except Exception as rest_exc:
            LOGGER.warning("Exception checking layer via REST API: %s", rest_exc)
        
        # Method 2: Query features via WFS to verify the layer has data
        LOGGER.info("Verification Method 2: Checking feature count via WFS...")
        try:
            wfs_verification = geo_dao.query_features(
                layer=geoserver_layer_name,
                max_features=1
            )
            
            if wfs_verification.status_code == 200:
                # Try to parse as XML first
                import xml.etree.ElementTree as ET
                try:
                    root = ET.fromstring(wfs_verification.text)
                    num_features = root.get("numberOfFeatures", "0")
                    feature_count = int(num_features) if num_features.isdigit() else 0
                    
                    if feature_count > 0:
                        verification_passed = True
                        LOGGER.info("✓ VERIFICATION PASSED: Layer '%s' has %s features. Layer is ready for display.", 
                                  geoserver_layer_name, feature_count)
                    else:
                        LOGGER.error("✗ CRITICAL: Layer '%s' was created but has 0 features!", geoserver_layer_name)
                except ET.ParseError as xml_exc:
                    # If XML parsing fails, try to parse as JSON (GeoJSON format)
                    try:
                        import json
                        json_data = json.loads(wfs_verification.text)
                        if isinstance(json_data, dict):
                            features = json_data.get("features", [])
                            feature_count = len(features) if isinstance(features, list) else 0
                            if feature_count > 0:
                                verification_passed = True
                                LOGGER.info("✓ VERIFICATION PASSED: Layer '%s' has %s features (from GeoJSON). Layer is ready for display.", 
                                          geoserver_layer_name, feature_count)
                            else:
                                LOGGER.error("✗ CRITICAL: Layer '%s' was created but has 0 features (from GeoJSON)!", geoserver_layer_name)
                        else:
                            LOGGER.warning("WFS response is not in expected format (JSON): %s", type(json_data))
                    except json.JSONDecodeError:
                        # Check if response contains error or feature count in text
                        response_text = wfs_verification.text or ""
                        if "numberOfFeatures=\"0\"" in response_text or "numberOfFeatures='0'" in response_text:
                            feature_count = 0
                            LOGGER.error("✗ CRITICAL: Layer '%s' has 0 features. GeoServer may be reading wrong files.", 
                                       geoserver_layer_name)
                        elif "numberOfFeatures" in response_text:
                            # Try to extract number from text
                            import re
                            match = re.search(r'numberOfFeatures[="\'](\d+)', response_text)
                            if match:
                                feature_count = int(match.group(1))
                                if feature_count > 0:
                                    verification_passed = True
                                    LOGGER.info("✓ VERIFICATION PASSED: Layer '%s' has %s features (extracted from response).", 
                                              geoserver_layer_name, feature_count)
                                else:
                                    LOGGER.error("✗ CRITICAL: Layer '%s' has 0 features!", geoserver_layer_name)
                            else:
                                LOGGER.warning("Could not parse WFS response (XML/JSON/text): %s", xml_exc)
                        else:
                            LOGGER.warning("Could not parse WFS response: %s. Response preview: %s", 
                                         xml_exc, response_text[:200])
            else:
                LOGGER.warning("Could not verify layer features via WFS (status %s): %s", 
                             wfs_verification.status_code,
                             wfs_verification.text[:200] if wfs_verification.text else "No response")
        except Exception as wfs_exc:
            LOGGER.warning("Exception during WFS verification: %s", wfs_exc)
        
        # Method 3: Check feature type details and verify nativeName matches actual files
        if not verification_passed:
            LOGGER.info("Verification Method 3: Checking feature type configuration and nativeName...")
            try:
                ft_details = geo_admin_service.get_feature_type_details(
                    workspace=GEOSERVER_WORKSPACE,
                    datastore=store_name,
                    feature_type=actual_feature_type_name,
                )
                if ft_details.status_code == 200:
                    ft_config = ft_details.json()
                    ft_info = ft_config.get("featureType", {})
                    native_name = ft_info.get("nativeName", "")
                    attributes = ft_info.get("attributes", {})
                    attr_list = attributes.get("attribute", [])
                    
                    LOGGER.info("Feature type nativeName: '%s'", native_name)
                    LOGGER.info("Feature type display name: '%s'", actual_feature_type_name)
                    
                    # Check if nativeName matches any shapefile in the datastore
                    if os.path.exists(datastore_path):
                        shp_files_in_dir = []
                        for root, dirs, files in os.walk(datastore_path):
                            for file in files:
                                if file.lower().endswith('.shp'):
                                    rel_path = os.path.relpath(os.path.join(root, file), datastore_path)
                                    shp_name = Path(rel_path).stem
                                    shp_files_in_dir.append(shp_name)
                        
                        LOGGER.info("Shapefiles found in datastore directory: %s", shp_files_in_dir)
                        
                        if native_name in shp_files_in_dir:
                            LOGGER.info("✓ nativeName '%s' matches a shapefile in the datastore directory", native_name)
                            
                            # Check if the shapefile has all required files
                            base_dir = None
                            for root, dirs, files in os.walk(datastore_path):
                                for file in files:
                                    if Path(file).stem == native_name and file.lower().endswith('.shp'):
                                        base_dir = os.path.relpath(root, datastore_path)
                                        break
                            
                            required_files = ['.shp', '.shx', '.dbf']
                            missing_files = []
                            for ext in required_files:
                                if base_dir and base_dir != '.':
                                    check_path = os.path.join(datastore_path, base_dir, native_name + ext)
                                else:
                                    check_path = os.path.join(datastore_path, native_name + ext)
                                
                                if not os.path.exists(check_path):
                                    missing_files.append(native_name + ext)
                            
                            if missing_files:
                                LOGGER.error("✗ CRITICAL: Missing required shapefile components: %s", ', '.join(missing_files))
                            else:
                                LOGGER.info("✓ All required shapefile components exist for nativeName '%s'", native_name)
                        else:
                            LOGGER.error("✗ CRITICAL: nativeName '%s' does NOT match any shapefile in the datastore directory!", native_name)
                            LOGGER.error("Available shapefiles: %s", shp_files_in_dir)
                            LOGGER.error("This is why GeoServer cannot read the data - the nativeName points to a non-existent file!")
                    
                    if isinstance(attr_list, list) and len(attr_list) > 0:
                        LOGGER.info("✓ Feature type has %d attributes configured. Layer structure looks correct.", len(attr_list))
                    else:
                        LOGGER.warning("⚠ Feature type has no attributes configured. This may indicate an issue.")
            except Exception as ft_exc:
                LOGGER.debug("Could not check feature type details: %s", ft_exc)
        
        # Final summary
        if verification_passed:
            LOGGER.debug("=" * 80)
            LOGGER.info("✓✓✓ LAYER VERIFICATION SUCCESSFUL ✓✓✓")
            LOGGER.info("Layer '%s' is ready for display in OpenLayers", geoserver_layer_name)
            LOGGER.debug("=" * 80)
        else:
            LOGGER.error("=" * 80)
            LOGGER.error("✗✗✗ LAYER VERIFICATION FAILED OR INCOMPLETE ✗✗✗")
            LOGGER.error("Layer '%s' may not display correctly in OpenLayers", geoserver_layer_name)
            if feature_count == 0:
                LOGGER.error("REASON: Layer has 0 features. GeoServer may be reading wrong/corrupted files.")
                LOGGER.error("SOLUTION: Check datastore directory: %s", 
                           f"{geoserver_data_dir}/{GEOSERVER_WORKSPACE}/{store_name}")
                LOGGER.error("Ensure the directory contains the correct shapefile files (.shp, .shx, .dbf, etc.)")
            LOGGER.error("=" * 80)
            
    except Exception as verify_exc:
        LOGGER.error("Exception during final verification: %s", verify_exc, exc_info=True)
    
    try:
        db_record = UploadLogDAO.get_by_id(upload_log.id, db)
        if db_record:
            # Use the actual feature type name (from GeoServer, which should match the shapefile name from zip)
            # Format: workspace:feature_type_name (e.g., "metastring:lyr_3_agar_soil")
            # The store_name (datastore) is separate from the feature type name
            db_record.geoserver_layer = geoserver_layer_name
            db.add(db_record)
            db.commit()
            db.refresh(db_record)
            upload_log.geoserver_layer = geoserver_layer_name
            
            LOGGER.info(
                "Updated upload log %s with GeoServer layer name: %s (store_name/datastore: %s, feature_type: %s)",
                upload_log.id, geoserver_layer_name, store_name, actual_feature_type_name
            )
    except Exception as exc:
        LOGGER.error(
            "Failed to update GeoServer publication details for upload log %s: %s",
            upload_log.id,
            exc,
        )
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="GeoServer upload succeeded, but updating the upload log failed.",
        ) from exc


@router.get("/", response_model=List[UploadLogOut], summary="List Upload Logs", description="Retrieve a list of upload logs with optional filtering. This endpoint allows you to query upload logs by various criteria such as layer name, file format, data type, CRS, source path, GeoServer layer, tags, uploaded by user, and upload date.")
def list_upload_logs(
    db: Session = Depends(get_db),
    id: Optional[UUID] = Query(default=None),
    layer_name: Optional[str] = Query(default=None),
    file_format: Optional[str] = Query(default=None),
    data_type: Optional[DataType] = Query(default=None),
    crs: Optional[str] = Query(default=None),
    source_path: Optional[str] = Query(default=None),
    geoserver_layer: Optional[str] = Query(default=None),
    tags: Optional[List[str]] = Query(default=None),
    uploaded_by: Optional[str] = Query(default=None),
    uploaded_on: Optional[str] = Query(default=None),
) -> List[UploadLogOut]:
    """List upload logs with optional filtering."""
    uploaded_on_dt: Optional[datetime] = None
    if uploaded_on:
        try:
            uploaded_on_dt = datetime.fromisoformat(uploaded_on)
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="uploaded_on must be ISO formatted") from exc

    filter_payload = UploadLogFilter(
        id=id,
        layer_name=layer_name,
        file_format=file_format,
        data_type=data_type,
        crs=crs,
        source_path=source_path,
        geoserver_layer=geoserver_layer,
        tags=tags,
        uploaded_by=uploaded_by,
        uploaded_on=uploaded_on_dt,
    )

    return UploadLogService.get_filtered(filter_payload, db)


@router.get("/{log_id}", response_model=UploadLogOut, summary="Get Upload Log by ID", description="Retrieve detailed information about a specific upload log by its unique identifier. Returns complete upload log metadata including file details, spatial information, and GeoServer publication status.")
def get_upload_log(log_id: UUID, db: Session = Depends(get_db)) -> UploadLogOut:
    record = UploadLogService.get_by_id(log_id, db)
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Upload log not found")
    return record


########################## Upload xlsx file ##########################

@router.post("/create-table-and-insert1/", summary="Upload XLSX/CSV and log the upload in the database and publish to GeoServer (Used for frontend api calls)", description="Upload an XLSX or CSV file and automatically create a PostGIS table with the data. This endpoint processes Excel or CSV files, creates a database table in the specified schema, inserts the data, publishes it to GeoServer as a layer, and optionally logs the upload if uploaded_by is provided.")
async def create_table_and_insert1(
    table_name: str = Form(...),
    db_schema: str = Form(..., alias="schema"),
    file: UploadFile = File(...),
    uploaded_by: Optional[str] = Form(None),  # Optional - only for logging
    layer_name: Optional[str] = Form(None),
    tags: Optional[List[str]] = Form(None),
    workspace: str = Form(default="metastring"),
    store_name: Optional[str] = Form(default=None),
    db: Session = Depends(get_db),
):
    if not file.filename or not (file.filename.endswith(".xlsx") or file.filename.endswith(".csv")):
        raise HTTPException(status_code=400, detail="Only XLSX and CSV files are allowed")

    try:
        # Generate dataset_id (will be used as id in upload_logs if logging is enabled)
        dataset_id = uuid4()
        created_log = None
        
        # Only create upload_log if uploaded_by is provided and not empty (backward compatible)
        if uploaded_by and uploaded_by.strip():
            try:
                # Persist the file first for logging
                stored_path = await _persist_upload(file)
                
                # Resolve layer_name
                resolved_layer_name = layer_name or table_name
                
                # Determine file format from filename
                file_format = "csv" if stored_path.suffix.lower() == ".csv" else "xlsx"
                
                # Create upload_log entry BEFORE any table creation/GeoServer operations
                upload_log = UploadLogCreate(
                    layer_name=resolved_layer_name,
                    file_format=file_format,
                    data_type=DataType.UNKNOWN,
                    crs="UNKNOWN",
                    bbox=None,
                    source_path=os.fspath(stored_path),
                    geoserver_layer=None,
                    tags=tags,
                    uploaded_by=uploaded_by.strip(),
                )
                
                # Create upload_log with specific id (dataset_id)
                LOGGER.info(f"Creating upload log with dataset_id: {dataset_id}, uploaded_by: {uploaded_by}")
                created_log = UploadLogService.create_with_id(upload_log, db, dataset_id)
                LOGGER.info(f"Successfully created upload log with id: {created_log.id}")
                
                # Re-open the file for processing
                file_handle = stored_path.open("rb")
                upload_file = SimpleNamespace(file=file_handle, filename=stored_path.name)
            except Exception as log_exc:
                LOGGER.error(f"Failed to create upload log: {log_exc}", exc_info=True)
                # Continue with the operation even if log creation fails (backward compatible)
                # But log the error for debugging
                upload_file = file
        else:
            # Old behavior: use file directly without persisting (backward compatible)
            LOGGER.info("uploaded_by not provided or empty, skipping upload log creation")
            upload_file = file
        
        try:
            # Proceed with table creation and GeoServer upload (same as before)
            message = await UploadLogService.create_table_and_insert1(
                table_name=table_name,
                schema=db_schema,
                file=upload_file,
                db=db,
                geo_service=geo_service,
                workspace=workspace,
                store_name=store_name,
                dataset_id=dataset_id,  # Use the generated dataset_id
                upload_log_id=created_log.id if created_log else None
            )
            
            # Update geoserver_layer after successful upload (only if logging was enabled)
            if created_log and created_log.id:
                try:
                    UploadLogDAO.update_geoserver_layer(created_log.id, table_name, db)
                except Exception as exc:
                    LOGGER.warning("Failed to update geoserver_layer for upload log %s: %s", created_log.id, exc)
                    # Don't fail the whole request if this update fails
            
            # Return response (backward compatible - old clients won't see upload_log_id)
            response = {"message": message}
            if created_log:
                response["upload_log_id"] = str(created_log.id)
            return response
        finally:
            # Only close if we opened a file handle (when logging is enabled)
            if uploaded_by and hasattr(upload_file, 'file') and hasattr(upload_file.file, "closed") and not upload_file.file.closed:
                upload_file.file.close()
                
    except Exception as e:
        LOGGER.error("Error creating table and inserting data: %s", e)
        raise HTTPException(status_code=500, detail=str(e))

