import logging
import os
import asyncio
from datetime import datetime
from pathlib import Path
from typing import List, Optional
from uuid import uuid4, UUID

import aiofiles
import requests
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
from utils.config import geoserver_host, geoserver_port, geoserver_username, geoserver_password
from types import SimpleNamespace
from pyproj import CRS

router = APIRouter()
LOGGER = logging.getLogger(__name__)
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
    layer_name: Optional[str] = Form(None),
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

    resolved_layer_name = layer_name or metadata.get("layer_name") or Path(file.filename).stem
    data_type = metadata.get("data_type") or DataType.UNKNOWN
    file_format = metadata.get("file_format") or stored_path.suffix.lstrip(".")

    upload_log = UploadLogCreate(
        layer_name=resolved_layer_name,
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


async def _wait_for_feature_type_ready(store_name: str, max_wait_seconds: int = 30, poll_interval: float = 1.0) -> None:
    """
    Poll GeoServer to wait for a feature type to become available in the datastore.
    
    This is necessary when GeoServer returns 202 (Accepted) or when processing
    takes time, especially in staging/production environments with higher load.
    
    Args:
        store_name: Name of the datastore to check
        max_wait_seconds: Maximum time to wait in seconds
        poll_interval: Time between polls in seconds
    """
    max_attempts = int(max_wait_seconds / poll_interval)
    attempt = 0
    
    while attempt < max_attempts:
        try:
            # List feature types in the datastore to check if any are available
            ft_list_response = geo_admin_service.list_datastore_tables(
                workspace=GEOSERVER_WORKSPACE,
                datastore=store_name,
            )
            
            if ft_list_response.status_code == 200:
                try:
                    ft_list_data = ft_list_response.json()
                    # Ensure ft_list_data is a dict, not a string
                    if isinstance(ft_list_data, str):
                        LOGGER.warning("GeoServer returned string instead of JSON for store %s: %s", store_name, ft_list_data[:200])
                        attempt += 1
                        if attempt < max_attempts:
                            await asyncio.sleep(poll_interval)
                        continue
                    if not isinstance(ft_list_data, dict):
                        LOGGER.warning("GeoServer returned unexpected type %s for store %s: %s", type(ft_list_data), store_name, str(ft_list_data)[:200])
                        attempt += 1
                        if attempt < max_attempts:
                            await asyncio.sleep(poll_interval)
                        continue
                    feature_types = ft_list_data.get("featureTypes", {}).get("featureType", [])
                    
                    if feature_types:
                        # Feature type is available
                        if isinstance(feature_types, list) and len(feature_types) > 0:
                            feature_type_name = feature_types[0].get("name", store_name)
                        elif isinstance(feature_types, dict):
                            feature_type_name = feature_types.get("name", store_name)
                        else:
                            feature_type_name = store_name
                        
                        LOGGER.info("Feature type %s is now available in store %s", feature_type_name, store_name)
                        return
                except (ValueError, AttributeError, TypeError) as json_error:
                    LOGGER.warning("Failed to parse JSON response for store %s: %s. Response text: %s", store_name, json_error, ft_list_response.text[:200] if hasattr(ft_list_response, 'text') else 'No response text')
                    attempt += 1
                    if attempt < max_attempts:
                        await asyncio.sleep(poll_interval)
                    continue
            else:
                # Status code is not 200
                LOGGER.debug("Feature type list returned status %d for store %s: %s", ft_list_response.status_code, store_name, ft_list_response.text[:200] if hasattr(ft_list_response, 'text') else 'No response text')
            
            # Feature type not ready yet, wait and retry
            attempt += 1
            if attempt < max_attempts:
                LOGGER.debug("Feature type not ready yet for store %s, waiting... (attempt %d/%d)", store_name, attempt, max_attempts)
                await asyncio.sleep(poll_interval)
        except Exception as exc:
            LOGGER.warning("Error checking feature type availability for store %s: %s. Retrying...", store_name, exc)
            attempt += 1
            if attempt < max_attempts:
                await asyncio.sleep(poll_interval)
    
    # If we get here, we've exhausted all attempts
    LOGGER.warning(
        "Feature type for store %s did not become available within %d seconds. "
        "Proceeding anyway, but SRS update may fail.",
        store_name,
        max_wait_seconds
    )


async def _publish_to_geoserver(upload_log: UploadLogOut, db: Session) -> None:
    if not upload_log.file_format or upload_log.file_format.lower() != "shp":
        return

    file_path = Path(upload_log.source_path)
    if not file_path.exists():
        LOGGER.error("Stored upload file not found for GeoServer publication: %s", file_path)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Stored upload file is missing; cannot publish to GeoServer.",
        )

    store_name = upload_log.layer_name
    try:
        file_handle = file_path.open("rb")
    except OSError as exc:
        LOGGER.error("Failed to open stored upload for GeoServer publication: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Unable to open stored upload file for GeoServer.",
        ) from exc

    upload_file = SimpleNamespace(file=file_handle, filename=file_path.name)

    try:
        response = await geo_service.upload_resource(
            workspace=GEOSERVER_WORKSPACE,
            store_name=store_name,
            resource_type="shapefile",
            file=upload_file,
        )
    except ValueError as exc:
        LOGGER.error("GeoServer rejected upload for layer %s: %s", store_name, exc)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except Exception as exc:
        LOGGER.error("Unexpected error uploading layer %s to GeoServer: %s", store_name, exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Unexpected error occurred while publishing to GeoServer.",
        ) from exc
    finally:
        if hasattr(upload_file.file, "closed") and not upload_file.file.closed:
            upload_file.file.close()

    # Handle different response codes
    if response.status_code == 202:
        # 202 Accepted means GeoServer is still processing the upload asynchronously
        # We need to wait and poll for the feature type to become available
        LOGGER.info("GeoServer returned 202 (Accepted) for layer %s. Waiting for processing to complete...", store_name)
        await _wait_for_feature_type_ready(store_name, max_wait_seconds=30)
    elif response.status_code not in (200, 201):
        LOGGER.error(
            "GeoServer upload failed for layer %s with status %s: %s",
            store_name,
            response.status_code,
            response.text,
        )
        raise HTTPException(status_code=response.status_code, detail=response.text)
    else:
        # 200/201 means upload succeeded, but we still need to wait for processing
        LOGGER.info("GeoServer returned %s for layer %s. Waiting for feature type to be ready...", response.status_code, store_name)
        await _wait_for_feature_type_ready(store_name, max_wait_seconds=15)

    # Update feature type with correct SRS from metadata
    try:
        # Normalize CRS to EPSG format
        normalized_crs = _normalize_crs_to_epsg(upload_log.crs)
        if normalized_crs:
            LOGGER.info("Updating feature type SRS for layer %s to %s", store_name, normalized_crs)
            
            # Get the actual feature type name from the store
            # When uploading a shapefile, GeoServer creates a feature type with the shapefile name
            # We need to list feature types in the store to find the correct name
            feature_type_name = store_name  # Default to store name
            
            try:
                # List feature types in the datastore to find the actual feature type name
                ft_list_response = geo_admin_service.list_datastore_tables(
                    workspace=GEOSERVER_WORKSPACE,
                    datastore=store_name,
                )
                
                if ft_list_response.status_code == 200:
                    try:
                        ft_list_data = ft_list_response.json()
                        # Ensure ft_list_data is a dict, not a string
                        if isinstance(ft_list_data, str):
                            LOGGER.warning("GeoServer returned string instead of JSON for store %s: %s", store_name, ft_list_data[:200])
                            feature_type_name = store_name
                        elif not isinstance(ft_list_data, dict):
                            LOGGER.warning("GeoServer returned unexpected type %s for store %s", type(ft_list_data), store_name)
                            feature_type_name = store_name
                        else:
                            feature_types = ft_list_data.get("featureTypes", {}).get("featureType", [])
                            if feature_types:
                                # Use the first feature type (should be the one we just uploaded)
                                if isinstance(feature_types, list) and len(feature_types) > 0:
                                    feature_type_name = feature_types[0].get("name", store_name)
                                elif isinstance(feature_types, dict):
                                    feature_type_name = feature_types.get("name", store_name)
                                LOGGER.info("Found feature type name: %s for store: %s", feature_type_name, store_name)
                            else:
                                feature_type_name = store_name
                    except (ValueError, AttributeError, TypeError) as json_error:
                        LOGGER.warning("Failed to parse JSON response for store %s: %s. Response text: %s", store_name, json_error, ft_list_response.text[:200] if hasattr(ft_list_response, 'text') else 'No response text')
                        feature_type_name = store_name
                    
                    if feature_types:
                        # Use the first feature type (should be the one we just uploaded)
                        if isinstance(feature_types, list) and len(feature_types) > 0:
                            feature_type_name = feature_types[0].get("name", store_name)
                        elif isinstance(feature_types, dict):
                            feature_type_name = feature_types.get("name", store_name)
                        LOGGER.info("Found feature type name: %s for store: %s", feature_type_name, store_name)
            except Exception as list_exc:
                LOGGER.warning("Could not list feature types for store %s: %s. Using store name as feature type name.", store_name, list_exc)
            
            # Get current feature type configuration
            ft_response = geo_admin_service.get_feature_type_details(
                workspace=GEOSERVER_WORKSPACE,
                datastore=store_name,
                feature_type=feature_type_name,
            )
            
            if ft_response.status_code == 200:
                ft_config = ft_response.json()
                feature_type = ft_config.get("featureType", {})
                
                # Get the native SRS that GeoServer read from the .prj file (if any)
                current_native_srs = feature_type.get("nativeSRS")
                if not current_native_srs:
                    current_native_srs = feature_type.get("srs")
                
                LOGGER.info("Current native SRS from GeoServer: %s, Our normalized CRS: %s", current_native_srs, normalized_crs)
                
                # GeoServer's REST API requires XML format to set nativeCRS (not nativeSRS)
                # Use XML format with nativeCRS to set the native SRS
                # Reference: https://terrestris.github.io/momo3-ws/en/geoserver/advanced/rest/update.html
                
                # Check if nativeSRS needs to be set
                needs_fix = not current_native_srs or current_native_srs != normalized_crs
                
                if needs_fix:
                    LOGGER.info("Setting native SRS using XML format with nativeCRS...")
                    
                    # Use XML format to set nativeCRS
                    xml_data = f"""<featureType>
  <enabled>{str(feature_type.get("enabled", True)).lower()}</enabled>
  <nativeCRS>{normalized_crs}</nativeCRS>
  <srs>{normalized_crs}</srs>
  <projectionPolicy>FORCE_DECLARED</projectionPolicy>
</featureType>"""
                    
                    # Update using XML format
                    update_url = f"{geo_admin_dao.base_url}/workspaces/{GEOSERVER_WORKSPACE}/datastores/{store_name}/featuretypes/{feature_type_name}"
                    update_headers = {"Content-type": "text/xml"}
                    xml_update_response = requests.put(
                        update_url,
                        auth=geo_admin_dao.auth,
                        data=xml_data,
                        headers=update_headers,
                    )
                    
                    if xml_update_response.status_code in (200, 201):
                        LOGGER.info("Successfully set nativeCRS to %s using XML format", normalized_crs)
                        await asyncio.sleep(0.5)
                        
                        # Recalculate bounding boxes
                        recalc_xml = f"""<featureType>
  <enabled>{str(feature_type.get("enabled", True)).lower()}</enabled>
</featureType>"""
                        recalc_url = f"{update_url}?recalculate=nativebbox,latlonbbox"
                        recalc_response = requests.put(
                            recalc_url,
                            auth=geo_admin_dao.auth,
                            data=recalc_xml,
                            headers=update_headers,
                        )
                        
                        if recalc_response.status_code in (200, 201):
                            LOGGER.info("Successfully recalculated bounding boxes")
                        else:
                            LOGGER.warning("Bounding box recalculation failed: %s", recalc_response.text)
                    else:
                        LOGGER.error("Failed to set nativeCRS via XML: %s", xml_update_response.text)
                        # Fall back to JSON update (which won't set nativeSRS but will set declared SRS)
                        update_response = geo_admin_service.update_feature_type(
                            workspace=GEOSERVER_WORKSPACE,
                            datastore=store_name,
                            feature_type=feature_type_name,
                            config={
                                "featureType": {
                                    "name": feature_type_name,
                                    "nativeName": feature_type.get("nativeName", feature_type_name),
                                    "srs": normalized_crs,
                                    "projectionPolicy": "FORCE_DECLARED",
                                    "enabled": feature_type.get("enabled", True),
                                }
                            },
                            recalculate=True,
                        )
                else:
                    # Native SRS is already correct, just update declared SRS
                    LOGGER.info("Native SRS is already correct. Updating declared SRS only...")
                    update_response = geo_admin_service.update_feature_type(
                        workspace=GEOSERVER_WORKSPACE,
                        datastore=store_name,
                        feature_type=feature_type_name,
                        config={
                            "featureType": {
                                "name": feature_type_name,
                                "nativeName": feature_type.get("nativeName", feature_type_name),
                                "srs": normalized_crs,
                                "projectionPolicy": "FORCE_DECLARED",
                                "enabled": feature_type.get("enabled", True),
                            }
                        },
                        recalculate=True,
                    )
                    
                    if update_response.status_code not in (200, 201):
                        LOGGER.warning(
                            "Failed to update SRS for feature type %s: status %s, response: %s",
                            feature_type_name,
                            update_response.status_code,
                            update_response.text,
                        )
            else:
                LOGGER.warning(
                    "Could not get feature type details for %s in store %s: status %s, response: %s",
                    feature_type_name,
                    store_name,
                    ft_response.status_code,
                    ft_response.text,
                )
        else:
            LOGGER.warning("Could not normalize CRS '%s' for layer %s, skipping SRS update", upload_log.crs, store_name)
    except Exception as exc:
        LOGGER.error(
            "Error updating SRS for layer %s: %s. Continuing with database update.",
            store_name,
            exc,
            exc_info=True,
        )
        # Don't fail the whole operation if SRS update fails

    try:
        db_record = UploadLogDAO.get_by_id(upload_log.id, db)
        if db_record:
            db_record.geoserver_layer = store_name
            db.add(db_record)
            db.commit()
            db.refresh(db_record)
            upload_log.geoserver_layer = store_name
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

