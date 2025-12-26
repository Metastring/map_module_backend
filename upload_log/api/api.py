import asyncio
import logging
import os
import zipfile
from datetime import datetime
from pathlib import Path
from typing import List, Optional
from uuid import uuid4, UUID

import aiofiles
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


def _extract_shapefile_name(file_path: Path) -> str:
    """
    Extract the shapefile name from a file path.
    For .zip files, looks for .shp files inside and returns the base name.
    For .shp files, returns the base name without extension.
    """
    if file_path.suffix.lower() == '.zip':
        try:
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                # Find the first .shp file in the zip
                for name in zip_ref.namelist():
                    if name.lower().endswith('.shp'):
                        # Get the base name without .shp extension
                        return Path(name).stem
        except Exception as exc:
            LOGGER.warning("Could not extract shapefile name from zip %s: %s. Using file stem.", file_path, exc)
            return file_path.stem
    elif file_path.suffix.lower() == '.shp':
        return file_path.stem
    
    # Fallback to file stem
    return file_path.stem


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


async def _publish_to_geoserver(upload_log: UploadLogOut, db: Session) -> None:
    if not upload_log.file_format or upload_log.file_format.lower() != "shp":
        LOGGER.info("Skipping GeoServer publication for file format: %s", upload_log.file_format)
        return

    file_path = Path(upload_log.source_path)
    if not file_path.exists():
        LOGGER.error("Stored upload file not found for GeoServer publication: %s", file_path)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Stored upload file is missing; cannot publish to GeoServer.",
        )

    store_name = upload_log.layer_name
    LOGGER.info("Publishing to GeoServer: workspace=%s, store=%s, file=%s, file_format=%s", 
                GEOSERVER_WORKSPACE, store_name, file_path, upload_log.file_format)
    
    # Upload directly using the persisted file path (works for both .zip and .shp files)
    try:
        response = geo_dao.upload_shapefile(
            workspace=GEOSERVER_WORKSPACE,
            store_name=store_name,
            file_path=os.fspath(file_path),
        )
        LOGGER.info("GeoServer upload response: status=%s, text=%s", response.status_code, response.text[:200] if response.text else "No response text")
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
        # Wait a bit for async processing
        await asyncio.sleep(2)
    
    # Extract the actual shapefile name from the uploaded file
    # This is important because GeoServer uses the shapefile name, not the store name
    shapefile_name = _extract_shapefile_name(file_path)
    LOGGER.info("Extracted shapefile name: %s from file: %s", shapefile_name, file_path)
    
    # Try to find the actual feature type name that was created
    # GeoServer might use the shapefile name from inside the zip, not the store name
    actual_feature_type_name = shapefile_name
    feature_type_exists = False
    
    try:
        # List feature types in the datastore to find what was actually created
        ft_list_response = geo_admin_service.list_datastore_tables(
            workspace=GEOSERVER_WORKSPACE,
            datastore=store_name,
        )
        LOGGER.info("Feature type list response status: %s", ft_list_response.status_code)
        
        if ft_list_response.status_code == 200:
            ft_list_data = ft_list_response.json()
            if isinstance(ft_list_data, dict):
                feature_types_obj = ft_list_data.get("featureTypes", {})
                if isinstance(feature_types_obj, dict):
                    feature_types = feature_types_obj.get("featureType", [])
                    if feature_types:
                        if isinstance(feature_types, list) and len(feature_types) > 0:
                            actual_feature_type_name = feature_types[0].get("name", shapefile_name) if isinstance(feature_types[0], dict) else shapefile_name
                            feature_type_exists = True
                        elif isinstance(feature_types, dict):
                            actual_feature_type_name = feature_types.get("name", shapefile_name)
                            feature_type_exists = True
                        LOGGER.info("Found existing feature type name: %s (store: %s)", actual_feature_type_name, store_name)
                    else:
                        LOGGER.warning("No feature types found in datastore %s after upload", store_name)
                else:
                    LOGGER.warning("Unexpected feature types structure in response")
            else:
                LOGGER.warning("Unexpected response format when listing feature types")
        else:
            LOGGER.warning("Failed to list feature types: status %s, response: %s", 
                          ft_list_response.status_code, ft_list_response.text[:200] if ft_list_response.text else "No response")
    except Exception as list_exc:
        LOGGER.warning("Could not list feature types to find actual name: %s. Will try to create feature type explicitly.", list_exc)
    
    # If feature type doesn't exist, create it explicitly
    if not feature_type_exists:
        LOGGER.info("Feature type does not exist, creating it explicitly: workspace=%s, datastore=%s, feature_type=%s", 
                   GEOSERVER_WORKSPACE, store_name, shapefile_name)
        try:
            normalized_crs = _normalize_crs_to_epsg(upload_log.crs)
            create_response = geo_admin_service.create_feature_type_from_shapefile(
                workspace=GEOSERVER_WORKSPACE,
                datastore=store_name,
                feature_type_name=shapefile_name,
                native_name=shapefile_name,
                enabled=True,
                srs=normalized_crs
            )
            
            if create_response.status_code in (200, 201):
                LOGGER.info("Successfully created feature type %s in datastore %s", shapefile_name, store_name)
                actual_feature_type_name = shapefile_name
                feature_type_exists = True
            elif create_response.status_code == 409:
                # Feature type already exists (race condition)
                LOGGER.info("Feature type %s already exists (status 409), continuing", shapefile_name)
                actual_feature_type_name = shapefile_name
                feature_type_exists = True
            else:
                LOGGER.error("Failed to create feature type %s: status %s, response: %s", 
                           shapefile_name, create_response.status_code, 
                           create_response.text[:500] if create_response.text else "No response")
                # Don't fail the whole operation, but log the error
        except Exception as create_exc:
            LOGGER.error("Exception while creating feature type %s: %s", shapefile_name, create_exc, exc_info=True)
            # Continue anyway - maybe it was created but we couldn't detect it

    # Update feature type with correct SRS from metadata if available
    # Only do this if the feature type exists
    if feature_type_exists:
        try:
            normalized_crs = _normalize_crs_to_epsg(upload_log.crs)
            if normalized_crs:
                # Try to update SRS using the actual feature type name
                try:
                    update_response = geo_admin_service.update_feature_type(
                        workspace=GEOSERVER_WORKSPACE,
                        datastore=store_name,
                        feature_type=actual_feature_type_name,
                        config={
                            "featureType": {
                                "name": actual_feature_type_name,
                                "srs": normalized_crs,
                                "projectionPolicy": "FORCE_DECLARED",
                            }
                        },
                        recalculate=True,
                    )
                    if update_response.status_code in (200, 201):
                        LOGGER.info("Successfully updated SRS for feature type %s to %s", actual_feature_type_name, normalized_crs)
                    else:
                        LOGGER.warning("SRS update returned status %s for layer %s: %s", 
                                     update_response.status_code, actual_feature_type_name,
                                     update_response.text[:200] if update_response.text else "No response")
                except Exception as srs_exc:
                    LOGGER.warning("Could not update SRS for layer %s: %s", actual_feature_type_name, srs_exc)
        except Exception as exc:
            LOGGER.warning("Error updating SRS for layer %s: %s. Continuing.", store_name, exc)
    else:
        LOGGER.warning("Skipping SRS update because feature type does not exist")
    
    # Verify that the layer is actually published and accessible
    layer_full_name = f"{GEOSERVER_WORKSPACE}:{actual_feature_type_name}"
    try:
        layer_response = geo_admin_service.get_layer_details(layer_full_name)
        if layer_response.status_code == 200:
            LOGGER.info("Layer %s is published and accessible", layer_full_name)
        else:
            LOGGER.warning("Layer %s may not be fully published (status %s)", layer_full_name, layer_response.status_code)
    except Exception as layer_check_exc:
        LOGGER.warning("Could not verify layer publication for %s: %s", layer_full_name, layer_check_exc)

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

