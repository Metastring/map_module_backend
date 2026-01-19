import json
import logging
import os
import shutil
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import List, Optional
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile, status
from sqlalchemy.orm import Session

from database.database import get_db
from upload_log.models.model import DataType, UploadLogCreate, UploadLogFilter, UploadLogOut
from upload_log.service.metadata import derive_file_metadata
from upload_log.service.service import (
    UploadLogService,
    extract_shapefile_name_from_zip,
    get_shapefile_schema,
    extract_shapefile_from_zip_for_schema,
    normalize_crs_to_epsg,
    cleanup_datastore_directory,
    resolve_feature_type_name,
    get_feature_type_from_response,
    wait_for_geoserver_processing,
    fix_subdirectory_files,
    verify_layer_features,
    persist_upload,
)
from geoserver.dao import GeoServerDAO
from geoserver.service import GeoServerService
from geoserver.admin.dao import GeoServerAdminDAO
from geoserver.admin.service import GeoServerAdminService
from upload_log.dao.dao import UploadLogDAO
from utils.config import geoserver_host, geoserver_port, geoserver_username, geoserver_password, geoserver_data_dir

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
    stored_path = await persist_upload(file, UPLOADS_DIR)

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
        actual_layer_name = extract_shapefile_name_from_zip(stored_path)
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
        store_name=resolved_store_name,
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
    
    LOGGER.debug("_publish_to_geoserver called for file_format: %s", upload_log.file_format)
    
    if not upload_log.file_format or upload_log.file_format.lower() != "shp":
        LOGGER.info("Skipping GeoServer publication for file format: %s", upload_log.file_format)
        return

    # Validate file path
    file_path = Path(upload_log.source_path)
    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Stored upload file is missing or invalid: {file_path}",
        )

    # The store_name from upload_log.layer_name is used as the store_name (datastore) in GeoServer.
    # However, GeoServer will create a feature type with the name from the shapefile inside the zip.
    # We should use the shapefile name as the feature type name, not try to rename it.
    store_name = upload_log.layer_name  # This is the store_name (datastore) provided by the user
    LOGGER.debug("Store name (datastore): %s", store_name)
    
    # Extract the expected feature type name from zip or use store_name
    expected_feature_type_name = resolve_feature_type_name(file_path, store_name)
    LOGGER.info("Publishing to GeoServer: workspace=%s, store_name=%s, expected_feature_type_name=%s", 
                GEOSERVER_WORKSPACE, store_name, expected_feature_type_name)
    
    # Clean up existing datastore and files to ensure clean upload
    datastore_path = f"{geoserver_data_dir}/{GEOSERVER_WORKSPACE}/{store_name}"
    try:
        datastore_response = geo_admin_service.get_datastore_details(
            workspace=GEOSERVER_WORKSPACE,
            datastore=store_name,
        )
        if datastore_response.status_code == 200:
            delete_ds_response = geo_admin_service.delete_datastore(
                workspace=GEOSERVER_WORKSPACE,
                datastore=store_name,
            )
            if delete_ds_response.status_code in (200, 204):
                await cleanup_datastore_directory(datastore_path)
                import asyncio
                await asyncio.sleep(2)
        elif datastore_response.status_code == 404:
            await cleanup_datastore_directory(datastore_path)
    except Exception as check_exc:
        LOGGER.debug("Exception checking/cleaning datastore: %s", check_exc)
    # Upload shapefile to GeoServer
    file_path_str = str(file_path.resolve())
    try:
        response = geo_dao.upload_shapefile(
            workspace=GEOSERVER_WORKSPACE,
            store_name=store_name,
            file_path=file_path_str,
        )
        created_feature_type_from_response = get_feature_type_from_response(response)
        
        if response.status_code not in (200, 201, 202):
            LOGGER.error("GeoServer upload failed: status %s, %s", response.status_code, response.text[:500])
            raise HTTPException(status_code=response.status_code, detail=response.text)
        
        LOGGER.info("GeoServer upload succeeded (status %s)", response.status_code)
        await wait_for_geoserver_processing(response.status_code)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except HTTPException:
        raise
    except Exception as exc:
        LOGGER.error("Error uploading to GeoServer: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Unexpected error occurred while publishing to GeoServer.",
        ) from exc
    
    # Reload datastore to force GeoServer to re-read files
    try:
        reload_response = geo_admin_service.reload_datastore(
            workspace=GEOSERVER_WORKSPACE,
            datastore=store_name,
        )
        if reload_response.status_code in (200, 201, 202):
            import asyncio
            await asyncio.sleep(2)
    except Exception:
        pass
    
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
                    temp_shp_path = extract_shapefile_from_zip_for_schema(file_path)
                    if temp_shp_path:
                        schema_result = get_shapefile_schema(temp_shp_path)
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
                    srs = normalize_crs_to_epsg(upload_log.crs)
                
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
                    
                    # If feature type created successfully, trigger recalculation
                    if create_ft_response.status_code in (200, 201):
                        LOGGER.info("✓ Successfully created feature type '%s'", expected_feature_type_name)
                        import asyncio
                        await asyncio.sleep(3)
                        
                        # Trigger bounding box recalculation
                        try:
                            ft_details = geo_admin_service.get_feature_type_details(
                                workspace=GEOSERVER_WORKSPACE,
                                datastore=store_name,
                                feature_type=expected_feature_type_name,
                            )
                            if ft_details.status_code == 200:
                                ft_config = ft_details.json()
                                if isinstance(ft_config, dict) and "featureType" in ft_config:
                                    native_name = ft_config["featureType"].get("nativeName", expected_feature_type_name)
                                    update_config = {"featureType": {"name": expected_feature_type_name, "nativeName": native_name}}
                                    if srs:
                                        update_config["featureType"].update({
                                            "srs": srs,
                                            "nativeSRS": srs,
                                            "projectionPolicy": "FORCE_DECLARED"
                                        })
                                    recalc_response = geo_admin_service.update_feature_type(
                                        workspace=GEOSERVER_WORKSPACE,
                                        datastore=store_name,
                                        feature_type=expected_feature_type_name,
                                        config=update_config,
                                        recalculate=True,
                                    )
                                    if recalc_response.status_code in (200, 201):
                                        await asyncio.sleep(2)
                        except Exception:
                            pass
                    else:
                        LOGGER.error("✗ Failed to create feature type '%s': status %s, response: %s", 
                                   expected_feature_type_name, create_ft_response.status_code,
                                   create_ft_response.text[:500] if create_ft_response.text else "No response")
                except Exception as create_exc:
                    LOGGER.error("✗ Exception while creating feature type: %s", create_exc, exc_info=True)
        except Exception as reload_exc:
            LOGGER.error("✗ Exception while checking/reloading datastore: %s", reload_exc, exc_info=True)
    
    # Determine actual feature type name (priority: response > expected > store_name)
    actual_feature_type_name = created_feature_type_from_response or expected_feature_type_name or store_name
    
    # Verify the feature type exists, or find the actual one created
    if not created_feature_type_from_response and expected_feature_type_name:
        try:
            ft_check = geo_admin_service.get_feature_type_details(
                workspace=GEOSERVER_WORKSPACE,
                datastore=store_name,
                feature_type=expected_feature_type_name,
            )
            if ft_check.status_code != 200:
                # Try to find it by listing all feature types
                ft_list = geo_admin_service.list_datastore_tables(
                    workspace=GEOSERVER_WORKSPACE,
                    datastore=store_name,
                )
                if ft_list.status_code == 200:
                    ft_data = ft_list.json()
                    feature_types = ft_data.get("featureTypes", {}).get("featureType", [])
                    if isinstance(feature_types, dict):
                        feature_types = [feature_types]
                    if feature_types:
                        # Try to match by name or nativeName, otherwise use last one
                        matching_ft = None
                        for ft in feature_types:
                            if isinstance(ft, dict) and ft.get("name") == expected_feature_type_name:
                                matching_ft = ft
                                break
                        if not matching_ft:
                            matching_ft = feature_types[-1] if feature_types else None
                        if matching_ft:
                            actual_feature_type_name = matching_ft.get("name", expected_feature_type_name)
        except Exception:
            pass

    # Update feature type with correct SRS from metadata if available
    # Use the actual feature type name (from GeoServer, which should match the shapefile name from zip)
    try:
        normalized_crs = normalize_crs_to_epsg(upload_log.crs)
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

    # Fix subdirectory files if needed
    datastore_path = f"{geoserver_data_dir}/{GEOSERVER_WORKSPACE}/{store_name}"
    if os.path.exists(datastore_path):
        if fix_subdirectory_files(datastore_path, expected_feature_type_name):
            try:
                reload_response = geo_admin_service.reload_datastore(
                    workspace=GEOSERVER_WORKSPACE,
                    datastore=store_name,
                )
                if reload_response.status_code in (200, 201, 202):
                    import asyncio
                    await asyncio.sleep(2)
            except Exception:
                pass
    
    # Final verification - check if layer has features
    geoserver_layer_name = f"{GEOSERVER_WORKSPACE}:{actual_feature_type_name}"
    import asyncio
    await asyncio.sleep(2)
    
    verification_passed, feature_count = verify_layer_features(geoserver_layer_name)
    if verification_passed:
        LOGGER.info("✓ Layer verification passed: '%s' has %s features", geoserver_layer_name, feature_count or 0)
    elif feature_count == 0:
        LOGGER.warning("⚠ Layer created but has 0 features. Check datastore directory: %s", datastore_path)
    
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
    store_name: Optional[str] = Query(default=None),
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
        store_name=store_name,
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
    store_name: Optional[str] = Form(None),
    tags: Optional[List[str]] = Form(None),
    workspace: str = Form(default="metastring"),
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
                stored_path = await persist_upload(file, UPLOADS_DIR)
                
                # Resolve store_name
                resolved_store_name = store_name or table_name
                
                # Determine file format from filename
                file_format = "csv" if stored_path.suffix.lower() == ".csv" else "xlsx"
                
                # Create upload_log entry BEFORE any table creation/GeoServer operations
                upload_log = UploadLogCreate(
                    store_name=resolved_store_name,
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

