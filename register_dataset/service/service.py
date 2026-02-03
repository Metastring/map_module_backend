import logging
import requests
from typing import Optional, List, Dict, Any
from uuid import UUID, uuid4
from datetime import datetime
from pathlib import Path
from sqlalchemy.orm import Session
from fastapi import HTTPException, UploadFile
from types import SimpleNamespace
from register_dataset.model.model import (
    RegisterDatasetRequest,
    RegisterDatasetResponse,
    RegisterShapefileRequest,
    RegisterShapefileResponse,
    StyleConfigForColumn,
)
from upload_log.service.service import (
    UploadLogService, 
    persist_upload,
    extract_shapefile_name_from_zip,
    extract_shapefile_from_zip_for_schema,
    get_shapefile_schema,
    resolve_feature_type_name,
    cleanup_datastore_directory,
    get_feature_type_from_response,
    wait_for_geoserver_processing,
    fix_subdirectory_files,
    verify_layer_features,
    normalize_crs_to_epsg,
)
from upload_log.service.metadata import derive_file_metadata
from upload_log.dao.dao import UploadLogDAO
from upload_log.models.model import UploadLogCreate, UploadLogOut, DataType
from geoserver.service import GeoServerService
from geoserver.admin.service import GeoServerAdminService
from geoserver.admin.dao import GeoServerAdminDAO
from geoserver.dao import GeoServerDAO
from metadata.service.service import MetadataService
from metadata.models.model import MetadataInput
from styles.service.style_service import StyleService
from styles.models.model import StyleGenerateRequest, DataSource
import os
import asyncio
import shutil
from utils.config import (
    geoserver_host,
    geoserver_port,
    geoserver_username,
    geoserver_password,
    geoserver_data_dir,
)

logger = logging.getLogger(__name__)

UPLOADS_DIR = Path(__file__).resolve().parents[2] / "uploads"


class RegisterDatasetService:
    """Service for registering complete datasets with upload, metadata, and styling."""

    def __init__(
        self,
        db: Session,
        geo_service: GeoServerService,
        geo_admin_service: GeoServerAdminService,
        style_service: StyleService,
        geo_dao: Optional[GeoServerDAO] = None,
    ):
        self.db = db
        self.geo_service = geo_service
        self.geo_admin_service = geo_admin_service
        self.style_service = style_service
        self.geo_dao = geo_dao or geo_service.dao

    async def register_dataset(
        self,
        request: RegisterDatasetRequest,
        file: UploadFile,
    ) -> RegisterDatasetResponse:
        """
        Main orchestration method to register a complete dataset.
        
        Steps:
        1. Call create-table-and-insert1 to upload and create table
        2. Configure GeoServer layer (SRS, bounding boxes, )
        3. Create metadata entry
        4. Generate styles for each configured column
        """
        try:
            dataset_id = uuid4()
            upload_log_id = None
            metadata_id = None
            styles_created = []

            # Step 1: Call create-table-and-insert1 API
            logger.info(f"Step 1: Creating table and inserting data for {request.table_name}")
            
            # Persist file for create_table_and_insert1
            stored_path = await persist_upload(file, UPLOADS_DIR)
            
            try:
                # Create upload log if uploaded_by is provided
                if request.uploaded_by and request.uploaded_by.strip():
                    
                    resolved_layer_name = request.layer_name or request.table_name
                    
                    # Determine file format from filename
                    file_format = "csv" if stored_path.suffix.lower() == ".csv" else "xlsx"
                    
                    upload_log = UploadLogCreate(
                        layer_name=resolved_layer_name,
                        file_format=file_format,
                        data_type=DataType.UNKNOWN,
                        crs="UNKNOWN",
                        bbox=None,
                        source_path=os.fspath(stored_path),
                        geoserver_layer=None,
                        tags=request.tags,
                        uploaded_by=request.uploaded_by.strip(),
                    )
                    created_log = UploadLogService.create_with_id(upload_log, self.db, dataset_id)
                    upload_log_id = created_log.id
                    logger.info(f"Created upload log with id: {upload_log_id}")
                    
                    # Re-open file for processing
                    file_handle = stored_path.open("rb")
                    upload_file = SimpleNamespace(file=file_handle, filename=stored_path.name)
                else:
                    upload_file = file

                # Call create_table_and_insert1
                message = await UploadLogService.create_table_and_insert1(
                    table_name=request.table_name,
                    schema=request.db_schema,
                    file=upload_file,
                    db=self.db,
                    geo_service=self.geo_service,
                    workspace=request.workspace,
                    store_name=request.store_name,
                    dataset_id=dataset_id,
                    upload_log_id=upload_log_id,
                )
                logger.info(f"Table creation result: {message}")

                # Update geoserver_layer in upload log if it exists
                if upload_log_id:
                    try:
                        UploadLogDAO.update_geoserver_layer(
                            upload_log_id, request.table_name, self.db
                        )
                    except Exception as exc:
                        logger.warning(f"Failed to update geoserver_layer: {exc}")

            finally:
                # Close file handle if we opened it
                if hasattr(upload_file, 'file') and hasattr(upload_file.file, 'closed') and not upload_file.file.closed:
                    upload_file.file.close()

            # Step 2: Configure GeoServer layer (SRS, bounding boxes, )
            logger.info(f"Step 2: Configuring GeoServer layer {request.table_name}")
            layer_name = request.layer_name or request.table_name
            store_name = request.store_name or f"{request.table_name}_store"
            
            try:
                await self._configure_geoserver_layer(
                    workspace=request.workspace,
                    datastore=store_name,
                    layer_name=layer_name,
                )
            except Exception as exc:
                logger.error(f"Failed to configure GeoServer layer: {exc}")
                # Continue even if configuration fails

            # Step 3: Create metadata entry
            logger.info(f"Step 3: Creating metadata entry")
            try:
                geoserver_layer_name = f"{request.workspace}:{layer_name}"
                metadata_input = MetadataInput(
                    dataset_id=dataset_id,
                    geoserver_name=geoserver_layer_name,
                    name_of_dataset=request.name_of_dataset,
                    theme=request.theme,
                    keywords=request.keywords or request.tags,  # Map tags to keywords if available
                    purpose_of_creating_data=request.purpose_of_creating_data,
                    access_constraints=request.access_constraints,
                    use_constraints=request.use_constraints,
                    data_type=request.data_type,
                    contact_person=request.contact_person,
                    organization=request.organization,
                    mailing_address=request.mailing_address,
                    city_locality_country=request.city_locality_country,
                    country=request.country,
                    contact_email=request.contact_email,
                )
                created_metadata = MetadataService.create_gql(metadata_input, self.db)
                metadata_id = created_metadata.id
                logger.info(f"Created metadata with id: {metadata_id}")
            except Exception as exc:
                logger.error(f"Failed to create metadata: {exc}")
                # Continue even if metadata creation fails

            # Step 4: Generate styles for each configured column
            logger.info(f"Step 4: Generating styles for {len(request.style_configs)} columns")
            for style_config in request.style_configs:
                try:
                    style_request = StyleGenerateRequest(
                        layer_table_name=request.table_name,
                        workspace=request.workspace,
                        color_by=style_config.color_by,
                        data_source=request.data_source,
                        layer_type=style_config.layer_type,
                        classification_method=style_config.classification_method,
                        num_classes=style_config.num_classes,
                        color_palette=style_config.color_palette,
                        custom_colors=style_config.custom_colors,
                        manual_breaks=style_config.manual_breaks,
                        publish_to_geoserver=request.publish_styles_to_geoserver,
                        attach_to_layer=request.attach_styles_to_layer,
                        user_id=request.user_id,
                        user_email=request.user_email,
                    )
                    
                    style_result = self.style_service.generate_style(
                        style_request, request.db_schema
                    )
                    
                    if style_result.success:
                        styles_created.append({
                            "color_by": style_config.color_by,
                            "style_name": style_result.style_name,
                            "success": True,
                        })
                        logger.info(f"Successfully created style for column {style_config.color_by}")
                    else:
                        styles_created.append({
                            "color_by": style_config.color_by,
                            "error": style_result.message,
                            "success": False,
                        })
                        logger.warning(f"Failed to create style for column {style_config.color_by}: {style_result.message}")
                except Exception as exc:
                    logger.error(f"Error generating style for column {style_config.color_by}: {exc}")
                    styles_created.append({
                        "color_by": style_config.color_by,
                        "error": str(exc),
                        "success": False,
                    })

            return RegisterDatasetResponse(
                success=True,
                message=f"Dataset '{request.name_of_dataset}' registered successfully",
                dataset_id=dataset_id,
                upload_log_id=upload_log_id,
                table_name=request.table_name,
                layer_name=layer_name,
                workspace=request.workspace,
                metadata_id=metadata_id,
                styles_created=styles_created,
            )

        except Exception as e:
            logger.error(f"Error registering dataset: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Failed to register dataset: {str(e)}")

    async def register_shapefile(
        self,
        request: RegisterShapefileRequest,
        file: UploadFile,
    ) -> RegisterShapefileResponse:
        """
        Main orchestration method to register a complete shapefile.
        
        Steps:
        1. Call the existing upload API logic (exact same as upload_log/api/api.py upload_dataset)
        2. Configure GeoServer layer (SRS, bounding boxes, )
        3. Create metadata entry
        4. Generate styles for each configured column
        """
        try:
            dataset_id = uuid4()
            upload_log_id = None
            metadata_id = None
            styles_created = []
            actual_feature_type_name = None
            geoserver_layer_name = None
            resolved_store_name = None

            # Step 1: Replicate the exact upload API flow (upload_log/api/api.py:59-124)
            logger.info(f"Step 1: Uploading shapefile using existing upload logic")
            stored_path = await persist_upload(file, UPLOADS_DIR)

            try:
                # Extract metadata from shapefile (same as upload API)
                try:
                    metadata = derive_file_metadata(stored_path)
                except Exception as exc:
                    logger.error(f"Failed to derive metadata for {stored_path}: {exc}")
                    stored_path.unlink(missing_ok=True)
                    raise HTTPException(status_code=400, detail="Unable to read spatial metadata") from exc

                # Resolve store_name: use provided store_name, or metadata layer_name, or filename stem
                # This store_name will be used as the GeoServer datastore name
                resolved_store_name = request.store_name or metadata.get("layer_name") or Path(file.filename).stem
                
                # Extract the actual layer name from the zip file if it's a zip
                # This will be used as the feature type name in GeoServer
                actual_layer_name = None
                if stored_path.suffix.lower() == '.zip':
                    actual_layer_name = extract_shapefile_name_from_zip(stored_path)
                    logger.info(f"Extracted layer name from zip file: '{actual_layer_name}' (file: {stored_path})")
                
                # Also check what metadata says (from fiona, which reads the shapefile's internal name)
                metadata_layer_name = metadata.get("layer_name")
                logger.info(f"Layer name from metadata (fiona): '{metadata_layer_name}'")
                
                # If we couldn't extract from zip, use metadata layer_name (which comes from the shapefile)
                # or fall back to resolved_store_name
                if not actual_layer_name:
                    actual_layer_name = metadata_layer_name or resolved_store_name
                    logger.info(f"Using layer name: '{actual_layer_name}' (from metadata or fallback)")
                
                # Log warning if zip extraction and metadata don't match
                if stored_path.suffix.lower() == '.zip' and actual_layer_name and metadata_layer_name:
                    if actual_layer_name != metadata_layer_name:
                        logger.warning(
                            f"Shapefile name mismatch: zip filename='{actual_layer_name}', fiona layer name='{metadata_layer_name}'. Using zip filename."
                        )
                
                data_type = metadata.get("data_type") or DataType.UNKNOWN
                file_format = metadata.get("file_format") or stored_path.suffix.lstrip(".")

                # Create upload log (same as upload API)
                uploaded_by = request.uploaded_by.strip() if request.uploaded_by and request.uploaded_by.strip() else "system"
                upload_log = UploadLogCreate(
                    store_name=resolved_store_name,
                    file_format=file_format,
                    data_type=data_type,
                    crs=metadata.get("crs"),
                    bbox=metadata.get("bbox"),
                    source_path=os.fspath(stored_path),
                    geoserver_layer=None,
                    tags=request.tags,
                    uploaded_by=uploaded_by,
                )

                created_log = UploadLogService.create_with_id(upload_log, self.db, dataset_id)
                upload_log_id = created_log.id
                logger.info(f"Created upload log with id: {upload_log_id}")

                # Step 2: Publish to GeoServer (call the exact same _publish_to_geoserver logic)
                logger.info(f"Step 2: Publishing shapefile to GeoServer")
                await self._publish_to_geoserver(created_log, self.db)
                
                # Get the actual feature type name from the updated upload log
                # Query the database to get the updated record (since created_log is a Pydantic model)
                updated_db_record = UploadLogDAO.get_by_id(created_log.id, self.db)
                if updated_db_record and updated_db_record.geoserver_layer:
                    geoserver_layer_name = updated_db_record.geoserver_layer
                    # Extract feature type name from "workspace:feature_type_name"
                    if ":" in geoserver_layer_name:
                        actual_feature_type_name = geoserver_layer_name.split(":")[1]
                    else:
                        actual_feature_type_name = geoserver_layer_name
                    logger.info(f"GeoServer layer created: {geoserver_layer_name}, feature type: {actual_feature_type_name}")
                else:
                    # Fallback: use the resolved layer name
                    actual_feature_type_name = actual_layer_name or resolved_store_name
                    geoserver_layer_name = f"{request.workspace}:{actual_feature_type_name}"
                    logger.warning(f"GeoServer layer name not found in upload log, using: {geoserver_layer_name}")

                # Step 3: Configure GeoServer layer (SRS, bounding boxes, )
                if file_format.lower() == "shp" and actual_feature_type_name:
                    logger.info(f"Step 3: Configuring GeoServer layer {actual_feature_type_name}")
                    try:
                        await self._configure_geoserver_layer(
                            workspace=request.workspace,
                            datastore=resolved_store_name,
                            layer_name=actual_feature_type_name,
                        )
                    except Exception as exc:
                        logger.error(f"Failed to configure GeoServer layer: {exc}")
                        # Continue even if configuration fails

                # Step 4: Create metadata entry
                logger.info(f"Step 4: Creating metadata entry")
                try:
                    metadata_input = MetadataInput(
                        dataset_id=dataset_id,
                        geoserver_name=geoserver_layer_name or f"{request.workspace}:{actual_feature_type_name}",
                        name_of_dataset=request.name_of_dataset,
                        theme=request.theme,
                        keywords=request.keywords or request.tags,
                        purpose_of_creating_data=request.purpose_of_creating_data,
                        access_constraints=request.access_constraints,
                        use_constraints=request.use_constraints,
                        data_type=request.data_type or str(data_type),
                        contact_person=request.contact_person,
                        organization=request.organization,
                        mailing_address=request.mailing_address,
                        city_locality_country=request.city_locality_country,
                        country=request.country,
                        contact_email=request.contact_email,
                    )
                    created_metadata = MetadataService.create_gql(metadata_input, self.db)
                    metadata_id = created_metadata.id
                    logger.info(f"Created metadata with id: {metadata_id}")
                except Exception as exc:
                    logger.error(f"Failed to create metadata: {exc}")
                    # Continue even if metadata creation fails

                # Step 5: Generate styles for each configured column
                logger.info(f"Step 5: Generating styles for {len(request.style_configs)} columns")
                for style_config in request.style_configs:
                    try:
                        # For shapefiles, layer_table_name should be the feature type name
                        style_request = StyleGenerateRequest(
                            layer_table_name=actual_feature_type_name,
                            workspace=request.workspace,
                            color_by=style_config.color_by,
                            data_source=request.data_source,  # Should be GEOSERVER for shapefiles
                            layer_type=style_config.layer_type,
                            classification_method=style_config.classification_method,
                            num_classes=style_config.num_classes,
                            color_palette=style_config.color_palette,
                            custom_colors=style_config.custom_colors,
                            manual_breaks=style_config.manual_breaks,
                            publish_to_geoserver=request.publish_styles_to_geoserver,
                            attach_to_layer=request.attach_styles_to_layer,
                            user_id=request.user_id,
                            user_email=request.user_email,
                        )
                        
                        # For shapefiles, we don't have a db_schema, so pass None
                        style_result = self.style_service.generate_style(
                            style_request, schema=None
                        )
                        
                        if style_result.success:
                            styles_created.append({
                                "color_by": style_config.color_by,
                                "style_name": style_result.style_name,
                                "success": True,
                            })
                            logger.info(f"Successfully created style for column {style_config.color_by}")
                        else:
                            styles_created.append({
                                "color_by": style_config.color_by,
                                "error": style_result.message,
                                "success": False,
                            })
                            logger.warning(f"Failed to create style for column {style_config.color_by}: {style_result.message}")
                    except Exception as exc:
                        logger.error(f"Error generating style for column {style_config.color_by}: {exc}")
                        styles_created.append({
                            "color_by": style_config.color_by,
                            "error": str(exc),
                            "success": False,
                        })

                return RegisterShapefileResponse(
                    success=True,
                    message=f"Shapefile '{request.name_of_dataset}' registered successfully",
                    dataset_id=dataset_id,
                    upload_log_id=upload_log_id,
                    store_name=resolved_store_name,
                    layer_name=actual_feature_type_name or resolved_store_name,
                    workspace=request.workspace,
                    metadata_id=metadata_id,
                    styles_created=styles_created,
                )

            finally:
                # Cleanup if needed
                pass

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error registering shapefile: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Failed to register shapefile: {str(e)}")

    async def _publish_to_geoserver(
        self,
        upload_log: UploadLogOut,
        db: Session,
    ) -> None:
        """
        Publish shapefile to GeoServer. This replicates the exact logic from upload_log/api/api.py _publish_to_geoserver.
        """
        logger.debug(f"_publish_to_geoserver called for file_format: {upload_log.file_format}")
        
        if not upload_log.file_format or upload_log.file_format.lower() != "shp":
            logger.info(f"Skipping GeoServer publication for file format: {upload_log.file_format}")
            return

        # Validate file path
        file_path = Path(upload_log.source_path)
        if not file_path.exists() or not file_path.is_file():
            raise HTTPException(
                status_code=500,
                detail=f"Stored upload file is missing or invalid: {file_path}",
            )

        # The store_name from upload_log.layer_name is used as the store_name (datastore) in GeoServer.
        # However, GeoServer will create a feature type with the name from the shapefile inside the zip.
        # We should use the shapefile name as the feature type name, not try to rename it.
        store_name = upload_log.layer_name  # This is the store_name (datastore) provided by the user
        logger.debug(f"Store name (datastore): {store_name}")
        
        # Extract the expected feature type name from zip or use store_name
        expected_feature_type_name = resolve_feature_type_name(file_path, store_name)
        logger.info(f"Publishing to GeoServer: workspace=metastring, store_name={store_name}, expected_feature_type_name={expected_feature_type_name}")
        
        # Clean up existing datastore and files to ensure clean upload
        datastore_path = f"{geoserver_data_dir}/metastring/{store_name}"
        try:
            datastore_response = self.geo_admin_service.get_datastore_details(
                workspace="metastring",
                datastore=store_name,
            )
            if datastore_response.status_code == 200:
                delete_ds_response = self.geo_admin_service.delete_datastore(
                    workspace="metastring",
                    datastore=store_name,
                )
                if delete_ds_response.status_code in (200, 204):
                    await cleanup_datastore_directory(datastore_path)
                    await asyncio.sleep(2)
            elif datastore_response.status_code == 404:
                await cleanup_datastore_directory(datastore_path)
        except Exception as check_exc:
            logger.debug(f"Exception checking/cleaning datastore: {check_exc}")
        
        # Upload shapefile to GeoServer
        file_path_str = str(file_path.resolve())
        created_feature_type_from_response = None
        try:
            response = self.geo_dao.upload_shapefile(
                workspace="metastring",
                store_name=store_name,
                file_path=file_path_str,
            )
            created_feature_type_from_response = get_feature_type_from_response(response)
            
            if response.status_code not in (200, 201, 202):
                logger.error(f"GeoServer upload failed: status {response.status_code}, {response.text[:500]}")
                raise HTTPException(status_code=response.status_code, detail=response.text)
            
            logger.info(f"GeoServer upload succeeded (status {response.status_code})")
            await wait_for_geoserver_processing(response.status_code)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except HTTPException:
            raise
        except Exception as exc:
            logger.error(f"Error uploading to GeoServer: {exc}")
            raise HTTPException(
                status_code=500,
                detail="Unexpected error occurred while publishing to GeoServer.",
            ) from exc
        
        # Reload datastore to force GeoServer to re-read files
        try:
            reload_response = self.geo_admin_service.reload_datastore(
                workspace="metastring",
                datastore=store_name,
            )
            if reload_response.status_code in (200, 201, 202):
                await asyncio.sleep(2)
        except Exception:
            pass
        
        # CRITICAL: After uploading the shapefile, check if feature type was auto-created
        # The upload should have used configure=all to trigger auto-creation
        # If not found, try reloading the datastore to trigger auto-discovery
        if expected_feature_type_name:
            logger.debug(f"Checking if feature type '{expected_feature_type_name}' was auto-created...")
            try:
                # Check if feature type already exists
                ft_check_response = self.geo_admin_service.get_feature_type_details(
                    workspace="metastring",
                    datastore=store_name,
                    feature_type=expected_feature_type_name,
                )
                
                if ft_check_response.status_code == 200:
                    logger.info(f"✓ Feature type '{expected_feature_type_name}' already exists in datastore '{store_name}'")
                    # Even if it exists, trigger bounding box recalculation to ensure they're correct
                    logger.info(f"Triggering bounding box recalculation for existing feature type '{expected_feature_type_name}'...")
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
                            recalc_response = self.geo_admin_service.update_feature_type(
                                workspace="metastring",
                                datastore=store_name,
                                feature_type=expected_feature_type_name,
                                config=update_config,
                                recalculate=True,
                            )
                            if recalc_response.status_code in (200, 201):
                                logger.info(f"✓ Successfully recalculated bounding boxes for existing feature type")
                    except Exception as recalc_exc:
                        logger.debug(f"Could not recalculate bounding boxes for existing feature type: {recalc_exc}")
                else:
                    # Feature type doesn't exist - explicitly create it with schema from shapefile
                    # The files are already uploaded, we need to read the schema and create the feature type
                    logger.debug(f"Feature type '{expected_feature_type_name}' not found (status: {ft_check_response.status_code}). Creating it explicitly with schema from shapefile...")
                    
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
                                    logger.info(f"✓ Read {len(attributes)} attributes from shapefile schema (CRS: {shapefile_crs or 'unknown'}, BBox: {shapefile_bbox})")
                                else:
                                    logger.warning("⚠ Could not read attributes from shapefile, will try without them")
                            else:
                                logger.warning("⚠ Could not read schema from shapefile")
                            # Clean up temp directory
                            temp_dir = temp_shp_path.parent
                            shutil.rmtree(temp_dir, ignore_errors=True)
                        else:
                            logger.warning("⚠ Could not extract shapefile from zip to read schema")
                    except Exception as schema_exc:
                        logger.warning(f"⚠ Failed to read shapefile schema: {schema_exc}. Will try creating feature type without attributes.")
                    
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
                    logger.info(f"Deleting any existing feature type '{expected_feature_type_name}' to ensure clean creation...")
                    try:
                        delete_response = self.geo_admin_service.delete_feature_type(
                            workspace="metastring",
                            datastore=store_name,
                            feature_type=expected_feature_type_name,
                        )
                        if delete_response.status_code in (200, 404):  # 404 means it didn't exist, which is fine
                            logger.info("✓ Deleted existing feature type (or it didn't exist)")
                            await asyncio.sleep(2)
                        else:
                            logger.warning(f"⚠ Could not delete existing feature type: status {delete_response.status_code}")
                    except Exception as delete_exc:
                        logger.warning(f"⚠ Exception while deleting existing feature type: {delete_exc}")
                    
                    # CRITICAL: For shapefiles, we should NOT specify attributes explicitly
                    logger.info("Creating feature type - GeoServer will auto-discover attributes from shapefile data")
                    
                    try:
                        # Try creating WITHOUT attributes first - let GeoServer auto-discover
                        create_ft_response = self.geo_admin_service.create_feature_type_from_shapefile(
                            workspace="metastring",
                            datastore=store_name,
                            shapefile_name=expected_feature_type_name,  # nativeName (the actual shapefile name)
                            feature_type_name=expected_feature_type_name,  # display name
                            enabled=True,
                            attributes=None,  # Don't specify attributes - let GeoServer auto-discover from shapefile
                            srs=srs,  # Pass the SRS if available
                            native_bbox=native_bbox  # Pass the bounding box explicitly
                        )
                        
                        # If that fails with "no attributes", try with attributes as fallback
                        if create_ft_response.status_code == 400 and "attributes" in (create_ft_response.text or "").lower():
                            logger.debug("GeoServer requires attributes. Creating with explicit attributes, then will remove them to force data read...")
                            create_ft_response = self.geo_admin_service.create_feature_type_from_shapefile(
                                workspace="metastring",
                                datastore=store_name,
                                shapefile_name=expected_feature_type_name,
                                feature_type_name=expected_feature_type_name,
                                enabled=True,
                                attributes=attributes,  # Fallback: use attributes we read
                                srs=srs,
                                native_bbox=native_bbox
                            )
                            
                            # If creation succeeded with attributes, we need to remove them and recalculate
                            if create_ft_response.status_code in (200, 201):
                                logger.info("Feature type created with attributes. Now removing attributes to force GeoServer to read from shapefile...")
                                await asyncio.sleep(2)
                                
                                # Get current config and remove attributes
                                ft_details = self.geo_admin_service.get_feature_type_details(
                                    workspace="metastring",
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
                                        remove_attrs_response = self.geo_admin_service.update_feature_type(
                                            workspace="metastring",
                                            datastore=store_name,
                                            feature_type=expected_feature_type_name,
                                            config=update_config_no_attrs,
                                            recalculate=True,  # This will force GeoServer to read from shapefile
                                        )
                                        
                                        if remove_attrs_response.status_code in (200, 201):
                                            logger.info("✓ Removed attributes and triggered recalculation - GeoServer will now read from shapefile")
                                            await asyncio.sleep(3)
                                        else:
                                            logger.warning(f"⚠ Failed to remove attributes: {remove_attrs_response.status_code}")
                        
                        # If feature type created successfully, trigger recalculation
                        if create_ft_response.status_code in (200, 201):
                            logger.info(f"✓ Successfully created feature type '{expected_feature_type_name}'")
                            await asyncio.sleep(3)
                            
                            # Trigger bounding box recalculation
                            try:
                                ft_details = self.geo_admin_service.get_feature_type_details(
                                    workspace="metastring",
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
                                        recalc_response = self.geo_admin_service.update_feature_type(
                                            workspace="metastring",
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
                            logger.error(f"✗ Failed to create feature type '{expected_feature_type_name}': status {create_ft_response.status_code}, response: {create_ft_response.text[:500] if create_ft_response.text else 'No response'}")
                    except Exception as create_exc:
                        logger.error(f"✗ Exception while creating feature type: {create_exc}", exc_info=True)
            except Exception as reload_exc:
                logger.error(f"✗ Exception while checking/reloading datastore: {reload_exc}", exc_info=True)
        
        # Determine actual feature type name (priority: response > expected > store_name)
        actual_feature_type_name = created_feature_type_from_response or expected_feature_type_name or store_name
        
        # Verify the feature type exists, or find the actual one created
        if not created_feature_type_from_response and expected_feature_type_name:
            try:
                ft_check = self.geo_admin_service.get_feature_type_details(
                    workspace="metastring",
                    datastore=store_name,
                    feature_type=expected_feature_type_name,
                )
                if ft_check.status_code != 200:
                    # Try to find it by listing all feature types
                    ft_list = self.geo_admin_service.list_datastore_tables(
                        workspace="metastring",
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
        try:
            normalized_crs = normalize_crs_to_epsg(upload_log.crs)
            if normalized_crs:
                # Try to update SRS using the actual feature type name
                try:
                    # Get the current feature type config to preserve nativeName
                    ft_details_response = self.geo_admin_service.get_feature_type_details(
                        workspace="metastring",
                        datastore=store_name,
                        feature_type=actual_feature_type_name,
                    )
                    
                    if ft_details_response.status_code == 200:
                        ft_config = ft_details_response.json()
                        if isinstance(ft_config, dict) and "featureType" in ft_config:
                            # Preserve the nativeName (points to the actual shapefile name in the datastore)
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
                            update_response = self.geo_admin_service.update_feature_type(
                                workspace="metastring",
                                datastore=store_name,
                                feature_type=actual_feature_type_name,
                                config=update_config,
                                recalculate=True,
                            )
                            if update_response.status_code in (200, 201):
                                logger.info(f"Successfully updated SRS for feature type {actual_feature_type_name} (nativeName: {native_name}) to {normalized_crs}")
                            else:
                                logger.debug(f"SRS update returned status {update_response.status_code} for layer {actual_feature_type_name}")
                        else:
                            logger.debug("Could not parse feature type config for SRS update")
                    else:
                        logger.debug(f"Could not get feature type details for SRS update. Status: {ft_details_response.status_code}")
                except Exception as srs_exc:
                    logger.debug(f"Could not update SRS for layer {actual_feature_type_name}: {srs_exc}")
        except Exception as exc:
            logger.debug(f"Error updating SRS for layer {actual_feature_type_name}: {exc}. Continuing.")

        # Fix subdirectory files if needed
        datastore_path = f"{geoserver_data_dir}/metastring/{store_name}"
        if os.path.exists(datastore_path):
            if fix_subdirectory_files(datastore_path, expected_feature_type_name):
                try:
                    reload_response = self.geo_admin_service.reload_datastore(
                        workspace="metastring",
                        datastore=store_name,
                    )
                    if reload_response.status_code in (200, 201, 202):
                        await asyncio.sleep(2)
                except Exception:
                    pass
        
        # Final verification - check if layer has features
        geoserver_layer_name = f"metastring:{actual_feature_type_name}"
        await asyncio.sleep(2)
        
        verification_passed, feature_count = verify_layer_features(geoserver_layer_name)
        if verification_passed:
            logger.info(f"✓ Layer verification passed: '{geoserver_layer_name}' has {feature_count or 0} features")
        elif feature_count == 0:
            logger.warning(f"⚠ Layer created but has 0 features. Check datastore directory: {datastore_path}")
        
        try:
            db_record = UploadLogDAO.get_by_id(upload_log.id, db)
            if db_record:
                # Use the actual feature type name (from GeoServer, which should match the shapefile name from zip)
                # Format: workspace:feature_type_name (e.g., "metastring:lyr_3_agar_soil")
                db_record.geoserver_layer = geoserver_layer_name
                db.add(db_record)
                db.commit()
                db.refresh(db_record)
                upload_log.geoserver_layer = geoserver_layer_name
                
                logger.info(
                    f"Updated upload log {upload_log.id} with GeoServer layer name: {geoserver_layer_name} (store_name/datastore: {store_name}, feature_type: {actual_feature_type_name})"
                )
        except Exception as exc:
            logger.error(
                f"Failed to update GeoServer publication details for upload log {upload_log.id}: {exc}"
            )
            db.rollback()
            raise HTTPException(
                status_code=500,
                detail="GeoServer upload succeeded, but updating the upload log failed.",
            ) from exc


########################################### Helper Methods ###########################################
######################################## Geoserver tilecatching layer configuration ###########################################
    async def _configure_geoserver_layer(
        self,
        workspace: str,
        datastore: str,
        layer_name: str,
    ):
        """
        Configure GeoServer layer with:
        - Native SRS → Declared SRS
        - Compute Native Bounding Box from Data
        - Compute Lat/lon Bounding Box from native bounds
        """
        try:
            # Get current feature type configuration
            ft_response = self.geo_admin_service.get_feature_type_details(
                workspace, datastore, layer_name
            )
            
            if ft_response.status_code != 200:
                logger.warning(f"Could not get feature type details: {ft_response.text}")
                return

            ft_config = ft_response.json()
            feature_type = ft_config.get("featureType", {})

            # Get native SRS
            native_srs = feature_type.get("srs")
            if not native_srs:
                # Try to get from nativeBoundingBox
                native_bbox = feature_type.get("nativeBoundingBox", {})
                native_srs = native_bbox.get("crs", "EPSG:4326")
            
            # Update configuration
            # Set Declared SRS = Native SRS
            updated_config = {
                "featureType": {
                    "name": layer_name,
                    "nativeName": layer_name,
                    "srs": native_srs,  # Declared SRS = Native SRS
                    "nativeSRS": native_srs,
                    "projectionPolicy": "FORCE_DECLARED",  # Force declared SRS
                    "enabled": feature_type.get("enabled", True),
                }
            }

            # For bounding boxes, we'll trigger recalculation by omitting them from the config
            # and using the recalculate query parameter in the PUT request
            # GeoServer will recalculate bounding boxes when the recalculate parameter is provided

            # Update feature type with recalculate parameter to trigger bounding box recalculation
            update_response = self.geo_admin_service.update_feature_type(
                workspace, datastore, layer_name, updated_config, recalculate=True
            )

            if update_response.status_code not in [200, 201]:
                logger.warning(f"Failed to update feature type: {update_response.text}")
            else:
                logger.info(f"Successfully configured feature type {layer_name} and triggered bounding box recalculation")

        except Exception as exc:
            logger.error(f"Error configuring GeoServer layer: {exc}", exc_info=True)
            raise

