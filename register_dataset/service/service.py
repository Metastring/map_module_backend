import logging
import requests
from typing import Optional, List, Dict, Any
from uuid import UUID, uuid4
from datetime import datetime
from sqlalchemy.orm import Session
from fastapi import HTTPException, UploadFile
from types import SimpleNamespace
from register_dataset.model.model import (
    RegisterDatasetRequest,
    RegisterDatasetResponse,
    StyleConfigForColumn,
)
from upload_log.service.service import UploadLogService
from upload_log.dao.dao import UploadLogDAO
from geoserver.service import GeoServerService
from geoserver.admin.service import GeoServerAdminService
from geoserver.admin.dao import GeoServerAdminDAO
from geoserver.dao import GeoServerDAO
from metadata.service.service import MetadataService
from metadata.models.model import MetadataInput
from styles.service.style_service import StyleService
from styles.models.model import StyleGenerateRequest, DataSource
from utils.config import (
    geoserver_host,
    geoserver_port,
    geoserver_username,
    geoserver_password,
)

logger = logging.getLogger(__name__)


class RegisterDatasetService:
    """Service for registering complete datasets with upload, metadata, and styling."""

    def __init__(
        self,
        db: Session,
        geo_service: GeoServerService,
        geo_admin_service: GeoServerAdminService,
        style_service: StyleService,
    ):
        self.db = db
        self.geo_service = geo_service
        self.geo_admin_service = geo_admin_service
        self.style_service = style_service

    async def register_dataset(
        self,
        request: RegisterDatasetRequest,
        file: UploadFile,
    ) -> RegisterDatasetResponse:
        """
        Main orchestration method to register a complete dataset.
        
        Steps:
        1. Call create-table-and-insert1 to upload and create table
        2. Configure GeoServer layer (SRS, bounding boxes, tile caching)
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
            from upload_log.api.api import _persist_upload
            stored_path = await _persist_upload(file)
            
            try:
                # Create upload log if uploaded_by is provided
                if request.uploaded_by and request.uploaded_by.strip():
                    from upload_log.models.model import UploadLogCreate, DataType
                    import os
                    
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

            # Step 2: Configure GeoServer layer (SRS, bounding boxes, tile caching)
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
        - Configure Tile Caching (formats and gridset)
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

            # Configure tile caching
            # Configure GeoWebCache (GWC) tile formats and gridset
            try:
                tile_formats = [
                    "application/json;type=geojson",
                    "application/json;type=topojson",
                    "application/vnd.mapbox-vector-tile"
                ]
                tile_cache_response = self.geo_admin_service.configure_layer_tile_caching(
                    workspace=workspace,
                    layer_name=layer_name,
                    tile_formats=tile_formats,
                    gridset="EPSG:3857"
                )
                if tile_cache_response.status_code in [200, 201]:
                    logger.info(
                        f"Successfully configured tile caching for layer {workspace}:{layer_name} "
                        f"with formats: {tile_formats} and gridset: EPSG:3857"
                    )
                else:
                    logger.warning(
                        f"Could not configure tile caching for {workspace}:{layer_name}: "
                        f"{tile_cache_response.status_code} - {tile_cache_response.text}"
                    )
            except Exception as tile_exc:
                logger.warning(
                    f"Error configuring tile caching for {workspace}:{layer_name}: {tile_exc}"
                )

        except Exception as exc:
            logger.error(f"Error configuring GeoServer layer: {exc}", exc_info=True)
            raise

