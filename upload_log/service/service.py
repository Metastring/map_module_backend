from typing import List, Optional
import logging
import io
import pandas as pd
import uuid
from uuid import UUID
from fastapi import UploadFile, HTTPException

from sqlalchemy.orm import Session

from upload_log.models.model import DataType, UploadLogCreate, UploadLogFilter, UploadLogOut
from upload_log.models.schema import UploadLog as UploadLogTable
from upload_log.dao.dao import UploadLogDAO
from geoserver.model import PostGISRequest, CreateLayerRequest
from geoserver.service import GeoServerService
from utils.config import host, port, username, password, database

logger = logging.getLogger(__name__)


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
        if hasattr(file, 'file') and hasattr(file, 'filename'):
            # It's a SimpleNamespace with file attribute (from API)
            contents = file.file.read()
        elif hasattr(file, 'read'):
            # It's an UploadFile or file-like object
            try:
                # Try async read first
                contents = await file.read()
            except (TypeError, AttributeError):
                # Fallback for sync file objects
                contents = file.read()
        else:
            raise ValueError("Invalid file object provided")

        try:
            # Use provided dataset_id or generate a new one
            if dataset_id is None:
                dataset_id = uuid.uuid4()
                logger.info(f"Generated dataset_id: {dataset_id} for table {schema}.{table_name}")
            else:
                logger.info(f"Using provided dataset_id: {dataset_id} for table {schema}.{table_name}")

            # Read the Excel file into a Pandas DataFrame
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

            # Step 4: Map geometry from world_geojson table
            logger.info(f"Mapping geometry from world_geojson to table {schema}.{table_name}")
            rows_updated = UploadLogDAO.map_geometry_from_world_geojson(table_name, schema, db)
            logger.info(f"Updated {rows_updated} rows with geometry data")

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
                        # Create layer from table
                        layer_request = CreateLayerRequest(
                            workspace=workspace,
                            store_name=final_store_name,
                            table_name=table_name,
                            layer_name=table_name
                        )
                        
                        logger.info(f"Creating layer '{table_name}' from table '{table_name}'")
                        layer_response = await geo_service.create_layer_from_table(layer_request)
                        
                        if layer_response.status_code in [200, 201]:
                            geoserver_message = f" Successfully uploaded to GeoServer workspace '{workspace}' as layer '{table_name}'."
                        else:
                            geoserver_message = f" PostGIS datastore created but layer creation failed: {layer_response.text}"
                    else:
                        geoserver_message = f" GeoServer upload failed: {response.text}"
                except Exception as geoserver_error:
                    logger.error(f"Error uploading to GeoServer: {geoserver_error}")
                    geoserver_message = f" GeoServer upload encountered an error: {str(geoserver_error)}"

            return (
                f"Table '{table_name}' created in schema '{schema}' and data inserted successfully. "
                f"Geometry column added and mapped from world_geojson ({rows_updated} rows updated)."
                + geoserver_message
            )
        except Exception as e:
            logger.error("Error in create_table_and_insert1: %s", e)
            raise e

