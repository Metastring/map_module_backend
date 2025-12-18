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
from geoserver.admin.service import GeoServerAdminService
from geoserver.admin.dao import GeoServerAdminDAO
from utils.config import host, port, username, password, database, geoserver_host, geoserver_port, geoserver_username, geoserver_password

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
                        
                        logger.info(f"Creating layer '{table_name}' from table '{table_name}'")
                        layer_response = await admin_service.create_layer_from_table(layer_request)
                        
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
                + geometry_mapping_message
                + geoserver_message
            )
        except Exception as e:
            logger.error("Error in create_table_and_insert1: %s", e)
            raise e

