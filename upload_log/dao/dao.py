from datetime import datetime
from typing import List, Optional
import uuid
from uuid import UUID
import logging
from sqlalchemy.orm import Session
from sqlalchemy import func, MetaData, Table, Column, Integer, Float, String, text
from sqlalchemy.dialects.postgresql import UUID as PostgresUUID
from sqlalchemy.exc import SQLAlchemyError

from upload_log.models.schema import UploadLog as UploadLogTable

logger = logging.getLogger(__name__)


def _quote_identifier(identifier: str) -> str:
    """
    Safely quote a PostgreSQL identifier to prevent SQL injection.
    Escapes any double quotes in the identifier and wraps it in double quotes.
    """
    # Replace any double quotes with double double quotes (PostgreSQL escaping)
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


class UploadLogDAO:
    @staticmethod
    def create(upload_log: UploadLogTable, db: Session) -> Optional[UploadLogTable]:
        """Create a new upload log record."""
        try:
            logger.info(
                "Adding upload log to session: id=%s, store_name=%s, uploaded_by=%s",
                upload_log.id,
                upload_log.layer_name,
                upload_log.uploaded_by,
            )
            db.add(upload_log)
            logger.info(f"Committing upload log to database: id={upload_log.id}")
            db.commit()
            logger.info(f"Refreshing upload log from database: id={upload_log.id}")
            db.refresh(upload_log)
            logger.info(f"Successfully created upload log record: id={upload_log.id}")
            return upload_log
        except Exception as exc:
            logger.error("Error creating upload log record: %s", exc, exc_info=True)
            db.rollback()
            raise

    @staticmethod
    def get_by_id(log_id: UUID, db: Session) -> Optional[UploadLogTable]:
        """Get upload log record by ID."""
        try:
            return db.query(UploadLogTable).filter(UploadLogTable.id == log_id).first()
        except Exception as exc:
            logger.error("Error fetching upload log by id %s: %s", log_id, exc)
            raise

    @staticmethod
    def get_filtered(filters, db: Session) -> List[UploadLogTable]:
        """Get upload log records filtered by the provided criteria."""
        try:
            query = db.query(UploadLogTable)

            if filters:
                if getattr(filters, "id", None):
                    query = query.filter(UploadLogTable.id == filters.id)
                if getattr(filters, "layer_name", None):
                    query = query.filter(UploadLogTable.layer_name.ilike(f"%{filters.layer_name}%"))
                if getattr(filters, "file_format", None):
                    query = query.filter(UploadLogTable.file_format.ilike(f"%{filters.file_format}%"))
                data_type_value = getattr(filters, "data_type", None)
                if data_type_value:
                    if hasattr(data_type_value, "value"):
                        data_type_value = data_type_value.value
                    query = query.filter(UploadLogTable.data_type.ilike(f"%{data_type_value}%"))
                if getattr(filters, "crs", None):
                    query = query.filter(UploadLogTable.crs.ilike(f"%{filters.crs}%"))
                if getattr(filters, "bbox", None):
                    query = query.filter(UploadLogTable.bbox == filters.bbox)
                if getattr(filters, "source_path", None):
                    query = query.filter(UploadLogTable.source_path.ilike(f"%{filters.source_path}%"))
                if getattr(filters, "geoserver_layer", None):
                    query = query.filter(UploadLogTable.geoserver_layer.ilike(f"%{filters.geoserver_layer}%"))
                if getattr(filters, "tags", None):
                    query = query.filter(UploadLogTable.tags.contains(filters.tags))
                if getattr(filters, "uploaded_by", None):
                    query = query.filter(UploadLogTable.uploaded_by == filters.uploaded_by)
                if getattr(filters, "uploaded_on", None):
                    uploaded_on = filters.uploaded_on
                    if isinstance(uploaded_on, datetime):
                        query = query.filter(func.date(UploadLogTable.uploaded_on) == uploaded_on.date())
                    else:
                        query = query.filter(UploadLogTable.uploaded_on == uploaded_on)

            return query.all()
        except Exception as exc:
            logger.error("Error fetching filtered upload logs: %s", exc)
            raise

    @staticmethod
    def update_geoserver_layer(log_id: UUID, geoserver_layer: str, db: Session) -> Optional[UploadLogTable]:
        """Update the geoserver_layer field for an upload log record."""
        try:
            db_record = UploadLogDAO.get_by_id(log_id, db)
            if db_record:
                db_record.geoserver_layer = geoserver_layer
                db.add(db_record)
                db.commit()
                db.refresh(db_record)
                logger.info(f"Updated geoserver_layer to '{geoserver_layer}' for upload log {log_id}")
                return db_record
            else:
                logger.warning(f"Upload log with id {log_id} not found for geoserver_layer update")
                return None
        except Exception as exc:
            logger.error("Error updating geoserver_layer for upload log %s: %s", log_id, exc)
            db.rollback()
            raise

    @staticmethod
    def create_table1(table_name: str, schema: str, df, db: Session):
        # Get the database engine
        engine = db.get_bind()

        # Create a new MetaData object with the specified schema
        metadata = MetaData(schema=schema)

        # Create columns dynamically based on the DataFrame's columns and dtypes
        columns = []
        # Add dataset_id column first (UUID type)
        columns.append(Column("dataset_id", PostgresUUID(as_uuid=True), nullable=False))
        
        for col_name, dtype in df.dtypes.items():
            if dtype in ["int64", "int32"]:
                columns.append(Column(col_name, Integer))
            elif dtype in ["float64", "float32"]:
                columns.append(Column(col_name, Float))
            else:
                columns.append(Column(col_name, String))

        # Define the table
        table = Table(table_name, metadata, *columns)

        # Create the table in the database
        metadata.create_all(engine)

    @staticmethod
    def insert_data_dynamic1(table_name: str, schema: str, df, db: Session, dataset_id: uuid.UUID):
        try:
            # Convert DataFrame to dictionaries
            data = df.to_dict(orient="records")

            # Add dataset_id to all rows
            for record in data:
                record["dataset_id"] = dataset_id

            # Dynamically map to the table
            engine = db.get_bind()
            metadata = MetaData(schema=schema)
            table = Table(table_name, metadata, autoload_with=engine)

            # Insert data into the table (bulk insert)
            if data:
                db.execute(table.insert(), data)
                db.commit()
        except SQLAlchemyError as e:
            logger.error("Error inserting data into table: %s", e)
            db.rollback()
            raise e

    @staticmethod
    def add_geometry_column(table_name: str, schema: str, db: Session):
        try:
            # Quote identifiers to prevent SQL injection
            quoted_schema = _quote_identifier(schema)
            quoted_table = _quote_identifier(table_name)
            
            # SQL to add geometry column with SRID 4326 as MULTIPOLYGON
            sql = text(f"""
                ALTER TABLE {quoted_schema}.{quoted_table} 
                ADD COLUMN IF NOT EXISTS geom geometry(MULTIPOLYGON, 4326)
            """)
            db.execute(sql)
            db.commit()
            logger.info(f"Geometry column 'geom' added to table {schema}.{table_name}")
        except SQLAlchemyError as e:
            logger.error(f"Error adding geometry column to table {schema}.{table_name}: {e}")
            db.rollback()
            raise e

    @staticmethod
    def map_geometry_from_world_geojson(table_name: str, schema: str, db: Session):
        try:
            # Quote identifiers to prevent SQL injection
            quoted_schema = _quote_identifier(schema)
            quoted_table = _quote_identifier(table_name)
            quoted_world_geojson = _quote_identifier("world_geojson")
            
            # SQL to update geometry from world_geojson table
            # Join on state = level_4_na (case-insensitive and whitespace-insensitive)
            # Cast geometry to MULTIPOLYGON to match the column type
            sql = text(f"""
                UPDATE {quoted_schema}.{quoted_table} AS t
                SET geom = ST_Multi(w.geom)::geometry(MULTIPOLYGON, 4326)
                FROM {quoted_schema}.{quoted_world_geojson} AS w
                WHERE LOWER(TRIM(t.state)) = LOWER(TRIM(w.level_4_na))
                AND t.geom IS NULL
            """)
            result = db.execute(sql)
            db.commit()
            rows_updated = result.rowcount
            logger.info(f"Updated {rows_updated} rows with geometry from world_geojson in table {schema}.{table_name}")
            return rows_updated
        except SQLAlchemyError as e:
            logger.error(f"Error mapping geometry from world_geojson to table {schema}.{table_name}: {e}")
            db.rollback()
            raise e

    @staticmethod
    def map_geometry_from_wkt(table_name: str, schema: str, db: Session):
        """
        Convert geometry_wkt column (WKT format) to MULTIPOLYGON and store in geom column.
        Handles POLYGON WKT format and converts it to MULTIPOLYGON.
        """
        try:
            # Quote identifiers to prevent SQL injection
            quoted_schema = _quote_identifier(schema)
            quoted_table = _quote_identifier(table_name)
            quoted_geometry_wkt = _quote_identifier("geometry_wkt")
            
            # SQL to convert WKT to MULTIPOLYGON geometry
            # ST_GeomFromText converts WKT string to geometry
            # ST_Multi converts POLYGON to MULTIPOLYGON
            # Only update rows where geometry_wkt is not null and geom is null
            sql = text(f"""
                UPDATE {quoted_schema}.{quoted_table} AS t
                SET geom = ST_Multi(ST_GeomFromText(t.{quoted_geometry_wkt}, 4326))::geometry(MULTIPOLYGON, 4326)
                WHERE t.{quoted_geometry_wkt} IS NOT NULL
                AND TRIM(t.{quoted_geometry_wkt}) != ''
                AND t.geom IS NULL
            """)
            result = db.execute(sql)
            db.commit()
            rows_updated = result.rowcount
            logger.info(f"Updated {rows_updated} rows with geometry from geometry_wkt column in table {schema}.{table_name}")
            return rows_updated
        except SQLAlchemyError as e:
            logger.error(f"Error mapping geometry from geometry_wkt to table {schema}.{table_name}: {e}")
            db.rollback()
            raise e

