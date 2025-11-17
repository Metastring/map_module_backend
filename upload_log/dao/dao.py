from datetime import datetime
from typing import List, Optional

import logging
from sqlalchemy.orm import Session
from sqlalchemy import func

from upload_log.models.schema import UploadLog as UploadLogTable

logger = logging.getLogger(__name__)


class UploadLogDAO:
    @staticmethod
    def create(upload_log: UploadLogTable, db: Session) -> Optional[UploadLogTable]:
        """Create a new upload log record."""
        try:
            db.add(upload_log)
            db.commit()
            db.refresh(upload_log)
            return upload_log
        except Exception as exc:
            logger.error("Error creating upload log record: %s", exc)
            db.rollback()
            raise

    @staticmethod
    def get_by_id(log_id: int, db: Session) -> Optional[UploadLogTable]:
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

