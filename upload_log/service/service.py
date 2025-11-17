from typing import List, Optional
import logging

from sqlalchemy.orm import Session

from upload_log.models.model import DataType, UploadLogCreate, UploadLogFilter, UploadLogOut
from upload_log.models.schema import UploadLog as UploadLogTable
from upload_log.dao.dao import UploadLogDAO

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
    def get_by_id(log_id: int, db: Session) -> Optional[UploadLogOut]:
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

