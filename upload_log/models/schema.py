from sqlalchemy import Column, DateTime, Integer, String, func, text
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, UUID
from sqlalchemy.ext.declarative import declarative_base
from database.database import engine
from utils.config import db_schema

SCHEMA = db_schema

Base = declarative_base()


class UploadLog(Base):
    __tablename__ = "upload_logs"
    __table_args__ = {"schema": SCHEMA}

    id = Column(UUID(as_uuid=True), primary_key=True, index=True, server_default=text("gen_random_uuid()"))
    layer_name = Column(String, nullable=False)
    file_format = Column(String, nullable=False)
    data_type = Column(String, nullable=False)
    crs = Column(String, nullable=False)
    bbox = Column(JSONB, nullable=True)
    source_path = Column(String, nullable=False)
    geoserver_layer = Column(String, nullable=True)
    tags = Column(ARRAY(String), nullable=True)
    uploaded_by = Column(String, nullable=False)
    uploaded_on = Column(DateTime(timezone=True), nullable=False, server_default=func.now())


# Create all tables
Base.metadata.create_all(bind=engine)

