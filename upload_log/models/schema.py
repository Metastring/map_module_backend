import configparser
from sqlalchemy import Column, DateTime, Integer, String, func
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.ext.declarative import declarative_base
from database.database import engine

# Read schema from secure.ini (if available) or use default
try:
    config = configparser.ConfigParser()
    config.read("secure.ini")
    SCHEMA = config.get("DB_SCHEMA", "schema")
except Exception:
    # Default schema name for CML
    SCHEMA = "public"

Base = declarative_base()


class UploadLog(Base):
    __tablename__ = "upload_logs"
    __table_args__ = {"schema": SCHEMA}

    id = Column(Integer, primary_key=True, index=True)
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

