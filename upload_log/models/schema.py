import configparser
from sqlalchemy import Column, DateTime, Integer, String, func, text
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, UUID
from sqlalchemy.ext.declarative import declarative_base
from database.database import engine

config = configparser.ConfigParser()
encodings_to_try = ['utf-8-sig', 'utf-8', 'latin-1', 'cp1252']
read_success = False
for encoding in encodings_to_try:
    try:
        if config.read("secure.ini", encoding=encoding):
            read_success = True
            break
    except (UnicodeDecodeError, UnicodeError):
        continue
    except Exception:
        continue

if not read_success:
    raise ValueError("Error reading secure.ini: Could not decode file with any supported encoding. Please ensure the file is saved as UTF-8.")

SCHEMA = config.get("DB_SCHEMA", "schema")

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

