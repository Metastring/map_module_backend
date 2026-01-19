from sqlalchemy import Column, DateTime, String, func, Text
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, UUID
from sqlalchemy.ext.declarative import declarative_base
from database.database import engine
from utils.config import db_schema

SCHEMA = db_schema

Base = declarative_base()


class Metadata(Base):
    __tablename__ = "metadata"
    __table_args__ = {"schema": SCHEMA}

    id = Column(UUID(as_uuid=True), primary_key=True, index=True)
    dataset_id = Column(UUID(as_uuid=True), nullable=True)
    geoserver_name = Column(String, nullable=False)
    name_of_dataset = Column(String, nullable=False)
    theme = Column(String, nullable=True)
    keywords = Column(ARRAY(String), nullable=True)
    purpose_of_creating_data = Column(Text, nullable=True)
    access_constraints = Column(Text, nullable=True)
    use_constraints = Column(Text, nullable=True)
    data_type = Column(String, nullable=True)
    contact_person = Column(String, nullable=True)
    organization = Column(String, nullable=True)
    mailing_address = Column(Text, nullable=True)
    city_locality_country = Column(String, nullable=True)
    country = Column(String, nullable=True)
    contact_email = Column(String, nullable=True)
    created_on = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_on = Column(DateTime(timezone=True), nullable=True, onupdate=func.now())


# Create all tables
Base.metadata.create_all(bind=engine)

