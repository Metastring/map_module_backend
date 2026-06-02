from sqlalchemy import Column, DateTime, String, Text, Integer, Boolean, func, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from database.database import engine
from utils.config import db_schema

SCHEMA = db_schema

Base = declarative_base()


class DatasetMaster(Base):
    __tablename__ = "dataset_master"
    __table_args__ = {"schema": SCHEMA}

    dataset_id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(Text)
    description = Column(Text)
    keywords = Column(Text)  # semicolon-separated
    dataset_type = Column(Text)
    theme = Column(String(255))
    access_constraints = Column(Text)
    use_constraints = Column(Text)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    map_layer = relationship("MapLayerInfo", back_populates="dataset_master", uselist=False)
    contacts = relationship("DatasetContact", back_populates="dataset_master")


class DatasetContact(Base):
    __tablename__ = "dataset_contacts"
    __table_args__ = {"schema": SCHEMA}

    contact_id = Column(Integer, primary_key=True, autoincrement=True)
    dataset_id = Column(Integer, ForeignKey(f"{SCHEMA}.dataset_master.dataset_id", ondelete="CASCADE"))
    name = Column(Text)
    role = Column(Text)
    email = Column(Text)
    organization = Column(Text)
    address = Column(Text)
    city = Column(Text)
    state = Column(Text)
    country = Column(Text)

    dataset_master = relationship("DatasetMaster", back_populates="contacts")


class MapLayerInfo(Base):
    __tablename__ = "map_layer_info"
    __table_args__ = {"schema": SCHEMA}

    id = Column(UUID(as_uuid=True), primary_key=True, index=True)
    dataset_id = Column(
        Integer,
        ForeignKey(f"{SCHEMA}.dataset_master.dataset_id", ondelete="CASCADE"),
        nullable=False,
    )
    geoserver_name = Column(String, nullable=False, unique=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    dataset_master = relationship("DatasetMaster", back_populates="map_layer")

    # --- Backward-compatible properties so existing calling code needs no changes ---

    def _first_contact(self):
        if self.dataset_master and self.dataset_master.contacts:
            return self.dataset_master.contacts[0]
        return None

    @property
    def name_of_dataset(self):
        return self.dataset_master.title if self.dataset_master else None

    @property
    def theme(self):
        return self.dataset_master.theme if self.dataset_master else None

    @property
    def keywords(self):
        kw = self.dataset_master.keywords if self.dataset_master else None
        if kw:
            return [k.strip() for k in kw.split(";") if k.strip()]
        return None

    @property
    def purpose_of_creating_data(self):
        return self.dataset_master.description if self.dataset_master else None

    @property
    def access_constraints(self):
        return self.dataset_master.access_constraints if self.dataset_master else None

    @property
    def use_constraints(self):
        return self.dataset_master.use_constraints if self.dataset_master else None

    @property
    def data_type(self):
        return self.dataset_master.dataset_type if self.dataset_master else None

    @property
    def contact_person(self):
        c = self._first_contact()
        return c.name if c else None

    @property
    def organization(self):
        c = self._first_contact()
        return c.organization if c else None

    @property
    def mailing_address(self):
        c = self._first_contact()
        return c.address if c else None

    @property
    def city_locality_country(self):
        c = self._first_contact()
        return c.city if c else None

    @property
    def country(self):
        c = self._first_contact()
        return c.country if c else None

    @property
    def contact_email(self):
        c = self._first_contact()
        return c.email if c else None

    @property
    def created_on(self):
        return self.dataset_master.created_at if self.dataset_master else None

    @property
    def updated_on(self):
        return self.dataset_master.updated_at if self.dataset_master else None


# Only create map_layer_info if it doesn't exist; dataset_master and dataset_contacts already exist
Base.metadata.create_all(bind=engine, tables=[
    Base.metadata.tables.get(f"{SCHEMA}.map_layer_info")
], checkfirst=True)
