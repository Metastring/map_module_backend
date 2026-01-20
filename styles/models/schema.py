import enum
import logging
from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
    JSON,
    Text,
    Boolean,
    Float,
    ForeignKey,
    Enum as SQLEnum,
    UniqueConstraint,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from database.database import engine
from utils.config import db_schema

SCHEMA = db_schema

Base = declarative_base()


class ClassificationMethodEnum(enum.Enum):
    """Classification methods for styling (DB-level enum)."""

    EQUAL_INTERVAL = "equal_interval"
    QUANTILE = "quantile"
    JENKS = "jenks"
    CATEGORICAL = "categorical"
    MANUAL = "manual"


class LayerTypeEnum(enum.Enum):
    """Layer geometry types (DB-level enum)."""

    POINT = "point"
    LINE = "line"
    POLYGON = "polygon"
    RASTER = "raster"


class StyleMetadata(Base):

    __tablename__ = "style_metadata"
    __table_args__ = (
        UniqueConstraint('layer_table_name', 'color_by', 'workspace', name='uq_style_layer_color_workspace'),
        {"schema": SCHEMA}
    )

    id = Column(Integer, primary_key=True, index=True)

    # Layer identification
    layer_table_name = Column(String(255), nullable=False, index=True)
    workspace = Column(String(100), nullable=False)
    layer_name = Column(String(255), nullable=True)  # Display name

    # Classification configuration
    color_by = Column(String(100), nullable=False)  # Column to classify by
    # Use native_enum=False to store as VARCHAR instead of database enum
    # This avoids enum value mismatch issues
    layer_type = Column(SQLEnum(LayerTypeEnum, native_enum=False), default=LayerTypeEnum.POLYGON)
    classification_method = Column(
        SQLEnum(ClassificationMethodEnum, native_enum=False),
        default=ClassificationMethodEnum.EQUAL_INTERVAL,
    )
    num_classes = Column(Integer, default=5)

    # Summary/cached data
    min_value = Column(Float, nullable=True)
    max_value = Column(Float, nullable=True)
    distinct_values = Column(JSON, nullable=True)  # For categorical
    data_type = Column(String(50), nullable=True)  # numeric, categorical

    # Color configuration
    color_palette = Column(String(100), default="YlOrRd")  # ColorBrewer palette name
    custom_colors = Column(JSON, nullable=True)  # Override with custom colors
    fill_opacity = Column(Float, default=0.7)
    stroke_color = Column(String(20), default="#333333")
    stroke_width = Column(Float, default=1.0)

    # Manual class breaks (optional)
    manual_breaks = Column(JSON, nullable=True)

    # Generated style info
    generated_style_name = Column(String(255), nullable=True)
    mbstyle_json = Column(JSON, nullable=True)

    # Status
    is_active = Column(Boolean, default=True)
    last_generated = Column(DateTime(timezone=True), nullable=True)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Relationships
    audit_logs = relationship("StyleAuditLog", back_populates="style_metadata")


class StyleAuditLog(Base):

    __tablename__ = "style_audit_log"
    __table_args__ = {"schema": SCHEMA}

    id = Column(Integer, primary_key=True, index=True)
    style_metadata_id = Column(Integer, ForeignKey(f"{SCHEMA}.style_metadata.id"), nullable=False)

    # Action details
    action = Column(
        String(50), nullable=False
    )  # created, updated, regenerated, published, deleted
    user_id = Column(String(100), nullable=True)  # Who performed the action
    user_email = Column(String(255), nullable=True)

    # Version tracking
    version = Column(Integer, default=1)

    # Change details
    changes = Column(JSON, nullable=True)  # What changed
    previous_style = Column(JSON, nullable=True)  # Previous MBStyle JSON
    new_style = Column(JSON, nullable=True)  # New MBStyle JSON

    # Status
    status = Column(String(50), default="success")  # success, failed
    error_message = Column(Text, nullable=True)

    # Timestamp
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    style_metadata = relationship("StyleMetadata", back_populates="audit_logs")


class StyleCache(Base):

    __tablename__ = "style_cache"
    __table_args__ = {"schema": SCHEMA}

    id = Column(Integer, primary_key=True, index=True)
    table_name = Column(String(255), nullable=False, index=True)
    column_name = Column(String(100), nullable=False, index=True)
    cache_type = Column(String(50), nullable=False)  # min_max, distinct, column_info
    cached_data = Column(JSON, nullable=False)

    # Cache validity
    row_count = Column(Integer, nullable=True)  # Used to detect data changes
    expires_at = Column(DateTime(timezone=True), nullable=True)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())


# Create all tables
# Wrap in try-except to prevent import-time failures if database is unavailable
try:
    Base.metadata.create_all(bind=engine)
except Exception as e:
    # Log the error but don't fail import
    # Tables will be created when database is available
    logger = logging.getLogger(__name__)
    logger.warning(f"Could not create tables at import time: {e}")
