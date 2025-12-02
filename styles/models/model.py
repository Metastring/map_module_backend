"""
SQLAlchemy models for style metadata and audit logging.
"""
from sqlalchemy import Column, Integer, String, DateTime, JSON, Text, Boolean, Float, ForeignKey, Enum as SQLEnum
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
import enum

Base = declarative_base()


class ClassificationMethodEnum(enum.Enum):
    """Classification methods for styling."""
    EQUAL_INTERVAL = "equal_interval"
    QUANTILE = "quantile"
    JENKS = "jenks"
    CATEGORICAL = "categorical"
    MANUAL = "manual"


class LayerTypeEnum(enum.Enum):
    """Layer geometry types."""
    POINT = "point"
    LINE = "line"
    POLYGON = "polygon"
    RASTER = "raster"


class StyleMetadata(Base):
    """
    Metadata table for layer styling configuration.
    Controls how styles are generated for each layer.
    """
    __tablename__ = "style_metadata"

    id = Column(Integer, primary_key=True, index=True)
    
    # Layer identification
    layer_table_name = Column(String(255), nullable=False, unique=True, index=True)
    workspace = Column(String(100), nullable=False)
    layer_name = Column(String(255), nullable=True)  # Display name
    
    # Classification configuration
    color_by = Column(String(100), nullable=False)  # Column to classify by
    layer_type = Column(SQLEnum(LayerTypeEnum), default=LayerTypeEnum.POLYGON)
    classification_method = Column(SQLEnum(ClassificationMethodEnum), default=ClassificationMethodEnum.EQUAL_INTERVAL)
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
    """
    Audit log for style generation and updates.
    Tracks who generated styles and when.
    """
    __tablename__ = "style_audit_log"

    id = Column(Integer, primary_key=True, index=True)
    style_metadata_id = Column(Integer, ForeignKey("style_metadata.id"), nullable=False)
    
    # Action details
    action = Column(String(50), nullable=False)  # created, updated, regenerated, published, deleted
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
    """
    Cache for expensive queries (distinct values, min/max).
    """
    __tablename__ = "style_cache"

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
