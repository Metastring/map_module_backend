"""
DAO (Data Access Object) layer for style-related database operations.
Handles queries to PostGIS for column info, classification data, and style metadata.
"""
from sqlalchemy.orm import Session
from sqlalchemy import text, func
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta, timezone
import logging

from styles.models.schema import StyleMetadata, StyleAuditLog, StyleCache
from styles.models.model import (
    StyleMetadataCreate, 
    ColumnInfo,
    ClassificationMethod,
)

logger = logging.getLogger(__name__)


class StyleDAO:
    """
    Data Access Object for style metadata and PostGIS column queries.
    """

    def __init__(self, db: Session):
        self.db = db

    # ==================== Column Information ====================

    def get_column_info(self, table_name: str, schema: str = "public") -> List[ColumnInfo]:
        """
        Get column information from information_schema.
        Returns column names, data types, and whether they're numeric/categorical.
        """
        query = text("""
            SELECT 
                column_name,
                data_type,
                is_nullable,
                CASE 
                    WHEN data_type IN ('integer', 'bigint', 'smallint', 'numeric', 
                                       'real', 'double precision', 'decimal') THEN true
                    ELSE false
                END as is_numeric,
                CASE 
                    WHEN data_type IN ('character varying', 'varchar', 'text', 'char') THEN true
                    ELSE false
                END as is_categorical
            FROM information_schema.columns
            WHERE table_name = :table_name
              AND table_schema = :schema
              AND column_name NOT IN ('geom', 'geometry', 'the_geom', 'id', 'gid', 'ogc_fid')
            ORDER BY ordinal_position
        """)
        
        result = self.db.execute(query, {"table_name": table_name, "schema": schema})
        columns = []
        for row in result:
            columns.append(ColumnInfo(
                column_name=row[0],
                data_type=row[1],
                is_nullable=row[2] == 'YES',
                is_numeric=row[3],
                is_categorical=row[4]
            ))
        return columns

    def get_column_data_type(self, table_name: str, column_name: str, schema: str = "public") -> Optional[str]:
        """Get the data type of a specific column."""
        query = text("""
            SELECT data_type
            FROM information_schema.columns
            WHERE table_name = :table_name
              AND column_name = :column_name
              AND table_schema = :schema
        """)
        result = self.db.execute(query, {
            "table_name": table_name, 
            "column_name": column_name,
            "schema": schema
        }).fetchone()
        return result[0] if result else None

    # ==================== Numeric Statistics ====================

    def get_numeric_stats(
        self, 
        table_name: str, 
        column_name: str, 
        schema: str = "public"
    ) -> Tuple[Optional[float], Optional[float], int]:
        """
        Get min, max, and count for a numeric column.
        Returns (min_value, max_value, count).
        """
        # Use parameterized query safely
        # Note: table/column names can't be parameterized, so we validate them first
        self._validate_identifier(table_name)
        self._validate_identifier(column_name)
        
        query = text(f"""
            SELECT 
                MIN("{column_name}")::float as min_val,
                MAX("{column_name}")::float as max_val,
                COUNT("{column_name}") as count
            FROM "{schema}"."{table_name}"
            WHERE "{column_name}" IS NOT NULL
        """)
        
        result = self.db.execute(query).fetchone()
        if result:
            return result[0], result[1], result[2]
        return None, None, 0

    def get_quantile_breaks(
        self, 
        table_name: str, 
        column_name: str, 
        num_classes: int,
        schema: str = "public"
    ) -> List[float]:
        """
        Get quantile breaks for a numeric column using percentile_cont.
        """
        self._validate_identifier(table_name)
        self._validate_identifier(column_name)
        
        # Generate percentile values
        percentiles = [i / num_classes for i in range(1, num_classes)]
        percentile_str = ", ".join([str(p) for p in percentiles])
        
        query = text(f"""
            SELECT percentile_cont(ARRAY[{percentile_str}]) 
                   WITHIN GROUP (ORDER BY "{column_name}"::float)
            FROM "{schema}"."{table_name}"
            WHERE "{column_name}" IS NOT NULL
        """)
        
        result = self.db.execute(query).fetchone()
        if result and result[0]:
            return list(result[0])
        return []

    def get_all_values_for_jenks(
        self, 
        table_name: str, 
        column_name: str, 
        schema: str = "public",
        sample_size: int = 10000
    ) -> List[float]:
        """
        Get all values for Jenks natural breaks calculation.
        Uses sampling for large datasets.
        """
        self._validate_identifier(table_name)
        self._validate_identifier(column_name)
        
        # First get count
        count_query = text(f"""
            SELECT COUNT(*) FROM "{schema}"."{table_name}" 
            WHERE "{column_name}" IS NOT NULL
        """)
        count = self.db.execute(count_query).scalar()
        
        if count <= sample_size:
            # Get all values
            query = text(f"""
                SELECT "{column_name}"::float
                FROM "{schema}"."{table_name}"
                WHERE "{column_name}" IS NOT NULL
                ORDER BY "{column_name}"
            """)
        else:
            # Sample values
            query = text(f"""
                SELECT "{column_name}"::float
                FROM "{schema}"."{table_name}"
                WHERE "{column_name}" IS NOT NULL
                ORDER BY RANDOM()
                LIMIT {sample_size}
            """)
        
        result = self.db.execute(query)
        return [row[0] for row in result if row[0] is not None]

    # ==================== Categorical Values ====================

    def get_distinct_values(
        self, 
        table_name: str, 
        column_name: str, 
        schema: str = "public",
        limit: int = 100
    ) -> List[str]:
        """
        Get distinct values for a categorical column.
        """
        self._validate_identifier(table_name)
        self._validate_identifier(column_name)
        
        query = text(f"""
            SELECT DISTINCT "{column_name}"::text as val
            FROM "{schema}"."{table_name}"
            WHERE "{column_name}" IS NOT NULL
            ORDER BY val
            LIMIT {limit}
        """)
        
        result = self.db.execute(query)
        return [row[0] for row in result if row[0]]

    # ==================== Table Information ====================

    def get_geometry_type(self, table_name: str, schema: str = "public") -> Optional[str]:
        """Get the geometry type of a table."""
        self._validate_identifier(table_name)
        
        query = text("""
            SELECT type
            FROM geometry_columns
            WHERE f_table_name = :table_name AND f_table_schema = :schema
        """)
        result = self.db.execute(query, {"table_name": table_name, "schema": schema}).fetchone()
        
        if result:
            geom_type = result[0].upper()
            if 'POINT' in geom_type:
                return 'point'
            elif 'LINE' in geom_type:
                return 'line'
            elif 'POLYGON' in geom_type:
                return 'polygon'
        return None

    # ==================== Style Metadata CRUD ====================

    def create_style_metadata(self, data: StyleMetadataCreate) -> StyleMetadata:
        """Create a new style metadata record."""
        # Import enum classes to convert API enums to DB enums
        from styles.models.schema import LayerTypeEnum, ClassificationMethodEnum
        
        # Convert API LayerType enum to DB LayerTypeEnum
        if hasattr(data.layer_type, 'value'):
            layer_type_value = data.layer_type.value
            # Map to database enum
            try:
                db_layer_type = LayerTypeEnum(layer_type_value)
            except ValueError:
                # Fallback to POLYGON if value doesn't match
                db_layer_type = LayerTypeEnum.POLYGON
        else:
            db_layer_type = LayerTypeEnum.POLYGON
        
        # Convert API ClassificationMethod enum to DB ClassificationMethodEnum
        if hasattr(data.classification_method, 'value'):
            method_value = data.classification_method.value
            try:
                db_classification_method = ClassificationMethodEnum(method_value)
            except ValueError:
                db_classification_method = ClassificationMethodEnum.EQUAL_INTERVAL
        else:
            db_classification_method = ClassificationMethodEnum.EQUAL_INTERVAL
        
        db_style = StyleMetadata(
            layer_table_name=data.layer_table_name,
            workspace=data.workspace,
            layer_name=data.layer_name,
            color_by=data.color_by,
            layer_type=db_layer_type,
            classification_method=db_classification_method,
            num_classes=data.num_classes,
            color_palette=data.color_palette,
            custom_colors=data.custom_colors,
            fill_opacity=data.fill_opacity,
            stroke_color=data.stroke_color,
            stroke_width=data.stroke_width,
            manual_breaks=data.manual_breaks,
        )
        self.db.add(db_style)
        self.db.commit()
        self.db.refresh(db_style)
        return db_style

    def get_style_metadata(self, style_id: int) -> Optional[StyleMetadata]:
        """Get style metadata by ID."""
        return self.db.query(StyleMetadata).filter(StyleMetadata.id == style_id).first()

    def get_style_by_workspace_table_color(
        self, workspace: str, table_name: str, color_by: str
    ) -> Optional[StyleMetadata]:
        """Get style metadata by workspace, table name, and color_by column."""
        return self.db.query(StyleMetadata).filter(
            StyleMetadata.workspace == workspace,
            StyleMetadata.layer_table_name == table_name,
            StyleMetadata.color_by == color_by
        ).first()

    def list_styles(
        self, 
        workspace: Optional[str] = None,
        is_active: Optional[bool] = True,
        skip: int = 0,
        limit: int = 100
    ) -> Tuple[List[StyleMetadata], int]:
        """List style metadata with optional filtering."""
        query = self.db.query(StyleMetadata)
        
        if workspace:
            query = query.filter(StyleMetadata.workspace == workspace)
        if is_active is not None:
            query = query.filter(StyleMetadata.is_active == is_active)
        
        total = query.count()
        items = query.order_by(StyleMetadata.created_at.desc()).offset(skip).limit(limit).all()
        
        return items, total

    def update_style_generated_info(
        self, 
        style_id: int, 
        style_name: str,
        mbstyle_json: Dict[str, Any],
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        distinct_values: Optional[List[str]] = None,
        data_type: Optional[str] = None
    ) -> Optional[StyleMetadata]:
        """Update style metadata after generation."""
        db_style = self.get_style_metadata(style_id)
        if not db_style:
            return None
        
        db_style.generated_style_name = style_name
        db_style.mbstyle_json = mbstyle_json
        db_style.min_value = min_value
        db_style.max_value = max_value
        db_style.distinct_values = distinct_values
        db_style.data_type = data_type
        db_style.last_generated = datetime.now(timezone.utc)
        
        self.db.commit()
        self.db.refresh(db_style)
        return db_style

    # ==================== Audit Logging ====================

    def get_audit_logs(
        self, 
        style_metadata_id: int, 
        skip: int = 0, 
        limit: int = 50
    ) -> List[StyleAuditLog]:
        """Get audit logs for a style."""
        return self.db.query(StyleAuditLog).filter(
            StyleAuditLog.style_metadata_id == style_metadata_id
        ).order_by(StyleAuditLog.created_at.desc()).offset(skip).limit(limit).all()

    def create_audit_log(
        self,
        style_metadata_id: int,
        action: str,
        user_id: Optional[str] = None,
        user_email: Optional[str] = None,
        changes: Optional[Dict[str, Any]] = None,
        previous_style: Optional[Dict[str, Any]] = None,
        new_style: Optional[Dict[str, Any]] = None,
        status: str = "success",
        error_message: Optional[str] = None
    ) -> StyleAuditLog:
        """Create an audit log entry."""
        # Get next version
        last_log = self.db.query(StyleAuditLog).filter(
            StyleAuditLog.style_metadata_id == style_metadata_id
        ).order_by(StyleAuditLog.version.desc()).first()
        
        version = (last_log.version + 1) if last_log else 1
        
        audit_log = StyleAuditLog(
            style_metadata_id=style_metadata_id,
            action=action,
            user_id=user_id,
            user_email=user_email,
            version=version,
            changes=changes,
            previous_style=previous_style,
            new_style=new_style,
            status=status,
            error_message=error_message
        )
        self.db.add(audit_log)
        self.db.commit()
        self.db.refresh(audit_log)
        return audit_log

    # ==================== Caching ====================

    def get_cached_data(
        self, 
        table_name: str, 
        column_name: str, 
        cache_type: str
    ) -> Optional[Dict[str, Any]]:
        """Get cached data if available and not expired."""
        cache = self.db.query(StyleCache).filter(
            StyleCache.table_name == table_name,
            StyleCache.column_name == column_name,
            StyleCache.cache_type == cache_type
        ).first()
        
        if cache:
            if cache.expires_at and cache.expires_at < datetime.now(timezone.utc):
                return None  # Expired
            return cache.cached_data
        return None

    def set_cached_data(
        self,
        table_name: str,
        column_name: str,
        cache_type: str,
        data: Dict[str, Any],
        row_count: Optional[int] = None,
        ttl_hours: int = 24
    ) -> StyleCache:
        """Set or update cached data."""
        cache = self.db.query(StyleCache).filter(
            StyleCache.table_name == table_name,
            StyleCache.column_name == column_name,
            StyleCache.cache_type == cache_type
        ).first()
        
        expires_at = datetime.now(timezone.utc) + timedelta(hours=ttl_hours)
        
        if cache:
            cache.cached_data = data
            cache.row_count = row_count
            cache.expires_at = expires_at
            cache.updated_at = datetime.now(timezone.utc)
        else:
            cache = StyleCache(
                table_name=table_name,
                column_name=column_name,
                cache_type=cache_type,
                cached_data=data,
                row_count=row_count,
                expires_at=expires_at
            )
            self.db.add(cache)
        
        self.db.commit()
        self.db.refresh(cache)
        return cache

    # ==================== Helpers ====================

    def _validate_identifier(self, identifier: str):
        """Validate SQL identifier to prevent injection."""
        import re
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', identifier):
            raise ValueError(f"Invalid identifier: {identifier}")
