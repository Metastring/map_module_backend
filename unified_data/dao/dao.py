"""
Data Access Object for Unified Data Management System
Handles all database operations for datasets and features
"""
from typing import List, Optional, Dict, Any, Tuple
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import func, text, and_, or_
from geoalchemy2.functions import ST_GeomFromGeoJSON, ST_AsGeoJSON, ST_Intersects, ST_Within, ST_Contains
from geoalchemy2.elements import WKTElement
import json
import logging

from unified_data.models.schema import (
    Dataset, DatasetCategory, DatasetFeature, 
    DatasetProcessingLog, DatasetAttributeSchema
)
from unified_data.models.model import (
    DatasetCreate, DatasetUpdate, DatasetQuery,
    AttributeFilter, SpatialFilter, BoundingBox
)

logger = logging.getLogger(__name__)


class UnifiedDataDAO:
    """Data Access Object for unified data management operations"""
    
    @staticmethod
    def create_category(db: Session, category_data: Dict[str, Any]) -> DatasetCategory:
        """Create a new dataset category"""
        category = DatasetCategory(**category_data)
        db.add(category)
        db.commit()
        db.refresh(category)
        return category
    
    @staticmethod
    def get_categories(db: Session) -> List[DatasetCategory]:
        """Get all dataset categories"""
        return db.query(DatasetCategory).order_by(DatasetCategory.display_name).all()
    
    @staticmethod
    def get_category_by_id(db: Session, category_id: int) -> Optional[DatasetCategory]:
        """Get category by ID"""
        return db.query(DatasetCategory).filter(DatasetCategory.id == category_id).first()
    
    @staticmethod
    def create_dataset(db: Session, dataset_data: Dict[str, Any]) -> Dataset:
        """Create a new dataset"""
        dataset = Dataset(**dataset_data)
        db.add(dataset)
        db.commit()
        db.refresh(dataset)
        return dataset
    
    @staticmethod
    def get_dataset_by_id(db: Session, dataset_id: int, include_category: bool = True) -> Optional[Dataset]:
        """Get dataset by ID"""
        query = db.query(Dataset)
        if include_category:
            query = query.options(joinedload(Dataset.category))
        return query.filter(Dataset.id == dataset_id).first()
    
    @staticmethod
    def get_dataset_by_uuid(db: Session, dataset_uuid: str, include_category: bool = True) -> Optional[Dataset]:
        """Get dataset by UUID"""
        query = db.query(Dataset)
        if include_category:
            query = query.options(joinedload(Dataset.category))
        return query.filter(Dataset.uuid == dataset_uuid).first()
    
    @staticmethod
    def update_dataset(db: Session, dataset_id: int, update_data: Dict[str, Any]) -> Optional[Dataset]:
        """Update dataset metadata"""
        dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
        if dataset:
            for key, value in update_data.items():
                if hasattr(dataset, key) and value is not None:
                    setattr(dataset, key, value)
            db.commit()
            db.refresh(dataset)
        return dataset
    
    @staticmethod
    def delete_dataset(db: Session, dataset_id: int) -> bool:
        """Delete dataset and all its features"""
        dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
        if dataset:
            db.delete(dataset)
            db.commit()
            return True
        return False
    
    @staticmethod
    def get_datasets(
        db: Session, 
        category_id: Optional[int] = None,
        dataset_type: Optional[str] = None,
        status: Optional[str] = None,
        search: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        include_category: bool = True
    ) -> Tuple[List[Dataset], int]:
        """Get datasets with filtering and pagination"""
        
        query = db.query(Dataset)
        if include_category:
            query = query.options(joinedload(Dataset.category))
        
        # Apply filters
        if category_id:
            query = query.filter(Dataset.category_id == category_id)
        
        if dataset_type:
            query = query.filter(Dataset.dataset_type == dataset_type)
        
        if status:
            query = query.filter(Dataset.status == status)
        
        if search:
            search_filter = or_(
                Dataset.name.ilike(f'%{search}%'),
                Dataset.display_name.ilike(f'%{search}%'),
                Dataset.description.ilike(f'%{search}%')
            )
            query = query.filter(search_filter)
        
        # Get total count
        total = query.count()
        
        # Apply pagination and ordering
        datasets = query.order_by(Dataset.created_at.desc()).offset(offset).limit(limit).all()
        
        return datasets, total
    
    @staticmethod
    def create_features(db: Session, dataset_id: int, features_data: List[Dict[str, Any]]) -> List[DatasetFeature]:
        """Create multiple features for a dataset"""
        features = []
        for feature_data in features_data:
            # Convert geometry dict to PostGIS geometry
            geometry_wkt = None
            if feature_data.get('geometry'):
                try:
                    geometry_wkt = ST_GeomFromGeoJSON(json.dumps(feature_data['geometry']))
                except Exception as e:
                    logger.warning(f"Invalid geometry in feature: {e}")
            
            feature = DatasetFeature(
                dataset_id=dataset_id,
                geometry=geometry_wkt,
                attributes=feature_data.get('attributes', {}),
                feature_id=feature_data.get('feature_id')
            )
            features.append(feature)
        
        db.add_all(features)
        db.commit()
        
        # Refresh to get IDs
        for feature in features:
            db.refresh(feature)
        
        return features
    
    @staticmethod
    def get_features_by_dataset(
        db: Session, 
        dataset_id: int,
        limit: int = 1000,
        offset: int = 0,
        include_geometry: bool = True
    ) -> Tuple[List[Dict[str, Any]], int]:
        """Get features for a specific dataset"""
        
        # Build query
        if include_geometry:
            query = db.query(
                DatasetFeature.id,
                DatasetFeature.feature_id,
                DatasetFeature.attributes,
                ST_AsGeoJSON(DatasetFeature.geometry).label('geometry_json'),
                DatasetFeature.created_at
            )
        else:
            query = db.query(
                DatasetFeature.id,
                DatasetFeature.feature_id,
                DatasetFeature.attributes,
                DatasetFeature.created_at
            )
        
        query = query.filter(DatasetFeature.dataset_id == dataset_id)
        
        # Get total count
        total = db.query(DatasetFeature).filter(DatasetFeature.dataset_id == dataset_id).count()
        
        # Apply pagination
        results = query.offset(offset).limit(limit).all()
        
        # Convert to dicts
        features = []
        for result in results:
            feature_dict = {
                'id': result.id,
                'feature_id': result.feature_id,
                'attributes': result.attributes,
                'created_at': result.created_at
            }
            
            if include_geometry and hasattr(result, 'geometry_json'):
                if result.geometry_json:
                    feature_dict['geometry'] = json.loads(result.geometry_json)
                else:
                    feature_dict['geometry'] = None
            
            features.append(feature_dict)
        
        return features, total
    
    @staticmethod
    def query_features(db: Session, query_params: DatasetQuery) -> Tuple[List[Dict[str, Any]], int]:
        """Advanced feature querying with spatial and attribute filters"""
        
        # Base query
        if query_params.include_geometry:
            base_query = db.query(
                DatasetFeature.id,
                DatasetFeature.dataset_id,
                DatasetFeature.feature_id,
                DatasetFeature.attributes,
                ST_AsGeoJSON(DatasetFeature.geometry).label('geometry_json'),
                DatasetFeature.created_at,
                Dataset.name.label('dataset_name'),
                Dataset.display_name.label('dataset_display_name')
            )
        else:
            base_query = db.query(
                DatasetFeature.id,
                DatasetFeature.dataset_id,
                DatasetFeature.feature_id,
                DatasetFeature.attributes,
                DatasetFeature.created_at,
                Dataset.name.label('dataset_name'),
                Dataset.display_name.label('dataset_display_name')
            )
        
        base_query = base_query.join(Dataset)
        
        # Apply filters
        filters = []
        
        # Dataset filters
        if query_params.dataset_ids:
            filters.append(Dataset.id.in_(query_params.dataset_ids))
        
        if query_params.category_ids:
            filters.append(Dataset.category_id.in_(query_params.category_ids))
        
        if query_params.dataset_types:
            filters.append(Dataset.dataset_type.in_([t.value for t in query_params.dataset_types]))
        
        # Bounding box filter
        if query_params.bbox:
            bbox_wkt = f"POLYGON(({query_params.bbox.minx} {query_params.bbox.miny}, {query_params.bbox.maxx} {query_params.bbox.miny}, {query_params.bbox.maxx} {query_params.bbox.maxy}, {query_params.bbox.minx} {query_params.bbox.maxy}, {query_params.bbox.minx} {query_params.bbox.miny}))"
            filters.append(ST_Intersects(DatasetFeature.geometry, WKTElement(bbox_wkt, srid=4326)))
        
        # Spatial filter
        if query_params.spatial_filter:
            spatial_geom = ST_GeomFromGeoJSON(json.dumps(query_params.spatial_filter.geometry))
            
            if query_params.spatial_filter.operation == 'intersects':
                filters.append(ST_Intersects(DatasetFeature.geometry, spatial_geom))
            elif query_params.spatial_filter.operation == 'within':
                filters.append(ST_Within(DatasetFeature.geometry, spatial_geom))
            elif query_params.spatial_filter.operation == 'contains':
                filters.append(ST_Contains(DatasetFeature.geometry, spatial_geom))
        
        # Attribute filters
        if query_params.attribute_filters:
            for attr_filter in query_params.attribute_filters:
                attr_condition = UnifiedDataDAO._build_attribute_filter(attr_filter)
                if attr_condition is not None:
                    filters.append(attr_condition)
        
        # Apply all filters
        if filters:
            filtered_query = base_query.filter(and_(*filters))
        else:
            filtered_query = base_query
        
        # Get total count
        count_query = db.query(func.count(DatasetFeature.id)).join(Dataset)
        if filters:
            count_query = count_query.filter(and_(*filters))
        total = count_query.scalar()
        
        # Apply pagination
        results = filtered_query.offset(query_params.offset).limit(query_params.limit).all()
        
        # Convert to dicts
        features = []
        for result in results:
            feature_dict = {
                'id': result.id,
                'dataset_id': result.dataset_id,
                'dataset_name': result.dataset_name,
                'dataset_display_name': result.dataset_display_name,
                'feature_id': result.feature_id,
                'attributes': result.attributes,
                'created_at': result.created_at
            }
            
            if query_params.include_geometry and hasattr(result, 'geometry_json'):
                if result.geometry_json:
                    feature_dict['geometry'] = json.loads(result.geometry_json)
                else:
                    feature_dict['geometry'] = None
            
            features.append(feature_dict)
        
        return features, total
    
    @staticmethod
    def _build_attribute_filter(attr_filter: AttributeFilter):
        """Build SQLAlchemy filter condition for JSONB attributes"""
        try:
            field_path = f"attributes->>'{attr_filter.field}'"
            
            if attr_filter.operator == 'eq':
                return text(f"{field_path} = :value").params(value=str(attr_filter.value))
            elif attr_filter.operator == 'ne':
                return text(f"{field_path} != :value").params(value=str(attr_filter.value))
            elif attr_filter.operator == 'gt':
                return text(f"({field_path})::float > :value").params(value=float(attr_filter.value))
            elif attr_filter.operator == 'lt':
                return text(f"({field_path})::float < :value").params(value=float(attr_filter.value))
            elif attr_filter.operator == 'gte':
                return text(f"({field_path})::float >= :value").params(value=float(attr_filter.value))
            elif attr_filter.operator == 'lte':
                return text(f"({field_path})::float <= :value").params(value=float(attr_filter.value))
            elif attr_filter.operator == 'like':
                return text(f"{field_path} LIKE :value").params(value=f'%{attr_filter.value}%')
            elif attr_filter.operator == 'ilike':
                return text(f"{field_path} ILIKE :value").params(value=f'%{attr_filter.value}%')
            elif attr_filter.operator == 'in' and isinstance(attr_filter.value, list):
                values = [str(v) for v in attr_filter.value]
                placeholders = ','.join([f':val{i}' for i in range(len(values))])
                params = {f'val{i}': val for i, val in enumerate(values)}
                return text(f"{field_path} IN ({placeholders})").params(**params)
        
        except (ValueError, TypeError) as e:
            logger.warning(f"Invalid attribute filter: {attr_filter.field} {attr_filter.operator} {attr_filter.value} - {e}")
        
        return None
    
    @staticmethod
    def get_dataset_statistics(db: Session) -> Dict[str, Any]:
        """Get overall dataset statistics"""
        total_datasets = db.query(Dataset).count()
        total_features = db.query(DatasetFeature).count()
        
        # Datasets by type
        datasets_by_type = dict(
            db.query(Dataset.dataset_type, func.count(Dataset.id))
            .group_by(Dataset.dataset_type)
            .all()
        )
        
        # Datasets by category
        datasets_by_category = dict(
            db.query(DatasetCategory.display_name, func.count(Dataset.id))
            .join(Dataset)
            .group_by(DatasetCategory.display_name)
            .all()
        )
        
        # Datasets by status
        datasets_by_status = dict(
            db.query(Dataset.status, func.count(Dataset.id))
            .group_by(Dataset.status)
            .all()
        )
        
        # Total storage
        total_storage_mb = db.query(func.sum(Dataset.file_size_mb)).scalar() or 0
        
        return {
            'total_datasets': total_datasets,
            'total_features': total_features,
            'datasets_by_type': datasets_by_type,
            'datasets_by_category': datasets_by_category,
            'datasets_by_status': datasets_by_status,
            'total_storage_mb': float(total_storage_mb)
        }
    
    @staticmethod
    def get_category_statistics(db: Session, category_id: int) -> Optional[Dict[str, Any]]:
        """Get statistics for a specific category"""
        category = db.query(DatasetCategory).filter(DatasetCategory.id == category_id).first()
        if not category:
            return None
        
        dataset_count = db.query(Dataset).filter(Dataset.category_id == category_id).count()
        
        feature_count = (
            db.query(func.count(DatasetFeature.id))
            .join(Dataset)
            .filter(Dataset.category_id == category_id)
            .scalar() or 0
        )
        
        storage_mb = (
            db.query(func.sum(Dataset.file_size_mb))
            .filter(Dataset.category_id == category_id)
            .scalar() or 0
        )
        
        latest_update = (
            db.query(func.max(Dataset.updated_at))
            .filter(Dataset.category_id == category_id)
            .scalar()
        )
        
        return {
            'category': category,
            'dataset_count': dataset_count,
            'feature_count': feature_count,
            'storage_mb': float(storage_mb),
            'latest_update': latest_update
        }
    
    @staticmethod
    def create_processing_log(db: Session, log_data: Dict[str, Any]) -> DatasetProcessingLog:
        """Create a processing log entry"""
        log_entry = DatasetProcessingLog(**log_data)
        db.add(log_entry)
        db.commit()
        db.refresh(log_entry)
        return log_entry
    
    @staticmethod
    def update_processing_log(db: Session, log_id: int, update_data: Dict[str, Any]) -> Optional[DatasetProcessingLog]:
        """Update a processing log entry"""
        log_entry = db.query(DatasetProcessingLog).filter(DatasetProcessingLog.id == log_id).first()
        if log_entry:
            for key, value in update_data.items():
                if hasattr(log_entry, key):
                    setattr(log_entry, key, value)
            db.commit()
            db.refresh(log_entry)
        return log_entry
    
    @staticmethod
    def get_processing_logs(db: Session, dataset_id: int) -> List[DatasetProcessingLog]:
        """Get processing logs for a dataset"""
        return (
            db.query(DatasetProcessingLog)
            .filter(DatasetProcessingLog.dataset_id == dataset_id)
            .order_by(DatasetProcessingLog.started_at.desc())
            .all()
        )
    
    @staticmethod
    def delete_features_by_dataset(db: Session, dataset_id: int) -> int:
        """Delete all features for a dataset (for reprocessing)"""
        deleted_count = db.query(DatasetFeature).filter(DatasetFeature.dataset_id == dataset_id).delete()
        db.commit()
        return deleted_count
    
    @staticmethod
    def get_datasets_by_bbox(
        db: Session, 
        bbox: BoundingBox,
        dataset_types: Optional[List[str]] = None,
        limit: int = 100
    ) -> List[Dataset]:
        """Get datasets that intersect with a bounding box"""
        
        # Create bounding box polygon
        bbox_wkt = f"POLYGON(({bbox.minx} {bbox.miny}, {bbox.maxx} {bbox.miny}, {bbox.maxx} {bbox.maxy}, {bbox.minx} {bbox.maxy}, {bbox.minx} {bbox.miny}))"
        
        query = db.query(Dataset).join(DatasetFeature)
        
        # Spatial filter
        query = query.filter(ST_Intersects(DatasetFeature.geometry, WKTElement(bbox_wkt, srid=4326)))
        
        # Type filter
        if dataset_types:
            query = query.filter(Dataset.dataset_type.in_(dataset_types))
        
        # Remove duplicates and limit
        query = query.distinct().limit(limit)
        
        return query.all()