"""
Unified Data Management Service
Orchestrates the entire workflow from upload to publication
"""
import os
import shutil
import tempfile
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from sqlalchemy.orm import Session
import logging

from unified_data.models.model import (
    DatasetUploadRequest, DatasetUploadResponse, DatasetQuery,
    DatasetCreate, DatasetUpdate, DatasetStatus, ProcessingLogCreate
)
from unified_data.models.schema import Dataset, DatasetFeature
from unified_data.dao.dao import UnifiedDataDAO
from unified_data.services.data_processor import DataProcessor
from geoserver.service import GeoServerService

logger = logging.getLogger(__name__)


class UnifiedDataService:
    """Main service for unified data management operations"""
    
    def __init__(self, 
                 storage_root: str = "data/unified_storage",
                 geoserver_service: Optional[GeoServerService] = None):
        self.storage_root = Path(storage_root)
        self.storage_root.mkdir(parents=True, exist_ok=True)
        
        self.processor = DataProcessor()
        self.geoserver_service = geoserver_service
        
        # Create storage directories
        (self.storage_root / "raw").mkdir(exist_ok=True)
        (self.storage_root / "processed").mkdir(exist_ok=True)
        (self.storage_root / "temp").mkdir(exist_ok=True)
    
    async def upload_dataset(
        self, 
        db: Session, 
        file_path: str, 
        upload_request: DatasetUploadRequest
    ) -> DatasetUploadResponse:
        """
        Main entry point for dataset upload and processing
        """
        processing_logs = []
        dataset = None
        
        try:
            # Step 1: Create initial dataset record
            log_id = self._create_log(db, None, "upload", "in_progress", "Starting dataset upload")
            processing_logs.append(log_id)
            
            dataset_data = {
                'uuid': str(uuid.uuid4()),
                'name': upload_request.name,
                'display_name': upload_request.name,
                'description': upload_request.description,
                'category_id': upload_request.category_id,
                'source': upload_request.source,
                'uploaded_by': upload_request.uploaded_by,
                'status': DatasetStatus.UPLOADED.value,
                'original_file_path': file_path,
                'file_size_mb': os.path.getsize(file_path) / (1024 * 1024)
            }
            
            dataset = UnifiedDataDAO.create_dataset(db, dataset_data)
            
            # Update log with dataset ID
            UnifiedDataDAO.update_processing_log(db, log_id, {
                'dataset_id': dataset.id,
                'status': 'success',
                'message': 'Dataset record created'
            })
            
            # Step 2: Move file to storage
            storage_log_id = self._create_log(db, dataset.id, "storage", "in_progress", "Moving file to storage")
            processing_logs.append(storage_log_id)
            
            stored_path = self._store_file(file_path, dataset.uuid, Path(file_path).suffix)
            
            UnifiedDataDAO.update_dataset(db, dataset.id, {
                'original_file_path': str(stored_path)
            })
            
            UnifiedDataDAO.update_processing_log(db, storage_log_id, {
                'status': 'success',
                'message': f'File stored at {stored_path}'
            })
            
            # Step 3: Process file and extract data
            process_log_id = self._create_log(db, dataset.id, "processing", "in_progress", "Processing file data")
            processing_logs.append(process_log_id)
            
            metadata, features = self.processor.process_file(str(stored_path), dataset_data)
            
            # Update dataset with extracted metadata
            UnifiedDataDAO.update_dataset(db, dataset.id, {
                **metadata,
                'status': DatasetStatus.PROCESSING.value
            })
            
            UnifiedDataDAO.update_processing_log(db, process_log_id, {
                'status': 'success',
                'message': f'Processed {len(features)} features',
                'details': {'feature_count': len(features), 'metadata': metadata}
            })
            
            # Step 4: Store features in database
            features_log_id = self._create_log(db, dataset.id, "features_storage", "in_progress", "Storing features in database")
            processing_logs.append(features_log_id)
            
            if features:
                # Store features in batches to avoid memory issues
                batch_size = 1000
                total_stored = 0
                
                for i in range(0, len(features), batch_size):
                    batch = features[i:i + batch_size]
                    UnifiedDataDAO.create_features(db, dataset.id, batch)
                    total_stored += len(batch)
                    
                    logger.info(f"Stored batch {i//batch_size + 1}: {total_stored}/{len(features)} features")
            
            UnifiedDataDAO.update_processing_log(db, features_log_id, {
                'status': 'success',
                'message': f'Stored {len(features)} features in database'
            })
            
            # Update dataset status
            UnifiedDataDAO.update_dataset(db, dataset.id, {
                'status': DatasetStatus.PROCESSED.value
            })
            
            # Step 5: Publish to GeoServer (if requested)
            if upload_request.auto_publish and self.geoserver_service:
                publish_log_id = self._create_log(db, dataset.id, "geoserver_publish", "in_progress", "Publishing to GeoServer")
                processing_logs.append(publish_log_id)
                
                try:
                    publication_result = await self._publish_to_geoserver(
                        db, dataset, upload_request.geoserver_workspace
                    )
                    
                    UnifiedDataDAO.update_processing_log(db, publish_log_id, {
                        'status': 'success',
                        'message': 'Published to GeoServer',
                        'details': publication_result
                    })
                    
                    # Update dataset with GeoServer info
                    UnifiedDataDAO.update_dataset(db, dataset.id, {
                        'status': DatasetStatus.PUBLISHED.value,
                        'is_published': True,
                        'geoserver_workspace': upload_request.geoserver_workspace,
                        'geoserver_layer_name': publication_result.get('layer_name'),
                        'wms_url': publication_result.get('wms_url'),
                        'wfs_url': publication_result.get('wfs_url')
                    })
                    
                except Exception as e:
                    logger.error(f"Failed to publish dataset {dataset.id} to GeoServer: {e}")
                    UnifiedDataDAO.update_processing_log(db, publish_log_id, {
                        'status': 'error',
                        'message': f'GeoServer publication failed: {str(e)}'
                    })
            
            # Return success response
            final_dataset = UnifiedDataDAO.get_dataset_by_id(db, dataset.id)
            
            return DatasetUploadResponse(
                dataset_id=dataset.id,
                dataset_uuid=uuid.UUID(dataset.uuid),
                status=DatasetStatus(final_dataset.status),
                message="Dataset processed successfully",
                processing_details={
                    'features_count': len(features),
                    'file_size_mb': dataset_data['file_size_mb'],
                    'processing_logs': processing_logs
                }
            )
            
        except Exception as e:
            logger.error(f"Error processing dataset upload: {e}")
            
            # Mark dataset as error if it exists
            if dataset:
                UnifiedDataDAO.update_dataset(db, dataset.id, {
                    'status': DatasetStatus.ERROR.value,
                    'error_message': str(e)
                })
                
                # Create error log
                error_log_id = self._create_log(db, dataset.id, "error", "error", f"Processing failed: {str(e)}")
                processing_logs.append(error_log_id)
            
            # Clean up stored file if processing failed
            try:
                if 'stored_path' in locals():
                    os.remove(stored_path)
            except:
                pass
            
            return DatasetUploadResponse(
                dataset_id=dataset.id if dataset else 0,
                dataset_uuid=uuid.UUID(dataset.uuid) if dataset else uuid.uuid4(),
                status=DatasetStatus.ERROR,
                message=f"Processing failed: {str(e)}",
                processing_details={'error': str(e), 'processing_logs': processing_logs}
            )
    
    def get_datasets(
        self,
        db: Session,
        category_id: Optional[int] = None,
        dataset_type: Optional[str] = None,
        status: Optional[str] = None,
        search: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> Tuple[List[Dataset], int]:
        """Get datasets with filtering"""
        return UnifiedDataDAO.get_datasets(
            db, category_id, dataset_type, status, search, limit, offset
        )
    
    def get_dataset_by_id(self, db: Session, dataset_id: int) -> Optional[Dataset]:
        """Get a dataset by ID"""
        return UnifiedDataDAO.get_dataset_by_id(db, dataset_id)
    
    def update_dataset(self, db: Session, dataset_id: int, update_data: DatasetUpdate) -> Optional[Dataset]:
        """Update dataset metadata"""
        update_dict = update_data.dict(exclude_unset=True)
        return UnifiedDataDAO.update_dataset(db, dataset_id, update_dict)
    
    def delete_dataset(self, db: Session, dataset_id: int) -> bool:
        """Delete a dataset and its associated files"""
        dataset = UnifiedDataDAO.get_dataset_by_id(db, dataset_id)
        if not dataset:
            return False
        
        try:
            # Delete files
            if dataset.original_file_path and os.path.exists(dataset.original_file_path):
                os.remove(dataset.original_file_path)
            
            if dataset.processed_file_path and os.path.exists(dataset.processed_file_path):
                os.remove(dataset.processed_file_path)
            
            # Delete from GeoServer if published
            if dataset.is_published and self.geoserver_service:
                try:
                    # Note: Implement GeoServer layer deletion in GeoServerService
                    pass
                except Exception as e:
                    logger.warning(f"Failed to delete GeoServer layer: {e}")
            
            # Delete from database
            return UnifiedDataDAO.delete_dataset(db, dataset_id)
            
        except Exception as e:
            logger.error(f"Error deleting dataset {dataset_id}: {e}")
            return False
    
    def query_features(self, db: Session, query: DatasetQuery) -> Tuple[List[Dict[str, Any]], int]:
        """Query features with advanced filtering"""
        return UnifiedDataDAO.query_features(db, query)
    
    def get_dataset_features(
        self, 
        db: Session, 
        dataset_id: int, 
        limit: int = 1000, 
        offset: int = 0,
        include_geometry: bool = True
    ) -> Tuple[List[Dict[str, Any]], int]:
        """Get features for a specific dataset"""
        return UnifiedDataDAO.get_features_by_dataset(db, dataset_id, limit, offset, include_geometry)
    
    async def reprocess_dataset(self, db: Session, dataset_id: int) -> bool:
        """Reprocess an existing dataset"""
        dataset = UnifiedDataDAO.get_dataset_by_id(db, dataset_id)
        if not dataset or not dataset.original_file_path:
            return False
        
        try:
            # Create reprocessing log
            log_id = self._create_log(db, dataset_id, "reprocessing", "in_progress", "Reprocessing dataset")
            
            # Delete existing features
            deleted_count = UnifiedDataDAO.delete_features_by_dataset(db, dataset_id)
            logger.info(f"Deleted {deleted_count} existing features for dataset {dataset_id}")
            
            # Reprocess file
            metadata, features = self.processor.process_file(dataset.original_file_path, {})
            
            # Update metadata
            UnifiedDataDAO.update_dataset(db, dataset_id, {
                **metadata,
                'status': DatasetStatus.PROCESSING.value,
                'error_message': None
            })
            
            # Store new features
            if features:
                batch_size = 1000
                for i in range(0, len(features), batch_size):
                    batch = features[i:i + batch_size]
                    UnifiedDataDAO.create_features(db, dataset_id, batch)
            
            # Update status
            UnifiedDataDAO.update_dataset(db, dataset_id, {
                'status': DatasetStatus.PROCESSED.value
            })
            
            UnifiedDataDAO.update_processing_log(db, log_id, {
                'status': 'success',
                'message': f'Reprocessed {len(features)} features'
            })
            
            return True
            
        except Exception as e:
            logger.error(f"Error reprocessing dataset {dataset_id}: {e}")
            UnifiedDataDAO.update_dataset(db, dataset_id, {
                'status': DatasetStatus.ERROR.value,
                'error_message': str(e)
            })
            return False
    
    async def publish_dataset(self, db: Session, dataset_id: int, workspace: str = "unified_data") -> bool:
        """Publish a processed dataset to GeoServer"""
        if not self.geoserver_service:
            logger.error("GeoServer service not configured")
            return False
        
        dataset = UnifiedDataDAO.get_dataset_by_id(db, dataset_id)
        if not dataset or dataset.status != DatasetStatus.PROCESSED.value:
            logger.error(f"Dataset {dataset_id} not found or not in processed state")
            return False
        
        try:
            log_id = self._create_log(db, dataset_id, "geoserver_publish", "in_progress", "Publishing to GeoServer")
            
            publication_result = await self._publish_to_geoserver(db, dataset, workspace)
            
            # Update dataset
            UnifiedDataDAO.update_dataset(db, dataset_id, {
                'status': DatasetStatus.PUBLISHED.value,
                'is_published': True,
                'geoserver_workspace': workspace,
                'geoserver_layer_name': publication_result.get('layer_name'),
                'wms_url': publication_result.get('wms_url'),
                'wfs_url': publication_result.get('wfs_url')
            })
            
            UnifiedDataDAO.update_processing_log(db, log_id, {
                'status': 'success',
                'message': 'Published to GeoServer',
                'details': publication_result
            })
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to publish dataset {dataset_id}: {e}")
            UnifiedDataDAO.update_processing_log(db, log_id, {
                'status': 'error',
                'message': f'Publication failed: {str(e)}'
            })
            return False
    
    def get_statistics(self, db: Session) -> Dict[str, Any]:
        """Get system statistics"""
        return UnifiedDataDAO.get_dataset_statistics(db)
    
    def get_category_statistics(self, db: Session, category_id: int) -> Optional[Dict[str, Any]]:
        """Get statistics for a specific category"""
        return UnifiedDataDAO.get_category_statistics(db, category_id)
    
    def _store_file(self, source_path: str, dataset_uuid: str, extension: str) -> Path:
        """Store uploaded file in organized directory structure"""
        # Create year/month directory structure
        now = datetime.now()
        storage_dir = self.storage_root / "raw" / str(now.year) / f"{now.month:02d}"
        storage_dir.mkdir(parents=True, exist_ok=True)
        
        # Create unique filename
        filename = f"{dataset_uuid}{extension}"
        destination = storage_dir / filename
        
        # Move file
        shutil.move(source_path, destination)
        
        return destination
    
    def _create_log(self, db: Session, dataset_id: Optional[int], step: str, status: str, message: str) -> int:
        """Create a processing log entry"""
        log_data = {
            'dataset_id': dataset_id,
            'processing_step': step,
            'status': status,
            'message': message
        }
        log_entry = UnifiedDataDAO.create_processing_log(db, log_data)
        return log_entry.id
    
    async def _publish_to_geoserver(self, db: Session, dataset: Dataset, workspace: str) -> Dict[str, Any]:
        """Publish dataset to GeoServer"""
        # This is a simplified version - you'll need to implement based on your GeoServer service
        # For vector data, you might create a PostGIS datastore connection
        # For raster data, you might upload the file directly
        
        layer_name = f"{dataset.name}_{dataset.id}"
        
        # Create workspace if it doesn't exist
        try:
            self.geoserver_service.create_workspace(workspace)
        except:
            pass  # Workspace might already exist
        
        if dataset.dataset_type in ['vector', 'shapefile', 'geojson']:
            # For vector data, create PostGIS datastore and layer
            # This assumes your GeoServer can connect to your PostGIS database
            
            # You'll need to implement this based on your specific setup
            # Example structure:
            datastore_name = f"unified_data_store"
            
            # Create or use existing PostGIS datastore
            # Create layer pointing to the dataset features table
            
            wms_url = f"{self.geoserver_service.dao.base_url.replace('/rest', '')}/wms"
            wfs_url = f"{self.geoserver_service.dao.base_url.replace('/rest', '')}/wfs"
            
            return {
                'layer_name': f"{workspace}:{layer_name}",
                'wms_url': wms_url,
                'wfs_url': wfs_url,
                'workspace': workspace,
                'datastore': datastore_name
            }
        
        elif dataset.dataset_type == 'raster':
            # For raster data, upload the file directly to GeoServer
            # Implementation depends on your raster handling approach
            
            wms_url = f"{self.geoserver_service.dao.base_url.replace('/rest', '')}/wms"
            
            return {
                'layer_name': f"{workspace}:{layer_name}",
                'wms_url': wms_url,
                'workspace': workspace
            }
        
        else:
            raise ValueError(f"Unsupported dataset type for GeoServer: {dataset.dataset_type}")