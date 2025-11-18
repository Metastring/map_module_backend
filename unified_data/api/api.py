"""
Unified Data Management REST API
Provides comprehensive frontend-ready APIs for dataset management
"""
from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile, status, BackgroundTasks
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from typing import List, Optional, Dict, Any
import tempfile
import os
import logging

from database.database import get_db
from unified_data.models.model import (
    DatasetCategoryCreate, DatasetCategoryOut,
    DatasetCreate, DatasetOut, DatasetUpdate,
    DatasetUploadRequest, DatasetUploadResponse,
    DatasetQuery, AttributeFilter, SpatialFilter, BoundingBox,
    DatasetStatistics, CategoryStatistics,
    ProcessingLogOut, DatasetType, DatasetStatus
)
from unified_data.services.unified_service import UnifiedDataService
from unified_data.dao.dao import UnifiedDataDAO
from geoserver.service import GeoServerService
from geoserver.dao import GeoServerDAO
from utils.config import geoserver_host, geoserver_port, geoserver_username, geoserver_password

# Initialize router
router = APIRouter(prefix="/unified-data", tags=["unified-data"])

# Initialize services
geo_dao = GeoServerDAO(
    base_url=f"http://{geoserver_host}:{geoserver_port}/geoserver/rest",
    username=geoserver_username,
    password=geoserver_password
)
geo_service = GeoServerService(geo_dao)
unified_service = UnifiedDataService(geoserver_service=geo_service)

logger = logging.getLogger(__name__)


# Category Management
@router.post("/categories", response_model=DatasetCategoryOut, status_code=status.HTTP_201_CREATED)
async def create_category(
    category: DatasetCategoryCreate,
    db: Session = Depends(get_db)
):
    """Create a new dataset category"""
    try:
        category_data = category.dict()
        db_category = UnifiedDataDAO.create_category(db, category_data)
        return db_category
    except Exception as e:
        logger.error(f"Error creating category: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/categories", response_model=List[DatasetCategoryOut])
async def list_categories(db: Session = Depends(get_db)):
    """List all dataset categories"""
    return UnifiedDataDAO.get_categories(db)


@router.get("/categories/{category_id}/statistics", response_model=CategoryStatistics)
async def get_category_statistics(
    category_id: int,
    db: Session = Depends(get_db)
):
    """Get statistics for a specific category"""
    stats = unified_service.get_category_statistics(db, category_id)
    if not stats:
        raise HTTPException(status_code=404, detail="Category not found")
    return stats


# Dataset Management
@router.post("/upload", response_model=DatasetUploadResponse, status_code=status.HTTP_201_CREATED)
async def upload_dataset(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    name: str = Form(...),
    category_id: int = Form(...),
    description: Optional[str] = Form(None),
    source: Optional[str] = Form(None),
    uploaded_by: str = Form(...),
    auto_publish: bool = Form(True),
    geoserver_workspace: str = Form("unified_data"),
    db: Session = Depends(get_db)
):
    """
    Upload and process a new dataset
    Supports: Shapefiles (ZIP), GeoJSON, CSV with coordinates, GeoTIFF, etc.
    """
    temp_file = None
    try:
        # Save uploaded file temporarily
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=f"_{file.filename}")
        temp_file.write(await file.read())
        temp_file.close()
        
        # Create upload request
        upload_request = DatasetUploadRequest(
            name=name,
            category_id=category_id,
            description=description,
            source=source,
            uploaded_by=uploaded_by,
            auto_publish=auto_publish,
            geoserver_workspace=geoserver_workspace
        )
        
        # Process dataset
        result = await unified_service.upload_dataset(db, temp_file.name, upload_request)
        
        return result
        
    except Exception as e:
        logger.error(f"Error uploading dataset: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        # Clean up temp file
        if temp_file and os.path.exists(temp_file.name):
            try:
                os.unlink(temp_file.name)
            except:
                pass


@router.get("/datasets", response_model=Dict[str, Any])
async def list_datasets(
    category_id: Optional[int] = Query(None, description="Filter by category"),
    dataset_type: Optional[DatasetType] = Query(None, description="Filter by type"),
    status: Optional[DatasetStatus] = Query(None, description="Filter by status"),
    search: Optional[str] = Query(None, description="Search in name/description"),
    limit: int = Query(100, ge=1, le=1000, description="Number of results"),
    offset: int = Query(0, ge=0, description="Results offset"),
    db: Session = Depends(get_db)
):
    """List datasets with filtering and pagination"""
    try:
        datasets, total = unified_service.get_datasets(
            db, category_id, dataset_type.value if dataset_type else None,
            status.value if status else None, search, limit, offset
        )
        
        return {
            "datasets": datasets,
            "total": total,
            "limit": limit,
            "offset": offset,
            "has_more": (offset + limit) < total
        }
    except Exception as e:
        logger.error(f"Error listing datasets: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/datasets/{dataset_id}", response_model=DatasetOut)
async def get_dataset(dataset_id: int, db: Session = Depends(get_db)):
    """Get detailed information about a dataset"""
    dataset = unified_service.get_dataset_by_id(db, dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return dataset


@router.put("/datasets/{dataset_id}", response_model=DatasetOut)
async def update_dataset(
    dataset_id: int,
    update_data: DatasetUpdate,
    db: Session = Depends(get_db)
):
    """Update dataset metadata"""
    dataset = unified_service.update_dataset(db, dataset_id, update_data)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return dataset


@router.delete("/datasets/{dataset_id}")
async def delete_dataset(dataset_id: int, db: Session = Depends(get_db)):
    """Delete a dataset and all associated data"""
    success = unified_service.delete_dataset(db, dataset_id)
    if not success:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return {"message": "Dataset deleted successfully"}


@router.get("/datasets/{dataset_id}/features")
async def get_dataset_features(
    dataset_id: int,
    limit: int = Query(1000, ge=1, le=10000, description="Number of features"),
    offset: int = Query(0, ge=0, description="Features offset"),
    include_geometry: bool = Query(True, description="Include geometry in response"),
    db: Session = Depends(get_db)
):
    """Get features for a specific dataset"""
    try:
        features, total = unified_service.get_dataset_features(
            db, dataset_id, limit, offset, include_geometry
        )
        
        return {
            "features": features,
            "total": total,
            "limit": limit,
            "offset": offset,
            "has_more": (offset + limit) < total
        }
    except Exception as e:
        logger.error(f"Error getting dataset features: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Advanced Querying
@router.post("/query/features")
async def query_features(query: DatasetQuery, db: Session = Depends(get_db)):
    """
    Advanced feature querying with spatial and attribute filters
    
    Examples:
    - Spatial filter: Find all features within a polygon
    - Attribute filter: Find features where temperature > 30
    - Combined: Find species in protected areas with population > 100
    """
    try:
        features, total = unified_service.query_features(db, query)
        
        return {
            "features": features,
            "total": total,
            "limit": query.limit,
            "offset": query.offset,
            "has_more": (query.offset + query.limit) < total,
            "query_summary": {
                "datasets_queried": len(query.dataset_ids) if query.dataset_ids else "all",
                "categories_queried": len(query.category_ids) if query.category_ids else "all",
                "has_spatial_filter": query.spatial_filter is not None,
                "attribute_filters_count": len(query.attribute_filters) if query.attribute_filters else 0
            }
        }
    except Exception as e:
        logger.error(f"Error querying features: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/query/bbox")
async def query_by_bbox(
    minx: float = Query(..., description="Minimum longitude"),
    miny: float = Query(..., description="Minimum latitude"),
    maxx: float = Query(..., description="Maximum longitude"),
    maxy: float = Query(..., description="Maximum latitude"),
    dataset_types: Optional[List[DatasetType]] = Query(None, description="Filter by dataset types"),
    limit: int = Query(100, ge=1, le=1000),
    db: Session = Depends(get_db)
):
    """Query datasets that intersect with a bounding box"""
    try:
        bbox = BoundingBox(minx=minx, miny=miny, maxx=maxx, maxy=maxy)
        type_values = [t.value for t in dataset_types] if dataset_types else None
        
        datasets = UnifiedDataDAO.get_datasets_by_bbox(db, bbox, type_values, limit)
        
        return {
            "datasets": datasets,
            "bbox": bbox.dict(),
            "count": len(datasets)
        }
    except Exception as e:
        logger.error(f"Error querying by bbox: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Dataset Operations
@router.post("/datasets/{dataset_id}/reprocess")
async def reprocess_dataset(
    dataset_id: int,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """Reprocess an existing dataset (useful after fixing data issues)"""
    dataset = unified_service.get_dataset_by_id(db, dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    # Run reprocessing in background
    background_tasks.add_task(unified_service.reprocess_dataset, db, dataset_id)
    
    return {"message": "Dataset reprocessing started", "dataset_id": dataset_id}


@router.post("/datasets/{dataset_id}/publish")
async def publish_dataset(
    dataset_id: int,
    workspace: str = Query("unified_data", description="GeoServer workspace"),
    db: Session = Depends(get_db)
):
    """Publish a processed dataset to GeoServer"""
    dataset = unified_service.get_dataset_by_id(db, dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    if dataset.status not in [DatasetStatus.PROCESSED.value, DatasetStatus.PUBLISHED.value]:
        raise HTTPException(
            status_code=400, 
            detail=f"Dataset must be processed before publishing. Current status: {dataset.status}"
        )
    
    try:
        success = await unified_service.publish_dataset(db, dataset_id, workspace)
        if success:
            return {"message": "Dataset published successfully", "dataset_id": dataset_id}
        else:
            raise HTTPException(status_code=500, detail="Publication failed")
    except Exception as e:
        logger.error(f"Error publishing dataset: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/datasets/{dataset_id}/processing-logs", response_model=List[ProcessingLogOut])
async def get_processing_logs(dataset_id: int, db: Session = Depends(get_db)):
    """Get processing logs for a dataset"""
    return UnifiedDataDAO.get_processing_logs(db, dataset_id)


# Statistics and Analytics
@router.get("/statistics", response_model=DatasetStatistics)
async def get_statistics(db: Session = Depends(get_db)):
    """Get overall system statistics"""
    return unified_service.get_statistics(db)


@router.get("/datasets/{dataset_id}/tile-url")
async def get_dataset_tile_url(dataset_id: int, db: Session = Depends(get_db)):
    """Get WMS tile URL for a published dataset"""
    dataset = unified_service.get_dataset_by_id(db, dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    if not dataset.is_published or not dataset.geoserver_layer_name:
        raise HTTPException(status_code=400, detail="Dataset is not published to GeoServer")
    
    try:
        tile_url = geo_service.get_tile_layer_url(dataset.geoserver_layer_name)
        return {
            "dataset_id": dataset_id,
            "layer_name": dataset.geoserver_layer_name,
            "wms_url": dataset.wms_url,
            "wfs_url": dataset.wfs_url,
            "tile_url": tile_url
        }
    except Exception as e:
        logger.error(f"Error getting tile URL: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Bulk Operations
@router.post("/datasets/bulk/publish")
async def bulk_publish_datasets(
    dataset_ids: List[int],
    workspace: str = Query("unified_data"),
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """Publish multiple datasets to GeoServer"""
    
    # Validate datasets exist and are processable
    valid_datasets = []
    for dataset_id in dataset_ids:
        dataset = unified_service.get_dataset_by_id(db, dataset_id)
        if dataset and dataset.status in [DatasetStatus.PROCESSED.value, DatasetStatus.PUBLISHED.value]:
            valid_datasets.append(dataset_id)
    
    if not valid_datasets:
        raise HTTPException(status_code=400, detail="No valid datasets found for publishing")
    
    # Start background publishing
    for dataset_id in valid_datasets:
        background_tasks.add_task(unified_service.publish_dataset, db, dataset_id, workspace)
    
    return {
        "message": f"Started publishing {len(valid_datasets)} datasets",
        "dataset_ids": valid_datasets,
        "skipped": len(dataset_ids) - len(valid_datasets)
    }


@router.get("/datasets/search/attributes")
async def search_by_attributes(
    field: str = Query(..., description="Attribute field name"),
    value: str = Query(..., description="Search value"),
    operator: str = Query("ilike", description="Comparison operator"),
    dataset_types: Optional[List[DatasetType]] = Query(None),
    category_ids: Optional[List[int]] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    db: Session = Depends(get_db)
):
    """
    Search features by attribute values
    
    Examples:
    - /search/attributes?field=species_name&value=tiger&operator=ilike
    - /search/attributes?field=temperature&value=30&operator=gt
    - /search/attributes?field=soil_type&value=clay&operator=eq
    """
    try:
        # Build query with attribute filter
        query = DatasetQuery(
            dataset_types=dataset_types,
            category_ids=category_ids,
            attribute_filters=[AttributeFilter(field=field, operator=operator, value=value)],
            limit=limit
        )
        
        features, total = unified_service.query_features(db, query)
        
        return {
            "features": features,
            "total": total,
            "search_criteria": {
                "field": field,
                "operator": operator,
                "value": value
            }
        }
    except Exception as e:
        logger.error(f"Error searching by attributes: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# File Information (before upload)
@router.post("/analyze-file")
async def analyze_file(file: UploadFile = File(...)):
    """
    Analyze an uploaded file without processing it
    Returns file information, detected type, estimated feature count, etc.
    """
    temp_file = None
    try:
        # Save file temporarily
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=f"_{file.filename}")
        temp_file.write(await file.read())
        temp_file.close()
        
        # Analyze file
        processor = unified_service.processor
        file_info = processor.get_file_info(temp_file.name)
        
        return {
            "filename": file.filename,
            "analysis": file_info,
            "recommendations": {
                "suitable_for_upload": file_info.get('detected_type') is not None,
                "detected_type": file_info.get('detected_type'),
                "estimated_processing_time": "fast" if file_info.get('size_mb', 0) < 10 else "moderate"
            }
        }
        
    except Exception as e:
        logger.error(f"Error analyzing file: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if temp_file and os.path.exists(temp_file.name):
            try:
                os.unlink(temp_file.name)
            except:
                pass


# Health and Status
@router.get("/health")
async def health_check(db: Session = Depends(get_db)):
    """Health check for unified data management system"""
    try:
        # Test database connection
        dataset_count = UnifiedDataDAO.get_dataset_statistics(db)['total_datasets']
        
        # Test GeoServer connection
        geoserver_status = "connected"
        try:
            geo_service.list_workspaces()
        except:
            geoserver_status = "disconnected"
        
        return {
            "status": "healthy",
            "service": "unified-data-management",
            "database_connection": "connected",
            "geoserver_connection": geoserver_status,
            "total_datasets": dataset_count
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return JSONResponse(
            status_code=503,
            content={
                "status": "unhealthy",
                "error": str(e)
            }
        )