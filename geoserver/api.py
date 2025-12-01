from urllib.parse import urlencode
from fastapi import APIRouter, HTTPException, Depends, Query, FastAPI, UploadFile, File, Form
from fastapi.responses import JSONResponse
import requests
from geoserver.model import UploadRequest
from geoserver.model import UpdateRequest
from geoserver.model import PostGISRequest
from geoserver.model import CreateLayerRequest
from geoserver.model import PublishUploadLogRequest, PublishUploadLogResponse
from geoserver.service import GeoServerService
from geoserver.dao import GeoServerDAO
import shutil
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
from utils.config import *
from typing import List, Dict, Optional
from sqlalchemy.orm import Session
from database.database import get_db
from metadata.service.service import MetadataService
from metadata.models.schema import Metadata
import logging

logger = logging.getLogger(__name__)

# Initialize router
router = APIRouter()

# Initialize DAO and Service with configuration
# You can override these values in your config.py file
# Use values directly from config.py
# geoserver_host, geoserver_port, geoserver_username, geoserver_password are imported from config

geo_dao = GeoServerDAO(
    base_url=f"http://{geoserver_host}:{geoserver_port}/geoserver/rest", 
    username=geoserver_username, 
    password=geoserver_password
)
geo_service = GeoServerService(geo_dao)
# geo_service = GeoServerService()

@router.post("/upload")
async def upload_resource(
    workspace: str = Form(...),
    store_name: str = Form(...),
    resource_type: str = Form(...),
    file: UploadFile = File(...)
):
    """
    Upload a resource (shapefile/style) to GeoServer from any device.
    """
    try:
        response = await geo_service.upload_resource(workspace, store_name, resource_type, file)
        if response.status_code in [200, 201]:
            return {"message": "Resource uploaded successfully!", "status_code": response.status_code}
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/upload-postgis")
async def upload_postgis(request: PostGISRequest):
    """
    Upload PostGIS database connection to GeoServer.
    """
    try:
        response = await geo_service.upload_postgis(request)
        if response.status_code in [200, 201]:
            return {
                "message": f"PostGIS datastore '{request.store_name}' created successfully!", 
                "status_code": response.status_code,
                "workspace": request.workspace,
                "store_name": request.store_name,
                "database": request.database,
                "host": request.host
            }
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
        
@router.get("/workspaces")
async def list_workspaces():
    """
    List all workspaces in GeoServer.
    """
    try:
        response = geo_service.list_workspaces()
        if response.status_code == 200:
            return response.json()
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/workspaces")
async def create_workspace(workspace_name: str):
    """
    Create a new workspace in GeoServer.
    """
    try:
        response = geo_service.create_workspace(workspace_name)
        if response.status_code in [200, 201]:
            return {"message": f"Workspace '{workspace_name}' created successfully!", "status_code": response.status_code}
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/workspaces/{workspace}")
async def get_workspace_details(workspace: str):
    """
    Get details of a specific workspace.
    """
    try:
        response = geo_service.get_workspace_details(workspace)
        if response.status_code == 200:
            return response.json()
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/workspaces/{workspace}/datastores")
async def list_datastores(workspace: str):
    """
    List all datastores in a workspace.
    """
    try:
        response = geo_service.list_datastores(workspace)
        if response.status_code == 200:
            return response.json()
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/workspaces/{workspace}/datastores/{datastore}")
async def get_datastore_details(workspace: str, datastore: str):
    """
    Get details of a specific datastore.
    """
    try:
        response = geo_service.get_datastore_details(workspace, datastore)
        if response.status_code == 200:
            return response.json()
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def _map_metadata_to_layer(metadata: Metadata) -> Dict:
    """
    Helper function to map metadata object to layer response dictionary.
    """
    return {
        "id": str(metadata.id),
        "geoserverName": metadata.geoserver_name,
        "nameOfDataset": metadata.name_of_dataset,
        "theme": metadata.theme,
        "keywords": metadata.keywords,
        "purposeOfCreatingData": metadata.purpose_of_creating_data,
        "dataType": metadata.data_type,
        "contactPerson": metadata.contact_person,
        "organization": metadata.organization,
        "contactEmail": metadata.contact_email,
        "country": metadata.country,
        "createdOn": metadata.created_on.isoformat() if metadata.created_on else None,
        "updatedOn": metadata.updated_on.isoformat() if metadata.updated_on else None,
        "accessConstraints": metadata.access_constraints,
        "useConstraints": metadata.use_constraints,
        "mailingAddress": metadata.mailing_address,
        "cityLocalityCountry": metadata.city_locality_country
    }


@router.get("/layers")
async def list_layers(db: Session = Depends(get_db)):
    """
    List all layers in GeoServer with metadata if available.
    Uses batch fetching to optimize database queries.
    """
    try:
        response = geo_service.list_layers()
        if response.status_code == 200:
            layers_data = response.json()
            
            # Extract layers list
            layers_list = layers_data.get("layers", {}).get("layer", [])
            
            if not layers_list:
                return []
            
            # Collect all layer names for batch metadata fetching
            layer_names = [layer.get("name") for layer in layers_list if layer.get("name")]
            
            # Batch fetch all metadata in one query (solves N+1 problem)
            metadata_dict: Dict[str, Metadata] = {}
            if layer_names:
                try:
                    metadata_list = MetadataService.get_by_geoserver_names(layer_names, db)
                    # Create a dictionary for O(1) lookup by geoserver_name
                    metadata_dict = {meta.geoserver_name: meta for meta in metadata_list}
                    logger.info(f"Found metadata for {len(metadata_dict)} out of {len(layer_names)} layers")
                except Exception as e:
                    logger.warning(f"Error batch fetching metadata: {str(e)}. Continuing without metadata.")
            
            # Enhance each layer with metadata if available
            enhanced_layers = []
            for layer in layers_list:
                layer_name = layer.get("name")
                enhanced_layer = {
                    "name": layer.get("name"),
                    "href": layer.get("href")
                }
                
                # Add metadata if available
                if layer_name and layer_name in metadata_dict:
                    metadata = metadata_dict[layer_name]
                    enhanced_layer.update(_map_metadata_to_layer(metadata))
                
                # Add WMS tile URL for frontend rendering
                if layer_name:
                    try:
                        tile_url = geo_service.get_tile_layer_url_cml(layer_name)
                        enhanced_layer["wms_link"] = tile_url
                    except Exception as e:
                        logger.warning(f"Failed to get WMS link for layer {layer_name}: {str(e)}")
                        enhanced_layer["wms_link"] = None
                
                enhanced_layers.append(enhanced_layer)
            
            # Return the enhanced response as a flat array
            return enhanced_layers
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except HTTPException:
        # Re-raise HTTPExceptions
        raise
    except Exception as e:
        logger.error(f"Error in list_layers: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/layers/{layer}")
async def get_layer_details(layer: str):
    """
    Get details of a specific layer.
    """
    try:
        response = geo_service.get_layer_details(layer)
        if response.status_code == 200:
            return response.json()
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/styles")
async def list_styles():
    """
    List all styles in GeoServer.
    """
    try:
        response = geo_service.list_styles()
        if response.status_code == 200:
            return response.json()
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/styles/{style}")
async def get_style_details(style: str):
    """
    Get details of a specific style.
    """
    try:
        response = geo_service.get_style_details(style)
        if response.status_code == 200:
            return response.json()
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
# DELETE APIs
@router.delete("/workspaces/{workspace}")
async def delete_workspace(workspace: str):
    """
    Delete a specific workspace.
    """
    try:
        response = geo_service.delete_workspace(workspace)
        if response.status_code == 200:
            return {"message": f"Workspace '{workspace}' deleted successfully!"}
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/workspaces/{workspace}/datastores/{datastore}")
async def delete_datastore(workspace: str, datastore: str):
    """
    Delete a specific datastore in a workspace.
    """
    try:
        response = geo_service.delete_datastore(workspace, datastore)
        if response.status_code == 200:
            return {"message": f"Datastore '{datastore}' in workspace '{workspace}' deleted successfully!"}
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/layers/{layer}")
async def delete_layer(layer: str):
    """
    Delete a specific layer.
    """
    try:
        response = geo_service.delete_layer(layer)
        if response.status_code == 200:
            return {"message": f"Layer '{layer}' deleted successfully!"}
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.delete("/styles/{style}")
async def delete_style(style: str):
    """
    Delete a specific style.
    """
    try:
        response = geo_service.delete_style(style)
        if response.status_code == 200:
            return {"message": f"Style '{style}' deleted successfully!"}
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.put("/workspaces/{workspace}")
async def update_workspace(workspace: str, request: UpdateRequest):
    """
    Update a specific workspace.
    """
    try:
        response = geo_service.update_workspace(workspace, request)
        if response.status_code == 200:
            return {"message": f"Workspace '{workspace}' updated successfully!"}
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/workspaces/{workspace}/datastores/{datastore}")
async def update_datastore(workspace: str, datastore: str, request: UpdateRequest):
    """
    Update a specific datastore in a workspace.
    """
    try:
        response = geo_service.update_datastore(workspace, datastore, request)
        if response.status_code == 200:
            return {"message": f"Datastore '{datastore}' in workspace '{workspace}' updated successfully!"}
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/layers/{layer}")
async def update_layer(layer: str, request: UpdateRequest):
    """
    Update a specific layer.
    """
    try:
        response = geo_service.update_layer(layer, request)
        if response.status_code == 200:
            return {"message": f"Layer '{layer}' updated successfully!"}
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/styles/{style}")
async def update_style(style: str, request: UpdateRequest):
    """
    Update a specific style.
    """
    try:
        response = geo_service.update_style(style, request)
        if response.status_code == 200:
            return {"message": f"Style '{style}' updated successfully!"}
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    



####################################API to Get Tile Layer URL#############################

@router.get("/layers/{layer}/tile_url")
async def get_layer_tile_url(layer: str):
    """
    Get the GeoServer WMS tile layer URL for frontend rendering.
    """
    try:
        tile_url = geo_service.get_tile_layer_url(layer)
        return {"tile_url": tile_url}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/layers/tile_urls")
async def get_tile_urls_for_datasets(datasets: List[str]):
    """
    Given dataset names (e.g., ["gbif", "kew_with_geom"]) return a map of dataset -> WMS tile URL.
    This lets the frontend render point and distribution layers immediately.
    """
    try:
        return geo_service.get_tile_urls_for_datasets(datasets)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/layers/{layer}/features")
async def query_layer(layer: str, bbox: str = None, filter_query: str = None):
    """
    Fetch features from GeoServer based on query parameters.
    """
    try:
        response = geo_service.query_layer_features(layer, bbox, filter_query)
        if response.status_code == 200:
            return response.json()
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/workspaces/{workspace}/datastores/{datastore}/tables")
async def list_datastore_tables(workspace: str, datastore: str):
    """
    List all available tables in a PostGIS datastore.
    """
    try:
        response = geo_service.list_datastore_tables(workspace, datastore)
        if response.status_code == 200:
            return response.json()
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/workspaces/{workspace}/datastores/{datastore}/schema/{schema}/tables")
async def list_postgis_schema_tables(workspace: str, datastore: str, schema: str):
    """
    List all tables in a specific PostGIS schema by querying the database directly.
    """
    try:
        response = geo_service.list_postgis_schema_tables(workspace, datastore, schema)
        if response.status_code == 200:
            return response.json()
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/workspaces/{workspace}/datastores/{datastore}/tables-direct")
async def list_postgis_tables_direct(workspace: str, datastore: str, schema: str = "public"):
    """
    List all tables in a PostGIS schema using direct database query.
    """
    try:
        response = geo_service.list_postgis_tables_direct(workspace, datastore, schema)
        if response.status_code == 200:
            return response.json()
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/create-layer")
async def create_layer_from_table(request: CreateLayerRequest):
    """
    Create a layer from a PostGIS table.
    """
    try:
        response = await geo_service.create_layer_from_table(request)
        if response.status_code in [200, 201]:
            return {
                "message": f"Layer '{request.layer_name or request.table_name}' created successfully from table '{request.table_name}'!", 
                "status_code": response.status_code,
                "workspace": request.workspace,
                "store_name": request.store_name,
                "table_name": request.table_name,
                "layer_name": request.layer_name or request.table_name
            }
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/workspaces/{workspace}/datastores/{datastore}/tables/{table}")
async def get_table_details(workspace: str, datastore: str, table: str):
    """
    Get details of a specific table in a datastore.
    """
    try:
        response = geo_service.get_table_details(workspace, datastore, table)
        if response.status_code == 200:
            return response.json()
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

#################################### New simplified Layer APIs To Get column and data #################################

@router.get("/layer/columns")
async def get_layer_columns(layer: str):
    """
    Return a simplified schema (columns) for the given layer (e.g., ws:layer).
    """
    try:
        result = geo_service.get_layer_columns(layer)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/layer/data")
async def get_layer_data(layer: str, maxFeatures: int = 100, bbox: str = None, filter: str = None, properties: str = None):
    """
    Return feature data for a layer via WFS with optional bbox/filter and maxFeatures.
    """
    try:
        response = geo_service.get_layer_data(
            layer,
            max_features=maxFeatures,
            bbox=bbox,
            filter_query=filter,
            properties=properties,
        )
        if response.status_code == 200:
            return response.json()
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/publish-upload-log")
async def publish_upload_log(request: PublishUploadLogRequest, db: Session = Depends(get_db)):
    """
    Publish a stored upload log to GeoServer.
    """
    try:
        response = await geo_service.publish_upload_log(request, db)
        if response.status_code == 200:
            return {"message": "Upload log published successfully!", "status_code": response.status_code}
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/upload_logs/{log_id}/publish", response_model=PublishUploadLogResponse)
async def publish_upload_log(
    log_id: int,
    request: PublishUploadLogRequest,
    db: Session = Depends(get_db),
):
    try:
        return geo_service.publish_upload_log(log_id, request, db)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        detail = str(exc)
        status_code = 404 if "not found" in detail.lower() else 400
        raise HTTPException(status_code=status_code, detail=detail) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc



