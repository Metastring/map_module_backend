from urllib.parse import urlencode
from fastapi import APIRouter, HTTPException, Depends
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import JSONResponse
import requests
from geoserver.model import UploadRequest
from geoserver.model import UpdateRequest
from geoserver.model import PostGISRequest
from geoserver.model import CreateLayerRequest
from geoserver.service import GeoServerService
from geoserver.dao import GeoServerDAO
import shutil
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
from utils.config import *
from typing import List

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
async def upload_resource(request: UploadRequest):
    """
    Common POST API to upload resources to GeoServer.
    """
    try:
        response = await geo_service.upload_resource(request)
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

@router.get("/layers")
async def list_layers():
    """
    List all layers in GeoServer.
    """
    try:
        response = geo_service.list_layers()
        if response.status_code == 200:
            return response.json()
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except Exception as e:
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

