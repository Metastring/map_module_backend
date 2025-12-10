import os
import sys
import logging
from fastapi import APIRouter, HTTPException
from geoserver.admin.dao import GeoServerAdminDAO
from geoserver.admin.model import UpdateRequest
from geoserver.admin.service import GeoServerAdminService
from geoserver.model import CreateLayerRequest
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'utils'))
from utils.config import *  # noqa: E402, F403

logger = logging.getLogger(__name__)

# Initialize router
router = APIRouter()

# Initialize DAO and Service with configuration
geo_admin_dao = GeoServerAdminDAO(
    base_url=f"http://{geoserver_host}:{geoserver_port}/geoserver/rest",
    username=geoserver_username,
    password=geoserver_password
)
geo_admin_service = GeoServerAdminService(geo_admin_dao)

# Workspace Management APIs
@router.get("/workspaces", summary="List All Workspaces", description="Retrieve a list of all workspaces in GeoServer. Workspaces are logical groupings of data stores and layers.")
async def list_workspaces():
    """
    List all workspaces in GeoServer.
    """
    try:
        response = geo_admin_service.list_workspaces()
        if response.status_code == 200:
            return response.json()
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/workspaces", summary="Create Workspace", description="Create a new workspace in GeoServer. Workspaces organize data stores and layers logically.")
async def create_workspace(workspace_name: str):
    """
    Create a new workspace in GeoServer.
    """
    try:
        response = geo_admin_service.create_workspace(workspace_name)
        if response.status_code in [200, 201]:
            return {
                "message": f"Workspace '{workspace_name}' created successfully!",
                "status_code": response.status_code
            }
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/workspaces/{workspace}", summary="Get Workspace Details", description="Retrieve detailed information about a specific workspace, including its configuration and properties.")
async def get_workspace_details(workspace: str):
    """
    Get details of a specific workspace.
    """
    try:
        response = geo_admin_service.get_workspace_details(workspace)
        if response.status_code == 200:
            return response.json()
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/workspaces/{workspace}", summary="Delete Workspace", description="Delete a specific workspace from GeoServer. This operation will also remove all associated data stores and layers.")
async def delete_workspace(workspace: str):
    """
    Delete a specific workspace.
    """
    try:
        response = geo_admin_service.delete_workspace(workspace)
        if response.status_code == 200:
            return {"message": f"Workspace '{workspace}' deleted successfully!"}
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/workspaces/{workspace}", summary="Update Workspace", description="Update the configuration and properties of a specific workspace in GeoServer.")
async def update_workspace(workspace: str, request: UpdateRequest):
    """
    Update a specific workspace.
    """
    try:
        response = geo_admin_service.update_workspace(workspace, request)
        if response.status_code == 200:
            return {"message": f"Workspace '{workspace}' updated successfully!"}
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Datastore Management APIs
@router.get("/workspaces/{workspace}/datastores", summary="List Datastores", description="Retrieve a list of all data stores in a specific workspace. Data stores are connections to spatial data sources.")
async def list_datastores(workspace: str):
    """
    List all datastores in a workspace.
    """
    try:
        response = geo_admin_service.list_datastores(workspace)
        if response.status_code == 200:
            return response.json()
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/workspaces/{workspace}/datastores/{datastore}", summary="Get Datastore Details", description="Retrieve detailed information about a specific data store, including connection parameters and configuration.")
async def get_datastore_details(workspace: str, datastore: str):
    """
    Get details of a specific datastore.
    """
    try:
        response = geo_admin_service.get_datastore_details(workspace, datastore)
        if response.status_code == 200:
            return response.json()
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/workspaces/{workspace}/datastores/{datastore}", summary="Delete Datastore", description="Delete a specific data store from a workspace. This will remove the connection but not the underlying data source.")
async def delete_datastore(workspace: str, datastore: str):
    """
    Delete a specific datastore in a workspace.
    """
    try:
        response = geo_admin_service.delete_datastore(workspace, datastore)
        if response.status_code == 200:
            return {"message": f"Datastore '{datastore}' in workspace '{workspace}' deleted successfully!"}
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/workspaces/{workspace}/datastores/{datastore}", summary="Update Datastore", description="Update the configuration and connection parameters of a specific data store in a workspace.")
async def update_datastore(workspace: str, datastore: str, request: UpdateRequest):
    """
    Update a specific datastore in a workspace.
    """
    try:
        response = geo_admin_service.update_datastore(workspace, datastore, request)
        if response.status_code == 200:
            return {
                "message": (
                    f"Datastore '{datastore}' in workspace '{workspace}' "
                    "updated successfully!"
                )
            }
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Layer Management APIs (DELETE and PUT only)
@router.delete("/layers/{layer}", summary="Delete Layer", description="Delete a specific layer from GeoServer. This removes the layer configuration but does not delete the underlying data.")
async def delete_layer(layer: str):
    """
    Delete a specific layer.
    """
    try:
        response = geo_admin_service.delete_layer(layer)
        if response.status_code == 200:
            return {"message": f"Layer '{layer}' deleted successfully!"}
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/layers/{layer}", summary="Update Layer", description="Update the configuration and properties of a specific layer, including style settings and default parameters.")
async def update_layer(layer: str, request: UpdateRequest):
    """
    Update a specific layer.
    """
    try:
        response = geo_admin_service.update_layer(layer, request)
        if response.status_code == 200:
            return {"message": f"Layer '{layer}' updated successfully!"}
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/layers/{layer}", summary="Get Layer Details", description="Retrieve detailed information about a specific layer in GeoServer. This includes layer configuration, default style, resource information, and other layer properties.")
async def get_layer_details(layer: str):
    """
    Get details of a specific layer.
    
    This endpoint returns comprehensive information about a single layer, including:
    - Layer name and path
    - Layer type (VECTOR or RASTER)
    - Default style configuration
    - Resource details
    - Bounding box information
    
    Layer name can be specified with or without workspace prefix (e.g., 'metastring:gbif' or 'gbif').
    """
    try:
        response = geo_admin_service.get_layer_details(layer)
        if response.status_code == 200:
            return response.json()
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Style Management APIs (DELETE and PUT only)
@router.delete("/styles/{style}", summary="Delete Style", description="Delete a specific style from GeoServer. Styles define how geographic features are rendered on maps.")
async def delete_style(style: str):
    """
    Delete a specific style.
    """
    try:
        response = geo_admin_service.delete_style(style)
        if response.status_code == 200:
            return {"message": f"Style '{style}' deleted successfully!"}
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/styles/{style}", summary="Update Style", description="Update the configuration and properties of a specific style, including style format and resource location.")
async def update_style(style: str, request: UpdateRequest):
    """
    Update a specific style.
    """
    try:
        response = geo_admin_service.update_style(style, request)
        if response.status_code == 200:
            return {"message": f"Style '{style}' updated successfully!"}
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Table Management APIs
@router.get("/workspaces/{workspace}/datastores/{datastore}/tables", summary="List Datastore Tables", description="List all available tables in a PostGIS data store. Tables represent spatial data that can be published as layers.")
async def list_datastore_tables(workspace: str, datastore: str):
    """
    List all available tables in a PostGIS datastore.
    """
    try:
        response = geo_admin_service.list_datastore_tables(workspace, datastore)
        if response.status_code == 200:
            return response.json()
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/workspaces/{workspace}/datastores/{datastore}/schema/{schema}/tables", summary="List Schema Tables", description="List all tables in a specific PostGIS schema by querying the database directly. This provides direct access to schema-level tables.")
async def list_postgis_schema_tables(workspace: str, datastore: str, schema: str):
    """
    List all tables in a specific PostGIS schema by querying the database directly.
    """
    try:
        response = geo_admin_service.list_postgis_schema_tables(workspace, datastore, schema)
        if response.status_code == 200:
            return response.json()
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/workspaces/{workspace}/datastores/{datastore}/tables-direct", summary="List Tables Direct", description="List all tables in a PostGIS schema using direct database query. Allows specifying a custom schema, defaulting to 'public'.")
async def list_postgis_tables_direct(workspace: str, datastore: str, schema: str = "public"):
    """
    List all tables in a PostGIS schema using direct database query.
    """
    try:
        response = geo_admin_service.list_postgis_tables_direct(workspace, datastore, schema)
        if response.status_code == 200:
            return response.json()
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/workspaces/{workspace}/datastores/{datastore}/tables/{table}", summary="Get Table Details", description="Retrieve detailed information about a specific table in a data store, including column definitions and spatial properties.")
async def get_table_details(workspace: str, datastore: str, table: str):
    """
    Get details of a specific table in a datastore.
    """
    try:
        response = geo_admin_service.get_table_details(workspace, datastore, table)
        if response.status_code == 200:
            return response.json()
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Layer Creation API
@router.post("/create-layer", summary="Create Layer from Table", description="Create a new GeoServer layer from an existing PostGIS table. This publishes a database table as a map layer with specified style and configuration.")
async def create_layer_from_table(request: CreateLayerRequest):
    """
    Create a layer from a PostGIS table.
    """
    try:
        response = await geo_admin_service.create_layer_from_table(request)
        if response.status_code in [200, 201]:
            layer_name = request.layer_name or request.table_name
            return {
                "message": (
                    f"Layer '{layer_name}' created successfully from "
                    f"table '{request.table_name}'!"
                ),
                "status_code": response.status_code,
                "workspace": request.workspace,
                "store_name": request.store_name,
                "table_name": request.table_name,
                "layer_name": layer_name
            }
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Style Management APIs (GET)
@router.get("/styles", summary="List All Styles", description="Retrieve a list of all styles available in GeoServer. Styles define how layers are rendered on maps, including colors, symbols, and other visual properties.")
async def list_styles():
    """
    List all styles in GeoServer.
    
    This endpoint returns all styles configured in GeoServer. Styles are used to control
    how geographic features are displayed on maps, including point symbols, line styles,
    and polygon fill patterns.
    """
    try:
        response = geo_admin_service.list_styles()
        if response.status_code == 200:
            return response.json()
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/styles/{style}", summary="Get Style Details", description="Retrieve detailed information about a specific style in GeoServer, including style format, filename, and language version.")
async def get_style_details(style: str):
    """
    Get details of a specific style.
    
    This endpoint returns detailed information about a single style, including:
    - Style name and filename
    - Style format (SLD, etc.)
    - Language version
    - Style resource location
    """
    try:
        response = geo_admin_service.get_style_details(style)
        if response.status_code == 200:
            return response.json()
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

