import os
import sys
import logging
from typing import List, Dict, Optional
from urllib.parse import urlencode
import requests
from fastapi import APIRouter, HTTPException, Depends, UploadFile, File, Form, Query
from sqlalchemy.orm import Session
from database.database import get_db
from geoserver.dao import GeoServerDAO
from geoserver.model import (CreateLayerRequest, PostGISRequest, PublishUploadLogRequest, PublishUploadLogResponse)
from geoserver.service import GeoServerService
from geoserver.admin.api import get_layer_bbox
from metadata.models.schema import Metadata
from metadata.service.service import MetadataService

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
from utils.config import *  # noqa: E402, F403

logger = logging.getLogger(__name__)

# Initialize router
router = APIRouter(tags=["geoserver"])

# Initialize DAO and Service with configuration
geo_dao = GeoServerDAO(
    base_url=f"http://{geoserver_host}:{geoserver_port}/geoserver/rest",
    username=geoserver_username,
    password=geoserver_password
)
geo_service = GeoServerService(geo_dao)

@router.post("/upload", summary="Upload Shapefile/Resource (Used for internal api calls)", description="Upload a shapefile or other resource to GeoServer. This API is used to upload shapefiles (as ZIP archives) to GeoServer. The shapefile must be in a ZIP format containing all required components (.shp, .shx, .dbf, etc.).")
async def upload_resource(
    workspace: str = Form(..., description="Target workspace name in GeoServer (e.g., 'metastring')"),
    store_name: str = Form(..., description="Name of the datastore where the resource will be stored"),
    resource_type: str = Form(..., description="Type of resource to upload (e.g., 'shapefile')"),
    file: UploadFile = File(..., description="The file to upload (must be a ZIP file for shapefiles)")
):
    try:
        response = await geo_service.upload_resource(workspace, store_name, resource_type, file)
        if response.status_code in [200, 201]:
            return {"message": "Resource uploaded successfully!", "status_code": response.status_code}
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/upload-postgis", summary="Create PostGIS Datastore (Used for internal api calls)", description="Create a PostGIS datastore connection in GeoServer. This API is used to connect GeoServer to a PostgreSQL/PostGIS database, allowing GeoServer to access spatial data stored in the database.")
async def upload_postgis(request: PostGISRequest):
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


def _map_metadata_to_layer1(metadata: Metadata) -> Dict:
    """
    Helper function to map metadata object to layer response dictionary for /layers1 endpoint.
    Uses renamed keys and excludes certain fields.
    """
    return {
        "id": str(metadata.id),
        "title": metadata.name_of_dataset,
        "tags": metadata.keywords,
        "purposeOfCreatingData": metadata.purpose_of_creating_data,
        "layerType": metadata.data_type,
        "createdBy": metadata.contact_person,
        "organization": metadata.organization,
        "contactEmail": metadata.contact_email,
        "country": metadata.country,
        "createdDate": metadata.created_on.isoformat() if metadata.created_on else None,
        "modifiedDate": metadata.updated_on.isoformat() if metadata.updated_on else None,
        "accessConstraints": metadata.access_constraints,
        "useConstraints": metadata.use_constraints,
        "mailingAddress": metadata.mailing_address,
        "cityLocalityCountry": metadata.city_locality_country,
        "attribution": None,
        "author": None,
        "pdfLink": None,
        "pageId": None,
        "downloadAccess": "ALL",
        "url": None,
        "license": None,
        "uploaderUserId": None,
        "isDownloadable": None,
        "layerStatus": None,
        "portalId": None
    }




@router.get("/layers", summary="List All Layers (Used for frontend api calls)", description="Retrieve a list of all layers in GeoServer with their metadata. This API returns enhanced layer information including metadata (if available) for each layer.")
async def list_layers(db: Session = Depends(get_db)):
    try:
        response = geo_service.list_layers()
        if response.status_code == 200:
            layers_data = response.json()

            # Extract layers list
            layers_list = layers_data.get("layers", {}).get("layer", [])

            if not layers_list:
                return {"layers": {"layer": []}}

            # Collect all layer names for batch metadata fetching
            layer_names = [layer.get("name") for layer in layers_list if layer.get("name")]

            # Batch fetch all metadata in one query (solves N+1 problem)
            metadata_dict: Dict[str, Metadata] = {}
            if layer_names:
                try:
                    metadata_list = MetadataService.get_by_geoserver_names(layer_names, db)
                    # Create a dictionary for O(1) lookup by geoserver_name
                    metadata_dict = {meta.geoserver_name: meta for meta in metadata_list}
                    logger.info(
                        f"Found metadata for {len(metadata_dict)} out of {len(layer_names)} layers"
                    )
                except Exception as e:
                    logger.warning(
                        f"Error batch fetching metadata: {str(e)}. Continuing without metadata."
                    )

            # Enhance each layer with metadata
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

                    # Add WMS link
                    if metadata.geoserver_name:
                        wms_link = geo_service.get_tile_layer_url(metadata.geoserver_name)
                        enhanced_layer["wms_link"] = wms_link

                enhanced_layers.append(enhanced_layer)

            # Return the enhanced response
            return {
                "layers": {
                    "layer": enhanced_layers
                }
            }
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except HTTPException:
        # Re-raise HTTPExceptions
        raise
    except Exception as e:
        logger.error(f"Error in list_layers: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/layers1", summary="List All Layers (Used for frontend api calls)", description="Retrieve a list of all layers in GeoServer with their metadata. This API returns enhanced layer information including metadata (if available) for each layer.")
async def list_layers1(db: Session = Depends(get_db)):
    try:
        response = geo_service.list_layers()
        if response.status_code == 200:
            layers_data = response.json()

            # Extract layers list
            layers_list = layers_data.get("layers", {}).get("layer", [])

            if not layers_list:
                return {
                    "layers": [],
                    "page": 1,
                    "totalPage": "",
                    "currPage": ""
                }

            # Collect all layer names for batch metadata fetching
            layer_names = [layer.get("name") for layer in layers_list if layer.get("name")]

            # Batch fetch all metadata in one query (solves N+1 problem)
            metadata_dict: Dict[str, Metadata] = {}
            if layer_names:
                try:
                    metadata_list = MetadataService.get_by_geoserver_names(layer_names, db)
                    # Create a dictionary for O(1) lookup by geoserver_name
                    metadata_dict = {meta.geoserver_name: meta for meta in metadata_list}
                    logger.info(
                        f"Found metadata for {len(metadata_dict)} out of {len(layer_names)} layers"
                    )
                except Exception as e:
                    logger.warning(
                        f"Error batch fetching metadata: {str(e)}. Continuing without metadata."
                    )

            # Enhance each layer with metadata
            enhanced_layers = []
            for layer in layers_list:
                layer_name = layer.get("name")
                enhanced_layer = {
                    "name": layer.get("name"),
                    "attribution": None,
                    "author": None,
                    "pdfLink": None,
                    "pageId": None,
                    "downloadAccess": "ALL",
                    "url": None,
                    "license": None,
                    "uploaderUserId": None,
                    "isDownloadable": None,
                    "layerStatus": None,
                    "portalId": None
                }

                # Add metadata if available
                if layer_name and layer_name in metadata_dict:
                    metadata = metadata_dict[layer_name]
                    enhanced_layer.update(_map_metadata_to_layer1(metadata))

                    # Add thumbnail (WMS link)
                    if metadata.geoserver_name:
                        wms_link = geo_service.get_tile_layer_url(metadata.geoserver_name)
                        enhanced_layer["thumbnail"] = wms_link

                # Fetch and add bounding box
                if layer_name:
                    bbox = get_layer_bbox(layer_name)
                    enhanced_layer["bbox"] = bbox

                enhanced_layers.append(enhanced_layer)

            # Return the enhanced response
            return {
                "layers": enhanced_layers,
                "page": 1,
                "totalPage": "",
                "currPage": ""
            }
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except HTTPException:
        # Re-raise HTTPExceptions
        raise
    except Exception as e:
        logger.error(f"Error in list_layers1: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

####################################API to Get Tile Layer URL#############################

@router.get("/layers/{layer}/tile_url", summary="Get Layer Tile URL (Used for frontend api calls)", description="Generate a WMS (Web Map Service) tile URL for a specific layer. This URL can be used by frontend applications to render map tiles for the layer.")
async def get_layer_tile_url(layer: str):
    try:
        tile_url = geo_service.get_tile_layer_url(layer)
        return {"tile_url": tile_url}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/layers/{layer}/vector_tile_url", summary="Get Layer Vector Tile URL (Used for frontend api calls)", description="Generate a vector tile URL (TMS/PBF) for a specific layer. This URL template can be used by frontend applications to render vector map tiles. The URL contains placeholders {z}, {x}, {-y} that should be replaced with actual tile coordinates.")
async def get_layer_vector_tile_url(layer: str):
    try:
        tile_url = geo_service.get_vectortile_layer_url(layer)
        return {"tile_url": tile_url}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/layers/tile_urls", summary="Get Tile URLs for Multiple Datasets", description="Retrieve WMS tile URLs for multiple datasets at once. This endpoint accepts a list of dataset names and returns a mapping of dataset names to their corresponding WMS tile URLs, enabling efficient batch retrieval for frontend applications.")
async def get_tile_urls_for_datasets(datasets: List[str]):
    try:
        return geo_service.get_tile_urls_for_datasets(datasets)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


############################## New simplified Layer APIs To Get column and data ###########################

@router.get("/layer/columns", summary="Get Layer Schema/Columns", description="Retrieve the schema (column definitions) for a specific layer. This endpoint returns information about all attributes/columns available in the layer, including data types and constraints.")
async def get_layer_columns(layer: str = Query(..., description="Layer name (e.g., 'metastring:gbif')")):
    try:
        result = geo_service.get_layer_columns(layer)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/layer/data", summary="Get Layer Feature Data", description="Retrieve actual feature data from a layer via WFS (Web Feature Service). This endpoint allows you to fetch geographic features with optional filtering, bounding box constraints, and property selection.")
async def get_layer_data(
    layer: str = Query(..., description="Layer name (e.g., 'metastring:gbif')"),
    maxFeatures: int = Query(100, description="Maximum number of features to return (default: 100)"),
    bbox: str = Query(None, description="Bounding box filter in format 'minx,miny,maxx,maxy'"),
    filter: str = Query(None, description="CQL filter expression for attribute-based filtering"),
    properties: str = Query(None, description="Comma-separated list of property names to return (if not specified, all properties are returned)")
):
    """
    Return feature data for a layer via WFS with optional bbox/filter and maxFeatures.
    
    This endpoint retrieves actual feature data from a layer with the following options:
    - **maxFeatures**: Limit the number of features returned (default: 100)
    - **bbox**: Filter by bounding box (format: "minx,miny,maxx,maxy")
    - **filter**: CQL filter for attribute-based filtering
    - **properties**: Comma-separated list of property names to return (if not specified, all properties are returned)
    
    Returns features in GeoJSON format with geometry and attributes.
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


@router.post("/upload_logs/{log_id}/publish (Used for internal api calls)", response_model=PublishUploadLogResponse, summary="Publish Upload Log to GeoServer", description="Publish a previously uploaded file (stored in upload logs) to GeoServer as a layer. This endpoint takes an upload log ID and publishes the associated file to the specified GeoServer workspace and datastore. It is used for internal api calls")
async def publish_upload_log(
    log_id: int,
    request: PublishUploadLogRequest,
    db: Session = Depends(get_db),
):
    """
    Publish a stored upload log to GeoServer.
    
    This endpoint publishes a file that was previously uploaded and stored in the upload log
    system. It creates a layer in GeoServer from the stored file, making it available for
    mapping and visualization. The file must exist at the stored path and be a valid
    spatial data format (e.g., shapefile).
    """
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
