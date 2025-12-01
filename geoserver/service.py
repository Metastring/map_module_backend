import os
from geoserver.dao import GeoServerDAO
from geoserver.model import (
    CreateLayerRequest,
    PostGISRequest,
    PublishUploadLogRequest,
    PublishUploadLogResponse,
    UpdateRequest,
)
from upload_log.dao.dao import UploadLogDAO
from upload_log.models.model import DataType, UploadLogOut
from sqlalchemy.orm import Session
from utils.config import DATASET_MAPPING
import tempfile
import shutil
from typing import List, Dict

class GeoServerService:
    def __init__(self, dao: GeoServerDAO):
        self.dao = dao
        # self.dao = GeoServerDAO(base_url="http://localhost:8080/geoserver/rest", username="admin", password="geoserver")


    async def upload_resource(self, workspace: str, store_name: str, resource_type: str, file):
        """
        Accept uploaded file from client and pass to DAO for GeoServer upload.
        """
        if resource_type != "shapefile":
            raise ValueError(f"Unsupported resource type: {resource_type}")

        # Save uploaded file temporarily
        suffix = ".zip" if file.filename.endswith(".zip") else ""
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            shutil.copyfileobj(file.file, tmp)
            tmp_path = tmp.name

        # Upload to GeoServer
        response = self.dao.upload_shapefile(workspace, store_name, tmp_path)

        # Clean up local temp file
        try:
            file.file.close()
        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)
        return response

    async def upload_postgis(self, request: PostGISRequest):
        """
        Handle PostGIS datastore creation.
        """
        if not request.store_name:
            raise ValueError("Store name is required for PostGIS uploads.")
        if not request.database:
            raise ValueError("Database name is required for PostGIS uploads.")
        if not request.host:
            raise ValueError("Host is required for PostGIS uploads.")
        if not request.username:
            raise ValueError("Username is required for PostGIS uploads.")
        if not request.password:
            raise ValueError("Password is required for PostGIS uploads.")
            
        return self.dao.upload_postgis(
            workspace=request.workspace,
            store_name=request.store_name,
            database=request.database,
            host=request.host,
            port=request.port,
            username=request.username,
            password=request.password,
            schema=request.db_schema,
            description=request.description,
            enabled=request.enabled
        )

    def publish_upload_log(
        self,
        log_id: int,
        publish_request: PublishUploadLogRequest,
        db: Session,
    ) -> PublishUploadLogResponse:
        record = UploadLogDAO.get_by_id(log_id, db)
        if not record:
            raise ValueError(f"Upload log with id {log_id} not found")

        file_path = record.source_path
        if not file_path or not os.path.exists(file_path):
            raise FileNotFoundError(f"Stored file not found at path: {file_path}")

        workspace = publish_request.workspace.strip()
        store_name = publish_request.store_name or record.layer_name
        layer_name = publish_request.layer_name or record.layer_name

        response = self.dao.upload_shapefile(workspace, store_name, file_path)
        if response.status_code not in (200, 201):
            raise RuntimeError(
                f"GeoServer upload failed with status {response.status_code}: {response.text}"
            )

        record.geoserver_layer = layer_name
        db.add(record)
        db.commit()
        db.refresh(record)

        return PublishUploadLogResponse(
            message=f"Uploaded to GeoServer workspace '{workspace}' store '{store_name}'",
            status_code=response.status_code,
            upload_log=self._convert_upload_log(record),
        )

    def _convert_upload_log(self, record) -> UploadLogOut:
        try:
            data_type = DataType(record.data_type)
        except ValueError:
            data_type = DataType.UNKNOWN

        return UploadLogOut(
            id=record.id,
            layer_name=record.layer_name,
            file_format=record.file_format,
            data_type=data_type,
            crs=record.crs,
            bbox=record.bbox,
            source_path=record.source_path,
            geoserver_layer=record.geoserver_layer,
            tags=record.tags,
            uploaded_by=record.uploaded_by,
            uploaded_on=record.uploaded_on,
        )

    def list_workspaces(self):
        return self.dao.list_workspaces()

    def create_workspace(self, workspace_name: str):
        """
        Create a new workspace in GeoServer.
        """
        if not workspace_name or not workspace_name.strip():
            raise ValueError("Workspace name is required.")
        return self.dao.create_workspace(workspace_name.strip())

    def get_workspace_details(self, workspace: str):
        return self.dao.get_workspace_details(workspace)

    def list_datastores(self, workspace: str):
        return self.dao.list_datastores(workspace)

    def get_datastore_details(self, workspace: str, datastore: str):
        return self.dao.get_datastore_details(workspace, datastore)

    def list_layers(self):
        return self.dao.list_layers()

    def get_layer_details(self, layer: str):
        return self.dao.get_layer_details(layer)
    
    def delete_workspace(self, workspace: str):
        """
        Delete a workspace.
        """
        return self.dao.delete_workspace(workspace)

    def delete_datastore(self, workspace: str, datastore: str):
        """
        Delete a datastore in a workspace.
        """
        return self.dao.delete_datastore(workspace, datastore)

    def delete_layer(self, layer: str):
        """
        Delete a layer.
        """
        return self.dao.delete_layer(layer)
    
    def delete_style(self, style: str):
        """
        Delete a style.
        """
        return self.dao.delete_style(style)
    
    def update_workspace(self, workspace: str, request: UpdateRequest):
        """
        Update a workspace.
        """
        return self.dao.update_workspace(workspace, request)

    def update_datastore(self, workspace: str, datastore: str, request: UpdateRequest):
        """
        Update a datastore in a workspace.
        """
        return self.dao.update_datastore(workspace, datastore, request)

    def update_layer(self, layer: str, request: UpdateRequest):
        """
        Update a layer.
        """
        return self.dao.update_layer(layer, request)

    def update_style(self, style: str, request: UpdateRequest):
        """
        Update a style.
        """
        return self.dao.update_style(style, request)
    
    def get_tile_layer_url(self, layer: str):
        return self.dao.get_tile_layer_url(layer)

    def get_tile_layer_url_cml(self, layer: str):
        return self.dao.get_tile_layer_url_cml(layer)

    def query_layer_features(self, layer: str, bbox: str = None, filter_query: str = None):
        return self.dao.query_features(layer, bbox, filter_query)

    def list_styles(self):
        """
        List all styles in GeoServer.
        """
        return self.dao.list_styles()

    def get_style_details(self, style_name: str):
        """
        Get details of a specific style.
        """
        if not style_name:
            raise ValueError("Style name is required.")
        return self.dao.get_style_details(style_name)

    def list_datastore_tables(self, workspace: str, datastore: str):
        """
        List all available tables in a PostGIS datastore.
        """
        if not workspace:
            raise ValueError("Workspace name is required.")
        if not datastore:
            raise ValueError("Datastore name is required.")
        return self.dao.list_datastore_tables(workspace, datastore)

    def list_postgis_schema_tables(self, workspace: str, datastore: str, schema: str = "public"):
        """
        List all tables in a specific PostGIS schema by querying the database directly.
        """
        if not workspace:
            raise ValueError("Workspace name is required.")
        if not datastore:
            raise ValueError("Datastore name is required.")
        if not schema:
            raise ValueError("Schema name is required.")
        return self.dao.list_postgis_schema_tables(workspace, datastore, schema)

    def list_postgis_tables_direct(self, workspace: str, datastore: str, schema: str = "public"):
        """
        List all tables in a PostGIS schema using direct database query.
        """
        if not workspace:
            raise ValueError("Workspace name is required.")
        if not datastore:
            raise ValueError("Datastore name is required.")
        if not schema:
            raise ValueError("Schema name is required.")
        return self.dao.list_postgis_tables_direct(workspace, datastore, schema)

    async def create_layer_from_table(self, request: CreateLayerRequest):
        """
        Create a layer from a PostGIS table.
        """
        if not request.workspace:
            raise ValueError("Workspace name is required.")
        if not request.store_name:
            raise ValueError("Store name is required.")
        if not request.table_name:
            raise ValueError("Table name is required.")
            
        return self.dao.create_layer_from_table(
            workspace=request.workspace,
            datastore=request.store_name,
            table_name=request.table_name,
            layer_name=request.layer_name,
            title=request.title,
            description=request.description,
            enabled=request.enabled,
            default_style=request.default_style
        )

    def get_table_details(self, workspace: str, datastore: str, table_name: str):
        """
        Get details of a specific table in a datastore.
        """
        if not workspace:
            raise ValueError("Workspace name is required.")
        if not datastore:
            raise ValueError("Datastore name is required.")
        if not table_name:
            raise ValueError("Table name is required.")
        return self.dao.get_table_details(workspace, datastore, table_name)

    def get_tile_urls_for_datasets(self, datasets: List[str]) -> Dict[str, str]:
        """
        Resolve dataset names to existing GeoServer layer names and return tile URLs.
        Strategy: fetch all layers, then for each dataset find a layer whose
        name is exactly the dataset or ends with ":{dataset}". Return URL map.
        """
        response = self.dao.list_layers()
        if response.status_code != 200:
            raise ValueError(f"Failed to list layers: {response.text}")
        data = response.json() or {}
        layers = (data.get("layers") or {}).get("layer") or []
        # Normalize to list of strings (names)
        layer_names: List[str] = []
        for item in layers:
            if isinstance(item, dict) and "name" in item:
                layer_names.append(item["name"])  # e.g., "ws:gbif"
            elif isinstance(item, str):
                layer_names.append(item)

        results: Dict[str, str] = {}
        for ds in datasets:
            # Map frontend dataset name to actual layer/table name if provided
            target = DATASET_MAPPING.get(ds, ds)
            match = None
            for lname in layer_names:
                if lname == target or lname.endswith(f":{target}"):
                    match = lname
                    break
            if match:
                results[ds] = self.get_tile_layer_url(match)
            else:
                results[ds] = ""  # Not found; caller can handle
        return results

    def get_layer_columns(self, layer: str):
        """
        Resolve a layer to its underlying feature type and return a simplified
        list of attribute definitions (columns).
        """
        layer_details = self.dao.get_layer_details(layer)
        if layer_details.status_code != 200:
            raise ValueError(f"Failed to get layer details: {layer_details.text}")
        layer_json = layer_details.json() or {}
        resource = (layer_json.get("layer") or {}).get("resource") or {}
        href = resource.get("href")
        if not href:
            raise ValueError("Layer resource href not found")
        if not href.endswith(".json"):
            href = href + ".json"

        ft_response = self.dao.get_url(href)
        if ft_response.status_code != 200:
            raise ValueError(f"Failed to get feature type details: {ft_response.text}")
        ft_json = ft_response.json() or {}
        attributes = ((ft_json.get("featureType") or {}).get("attributes") or {}).get("attribute") or []

        columns = []
        for attr in attributes:
            if isinstance(attr, dict):
                columns.append({
                    "name": attr.get("name"),
                    "type": attr.get("binding"),
                    "nillable": attr.get("nillable"),
                    "minOccurs": attr.get("minOccurs"),
                    "maxOccurs": attr.get("maxOccurs")
                })
        return {"columns": columns}

    def get_layer_data(self, layer: str, max_features: int = 100, bbox: str = None, filter_query: str = None, properties: str = None):
        """
        Fetch data for a layer via WFS with optional bbox/filter and max features.
        """
        return self.dao.query_features(
            layer,
            bbox=bbox,
            filter_query=filter_query,
            max_features=max_features,
            property_names=properties,
        )

