from geoserver.dao import GeoServerDAO
from geoserver.model import UploadRequest, UpdateRequest, PostGISRequest, CreateLayerRequest
from typing import List, Dict
from utils.config import DATASET_MAPPING

class GeoServerService:
    def __init__(self, dao: GeoServerDAO):
        self.dao = dao
        # self.dao = GeoServerDAO(base_url="http://localhost:8080/geoserver/rest", username="admin", password="geoserver")


    async def upload_resource(self, request: UploadRequest):
        """
        Handle resource upload based on the resource type.
        """
        if request.resource_type == "shapefile":
            if not request.store_name:
                raise ValueError("Store name is required for shapefile uploads.")
            return self.dao.upload_shapefile(request.workspace, request.store_name, request.file_path)
        elif request.resource_type == "style":
            if not request.style_name:
                raise ValueError("Style name is required for style uploads.")
            return self.dao.upload_style(request.workspace, request.style_name, request.file_path)
        elif request.resource_type == "postgis":
            raise ValueError("PostGIS uploads should use the dedicated upload_postgis method.")
        else:
            raise ValueError(f"Unsupported resource type: {request.resource_type}")

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

