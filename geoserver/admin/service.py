from typing import List

from geoserver.admin.dao import GeoServerAdminDAO
from geoserver.admin.model import UpdateRequest
from geoserver.model import CreateLayerRequest


class GeoServerAdminService:
    def __init__(self, dao: GeoServerAdminDAO):
        self.dao = dao

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

    def delete_layer(self, layer: str):
        """
        Delete a layer.
        """
        return self.dao.delete_layer(layer)

    def update_layer(self, layer: str, request: UpdateRequest):
        """
        Update a layer.
        """
        return self.dao.update_layer(layer, request)

    def delete_style(self, style: str):
        """
        Delete a style.
        """
        return self.dao.delete_style(style)

    def update_style(self, style: str, request: UpdateRequest):
        """
        Update a style.
        """
        return self.dao.update_style(style, request)

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

    def get_layer_details(self, layer: str):
        """
        Get details of a specific layer.
        """
        return self.dao.get_layer_details(layer)

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

    def get_feature_type_details(self, workspace: str, datastore: str, feature_type: str):
        """
        Get details of a specific feature type.
        """
        if not workspace:
            raise ValueError("Workspace name is required.")
        if not datastore:
            raise ValueError("Datastore name is required.")
        if not feature_type:
            raise ValueError("Feature type name is required.")
        return self.dao.get_feature_type_details(workspace, datastore, feature_type)

    def update_feature_type(self, workspace: str, datastore: str, feature_type: str, config: dict):
        """
        Update a feature type configuration.
        """
        if not workspace:
            raise ValueError("Workspace name is required.")
        if not datastore:
            raise ValueError("Datastore name is required.")
        if not feature_type:
            raise ValueError("Feature type name is required.")
        return self.dao.update_feature_type(workspace, datastore, feature_type, config)

    def configure_layer_tile_caching(
        self,
        workspace: str,
        layer_name: str,
        tile_formats: List[str] = None,
        gridset: str = "EPSG:3857"
    ):
        """
        Configure tile caching for a layer.
        """
        if not workspace:
            raise ValueError("Workspace name is required.")
        if not layer_name:
            raise ValueError("Layer name is required.")
        return self.dao.configure_layer_tile_caching(workspace, layer_name, tile_formats, gridset)

