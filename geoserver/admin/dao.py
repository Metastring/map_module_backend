import requests
from geoserver.admin.model import UpdateRequest


class GeoServerAdminDAO:
    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url
        self.auth = (username, password)

    def list_workspaces(self):
        url = f"{self.base_url}/workspaces.json"
        return requests.get(url, auth=self.auth)

    def get_workspace_details(self, workspace: str):
        url = f"{self.base_url}/workspaces/{workspace}.json"
        return requests.get(url, auth=self.auth)

    def list_datastores(self, workspace: str):
        url = f"{self.base_url}/workspaces/{workspace}/datastores.json"
        return requests.get(url, auth=self.auth)

    def get_datastore_details(self, workspace: str, datastore: str):
        url = f"{self.base_url}/workspaces/{workspace}/datastores/{datastore}.json"
        return requests.get(url, auth=self.auth)

    def delete_workspace(self, workspace: str):
        """
        Delete a workspace.
        """
        url = f"{self.base_url}/workspaces/{workspace}"
        return requests.delete(url, auth=self.auth)

    def delete_datastore(self, workspace: str, datastore: str):
        """
        Delete a datastore in a workspace.
        """
        url = f"{self.base_url}/workspaces/{workspace}/datastores/{datastore}"
        return requests.delete(url, auth=self.auth)

    def update_workspace(self, workspace: str, request: UpdateRequest):
        """
        Update a workspace.
        """
        url = f"{self.base_url}/workspaces/{workspace}"
        data = {"workspace": {"name": request.new_name}} if request.new_name else {}
        return requests.put(url, auth=self.auth, json=data)

    def update_datastore(self, workspace: str, datastore: str, request: UpdateRequest):
        """
        Update a datastore in a workspace.
        """
        url = f"{self.base_url}/workspaces/{workspace}/datastores/{datastore}"
        data = {"dataStore": {"name": request.new_name}} if request.new_name else {}
        return requests.put(url, auth=self.auth, json=data)

    def delete_layer(self, layer: str):
        """
        Delete a layer.
        """
        url = f"{self.base_url}/layers/{layer}"
        return requests.delete(url, auth=self.auth)

    def update_layer(self, layer: str, request: UpdateRequest):
        """
        Update a layer.
        """
        url = f"{self.base_url}/layers/{layer}"
        data = {"layer": {"name": request.new_name}} if request.new_name else {}
        return requests.put(url, auth=self.auth, json=data)

    def delete_style(self, style: str):
        """
        Delete a style.
        """
        url = f"{self.base_url}/styles/{style}"
        return requests.delete(url, auth=self.auth)

    def update_style(self, style: str, request: UpdateRequest):
        """
        Update a style.
        """
        url = f"{self.base_url}/styles/{style}"
        data = {"style": {"name": request.new_name}} if request.new_name else {}
        return requests.put(url, auth=self.auth, json=data)

    def list_datastore_tables(self, workspace: str, datastore: str):
        """
        List all available tables in a PostGIS datastore.
        """
        url = f"{self.base_url}/workspaces/{workspace}/datastores/{datastore}/featuretypes.json"
        return requests.get(url, auth=self.auth)

    def list_postgis_schema_tables(
        self, workspace: str, datastore: str, schema: str = "public"
    ):
        """
        List all tables in a specific PostGIS schema by querying the database directly.
        This uses GeoServer's SQL view functionality to query the information_schema.
        """
        # First, get the datastore details to extract connection info
        datastore_url = (
            f"{self.base_url}/workspaces/{workspace}/datastores/{datastore}.json"
        )
        datastore_response = requests.get(datastore_url, auth=self.auth)

        if datastore_response.status_code != 200:
            raise Exception(f"Failed to get datastore details: {datastore_response.text}")

        # Use GeoServer's SQL view to query the database
        sql_view_url = (
            f"{self.base_url}/workspaces/{workspace}/datastores/{datastore}/featuretypes"
        )
        headers = {"Content-type": "application/json"}

        # SQL query to get all tables in the specified schema
        sql_query = f"""
        SELECT
            table_name,
            table_type,
            table_schema
        FROM information_schema.tables
        WHERE table_schema = '{schema}'
        AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """

        sql_view_config = {
            "featureType": {
                "name": f"schema_tables_{schema}",
                "nativeName": f"schema_tables_{schema}",
                "title": f"Tables in {schema} schema",
                "srs": "EPSG:4326",
                "nativeBoundingBox": {
                    "minx": -180,
                    "maxx": 180,
                    "miny": -90,
                    "maxy": 90,
                    "crs": "EPSG:4326"
                },
                "latLon": {
                    "minx": -180,
                    "maxx": 180,
                    "miny": -90,
                    "maxy": 90,
                    "crs": "EPSG:4326"
                },
                "metadata": {
                    "entry": [
                        {
                            "@key": "JDBC_VIRTUAL_TABLE",
                            "virtualTable": {
                                "name": f"schema_tables_{schema}",
                                "sql": sql_query,
                                "escapeSql": False
                            }
                        }
                    ]
                }
            }
        }

        # Create a temporary SQL view to query the database
        response = requests.post(
            sql_view_url, auth=self.auth, json=sql_view_config, headers=headers
        )

        if response.status_code in [200, 201]:
            # Now get the data from this SQL view
            data_url = (
                f"{self.base_url}/workspaces/{workspace}/datastores/{datastore}/"
                f"featuretypes/schema_tables_{schema}.json"
            )
            data_response = requests.get(data_url, auth=self.auth)

            # Clean up the temporary SQL view
            delete_url = (
                f"{self.base_url}/workspaces/{workspace}/datastores/{datastore}/"
                f"featuretypes/schema_tables_{schema}"
            )
            requests.delete(delete_url, auth=self.auth)

            return data_response
        else:
            raise Exception(f"Failed to create SQL view: {response.text}")

    def list_postgis_tables_direct(
        self, workspace: str, datastore: str, schema: str = "public"
    ):
        """
        List all tables in a PostGIS schema by using GeoServer's SQL view endpoint.
        This method creates a temporary SQL view to query the information_schema.
        """
        # Create a SQL view to query the database schema
        sql_view_url = (
            f"{self.base_url}/workspaces/{workspace}/datastores/{datastore}/featuretypes"
        )
        headers = {"Content-type": "application/json"}

        # SQL query to get all tables in the specified schema
        sql_query = f"""
        SELECT
            table_name,
            table_type,
            table_schema
        FROM information_schema.tables
        WHERE table_schema = '{schema}'
        AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """

        sql_view_config = {
            "featureType": {
                "name": f"temp_schema_tables_{schema}",
                "nativeName": f"temp_schema_tables_{schema}",
                "title": f"Tables in {schema} schema",
                "srs": "EPSG:4326",
                "nativeBoundingBox": {
                    "minx": -180,
                    "maxx": 180,
                    "miny": -90,
                    "maxy": 90,
                    "crs": "EPSG:4326"
                },
                "latLon": {
                    "minx": -180,
                    "maxx": 180,
                    "miny": -90,
                    "maxy": 90,
                    "crs": "EPSG:4326"
                },
                "metadata": {
                    "entry": [
                        {
                            "@key": "JDBC_VIRTUAL_TABLE",
                            "virtualTable": {
                                "name": f"temp_schema_tables_{schema}",
                                "sql": sql_query,
                                "escapeSql": False
                            }
                        }
                    ]
                }
            }
        }

        # Create temporary SQL view
        create_response = requests.post(
            sql_view_url, auth=self.auth, json=sql_view_config, headers=headers
        )

        if create_response.status_code in [200, 201]:
            try:
                # Get the data from the SQL view
                data_url = (
                    f"{self.base_url}/workspaces/{workspace}/datastores/{datastore}/"
                    f"featuretypes/temp_schema_tables_{schema}.json"
                )
                data_response = requests.get(data_url, auth=self.auth)

                # Return the response with table information
                return data_response
            finally:
                # Clean up the temporary SQL view
                delete_url = (
                    f"{self.base_url}/workspaces/{workspace}/datastores/{datastore}/"
                    f"featuretypes/temp_schema_tables_{schema}"
                )
                requests.delete(delete_url, auth=self.auth)
        else:
            raise Exception(f"Failed to create SQL view: {create_response.text}")

    def create_layer_from_table(
        self,
        workspace: str,
        datastore: str,
        table_name: str,
        layer_name: str = None,
        title: str = None,
        description: str = None,
        enabled: bool = True,
        default_style: str = None
    ):
        """
        Create a layer from a PostGIS table.
        """
        url = (
            f"{self.base_url}/workspaces/{workspace}/datastores/{datastore}/featuretypes"
        )
        headers = {"Content-type": "application/json"}

        # Use table_name as layer_name if not provided
        if not layer_name:
            layer_name = table_name

        feature_type_config = {
            "featureType": {
                "name": layer_name,
                "nativeName": table_name,
                "enabled": enabled
            }
        }

        if title:
            feature_type_config["featureType"]["title"] = title
        if description:
            feature_type_config["featureType"]["description"] = description
        if default_style:
            feature_type_config["featureType"]["defaultStyle"] = {
                "name": default_style
            }

        response = requests.post(
            url, auth=self.auth, json=feature_type_config, headers=headers
        )
        return response

    def get_table_details(self, workspace: str, datastore: str, table_name: str):
        """
        Get details of a specific table in a datastore.
        """
        url = (
            f"{self.base_url}/workspaces/{workspace}/datastores/{datastore}/"
            f"featuretypes/{table_name}.json"
        )
        return requests.get(url, auth=self.auth)

    def create_workspace(self, workspace_name: str):
        """
        Create a new workspace in GeoServer.
        """
        url = f"{self.base_url}/workspaces"
        headers = {"Content-type": "application/json"}
        data = {"workspace": {"name": workspace_name}}
        return requests.post(url, auth=self.auth, json=data, headers=headers)

    def get_layer_details(self, layer: str):
        """
        Get details of a specific layer.
        """
        url = f"{self.base_url}/layers/{layer}.json"
        return requests.get(url, auth=self.auth)

    def list_styles(self):
        """
        List all styles in GeoServer.
        """
        url = f"{self.base_url}/styles.json"
        return requests.get(url, auth=self.auth)

    def get_style_details(self, style_name: str):
        """
        Get details of a specific style.
        """
        url = f"{self.base_url}/styles/{style_name}.json"
        return requests.get(url, auth=self.auth)

