from urllib.parse import urlencode
import requests
import os
import json


class GeoServerDAO:
    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url
        self.auth = (username, password)

    def upload_shapefile(self, workspace: str, store_name: str, file_path: str):
        """
        Upload a shapefile to GeoServer.
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
            
        url = f"{self.base_url}/workspaces/{workspace}/datastores/{store_name}/file.shp"
        headers = {"Content-type": "application/zip"}
        
        # Check if file is a zip file
        if not file_path.lower().endswith('.zip'):
            raise ValueError("Shapefile must be uploaded as a ZIP file containing all shapefile components (.shp, .shx, .dbf, .prj)")
            
        with open(file_path, "rb") as f:
            response = requests.put(url, auth=self.auth, data=f, headers=headers)
        return response

    def upload_style(self, workspace: str, style_name: str, file_path: str):
        """
        Upload a style (SLD file) to GeoServer.
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
            
        url = f"{self.base_url}/workspaces/{workspace}/styles"
        headers = {"Content-type": "application/vnd.ogc.sld+xml"}
        with open(file_path, "rb") as f:
            data = f.read()
        response = requests.post(url, auth=self.auth, data=data, headers=headers, params={"name": style_name})
        return response

    def upload_postgis(self, workspace: str, store_name: str, database: str, host: str, port: int, 
                      username: str, password: str, schema: str = "public", description: str = None, enabled: bool = True):
        """
        Create a PostGIS datastore in GeoServer.
        """
        url = f"{self.base_url}/workspaces/{workspace}/datastores"
        headers = {"Content-type": "application/json"}
        
        data_store_config = {
            "dataStore": {
                "name": store_name,
                "type": "PostGIS",
                "enabled": enabled,
                "connectionParameters": {
                    "entry": [
                        {"@key": "database", "$": database},
                        {"@key": "host", "$": host},
                        {"@key": "port", "$": str(port)},
                        {"@key": "user", "$": username},
                        {"@key": "passwd", "$": password},
                        {"@key": "schema", "$": schema},
                        {"@key": "dbtype", "$": "postgis"},
                        {"@key": "validate connections", "$": "true"},
                        {"@key": "max connections", "$": "10"},
                        {"@key": "min connections", "$": "1"}
                    ]
                }
            }
        }
        
        if description:
            data_store_config["dataStore"]["description"] = description
            
        response = requests.post(url, auth=self.auth, json=data_store_config, headers=headers)
        return response
    
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

    def list_layers(self):
        url = f"{self.base_url}/layers.json"
        return requests.get(url, auth=self.auth)

    def get_layer_details(self, layer: str):
        url = f"{self.base_url}/layers/{layer}.json"
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

    def delete_layer(self, layer: str):
        """
        Delete a layer.
        """
        url = f"{self.base_url}/layers/{layer}"
        return requests.delete(url, auth=self.auth)
    
    def delete_style(self, style: str):
        """
        Delete a style.
        """
        url = f"{self.base_url}/styles/{style}"
        return requests.delete(url, auth=self.auth)
    
    def update_workspace(self, workspace: str, request):
        """
        Update a workspace.
        """
        url = f"{self.base_url}/workspaces/{workspace}"
        data = {"workspace": {"name": request.new_name}} if request.new_name else {}
        return requests.put(url, auth=self.auth, json=data)

    def update_datastore(self, workspace: str, datastore: str, request):
        """
        Update a datastore in a workspace.
        """
        url = f"{self.base_url}/workspaces/{workspace}/datastores/{datastore}"
        data = {"dataStore": {"name": request.new_name}} if request.new_name else {}
        return requests.put(url, auth=self.auth, json=data)

    def update_layer(self, layer: str, request):
        """
        Update a layer.
        """
        url = f"{self.base_url}/layers/{layer}"
        data = {"layer": {"name": request.new_name}} if request.new_name else {}
        return requests.put(url, auth=self.auth, json=data)

    def update_style(self, style: str, request):
        """
        Update a style.
        """
        url = f"{self.base_url}/styles/{style}"
        data = {"style": {"name": request.new_name}} if request.new_name else {}
        return requests.put(url, auth=self.auth, json=data)
    
    def get_tile_layer_url(self, layer: str):
        """
        Construct a WMS URL for fetching the tile layer.
        """
        wms_url = self.base_url.replace("/rest", "") + "/wms"  # WMS endpoint
        params = {
            "service": "WMS",
            "version": "1.1.1",
            "request": "GetMap",
            "layers": layer,
            "styles": "",
            "bbox": "-180,-90,180,90",  # Change this to your actual dataset extent
            "width": "256",
            "height": "256",
            "srs": "EPSG:4326",
            "format": "image/png",
            "transparent": "true"
        }
        return wms_url + "?" + urlencode(params)

    def query_features(self, layer: str, bbox: str = None, filter_query: str = None, max_features: int = None, property_names: str = None):
        """
        Query features from a GeoServer WFS service.
        """
        wfs_url = self.base_url.replace("/rest", "") + "/wfs"  # WFS endpoint
        params = {
            "service": "WFS",
            "version": "1.1.0",
            "request": "GetFeature",
            "typeName": layer,
            "outputFormat": "application/json",
        }
        if bbox:
            params["bbox"] = bbox
        if filter_query:
            params["CQL_FILTER"] = filter_query
        if max_features is not None:
            params["maxFeatures"] = str(max_features)
        if property_names:
            # Comma-separated list of attribute names
            params["propertyName"] = property_names

        return requests.get(wfs_url, params=params, auth=self.auth)

    def create_workspace(self, workspace_name: str):
        """
        Create a new workspace in GeoServer.
        """
        url = f"{self.base_url}/workspaces"
        headers = {"Content-type": "application/json"}
        data = {"workspace": {"name": workspace_name}}
        return requests.post(url, auth=self.auth, json=data, headers=headers)

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

    def list_datastore_tables(self, workspace: str, datastore: str):
        """
        List all available tables in a PostGIS datastore.
        """
        url = f"{self.base_url}/workspaces/{workspace}/datastores/{datastore}/featuretypes.json"
        return requests.get(url, auth=self.auth)

    def list_postgis_schema_tables(self, workspace: str, datastore: str, schema: str = "public"):
        """
        List all tables in a specific PostGIS schema by querying the database directly.
        This uses GeoServer's SQL view functionality to query the information_schema.
        """
        # First, get the datastore details to extract connection info
        datastore_url = f"{self.base_url}/workspaces/{workspace}/datastores/{datastore}.json"
        datastore_response = requests.get(datastore_url, auth=self.auth)
        
        if datastore_response.status_code != 200:
            raise Exception(f"Failed to get datastore details: {datastore_response.text}")
        
        # Use GeoServer's SQL view to query the database
        sql_view_url = f"{self.base_url}/workspaces/{workspace}/datastores/{datastore}/featuretypes"
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
        response = requests.post(sql_view_url, auth=self.auth, json=sql_view_config, headers=headers)
        
        if response.status_code in [200, 201]:
            # Now get the data from this SQL view
            data_url = f"{self.base_url}/workspaces/{workspace}/datastores/{datastore}/featuretypes/schema_tables_{schema}.json"
            data_response = requests.get(data_url, auth=self.auth)
            
            # Clean up the temporary SQL view
            delete_url = f"{self.base_url}/workspaces/{workspace}/datastores/{datastore}/featuretypes/schema_tables_{schema}"
            requests.delete(delete_url, auth=self.auth)
            
            return data_response
        else:
            raise Exception(f"Failed to create SQL view: {response.text}")

    def list_postgis_tables_simple(self, workspace: str, datastore: str, schema: str = "public"):
        """
        Alternative method: Use WFS to query the database for table information.
        This is simpler but requires the datastore to be properly configured.
        """
        wfs_url = self.base_url.replace("/rest", "") + "/wfs"
        params = {
            "service": "WFS",
            "version": "1.1.0",
            "request": "GetFeature",
            "typeName": f"{workspace}:{datastore}",
            "outputFormat": "application/json",
            "maxFeatures": "1"  # Just to test connection
        }
        
        response = requests.get(wfs_url, params=params, auth=self.auth)
        return response

    def list_postgis_tables_direct(self, workspace: str, datastore: str, schema: str = "public"):
        """
        List all tables in a PostGIS schema by using GeoServer's SQL view endpoint.
        This method creates a temporary SQL view to query the information_schema.
        """
        # Create a SQL view to query the database schema
        sql_view_url = f"{self.base_url}/workspaces/{workspace}/datastores/{datastore}/featuretypes"
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
        create_response = requests.post(sql_view_url, auth=self.auth, json=sql_view_config, headers=headers)
        
        if create_response.status_code in [200, 201]:
            try:
                # Get the data from the SQL view
                data_url = f"{self.base_url}/workspaces/{workspace}/datastores/{datastore}/featuretypes/temp_schema_tables_{schema}.json"
                data_response = requests.get(data_url, auth=self.auth)
                
                # Return the response with table information
                return data_response
            finally:
                # Clean up the temporary SQL view
                delete_url = f"{self.base_url}/workspaces/{workspace}/datastores/{datastore}/featuretypes/temp_schema_tables_{schema}"
                requests.delete(delete_url, auth=self.auth)
        else:
            raise Exception(f"Failed to create SQL view: {create_response.text}")

    def create_layer_from_table(self, workspace: str, datastore: str, table_name: str, 
                               layer_name: str = None, title: str = None, 
                               description: str = None, enabled: bool = True, 
                               default_style: str = None):
        """
        Create a layer from a PostGIS table.
        """
        url = f"{self.base_url}/workspaces/{workspace}/datastores/{datastore}/featuretypes"
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
            
        response = requests.post(url, auth=self.auth, json=feature_type_config, headers=headers)
        return response

    def get_table_details(self, workspace: str, datastore: str, table_name: str):
        """
        Get details of a specific table in a datastore.
        """
        url = f"{self.base_url}/workspaces/{workspace}/datastores/{datastore}/featuretypes/{table_name}.json"
        return requests.get(url, auth=self.auth)

    def get_url(self, url: str):
        """
        Perform an authenticated GET to an absolute GeoServer REST URL.
        """
        return requests.get(url, auth=self.auth)

