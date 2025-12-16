import os
import tempfile
import zipfile
from urllib.parse import urlencode
import requests


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

        # Determine whether the provided path is a zip archive or a loose shapefile
        cleanup_path = None
        upload_path = file_path

        if file_path.lower().endswith('.zip'):
            upload_path = file_path
        elif file_path.lower().endswith('.shp'):
            base_name, _ = os.path.splitext(file_path)
            directory = os.path.dirname(file_path) or "."
            basename_only = os.path.basename(base_name)
            basename_lower = basename_only.lower()

            required_extensions = ['.shp', '.shx', '.dbf']
            matching_files = []
            available_extensions = set()
            for filename in os.listdir(directory):
                file_full_path = os.path.join(directory, filename)
                if not os.path.isfile(file_full_path):
                    continue
                name_root, ext = os.path.splitext(filename)
                if name_root.lower() == basename_lower and ext.lower() != '.zip':
                    matching_files.append(filename)
                    available_extensions.add(ext.lower())

            missing_components = [
                ext for ext in required_extensions if ext not in available_extensions
            ]
            if missing_components:
                missing_str = ', '.join(missing_components)
                raise ValueError(
                    f"Missing required shapefile component(s) for '{file_path}': {missing_str}"
                )

            fd, temp_zip_path = tempfile.mkstemp(suffix=".zip")
            os.close(fd)
            try:
                with zipfile.ZipFile(temp_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                    for filename in matching_files:
                        file_to_add = os.path.join(directory, filename)
                        zipf.write(file_to_add, arcname=filename)
                upload_path = temp_zip_path
                cleanup_path = temp_zip_path
            except Exception:
                if os.path.exists(temp_zip_path):
                    os.remove(temp_zip_path)
                raise
        else:
            raise ValueError(
                "Shapefile must be provided as a .zip archive or a .shp file "
                "with accompanying components."
            )

        try:
            with open(upload_path, "rb") as f:
                response = requests.put(url, auth=self.auth, data=f, headers=headers)
        finally:
            if cleanup_path and os.path.exists(cleanup_path):
                os.remove(cleanup_path)
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
        response = requests.post(
            url, auth=self.auth, data=data, headers=headers, params={"name": style_name}
        )
        return response

    def upload_postgis(
        self,
        workspace: str,
        store_name: str,
        database: str,
        host: str,
        port: int,
        username: str,
        password: str,
        schema: str = "public",
        description: str = None,
        enabled: bool = True
    ):
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

        response = requests.post(
            url, auth=self.auth, json=data_store_config, headers=headers
        )
        return response

    def list_layers(self):
        url = f"{self.base_url}/layers.json"
        return requests.get(url, auth=self.auth)

    def get_layer_details(self, layer: str):
        url = f"{self.base_url}/layers/{layer}.json"
        return requests.get(url, auth=self.auth)


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

    def get_tile_layer_url_cml(self, layer: str):
        """
        Construct a WMS URL for fetching the tile layer in the format expected
        by the CML frontend.
        Format: /wms?bbox={bbox-epsg-3857}&format=image/png&service=WMS&
        version=1.1.1&request=GetMap&srs=EPSG:3857&width=256&height=256&
        transparent=true&layers=biodiv:${layer_name}
        """
        # Extract layer name from workspace:layer format
        # (e.g., "metastring:gbif" -> "gbif")
        layer_name = layer.split(":")[-1] if ":" in layer else layer

        # Prepend "biodiv:" prefix as expected by frontend
        layers_param = f"biodiv:{layer_name}"

        # Construct relative WMS URL path (frontend will prepend the endpoint)
        params = {
            "bbox": "{bbox-epsg-3857}",  # Placeholder for frontend to replace
            "format": "image/png",
            "service": "WMS",
            "version": "1.1.1",
            "request": "GetMap",
            "srs": "EPSG:3857",
            "width": "256",
            "height": "256",
            "transparent": "true",
            "layers": layers_param
        }
        return "/wms?" + urlencode(params)

    def query_features(
        self,
        layer: str,
        bbox: str = None,
        filter_query: str = None,
        max_features: int = None,
        property_names: str = None
    ):
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

    def create_mbstyle(self, workspace: str, style_name: str, style_content: str):
        """
        Create or update an MBStyle (Mapbox Style) in GeoServer.
        
        Args:
            workspace: GeoServer workspace name
            style_name: Name for the style
            style_content: MBStyle JSON content as string
        
        Returns:
            Response object from GeoServer REST API
        """
        url = f"{self.base_url}/workspaces/{workspace}/styles"
        headers = {"Content-type": "application/vnd.geoserver.mbstyle+json"}
        
        # First, try to create the style
        response = requests.post(
            url,
            auth=self.auth,
            data=style_content,
            headers=headers,
            params={"name": style_name}
        )
        
        # If style already exists (409), update it instead
        if response.status_code == 409:
            update_url = f"{self.base_url}/workspaces/{workspace}/styles/{style_name}"
            response = requests.put(
                update_url,
                auth=self.auth,
                data=style_content,
                headers=headers
            )
        
        return response

    def set_layer_default_style(self, workspace: str, layer_name: str, style_name: str):
        """
        Set the default style for a layer.
        
        Args:
            workspace: GeoServer workspace name
            layer_name: Layer name (without workspace prefix)
            style_name: Style name to set as default
        
        Returns:
            Response object from GeoServer REST API
        """
        url = f"{self.base_url}/layers/{workspace}:{layer_name}"
        headers = {"Content-type": "application/json"}
        
        data = {
            "layer": {
                "defaultStyle": {
                    "name": style_name
                }
            }
        }
        
        response = requests.put(
            url,
            auth=self.auth,
            json=data,
            headers=headers
        )
        
        return response

    def get_url(self, url: str):
        """
        Perform an authenticated GET to an absolute GeoServer REST URL.
        """
        return requests.get(url, auth=self.auth)

    def get_vectortile_layer_url(self, layer: str, epsg: int = 3857):
        """
        Construct a vector tile URL (TMS/PBF) for fetching the tile layer.
        Format: /geoserver/gwc/service/tms/1.0.0/{layer}@{gridSet}@pbf/{z}/{x}/{-y}.pbf
        
        Args:
            layer: Layer name (e.g., 'metastring:gbif' or 'kew')
            epsg: EPSG code for projection (default: 3857 for Web Mercator)
        
        Returns:
            Full vector tile URL template with placeholders for z, x, y
        
        Note: GeoWebCache uses gridset names (like 'WebMercatorQuad' for EPSG:3857)
        instead of EPSG codes directly in the TMS URL.
        """
        # Get base URL without /rest
        base_url = self.base_url.replace("/rest", "")
        
        # Map EPSG codes to GeoWebCache gridset names
        # GeoWebCache uses 'WebMercatorQuad' for EPSG:3857 (Web Mercator)
        gridset_mapping = {
            3857: "WebMercatorQuad",
            900913: "WebMercatorQuad",  # Google's alternate code for Web Mercator
            4326: "EPSG:4326",  # EPSG:4326 is used directly
        }
        
        # Get the gridset name, defaulting to EPSG:{code} format if not in mapping
        gridset_name = gridset_mapping.get(epsg, f"EPSG:{epsg}")
        
        # Construct vector tile URL template
        # Format: /gwc/service/tms/1.0.0/{layer}@{gridSet}@pbf/{z}/{x}/{-y}.pbf
        # Note: {-y} is used for TMS Y coordinate inversion
        # The layer name is used as-is in the path (GeoServer TMS handles : in layer names)
        # base_url already contains /geoserver, so we only add /gwc/...
        tile_url_template = f"{base_url}/gwc/service/tms/1.0.0/{layer}@{gridset_name}@pbf/{{z}}/{{x}}/{{-y}}.pbf"
        
        return tile_url_template

