"""
Data Processing Service - Handles different file types and geometry conversion
Supports vector, raster, shapefile, GeoJSON, CSV with coordinates, etc.
"""
import os
import json
import tempfile
import zipfile
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Union
from datetime import datetime
import logging

import fiona
import rasterio
from rasterio.warp import calculate_default_transform, reproject, Resampling
import geopandas as gpd
from shapely.geometry import Point, LineString, Polygon, MultiPoint, MultiLineString, MultiPolygon
from shapely.geometry.base import BaseGeometry
from shapely import wkt, wkb
from pyproj import CRS, Transformer
import numpy as np

from unified_data.models.model import DatasetType, GeometryType, DatasetFeatureCreate
from unified_data.models.schema import Dataset, DatasetFeature

logger = logging.getLogger(__name__)


class DataProcessor:
    """
    Main data processing class that handles various file formats
    and converts them to a unified format for storage
    """
    
    SUPPORTED_VECTOR_EXTENSIONS = {'.shp', '.geojson', '.json', '.gpkg', '.gdb', '.kml', '.kmz'}
    SUPPORTED_RASTER_EXTENSIONS = {'.tif', '.tiff', '.img', '.nc', '.netcdf', '.hdf', '.jp2'}
    SUPPORTED_TABULAR_EXTENSIONS = {'.csv', '.xlsx', '.xls'}
    
    def __init__(self, temp_dir: Optional[str] = None):
        self.temp_dir = temp_dir or tempfile.gettempdir()
        
    def process_file(self, file_path: str, dataset_metadata: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """
        Main entry point for processing any file type
        Returns: (updated_metadata, features_list)
        """
        file_path = Path(file_path)
        extension = file_path.suffix.lower()
        
        logger.info(f"Processing file: {file_path} with extension: {extension}")
        
        # Determine processing method based on file extension
        if extension == '.zip':
            return self._process_zip_file(file_path, dataset_metadata)
        elif extension in self.SUPPORTED_VECTOR_EXTENSIONS:
            return self._process_vector_file(file_path, dataset_metadata)
        elif extension in self.SUPPORTED_RASTER_EXTENSIONS:
            return self._process_raster_file(file_path, dataset_metadata)
        elif extension in self.SUPPORTED_TABULAR_EXTENSIONS:
            return self._process_tabular_file(file_path, dataset_metadata)
        else:
            raise ValueError(f"Unsupported file format: {extension}")
    
    def _process_zip_file(self, file_path: Path, metadata: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """Process ZIP files that may contain shapefiles or other formats"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Extract ZIP file
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)
            
            # Find the main file to process
            extracted_files = list(Path(temp_dir).rglob('*'))
            
            # Look for shapefiles first
            shapefiles = [f for f in extracted_files if f.suffix.lower() == '.shp']
            if shapefiles:
                main_file = shapefiles[0]
                metadata['dataset_type'] = DatasetType.SHAPEFILE
                metadata['file_format'] = '.shp'
                return self._process_vector_file(main_file, metadata)
            
            # Look for other vector formats
            vector_files = [f for f in extracted_files if f.suffix.lower() in self.SUPPORTED_VECTOR_EXTENSIONS]
            if vector_files:
                main_file = vector_files[0]
                return self._process_vector_file(main_file, metadata)
            
            # Look for raster files
            raster_files = [f for f in extracted_files if f.suffix.lower() in self.SUPPORTED_RASTER_EXTENSIONS]
            if raster_files:
                main_file = raster_files[0]
                return self._process_raster_file(main_file, metadata)
            
            raise ValueError("No supported files found in ZIP archive")
    
    def _process_vector_file(self, file_path: Path, metadata: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """Process vector files (shapefile, GeoJSON, etc.)"""
        try:
            # Read with GeoPandas for unified handling
            gdf = gpd.read_file(file_path)
            
            if gdf.empty:
                raise ValueError("Vector file contains no features")
            
            # Update metadata
            metadata.update({
                'dataset_type': self._detect_dataset_type(file_path),
                'file_format': file_path.suffix.lower(),
                'geometry_type': self._detect_geometry_type(gdf),
                'crs': str(gdf.crs) if gdf.crs else 'EPSG:4326'
            })
            
            # Calculate bounding box
            bounds = gdf.total_bounds
            metadata.update({
                'bbox_minx': float(bounds[0]),
                'bbox_miny': float(bounds[1]),
                'bbox_maxx': float(bounds[2]),
                'bbox_maxy': float(bounds[3])
            })
            
            # Ensure CRS is EPSG:4326 for storage
            if gdf.crs and gdf.crs != 'EPSG:4326':
                gdf = gdf.to_crs('EPSG:4326')
            
            # Convert to features
            features = []
            for idx, row in gdf.iterrows():
                # Extract geometry
                geom_dict = None
                if row.geometry and not row.geometry.is_empty:
                    geom_dict = json.loads(gpd.GeoSeries([row.geometry]).to_json())['features'][0]['geometry']
                
                # Extract attributes (exclude geometry column)
                attributes = {}
                for col in gdf.columns:
                    if col != 'geometry':
                        value = row[col]
                        # Handle NaN/None values
                        if pd.isna(value):
                            attributes[col] = None
                        elif isinstance(value, (np.integer, np.floating)):
                            attributes[col] = float(value) if isinstance(value, np.floating) else int(value)
                        else:
                            attributes[col] = str(value)
                
                features.append({
                    'geometry': geom_dict,
                    'attributes': attributes,
                    'feature_id': str(idx)
                })
            
            logger.info(f"Processed {len(features)} vector features")
            return metadata, features
            
        except Exception as e:
            logger.error(f"Error processing vector file {file_path}: {e}")
            raise
    
    def _process_raster_file(self, file_path: Path, metadata: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """Process raster files (GeoTIFF, etc.)"""
        try:
            with rasterio.open(file_path) as src:
                # Update metadata
                metadata.update({
                    'dataset_type': DatasetType.RASTER,
                    'file_format': file_path.suffix.lower(),
                    'geometry_type': GeometryType.RASTER,
                    'crs': str(src.crs) if src.crs else 'EPSG:4326',
                    'spatial_resolution': f"{src.res[0]}x{src.res[1]} units"
                })
                
                # Calculate bounding box
                bounds = src.bounds
                metadata.update({
                    'bbox_minx': float(bounds.left),
                    'bbox_miny': float(bounds.bottom),
                    'bbox_maxx': float(bounds.right),
                    'bbox_maxy': float(bounds.top)
                })
                
                # For raster, we can either:
                # 1. Store just metadata and file path (for large rasters)
                # 2. Sample points from raster (for analysis)
                # 3. Store raster in PostGIS raster column (for smaller rasters)
                
                # Option 2: Sample points approach for demonstration
                features = self._sample_raster_points(src, metadata.get('sample_points', 1000))
                
                # Add raster-specific metadata
                metadata.update({
                    'raster_width': src.width,
                    'raster_height': src.height,
                    'raster_bands': src.count,
                    'raster_dtype': str(src.dtypes[0]) if src.dtypes else None,
                    'raster_nodata': src.nodata
                })
                
            logger.info(f"Processed raster with {len(features)} sample points")
            return metadata, features
            
        except Exception as e:
            logger.error(f"Error processing raster file {file_path}: {e}")
            raise
    
    def _process_tabular_file(self, file_path: Path, metadata: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """Process tabular files (CSV, Excel) with coordinate columns"""
        try:
            # Read tabular data
            if file_path.suffix.lower() == '.csv':
                df = pd.read_csv(file_path)
            else:  # Excel files
                df = pd.read_excel(file_path)
            
            if df.empty:
                raise ValueError("Tabular file contains no data")
            
            # Detect coordinate columns
            coord_columns = self._detect_coordinate_columns(df)
            if not coord_columns:
                # No coordinates found, treat as non-spatial data
                metadata.update({
                    'dataset_type': DatasetType.CSV,
                    'file_format': file_path.suffix.lower(),
                    'geometry_type': None
                })
                
                features = []
                for idx, row in df.iterrows():
                    attributes = {}
                    for col in df.columns:
                        value = row[col]
                        if pd.isna(value):
                            attributes[col] = None
                        else:
                            attributes[col] = str(value)
                    
                    features.append({
                        'geometry': None,
                        'attributes': attributes,
                        'feature_id': str(idx)
                    })
            else:
                # Has coordinates, create point geometries
                lon_col, lat_col = coord_columns
                metadata.update({
                    'dataset_type': DatasetType.CSV,
                    'file_format': file_path.suffix.lower(),
                    'geometry_type': GeometryType.POINT,
                    'crs': 'EPSG:4326'
                })
                
                # Calculate bounding box
                valid_coords = df.dropna(subset=[lon_col, lat_col])
                if not valid_coords.empty:
                    metadata.update({
                        'bbox_minx': float(valid_coords[lon_col].min()),
                        'bbox_miny': float(valid_coords[lat_col].min()),
                        'bbox_maxx': float(valid_coords[lon_col].max()),
                        'bbox_maxy': float(valid_coords[lat_col].max())
                    })
                
                features = []
                for idx, row in df.iterrows():
                    # Create point geometry if coordinates are valid
                    geom_dict = None
                    if pd.notna(row[lon_col]) and pd.notna(row[lat_col]):
                        try:
                            point = Point(float(row[lon_col]), float(row[lat_col]))
                            geom_dict = json.loads(gpd.GeoSeries([point]).to_json())['features'][0]['geometry']
                        except (ValueError, TypeError):
                            pass  # Invalid coordinates, keep geometry as None
                    
                    # Extract attributes
                    attributes = {}
                    for col in df.columns:
                        value = row[col]
                        if pd.isna(value):
                            attributes[col] = None
                        elif isinstance(value, (np.integer, np.floating)):
                            attributes[col] = float(value) if isinstance(value, np.floating) else int(value)
                        else:
                            attributes[col] = str(value)
                    
                    features.append({
                        'geometry': geom_dict,
                        'attributes': attributes,
                        'feature_id': str(idx)
                    })
            
            logger.info(f"Processed {len(features)} tabular records")
            return metadata, features
            
        except Exception as e:
            logger.error(f"Error processing tabular file {file_path}: {e}")
            raise
    
    def _detect_dataset_type(self, file_path: Path) -> DatasetType:
        """Detect dataset type from file extension"""
        extension = file_path.suffix.lower()
        
        if extension == '.shp':
            return DatasetType.SHAPEFILE
        elif extension in ['.geojson', '.json']:
            return DatasetType.GEOJSON
        elif extension == '.gpkg':
            return DatasetType.GEOPACKAGE
        elif extension in ['.kml', '.kmz']:
            return DatasetType.VECTOR
        elif extension in self.SUPPORTED_RASTER_EXTENSIONS:
            return DatasetType.RASTER
        elif extension in self.SUPPORTED_TABULAR_EXTENSIONS:
            return DatasetType.CSV
        else:
            return DatasetType.VECTOR  # Default
    
    def _detect_geometry_type(self, gdf: gpd.GeoDataFrame) -> GeometryType:
        """Detect the primary geometry type in a GeoDataFrame"""
        if gdf.empty or gdf.geometry.isna().all():
            return None
        
        # Get the most common geometry type
        geom_types = gdf.geometry.dropna().geom_type.value_counts()
        primary_type = geom_types.index[0]
        
        type_mapping = {
            'Point': GeometryType.POINT,
            'MultiPoint': GeometryType.MULTIPOINT,
            'LineString': GeometryType.LINESTRING,
            'MultiLineString': GeometryType.MULTILINESTRING,
            'Polygon': GeometryType.POLYGON,
            'MultiPolygon': GeometryType.MULTIPOLYGON
        }
        
        return type_mapping.get(primary_type, GeometryType.POLYGON)
    
    def _detect_coordinate_columns(self, df: pd.DataFrame) -> Optional[Tuple[str, str]]:
        """Detect longitude and latitude columns in a DataFrame"""
        columns = [col.lower() for col in df.columns]
        
        # Common coordinate column names
        lon_names = ['longitude', 'lon', 'lng', 'x', 'long', 'decimal_longitude']
        lat_names = ['latitude', 'lat', 'y', 'decimal_latitude']
        
        lon_col = None
        lat_col = None
        
        # Find longitude column
        for name in lon_names:
            matching = [col for col in df.columns if col.lower() == name or name in col.lower()]
            if matching:
                lon_col = matching[0]
                break
        
        # Find latitude column
        for name in lat_names:
            matching = [col for col in df.columns if col.lower() == name or name in col.lower()]
            if matching:
                lat_col = matching[0]
                break
        
        if lon_col and lat_col:
            return (lon_col, lat_col)
        
        return None
    
    def _sample_raster_points(self, raster_src, max_points: int = 1000) -> List[Dict[str, Any]]:
        """Sample points from a raster for storage as vector features"""
        features = []
        
        # Calculate sampling grid
        width, height = raster_src.width, raster_src.height
        
        # Determine step size for sampling
        total_pixels = width * height
        if total_pixels <= max_points:
            step_x = step_y = 1
        else:
            step = int(np.sqrt(total_pixels / max_points))
            step_x = step_y = step
        
        # Sample points
        for y in range(0, height, step_y):
            for x in range(0, width, step_x):
                # Get pixel value(s)
                try:
                    values = raster_src.read(window=((y, y+1), (x, x+1)))
                    
                    # Convert pixel coordinates to geographic coordinates
                    lon, lat = raster_src.xy(y, x)
                    
                    # Create point geometry
                    point = Point(lon, lat)
                    geom_dict = json.loads(gpd.GeoSeries([point]).to_json())['features'][0]['geometry']
                    
                    # Create attributes from band values
                    attributes = {}
                    for band_idx in range(raster_src.count):
                        band_value = values[band_idx, 0, 0]
                        if band_value != raster_src.nodata:
                            attributes[f'band_{band_idx + 1}'] = float(band_value)
                        else:
                            attributes[f'band_{band_idx + 1}'] = None
                    
                    # Add pixel coordinates
                    attributes['pixel_x'] = x
                    attributes['pixel_y'] = y
                    
                    features.append({
                        'geometry': geom_dict,
                        'attributes': attributes,
                        'feature_id': f'{x}_{y}'
                    })
                    
                    if len(features) >= max_points:
                        break
                        
                except Exception as e:
                    logger.warning(f"Error sampling pixel at ({x}, {y}): {e}")
                    continue
            
            if len(features) >= max_points:
                break
        
        return features
    
    def validate_crs(self, crs_string: str) -> str:
        """Validate and normalize CRS string"""
        try:
            crs = CRS.from_string(crs_string)
            return crs.to_authority_string() or crs_string
        except Exception:
            logger.warning(f"Invalid CRS: {crs_string}, defaulting to EPSG:4326")
            return "EPSG:4326"
    
    def get_file_info(self, file_path: str) -> Dict[str, Any]:
        """Get basic file information without full processing"""
        file_path = Path(file_path)
        
        info = {
            'filename': file_path.name,
            'extension': file_path.suffix.lower(),
            'size_mb': file_path.stat().st_size / (1024 * 1024),
            'detected_type': None,
            'estimated_features': None
        }
        
        try:
            extension = file_path.suffix.lower()
            
            if extension in self.SUPPORTED_VECTOR_EXTENSIONS:
                with fiona.open(file_path) as src:
                    info['detected_type'] = 'vector'
                    info['estimated_features'] = len(src)
                    info['crs'] = str(src.crs) if src.crs else None
                    info['bounds'] = src.bounds
                    
            elif extension in self.SUPPORTED_RASTER_EXTENSIONS:
                with rasterio.open(file_path) as src:
                    info['detected_type'] = 'raster'
                    info['crs'] = str(src.crs) if src.crs else None
                    info['bounds'] = src.bounds
                    info['raster_shape'] = (src.width, src.height)
                    info['raster_bands'] = src.count
                    
            elif extension in self.SUPPORTED_TABULAR_EXTENSIONS:
                if extension == '.csv':
                    df = pd.read_csv(file_path, nrows=5)  # Just peek at first few rows
                else:
                    df = pd.read_excel(file_path, nrows=5)
                    
                info['detected_type'] = 'tabular'
                info['estimated_features'] = None  # Would need to read full file
                info['columns'] = df.columns.tolist()
                info['has_coordinates'] = self._detect_coordinate_columns(df) is not None
                
        except Exception as e:
            logger.warning(f"Error getting file info for {file_path}: {e}")
        
        return info