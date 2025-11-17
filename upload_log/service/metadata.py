import json
import logging
import tempfile
import zipfile
from pathlib import Path
from typing import Dict, Optional, Tuple

import fiona
import rasterio
from pyproj import CRS

from upload_log.models.model import DataType

LOGGER = logging.getLogger(__name__)

VECTOR_EXTENSIONS = {".shp", ".zip", ".geojson", ".json", ".gpkg", ".gml", ".kml"}
RASTER_EXTENSIONS = {".tif", ".tiff", ".geotiff", ".img", ".nc", ".netcdf", ".vrt"}


def derive_file_metadata(file_path: Path) -> Dict[str, Optional[object]]:
    """Compute core metadata (format, type, CRS, bounds, layer name) for an upload."""
    suffix = file_path.suffix.lower()
    file_format = suffix.lstrip(".") if suffix else file_path.name
    data_type = determine_data_type(file_path)

    if suffix == ".zip":
        return _handle_zip_archive(file_path)

    if data_type == DataType.VECTOR:
        layer_name, crs, bbox = _vector_metadata(file_path)
    elif data_type == DataType.RASTER:
        layer_name, crs, bbox = _raster_metadata(file_path)
    else:
        layer_name, crs, bbox = file_path.stem, None, None

    return {
        "layer_name": layer_name,
        "file_format": file_format,
        "data_type": data_type,
        "crs": crs,
        "bbox": bbox,
    }


def determine_data_type(file_path: Path) -> DataType:
    suffix = file_path.suffix.lower()
    if suffix in VECTOR_EXTENSIONS:
        return DataType.VECTOR
    if suffix in RASTER_EXTENSIONS:
        return DataType.RASTER
    return DataType.UNKNOWN


def _handle_zip_archive(file_path: Path) -> Dict[str, Optional[object]]:
    """Inspect a zipped archive, attempting to extract shapefile or other known formats."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        with zipfile.ZipFile(file_path, "r") as archive:
            archive.extractall(tmp_dir)

        extracted_root = Path(tmp_dir)
        shapefiles = list(extracted_root.rglob("*.shp"))
        geopackages = list(extracted_root.rglob("*.gpkg"))

        candidate = None
        if shapefiles:
            candidate = shapefiles[0]
        elif geopackages:
            candidate = geopackages[0]

        if candidate:
            layer_name, crs, bbox = _vector_metadata(candidate)
            return {
                "layer_name": layer_name,
                "file_format": candidate.suffix.lstrip("."),
                "data_type": DataType.VECTOR,
                "crs": crs,
                "bbox": bbox,
            }

    LOGGER.warning("Unsupported zip archive structure for file: %s", file_path)
    return {
        "layer_name": file_path.stem,
        "file_format": file_path.suffix.lstrip("."),
        "data_type": DataType.UNKNOWN,
        "crs": None,
        "bbox": None,
    }


def _vector_metadata(file_path: Path) -> Tuple[str, Optional[str], Optional[Dict[str, float]]]:
    try:
        with fiona.open(file_path) as src:
            layer_name = src.name or file_path.stem
            crs = _format_crs(src.crs or src.crs_wkt)
            bounds = src.bounds if src.bounds else None
    except Exception as exc:
        LOGGER.warning("Failed to read vector metadata for %s: %s", file_path, exc)
        return file_path.stem, None, None

    bbox = _bounds_to_dict(bounds) if bounds else None
    return layer_name, crs, bbox


def _raster_metadata(file_path: Path) -> Tuple[str, Optional[str], Optional[Dict[str, float]]]:
    try:
        with rasterio.open(file_path) as src:
            layer_name = Path(src.name).stem or file_path.stem
            crs = src.crs.to_string() if src.crs else None
            bounds = src.bounds
    except Exception as exc:
        LOGGER.warning("Failed to read raster metadata for %s: %s", file_path, exc)
        return file_path.stem, None, None

    bbox = _bounds_to_dict((bounds.left, bounds.bottom, bounds.right, bounds.top))
    return layer_name, crs, bbox


def _format_crs(crs_input) -> Optional[str]:
    if not crs_input:
        return None

    if isinstance(crs_input, str):
        return crs_input

    try:
        return CRS.from_user_input(crs_input).to_string()
    except Exception:
        try:
            return json.dumps(crs_input)
        except TypeError:
            return str(crs_input)


def _bounds_to_dict(bounds: Tuple[float, float, float, float]) -> Dict[str, float]:
    minx, miny, maxx, maxy = bounds
    return {"min_x": minx, "min_y": miny, "max_x": maxx, "max_y": maxy}
