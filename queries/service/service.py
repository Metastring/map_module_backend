from typing import List
from shapely.geometry import Polygon
from queries.dao.dao import get_polygon_data_from_datasets


def fetch_polygon_query(dataset: List[str], polygon_detail: List[dict], limit: int = 1000, offset: int = 0):
    if not polygon_detail:
        return {"data": []}

    # Extract polygon coordinates (first polygon only)
    coordinates = polygon_detail[0].geometry.coordinates[0]  # Exterior ring only
    polygon = Polygon(coordinates)

    results = get_polygon_data_from_datasets(dataset, polygon, limit, offset)
    return {"data": results}



######## This logic because this way we won't have to define the model individually it wll give response for any number of datasets but it will return entire data ################