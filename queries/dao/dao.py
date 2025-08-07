from shapely.geometry import Polygon
from sqlalchemy.sql import text
from database.database import engine
from typing import List, Dict

# Accepts a Polygon object and returns results from datasets
def get_polygon_data_from_datasets(dataset: List[str], polygon: Polygon, limit: int = 1000, offset: int = 0) -> List[Dict]:
    wkt = polygon.wkt
    results = []
    with engine.connect() as conn:
        for table in dataset:
            query = text(f"""
                SELECT DISTINCT t.*,
                       ST_X((dp).geom) AS longitude,
                       ST_Y((dp).geom) AS latitude
                FROM public.{table} t,
                     LATERAL ST_DumpPoints(t.geom) AS dp
                WHERE ST_Intersects(
                    (dp).geom,
                    ST_SetSRID(ST_GeomFromText(:wkt), 4326)
                )
                LIMIT :limit OFFSET :offset
            """)
            res = conn.execute(query, {"wkt": wkt, "limit": limit, "offset": offset})
            results.extend([dict(row._mapping) for row in res])
    return results