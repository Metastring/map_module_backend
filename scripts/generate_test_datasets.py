import os, random
import pandas as pd
import numpy as np
import geopandas as gpd
from shapely.geometry import Point, Polygon
import rasterio
from rasterio.transform import from_origin

OUT_DIR = "data_samples"
os.makedirs(OUT_DIR, exist_ok=True)

def temperature_excel(rows=500):
    rows_out = []
    for i in range(rows):
        rows_out.append({
            "station_id": f"STN_{i:05d}",
            "date": f"2025-01-{(i%30)+1:02d}",
            "temp_min": round(random.uniform(5, 25), 2),
            "temp_max": round(random.uniform(26, 45), 2),
            "humidity": round(random.uniform(30, 95), 1),
            "rainfall_mm": round(random.uniform(0, 120), 1),
            "wind_speed_ms": round(random.uniform(0, 14), 2),
            "lat": 22 + random.uniform(-5, 5),
            "lon": 78 + random.uniform(-5, 5),
            "elevation_m": round(random.uniform(150, 950), 1)
        })
    df = pd.DataFrame(rows_out)
    df.to_excel(f"{OUT_DIR}/temperature_dataset.xlsx", index=False)
    df.to_csv(f"{OUT_DIR}/temperature_dataset.csv", index=False)

def biodiversity_shapefile(records=150):
    species = ["Panthera tigris", "Elephas maximus", "Bos gaurus", "Axis axis", "Macaca radiata"]
    geoms = []
    data = []
    for i in range(records):
        s = random.choice(species)
        cx, cy = 76 + random.uniform(-2, 2), 15 + random.uniform(-2, 2)
        poly = Polygon([
            (cx-0.1, cy-0.1),
            (cx+0.12, cy-0.08),
            (cx+0.08, cy+0.11),
            (cx-0.09, cy+0.09),
            (cx-0.1, cy-0.1)
        ])
        geoms.append(poly)
        data.append({
            "record_id": i,
            "species_name": s,
            "iucn_status": random.choice(["EN","VU","NT","LC","CR"]),
            "density_per_km2": round(random.uniform(0.1, 7.5),2),
            "survey_year": random.choice([2018,2019,2020,2021,2022,2023,2024]),
            "protected_area": random.choice(["Reserve_A","Reserve_B","Reserve_C","None"])
        })
    gdf = gpd.GeoDataFrame(data, geometry=geoms, crs="EPSG:4326")
    gdf.to_file(f"{OUT_DIR}/biodiversity_distribution.shp")
    gdf.to_file(f"{OUT_DIR}/biodiversity_distribution.geojson", driver="GeoJSON")

def raster_temperature_surface(width=120, height=80):
    cell_size = 0.025
    transform = from_origin(75.0, 25.0, cell_size, cell_size)
    base = np.linspace(20, 42, width).reshape(1, width)
    raster = np.repeat(base, height, axis=0)
    noise = np.random.normal(0, 2.5, size=raster.shape)
    data = (raster + noise).astype("float32")
    out_path = f"{OUT_DIR}/temperature_surface.tif"
    with rasterio.open(
        out_path,
        "w",
        driver="GTiff",
        height=height,
        width=width,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=transform,
    ) as dst:
        dst.write(data, 1)

def health_points(records=300):
    diseases = ["Dengue", "Malaria", "Chikungunya", "Typhoid", "Hepatitis"]
    geoms, rows = [], []
    for i in range(records):
        lat = 19 + random.uniform(-3, 3)
        lon = 73 + random.uniform(-3, 3)
        geoms.append(Point(lon, lat))
        rows.append({
            "case_id": i,
            "disease": random.choice(diseases),
            "reported_date": f"2025-02-{(i%28)+1:02d}",
            "severity": random.choice(["Low","Medium","High","Critical"]),
            "age": random.randint(1, 90),
            "gender": random.choice(["M","F"]),
            "district": random.choice(["D1","D2","D3","D4","D5"]),
            "confirmed": random.choice([True, False])
        })
    gdf = gpd.GeoDataFrame(rows, geometry=geoms, crs="EPSG:4326")
    gdf.to_file(f"{OUT_DIR}/health_cases.shp")
    gdf.to_file(f"{OUT_DIR}/health_cases.geojson", driver="GeoJSON")

def large_distribution_dataset(rows=20000):
    cols = [f"attr_{i:02d}" for i in range(1, 21)]
    records = []
    for i in range(rows):
        row = {c: round(random.uniform(0, 100), 3) for c in cols}
        row.update({
            "entity_id": i,
            "region": random.choice(["North","South","East","West","Central"]),
            "category": random.choice(["TypeA","TypeB","TypeC"]),
            "distribution_class": random.choice(["Sparse","Medium","Dense"])
        })
        records.append(row)
    df = pd.DataFrame(records)
    df.to_csv(f"{OUT_DIR}/distribution_20x{rows}.csv", index=False)
    df.head(200).to_excel(f"{OUT_DIR}/distribution_sample.xlsx", index=False)

def main():
    temperature_excel()
    biodiversity_shapefile()
    raster_temperature_surface()
    health_points()
    large_distribution_dataset()  # now 20,000 rows
    print(f"Generated sample datasets in: {OUT_DIR}")

if __name__ == "__main__":
    main()