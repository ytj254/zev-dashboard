import geopandas as gpd
from sqlalchemy import text
from common_data_update import engine

# 1. Load GeoJSON
gdf = gpd.read_file("assets/Environmental_Justice_Areas_-_PennEnviroScreen_2024.geojson")

# 2. Standardize EJAREA column -> boolean
if "EJAREA" in gdf.columns:
    gdf["ejarea"] = gdf["EJAREA"].astype(str).str.upper().isin(["Y", "YES", "TRUE", "1"])
else:
    raise ValueError("GeoJSON does not contain 'EJAREA' column")

# Keep only needed columns
gdf = gdf[["ejarea", "geometry"]]

# 3. Drop old data but keep schema with SERIAL id
with engine.begin() as conn:
    conn.execute(text("DROP TABLE IF EXISTS ej_area CASCADE"))
    conn.execute(text("""
        CREATE TABLE ej_area (
            id SERIAL PRIMARY KEY,
            ejarea boolean,
            geometry geometry(MultiPolygon, 4326)
        )
    """))

# 4. Load new EJ polygons (id will auto-generate)
gdf.to_postgis("ej_area", engine, if_exists="append", index=False)

# 5. Add spatial index if missing
with engine.begin() as conn:
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_ej_area_geom ON ej_area USING GIST (geometry)"))

print("✅ EJ area data imported successfully into table 'ej_area' with SERIAL id")
