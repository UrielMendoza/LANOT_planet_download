'''
Script para dividir la malla de 400 km en cuadrantes de 100 km.
@autor: UrielMendoza
@date: 2023-06-01
'''

import geopandas as gpd

shp_path = r"layers/malla_400km_terrestre/malla_400km_terrestre.shp"
shp_data = gpd.read_file(shp_path)

for index, row in shp_data.iterrows():
    print(row)
    output_file = f"layers/malla_400km_terrestre/malla_400km_terrestre_{row['pathrow']}.geojson"
    row_geojson = gpd.GeoSeries(row['geometry']).to_json()
    # Añade el pathrow al geojson
    row_geojson = row_geojson[:-1] + f',"pathrow":"{row["pathrow"]}"}}'
    
    with open(output_file, 'w') as f:
        f.write(row_geojson)


