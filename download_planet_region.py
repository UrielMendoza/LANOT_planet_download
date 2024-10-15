'''
Script para obtener los ids de las imágenes satelitales de Planet, y su posterior descarga.

@autor: UrielMendoza
@date: 2024-09-01
'''
import os
import requests
from requests.auth import HTTPBasicAuth
from shapely.geometry import Point, Polygon, mapping, shape
import time
import fiona
from pyproj import Transformer
from shapely.ops import transform
from datetime import datetime
from requests.exceptions import ChunkedEncodingError

# Si la variable API está en el sistema operativo, se usa, de lo contrario, se usa la API_KEY
API_KEY = os.getenv('PL_API_KEY', '')

def latlon_to_geojson(lat, lon):
    """Convierte una coordenada de latitud y longitud a un GeoJSON compatible con la API de Planet."""
    point = Point(lon, lat)
    buffer = point.buffer(0.01)  # Un pequeño buffer alrededor de la coordenada para crear un polígono
    return {
        "type": "Polygon",
        "coordinates": [list(buffer.exterior.coords)]
    }

def shapefile_to_geojson(shapefile_path):
    """Convierte cada cuadrante del shapefile a GeoJSON en EPSG:4326."""
    with fiona.open(shapefile_path, 'r') as shapefile:
        crs = shapefile.crs
        # Verifica si el shapefile no está en EPSG:4326 y aplica la transformación
        if crs['init'] != 'epsg:4326':
            transformer = Transformer.from_crs(crs, "EPSG:4326", always_xy=True)
        else:
            transformer = None  # No hace falta transformar si ya está en EPSG:4326

        geojson_quadrants = []
        
        for feature in shapefile:
            geom = shape(feature['geometry'])  # Geometría original
            if transformer:
                # Transformar la geometría completa de Shapely usando shapely.ops.transform
                geom = transform(transformer.transform, geom)
            geojson_quadrant = mapping(geom)  # mapping convierte la geometría de Shapely de nuevo a GeoJSON
            geojson_quadrants.append(geojson_quadrant)
            
        return geojson_quadrants

def input_coordinates():
    """Solicita al usuario ingresar una coordenada geográfica (latitud y longitud)."""
    lat = float(input("Ingrese la latitud: "))
    lon = float(input("Ingrese la longitud: "))
    return latlon_to_geojson(lat, lon)

def create_default_dir():
    """Crea el directorio de salida por defecto si no existe."""
    if not os.path.exists("./output"):
        os.makedirs("./output")

def search_and_download_images(output_dir, geojson_quadrants, visibility=90.0, cloud_cover=10.0, start_year=2020, end_year=2023, seasons=False):
    """Busca y descarga solo la primera imagen de cada cuadrante que cumpla con los parámetros dados."""
    total_quadrants = len(geojson_quadrants)
    print(f"Total de cuadrantes: {total_quadrants}")
    
    for idx, quadrant in enumerate(geojson_quadrants, start=1):
        print(f"Procesando cuadrante {idx}/{total_quadrants}...")
        for year in range(start_year, end_year + 1):
            if seasons:
                periods = [
                    (f"{year}-06-01T00:00:00.000Z", f"{year}-10-31T23:59:59.999Z", "lluvias"),
                    (f"{year}-01-01T00:00:00.000Z", f"{year}-05-31T23:59:59.999Z", "secas"),
                    (f"{year}-11-01T00:00:00.000Z", f"{year}-12-31T23:59:59.999Z", "secas"),
                ]
            else:
                periods = [(f"{year}-01-01T00:00:00.000Z", f"{year}-12-31T23:59:59.999Z", "completo")]
            
            for start_date, end_date, season in periods:
                geometry_filter = {
                    "type": "GeometryFilter",
                    "field_name": "geometry",
                    "config": quadrant
                }

                date_range_filter = {
                    "type": "DateRangeFilter",
                    "field_name": "acquired",
                    "config": {
                        "gte": start_date,
                        "lte": end_date
                    }
                }

                cloud_cover_filter = {
                    "type": "RangeFilter",
                    "field_name": "cloud_cover",
                    "config": {
                        "lte": cloud_cover / 100.0
                    }
                }

                visibility_filter = {
                    "type": "RangeFilter",
                    "field_name": "clear_percent",
                    "config": {
                        "gte": visibility / 100.0
                    }
                }

                combined_filter = {
                    "type": "AndFilter",
                    "config": [geometry_filter, date_range_filter, cloud_cover_filter, visibility_filter]
                }

                search_request = {
                    "item_types": ["PSScene"],
                    "filter": combined_filter
                }

                try:
                    response = requests.post(
                        'https://api.planet.com/data/v1/quick-search',
                        auth=HTTPBasicAuth(API_KEY, ''),
                        json=search_request
                    )

                    if response.status_code == 200:
                        features = response.json().get('features', [])
                        if features:
                            print(f"Se encontraron {len(features)} imágenes para el año {year}, temporada {season}. Activando y descargando la primera imagen para el cuadrante {idx}.")
                            # Descarga solo la primera imagen encontrada
                            image_id = features[0]['id']
                            if check_image_exists(output_dir, image_id, year, season):
                                print(f"La imagen {image_id} ya existe. No se descargará nuevamente.")
                            else:
                                activate_and_download_image(features[0], output_dir, year, season)
                            break  # Se descarga la primera imagen que cumple para este cuadrante y se pasa al siguiente cuadrante
                        else:
                            print(f"No se encontraron imágenes para el cuadrante {idx} y el año {year}, temporada {season}.")
                    else:
                        print(f"Error al buscar imágenes para el cuadrante {idx}: {response.status_code} - {response.text}")
                except ChunkedEncodingError as e:
                    print(f"Error de conexión durante la búsqueda o descarga: {e}. Saltando a la siguiente imagen.")

def check_image_exists(output_dir, image_id, year, season):
    """Verifica si la imagen ya existe en el directorio de salida."""
    year_season_dir = os.path.join(output_dir, str(year), season)
    image_path = os.path.join(year_season_dir, f"{image_id}.tif")
    return os.path.exists(image_path)

def activate_and_download_image(feature, output_dir, year, season):
    """Activa y descarga la imagen especificada."""
    image_id = feature['id']
    assets_url = feature['_links']['assets']

    try:
        assets_response = requests.get(assets_url, auth=HTTPBasicAuth(API_KEY, ''))
    
        if assets_response.status_code == 200:
            assets = assets_response.json()
            
            product_type = 'ortho_analytic_8b_sr'
            
            if product_type not in assets:
                print(f"El tipo de producto {product_type} no está disponible para la imagen {image_id}. Intentando con ortho_analytic_4b_sr...")
                product_type = 'ortho_analytic_4b_sr'
            
            if product_type not in assets:
                print(f"Ninguno de los productos requeridos está disponible para la imagen {image_id}.")
                return
            
            activation_url = assets[product_type]['_links']['activate']
            status = assets[product_type]['status']
            
            if status != 'active':
                print(f"Activando {product_type} para {image_id}...")
                requests.get(activation_url, auth=HTTPBasicAuth(API_KEY, ''))
                print(f"{product_type} para {image_id} activado, esperando 10 segundos...")
                time.sleep(5)
            
            download_image(assets, product_type, image_id, output_dir, year, season)
        else:
            print(f"Error al obtener assets de la imagen {image_id}: {assets_response.status_code}")
    except ChunkedEncodingError as e:
        print(f"Error de conexión durante la activación o descarga: {e}. Saltando a la siguiente imagen.")

def download_image(assets, product_type, image_id, output_dir, year, season):
    """Descarga la imagen especificada y la guarda en el directorio dado."""
    status = assets[product_type]['status']
    
    if status == 'active':
        download_url = assets[product_type]['location']
        
        year_season_dir = os.path.join(output_dir, str(year), season)
        if not os.path.exists(year_season_dir):
            os.makedirs(year_season_dir)
        
        image_path = os.path.join(year_season_dir, f"{image_id}.tif")
        
        try:
            print(f"Descargando imagen {image_id} en la carpeta {year}/{season}...")
            image_data = requests.get(download_url, stream=True)
            
            with open(image_path, 'wb') as file:
                for chunk in image_data.iter_content(chunk_size=8192):
                    file.write(chunk)
            
            print(f"Imagen {image_id} descargada y guardada en {image_path}.")
        except ChunkedEncodingError as e:
            print(f"Error de conexión durante la descarga de la imagen {image_id}: {e}. Saltando a la siguiente imagen.")
    else:
        print(f"La imagen {image_id} aún no está activa. Se omitirá la descarga.")

def main():
    """Función principal del script con un menú para elegir opciones."""
    print("Seleccione el método de búsqueda:")
    print("1. Búsqueda por coordenadas geográficas (lat, lon)")
    print("2. Búsqueda por shapefile (por cuadrantes)")
    
    option = int(input("Ingrese la opción (1 o 2): "))
    
    if option == 1:
        geojson_geometry = input_coordinates()
        geojson_quadrants = [geojson_geometry]  # Convertir a lista para tratarlo igual que los cuadrantes del shapefile
    elif option == 2:
        shapefile_path = input("Ingrese la ruta del archivo shapefile: ")
        geojson_quadrants = shapefile_to_geojson(shapefile_path)
    else:
        print("Opción no válida. Terminando.")
        return
    
    visibility = float(input("Ingrese el porcentaje de visibilidad mínima (0-100): "))
    cloud_cover = float(input("Ingrese el porcentaje máximo de cobertura de nubes (0-100): "))
    start_year = int(input("Ingrese el año de inicio: "))
    end_year = int(input("Ingrese el año de fin: "))
    
    output_dir = input("Ingrese el directorio de salida (déjelo vacío para usar './output'): ") or "./output"
    create_default_dir()

    seasons = input("¿Desea realizar la búsqueda por temporadas (lluvias/secas)? (s/n): ").lower() == 's'
    
    search_and_download_images(output_dir, geojson_quadrants, visibility, cloud_cover, start_year, end_year, seasons)

if __name__ == '__main__':
    main()
