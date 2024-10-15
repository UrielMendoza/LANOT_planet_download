'''
Script para descargar las imágenes satelitales de Planet via su Id y una base de datos en PostgreSQL.

@autor: UrielMendoza
@date: 2023-06-01
'''

from glob import glob
import os
import json
import shutil
from matplotlib import pyplot as plt
import numpy as np
import rasterio
import requests
from requests.auth import HTTPBasicAuth
import psycopg2
import csv
import paramiko
from PIL import Image
import warnings

# Ignora los warnings de rasterio
warnings.filterwarnings("ignore", category=UserWarning, module="PIL")

# Si la variable API esta en el sistema operativo, se usa, de lo contrario se usa la API_KEY
if os.environ.get('PL_API_KEY', ''):
    API_KEY = os.environ.get('PL_API_KEY', '')
else:
    API_KEY = ''

# Si la variable API esta en el sistema operativo, se usa, de lo contrario se usa la API_KEY
if os.environ.get('PL_API_KEY', ''):
    API_KEY = os.environ.get('PL_API_KEY', '')
else:
    API_KEY = ''

def conect_db():
    '''Funcion que conecta a la base de datos'''
    print('Conectando a la base de datos')
    print('\n')
    # Crea una conexión a la base de datos
    conn = psycopg2.connect(database="", \
                            user="", \
                            password="", \
                            host="", \
                            port="")
    
    return conn

def create_db():
    '''Funcion que crea la base de datos'''
    print('Creando base de datos')
    # Crea una conexión a la base de datos
    conn = conect_db()
    cursor = conn.cursor()
    
    # Crea una tabla para almacenar los datos del CSV con el id secuencial, el id de planet, el pathrow, la fecha, la nubosidad, la visibilidad, el tipo y si ha sido descargada
    cursor.execute('''CREATE TABLE imagenes_planet
                (id SERIAL PRIMARY KEY,
                id_planet TEXT,
                linea_numero TEXT,
                pathrow TEXT,
                id_mex INTEGER,
                fecha DATE,
                nubosidad FLOAT,
                visibilidad FLOAT,
                tipo TEXT,
                temporada TEXT,
                descargada BOOLEAN);''')
    
    # Guarda los cambios en la base de datos
    conn.commit()

    # Cierra la conexión
    conn.close()

def check_db():
    '''Funcion que verifica si la base de datos existe'''
    print('Verificando si la base de datos existe')
    # Crea una conexión a la base de datos
    conn = conect_db()

    # Verifica si la tabla existe
    cursor = conn.cursor()
    cursor.execute("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name=%s)", ('imagenes_planet',))
    exists = cursor.fetchone()[0]

    # Cierra la conexión
    conn.close()

    return exists

def check_pathrow(pathrow):
    '''Funcion que verifica si el pathrow existe'''
    print('Verificando si el pathrow existe')
    # Crea una conexión a la base de datos
    conn = conect_db()

    # Verifica si la tabla existe
    cursor = conn.cursor()
    cursor.execute("SELECT EXISTS(SELECT * FROM imagenes_planet WHERE pathrow=%s)", (pathrow,))
    exists = cursor.fetchone()[0]

    # Cierra la conexión
    conn.close()

    return exists

# Funcion que verifica los pathrows unicos con otra lista de pathrows los que aun tienen al menos una imagen no descargada
def check_pathrow_not_download(pathrows):
    '''Funcion que verifica si el pathrow existe'''
    print('Verificando si el pathrow tiene imagenes no descargadas')
    # Crea una conexión a la base de datos
    conn = conect_db()

    # Verifica los pathrows que ya no tienen imagenes no descargadas
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT pathrow FROM imagenes_planet WHERE descargada = false")
    pathrows_download = cursor.fetchall()

    # Paso la lista de tuplas a una lista de strings
    pathrows_download = [pathrow[0] for pathrow in pathrows_download]
    
    # Elimina de la lista de pathrows los que ya no tienen imagenes no descargadas
    pathrows_not_download = []
    for pathrow in pathrows:
        if pathrow in pathrows_download:
            pathrows_not_download.append(pathrow)

    # Cierra la conexión
    conn.close()

    return pathrows_not_download

def update_db(csv_file):
    '''Funcion que actualiza la base de datos con los datos del CSV'''
    print('Actualizando base de datos')
    # Crea una conexión a la base de datos
    conn = conect_db()
    cursor = conn.cursor()
    
    # Lee los datos del CSV y los inserta en la tabla
    with open(csv_file, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        data = [(row['id_planet'], row['linea_numero'], row['pathrow'], row['id_mex'], row['fecha'], row['nubosidad'], row['visibilidad'], row['tipo'], row['temporada'], row['descargada'].lower() == 'true') for row in reader]

        # Verifica si los datos ya existen en la base de datos
        for row in data:
            cursor.execute("SELECT * FROM imagenes_planet WHERE id_planet = %s", (row[0],))
            if cursor.fetchone() is None:
                # Si no existen, los inserta
                cursor.execute("INSERT INTO imagenes_planet (id_planet, linea_numero, pathrow, id_mex, fecha, nubosidad, visibilidad, tipo, temporada, descargada) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", row)

    # Guarda los cambios en la base de datos
    conn.commit()

    # Cierra la conexión
    conn.close()


def select_db(query,value):
    '''Funcion que selecciona los datos de la base de datos'''
    print('Consulatando ids de acuerdo a la variable de consulta')
    # Crea una conexión a la base de datos
    conn = conect_db()
    # Selecciona los ids de las imágenes
    cursor = conn.cursor()
    # Si es una consulta de tipo fecha, se usa el operador >=
    if query == 'fecha':
        cursor.execute('SELECT * FROM imagenes_planet WHERE '+query+' >= %s', (value,))
    else:
        cursor.execute('SELECT * FROM imagenes_planet WHERE '+query+' = %s', (value,))
    # Obtiene los ids de las imágenes
    ids_planet = []
    for row in cursor:
        ids_planet.append(row)
    # Cierra la conexión
    cursor.close()
    conn.close()
    return ids_planet

def select_db_not_download(query,values):
    '''Funcion que selecciona los datos de la base de datos que no han sido descargadas'''
    print('Consulatando ids de acuerdo a la variable de consulta')
    # Crea una conexión a la base de datos
    conn = conect_db()
    # Selecciona los ids de las imágenes que no han sido descargadas
    cursor = conn.cursor()
    # Define la lista de ids de las imágenes
    ids_planet = []
    # Intera sobre los pathrows
    for value in values:
        cursor.execute('SELECT * FROM imagenes_planet WHERE '+query+' = %s AND descargada = %s', (value, False))
        # Obtiene los ids de las imágenes    
        for row in cursor:
            ids_planet.append(row)
    # Cierra la conexión
    cursor.close()
    conn.close()
    return ids_planet

def update_db_downloaded(ids_planet):
    '''Funcion que actualiza la base de datos con los ids de las imagenes que han sido descargadas'''
    print('Actualizando base de datos')
    # Crea una conexión a la base de datos
    conn = conect_db()
    cursor = conn.cursor()
    
    # Actualiza la base de datos con los ids de las imagenes que han sido descargadas
    for row in ids_planet:
        cursor.execute("UPDATE imagenes_planet SET descargada = %s WHERE id_planet = %s", (True, row[0]))

    # Guarda los cambios en la base de datos
    conn.commit()

    # Cierra la conexión
    cursor.close()
    conn.close()

def print_data(ids_planet):
    '''Funcion que imprime los datos de las imagenes'''
    print('Imprimiendo datos')
    for row in ids_planet:
        print(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8])
    # Imprime el numero de imagenes
    print('Numero de imagenes: ', len(ids_planet))

def get_pathrow(image_id):
    '''Funcion que obtiene el pathrow de acuerdo al id de la imagen'''
    # Crea una conexión a la base de datos
    conn = conect_db()
    # Selecciona los ids de las imágenes
    cursor = conn.cursor()
    cursor.execute('SELECT pathrow FROM imagenes_planet WHERE id_planet = %s', (image_id,))
    # Obtiene el pathrow de la imagen
    pathrow = cursor.fetchone()[0]
    # Cierra la conexión
    cursor.close()
    conn.close()
    return pathrow

def obtain_url(image_id, item_type, product_type):
    '''Funcion que obtiene la URL de descarga de la imagen satelital usando el id de la imagen y el item_type
    item_type = "PSScene"
    product_type = "ortho_analytic_8b_sr"'''

    # Obtiene la URL de descarga de la imagen    
    url = "https://api.planet.com/data/v1/item-types/{}/items/{}/assets/".format(item_type, image_id)
    response = requests.get(url, auth=HTTPBasicAuth(API_KEY, ''))

    print(response.json()[product_type]['status'])

    link = response.json()[product_type]["_links"]
    self_link = link["_self"]
    activation_link = link["activate"]

    # Activar la imagen
    activate_result = \
        requests.get(
            activation_link,
            auth=HTTPBasicAuth(API_KEY, '')
        )    
    activation_status_result = \
        requests.get(
            self_link,
            auth=HTTPBasicAuth(API_KEY, '')
        )
    
    activation_status = activation_status_result.json()[u"status"]    
    print(activation_status)
    # Hasta que se active la imagen, se espera
    """ while activation_status != u"active":
        activation_status_result = \
            requests.get(
                self_link,
                auth=HTTPBasicAuth(API_KEY, '')
            )
        activation_status = activation_status_result.json()[u"status"]
        print(activation_status) """

    # Activar todas las imagenes
    activation_status_result = \
    requests.get(
        self_link,
        auth=HTTPBasicAuth(API_KEY, '')
    )
    activation_status = activation_status_result.json()[u"status"]

    # Si esta activa, se obtiene la URL de descarga
    if activation_status == u"active":        
        print(activation_status)
        download_link = activation_status_result.json()["location"]
        print(download_link)
        return download_link
    else:
        print('La imagen {} se esta activando'.format(image_id))
        return None

def move_image_server(files, pathrow):
    # Ruta donde se guardan las imagenes
    path = './planet_images/'
    pathTmp = './tmp/'
    # Crea una instancia del cliente SSH
    ssh = paramiko.SSHClient()
    # Configura el cliente SSH para que acepte la clave del host automáticamente
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    # Conéctate al servidor remoto
    ssh.connect('', username='', password='')
    # Crea una instancia del objeto SFTP
    sftp = ssh.open_sftp()

    # Si no existe el directorio de planet_image mas el pathrow, se crea
    try:
        sftp.chdir(path + pathrow)
    except IOError:
        sftp.mkdir(path + pathrow)
        sftp.chdir(path + pathrow)
    #try:
        #sftp_destino.chdir(path + pathrow)
        #sftp.chdir(path + pathrow)
    #except IOError:
        #sftp_destino.mkdir(path + pathrow)
        #sftp_destino.chdir(path + pathrow)
        #sftp.mkdir(path + pathrow)
        #sftp.chdir(path + pathrow)
    # Transfiere los archivos al servidor remoto
    for file in files:
        #sftp.put(file, file)
    # Mueve las imagenes al almacenamiento de imagenes de planet de mas capacidad
        #sftp.get(file, sftp_destino.put)
        sftp.put(file, file.split('\\')[-1])
        # Elimina las imagenes del servidor local
        os.remove(file)
    # Cierra la conexión SFTP y SSH
    sftp.close()
    #sftp_destino.close()
    ssh.close()
    #ssh_destino.close()

def extract_rgb(pathImg):
    '''Función que extrae las bandas 6, 4 y 2 de una imagen satelital y las guarda en una lista'''
    # Crear nueva lista para rgb -> numpy
    lista_bandas = []
    # Se abre la imagen, se leen y guardan las bandas en lista, el crs y la transformada
    with rasterio.open(pathImg + '.tif') as src:
        lista_bandas = [src.read(band) for band in [6, 4, 2]]
        cord_system = src.crs
        transformada = src.transform

    # La función devuelve la lista con los numpy, la crs y transformada
    return lista_bandas, cord_system, transformada

def create_png(filename):
    '''Función que crea un archivo png georreferenciado a partir de un archivo tif'''
    lista_bandas, cord_system, transformada = extract_rgb(filename)
    # Obtener los valores mínimos y máximos de todas las bandas
    min_value = min([band.min() for band in lista_bandas])
    max_value = max([band.max() for band in lista_bandas])   
    # Reescalar cada banda de 0 a 255
    rescaled_bandas = [(band - min_value) * (255 / (max_value - min_value)) for band in lista_bandas]
    # Convertir las bandas a tipo de datos uint8
    rescaled_bandas = [band.astype('uint8') for band in rescaled_bandas]
    # Combinar las bandas en una imagen RGB
    rgb_image = np.dstack(rescaled_bandas)
    # Guardar la imagen PNG 
    output_file = filename + '.png'
    plt.imsave(output_file, rgb_image, origin='upper', vmin=1)
    # Abre el archivo PNG y baja su resolución al 25% pero mantiene el tamaño
    img = Image.open(output_file)
    # Agrega lo trasparencia al PNG
    # Obtener la matriz de píxeles de la imagen
    pixels = img.load()
    # Iterar sobre los píxeles de la imagen
    for y in range(img.height):
        for x in range(img.width):
            # Verificar si el valor del píxel es 0, 0, 0, 255
            if pixels[x, y] == (0, 0, 0, 255):
                # Establecer el valor del píxel como transparente (0, 0, 0, 0)
                pixels[x, y] = (0, 0, 0, 0)
    # Guarda la imagen con la transparencia
    img.save(output_file, format="PNG", compress_level=5)
    # Abrir el archivo PNG y agregar los metadatos
    with rasterio.open(output_file, 'r+') as dst:
        dst.crs = cord_system
        dst.transform = transformada

def download_image(descarga, pathrow, image_id, mex_id, item_type = 'PSScene', product_type = 'ortho_analytic_8b_sr'):
    # Funcion que descarga la imagen satelital
    # item_type = "PSScene"
    # product_type = "ortho_analytic_8b_sr"

    # Imprime el id de la imagen que se esta descargando
    print('Descargando imagen {}'.format(image_id))

    # Obtiene la URL de descarga
    #try:
    download_link = obtain_url(image_id, item_type, product_type)
    #except KeyError as e:
    #    print('No se pudo obtener la URL de descarga de la imagen {}'.format(image_id))
    #    return

    # Si no se obtuvo la URL de descarga, se sale de la funcion
    if download_link is None:
        return
    
    else:
        # Descarga la imagen
        r = requests.get(download_link, auth=HTTPBasicAuth(API_KEY, ''))

        # Guarda la imagen en el disco local
        pathTmp = './tmp/'
        name = "{}_{}".format(image_id, mex_id)
        # Verifica si el directorio existe, si no existe lo crea
        if not os.path.exists(pathTmp):
            os.makedirs(pathTmp)
        # Guarda la imagen en el directorio
        with open(pathTmp + name + '.tif', 'wb') as f:
            f.write(r.content)
            # Verifica si la imagen se descargo correctamente
            if os.path.exists(pathTmp + name + '.tif'):
                print('Imagen {} descargada correctamente'.format(image_id))
                # Actualiza la base de datos con el id de la imagen que ha sido descargada
                update_db_downloaded([(image_id,)])

                # Comprime la imagen descargada en un archivo .tar
                #tar_name = "{}.tar".format(image_id)
                #with tarfile.open(tar_name, "w") as tar:
                #    tar.add(name)
                
                # Borra la imagen .tif
                #os.remove(name)
                #print('Imagen {} comprimida y eliminada'.format(image_id))
            else:
                print('Error al descargar la imagen {}'.format(image_id))
                return
        
        # Obtiene el pathrow de la imagen con el id
        pathrow = get_pathrow(image_id)

        print('Pathrow: {}'.format(pathrow))

        # Crea un archivo png georreferenciado a partir de la imagen tif
        create_png(pathTmp + name)
        
        # Enlista los archivos .tif, .png y .xml
        files = glob(pathTmp + name + '*')
        
        # Si la descarga es en local la deja en la carpeta planet_images
        if descarga == 'local':
            # Si no existe la carpeta planet_images mas el pathrow, la crea
            if not os.path.exists('planet_images/{}'.format(pathrow)):
                os.makedirs('planet_images/{}'.format(pathrow))
            # Mueve la imagen de la carpeta actual a la carpeta planet_images mas el pathrow
            for file in files:
                shutil.move(file, 'planet_images/{}/'.format(pathrow))
        # Si la descarga es en servidor la mueve de la carpeta planet_images al servidor
        elif descarga == 'servidor':
            move_image_server(files, pathrow)

def menu():
    '''Funcion que muestra el menu de opciones'''
    print('1. Descargar imagenes')
    print('2. Actualizar base de datos')
    print('3. Consultar base de datos')
    print('4. Salir')
    # Solicita la opcion al usuario
    opcion = input('Ingrese la opcion: ')
    print('\n')


    # OPTION 1: Descarga de imagenes
    if opcion == '1':
        # Descarga las imagenes
        # Solicita la opcion si se quiere decaragr por id o por usuario
        print('1. Descargar por pathrow')
        print('2. Descargar por usuario')
        opcion = input('Ingrese la opcion: ')
        print('\n')

        # Option 1: Descarga por pathrow
        if opcion == '1':
            # Selecciona los ids de las imagenes deacuerdo al pathrow
            pathrow = input('Ingrese el pathrow a descargar: ')
            if check_pathrow(pathrow) == False:
                print('El pathrow {} no existe'.format(pathrow))
                return
            ids_planet = select_db_not_download('pathrow', pathrow)
            # Imprime el numero de imagenes a descargar y el pathrow
            print('Estan disponibles para descarga {} imagenes del pathrow {}'.format(len(ids_planet), pathrow))
            print('\n')
            # Menu de ruta de descarga
            print('1. Descargar en local')
            print('2. Descargar en servidor')
            opcion = input('Ingrese la opcion: ')
            if opcion == '1':
                # Descarga en local
                for row in ids_planet:
                    image_id = row[1]
                    mex_id = row[4]
                    download_image('local', pathrow, image_id, mex_id)
            elif opcion == '2':
                # Descarga en servidor
                for row in ids_planet:
                    image_id = row[1]
                    mex_id = row[4]
                    download_image('servidor', pathrow, image_id, mex_id)

        # Option 2: Descarga por usuario
        elif opcion == '2':
            # Seleciona los pathrows de las imagenes deacuerdo al usuario
            print('1.Akemi')
            print('2.Fernando')
            print('3.Paty')
            print('4.Uriel')
            opcion = input('Ingrese la opcion: ')
            if opcion == '1':
                user = 'Akemi'
                pathrow = ['B27', 'B28', 'B29', 'C313', 'C315', 'C316', 'E526']
                #pathrow = ['F635', 'H846']
                pathrow = check_pathrow_not_download(pathrow)
            elif opcion == '2':
                user = 'Fernando'
                pathrow = ['A11', 'A12', 'D419', 'D420', 'D421', 'D422', 'D423']
                #pathrow = ['I952', 'E528']
                pathrow = check_pathrow_not_download(pathrow)
            elif opcion == '3':
                user = 'Paty'
                pathrow = ['C314', 'E527', 'E528', 'E529', 'F632', 'F633', 'F634']
                #pathrow = ['E529', 'E527']
                pathrow = check_pathrow_not_download(pathrow)
            elif opcion == '4':
                user = 'Uriel'
                pathrow = ['F635', 'F636', 'G741', 'G742', 'H846', 'H847', 'H848', 'I952', 'I953']
                #pathrow = ['F634', 'D419', 'A11']
                pathrow = check_pathrow_not_download(pathrow)     
            # Obtiene los ids de las imagenes deacuerdo al pathrow y no descargadas      
            ids_planet = select_db_not_download('pathrow', pathrow)
            # Imprime el numero de imagenes a descargar por usuario y el pathrow
            print('Estan disponibles para descargar {} imagenes del usuario {}'.format(len(ids_planet), user))
            print('Pathrows del usuario {} por completar: {}'.format(user, pathrow))
            print('\n')
            # Menu de ruta de descarga
            print('1. Descargar en local')
            print('2. Descargar en servidor')
            opcion = input('Ingrese la opcion: ')
            if opcion == '1':
                # Descarga en local
                for row in ids_planet:
                    image_id = row[1]
                    mex_id = row[4]
                    try:
                        download_image('local', pathrow, image_id, mex_id)
                    except rasterio.errors.RasterioIOError as rioe:
                        print('Error: {}'.format(rioe))
                        print('No se pudo descargar la imagen {} del pathrow {}'.format(image_id, pathrow))
                        os.remove('./tmp/*')
                        continue
                         
            elif opcion == '2':
                # Descarga en servidor
                for row in ids_planet:
                    image_id = row[1]
                    mex_id = row[4]
                    try:
                        download_image('servidor', pathrow, image_id, mex_id)
                    except rasterio.errors.RasterioIOError as rioe:
                        print('Error: {}'.format(rioe))
                        print('No se pudo descargar la imagen {} del pathrow {}'.format(image_id, pathrow))
                        os.remove('./tmp/*')
                        continue

    # OPTION 2: Actualizar base de datos
    elif opcion == '2':    
        # Actualiza la base de datos
        pathCSV = input('Ingrese la ruta del archivo CSV: ')
        update_db(pathCSV)
        print('\n')


    # OPTION 3: Consultar base de datos
    elif opcion == '3':
        # Consulta la base de datos
        # Menu de opciones de consulta
        print('1. Consulta por pathrow')
        print('2. Consulta por fecha')
        print('3. Consulta por estado de descarga')
        opcion = input('Ingrese la opcion de consulta: ')
        print('\n')
        # Option 1: Consulta por pathrow
        if opcion == '1':
            # Consulta por pathrow
            pathrow = input('Ingrese el pathrow: ')
            # verifica si el pathrow existe
            if check_pathrow(pathrow) == False:
                print('El pathrow {} no existe'.format(pathrow))
                return
            ids_planet = select_db('pathrow', pathrow)
            print_data(ids_planet)
        # Option 2: Consulta por fecha
        elif opcion == '2':
            # Consulta por fecha
            fecha = input('Ingrese la fecha: ')
            ids_planet = select_db('fecha', fecha)
            print_data(ids_planet)
        # Option 3: Consulta por descargada
        elif opcion == '3':
            # Consulta por descargada
            descargada = input('Ingrese la opcion de descargada: ')
            ids_planet = select_db('descargada', descargada)
            print_data(ids_planet)
        else:
            print('Opcion incorrecta')


    # OPTION 4: Salir
    elif opcion == '4':
        # Salir
        print('Saliendo...')
        exit()

if __name__ == '__main__':
    # Funcion principal de descarga de imagenes satelitales de Planet

    # Comprueba si la base de datos existe
    if check_db() == False:
        # Crea la base de datos
        create_db()
    # Muestra el menu de opciones
    menu()

