import requests
import csv
import psycopg2

# Función para establecer conexión con DWHouse


def conectar_DWHouse():
    try:
        conn = psycopg2.connect(
            host='localhost',
            dbname="monitor_estaciones",
            user="rds",
            password="rds",
            port='5148'
        )
        print("Conectado a DWHouse de forma satisfactoria!")
        return conn
    except Exception as e:
        print("No fue posible conectarse a DWHouse.")
        print(e)
        return None


try:
    # Establecer conexión con DWHouse
    conexion = conectar_DWHouse()
    if not conexion:
        raise Exception(
            "No se pudo establecer la conexión con la base de datos.")

    # URL del endpoint
    url = "https://apitransporte.buenosaires.gob.ar/ecobici/gbfs/stationInformation"
    # Parámetros de la solicitud GET
    params = {
        "client_id": "9230a7da108044669e306d8b03e52dca",
        "client_secret": "b3db54bF5AbF42C68f6A1f4079fa4f84"
    }

    # Realizar la solicitud GET y obtener la respuesta JSON
    response = requests.get(url, params=params)
    data = response.json()

    # Verificar el código de respuesta
    if response.status_code != 200:
        print(f"Error al realizar la solicitud. Código de estado: {
              response.status_code}")
        exit()

    # Ruta del archivo de destino
    ruta_archivo = r'c:\tempo\estaciones.csv'

    # Definir los encabezados que queremos incluir
    headers = ["station_id", "name", "address", "capacity"]

    # Abrir el archivo en modo escritura y escribir encabezados
    with open(ruta_archivo, mode='w', newline='', encoding='utf-8') as archivo_csv:
        writer = csv.DictWriter(archivo_csv, fieldnames=headers)
        writer.writeheader()

        # Escribir datos de estaciones al archivo CSV
        for station in data["data"]["stations"]:
            writer.writerow({header: station.get(header, None)
                            for header in headers})

    print(f"Los datos de las estaciones han sido guardados en: {ruta_archivo}")

    # Obtener cursor para ejecutar consultas
    cursor = conexion.cursor()

    # Crear la tabla si no existe
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS datos_estaciones_bici (
            station_id INT,
            name VARCHAR(60),
            address VARCHAR(60),
            capacity INT
        )
    ''')

    # Leer el archivo CSV y agregar los datos a la tabla
    with open(ruta_archivo, newline='', encoding='utf-8') as archivo_csv:
        reader = csv.DictReader(archivo_csv)
        for row in reader:
            cursor.execute('''
                INSERT INTO datos_estaciones_bici (station_id, name, address, capacity)
                VALUES (%s, %s, %s, %s)
            ''', (row['station_id'], row['name'], row['address'], row['capacity']))

    # Confirmar la transacción y cerrar la conexión
    conexion.commit()
    print("Los datos han sido insertados en la base de datos.")

except psycopg2.Error as e:
    print(f"Error al conectar a PostgreSQL: {e}")

except Exception as e:
    print(f"Error general: {e}")

finally:
    if conexion:
        cursor.close()
        conexion.close()
