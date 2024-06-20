import psycopg2
import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine

# Datos para la conexión a DWHouse
db_user = 'rds'
db_password = 'rds'
db_host = 'localhost'
db_port = '5148'
db_name = 'monitor_estaciones'

# Función para establecer conexión con DWHouse


def conectar_DWHouse():
    try:
        conn = psycopg2.connect(
            host=db_host,
            dbname=db_name,
            user=db_user,
            password=db_password,
            port=db_port
        )
        print("Conectado a DWHouse de forma satisfactoria!")
        return conn
    except Exception as e:
        print("No fue posible conectarse a DWHouse.")
        print(e)
        return None


# URL del endpoint de la API
url = "https://apitransporte.buenosaires.gob.ar/ecobici/gbfs/stationStatus"

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

# Obtener la fecha y hora actual
fecha_hora_actual = datetime.now()

# Separar fecha y hora en campos diferentes
fecha_actual = fecha_hora_actual.strftime("%Y-%m-%d")
hora_actual = fecha_hora_actual.strftime("%H:%M:%S")

# Crear una lista para almacenar los registros
registros = []

# Agregar fecha y hora a cada registro del diccionario y guardarlos en la lista
for station in data["data"]["stations"]:
    station["fecha"] = fecha_actual
    station["hora"] = hora_actual
    # Validar el campo last_reported
    station["last_reported"] = station.get(
        "last_reported", 0) if station.get("last_reported", 0) > 0 else 0
    station["IN_SERVICE"] = True if station["status"] == "IN_SERVICE" else False
    registros.append(station)

# Crear un DataFrame de Pandas con los registros
df = pd.DataFrame(registros)
monitor_estaciones_bici = df

# Conectar a DWHouse
conn = conectar_DWHouse()
if conn is not None:
    try:
        # Crear cursor
        cur = conn.cursor()

        # Iterar sobre cada fila del DataFrame y insertar en la tabla de DWHouse
        for index, row in df.iterrows():
            cur.execute("""
                INSERT INTO monitor_estaciones_bici (
                    station_id, 
                    fecha, 
                    hora, 
                    last_reported, 
                    IN_SERVICE, 
                    num_bikes_available, 
                    num_bikes_disabled, 
                    num_docks_available, 
                    num_docks_disabled
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                """,
                        (
                            row["station_id"],
                            row["fecha"],
                            row["hora"],
                            row["last_reported"],
                            row["IN_SERVICE"],
                            row["num_bikes_available"],
                            row["num_bikes_disabled"],
                            row["num_docks_available"],
                            row["num_docks_disabled"]
                        )
                        )

        # Commit de los cambios
        conn.commit()
        print("Registros insertados en DWHouse exitosamente!")

    except Exception as e:
        print("Error al insertar registros en DWHouse.")
        print(e)

    finally:
        # Cerrar cursor y conexión
        cur.close()
        conn.close()
else:
    print("No se pudo conectar a DWHouse, no se realizaron inserciones.")

# Crear la cadena de conexión SQLAlchemy
engine = create_engine(
    f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

# Leer los datos de la tabla de estaciones
datos_estaciones_bici = pd.read_sql(
    'SELECT * FROM datos_estaciones_bici', engine)

# Asegurar que las columnas station_id sean del mismo tipo
monitor_estaciones_bici['station_id'] = monitor_estaciones_bici['station_id'].astype(
    int)
datos_estaciones_bici['station_id'] = datos_estaciones_bici['station_id'].astype(
    int)

# Realizar un merge entre las dos tablas para combinar la información estática con la dinámica
combined_df = pd.merge(monitor_estaciones_bici,
                       datos_estaciones_bici, on='station_id')

# Filtrar las filas que cumplen con las condiciones especificadas
filtered_df = combined_df[
    (combined_df['num_bikes_disabled'] > 0.6 * combined_df['capacity'])
]

# Agregar la columna 'Alerta_Enviada' con el valor False
# filtered_df['alerta_enviada'] = False

# Seleccionar solo las columnas necesarias para la tabla de alertas
alertas_df = filtered_df[['station_id', 'name', 'address', 'capacity', 'fecha',
                          'hora', 'num_bikes_disabled', 'num_docks_available']]

# Insertar los registros que cumplen las condiciones en la tabla de alertas
alertas_df.to_sql('alertas', engine, if_exists='append', index=False)
print("Registros ALERTAS insertados en DWHouse exitosamente!")
