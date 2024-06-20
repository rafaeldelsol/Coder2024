import psycopg2
import requests
import os
import json
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine
import smtplib
from email.message import EmailMessage

default_args = {
    'owner': 'RdS',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('Proyecto_Final_Coder04_Estaciones_Bici', default_args=default_args,
          schedule_interval='0 */4 * * *')

CLIENT_ID_API = os.environ.get('CLIENT_ID_API')
CLIENT_SECRET = os.environ.get('CLIENT_SECRET')
USER = os.environ.get('USER')
PASS = os.environ.get('PASS')
MAIL = os.environ.get('MAIL')
PASSMAIL = os.environ.get('PASSMAIL')

with open('dags/param.json', 'r') as datos:
    param = json.load(datos)

    max_bicis_disable = param["Max_Bicis_Disable"]
    destinatarios = param["Destinatarios"]


def extract_data():
    url = "https://apitransporte.buenosaires.gob.ar/ecobici/gbfs/stationStatus"
    params = {
        "client_id": CLIENT_ID_API,
        "client_secret": CLIENT_SECRET
    }
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  # Verificar si hay errores de solicitud
        data = response.json()
        with open('/opt/airflow/raw_data/eco_bici_data_extracted.json', 'w') as f:
            json.dump(data, f)
    except requests.exceptions.RequestException as e:
        raise ValueError("Error al extraer datos: " + str(e))


def transform_data():
    with open('/opt/airflow/raw_data/eco_bici_data_extracted.json', 'r') as f:
        data = json.load(f)
    fecha_actual = datetime.now().strftime("%Y-%m-%d")
    hora_actual = datetime.now().strftime("%H:%M:%S")
    registros = []
    for station in data["data"]["stations"]:
        station["fecha"] = fecha_actual
        station["hora"] = hora_actual
        # Validar el campo last_reported
        station["last_reported"] = station.get(
            "last_reported", 0) if station.get("last_reported", 0) > 0 else 0
        station["IN_SERVICE"] = True if station["status"] == "IN_SERVICE" else False
        registros.append(station)
    with open('/opt/airflow/processed_data/eco_bici_data_transformed.json', 'w') as f:
        json.dump(registros, f)


def conectar_DWHouse():
    try:
        conn = psycopg2.connect(
            host='host.docker.internal',
            dbname="monitor_estaciones",
            user=USER,
            password=PASS,
            port='5148'
        )
        return conn
    except psycopg2.Error as e:
        raise ValueError("No fue posible conectarse a DWHouse: " + str(e))


def load_data():
    with open('/opt/airflow/processed_data/eco_bici_data_transformed.json', 'r') as f:
        registros = json.load(f)

    conn = conectar_DWHouse()
    if conn is not None:
        try:
            cur = conn.cursor()
            for row in registros:
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
            conn.commit()
            print("Registros insertados en DWHouse exitosamente!")
        except psycopg2.Error as e:
            raise ValueError(
                "Error al insertar registros en DWHouse: " + str(e))
        finally:
            cur.close()
            conn.close()
    else:
        print("No se pudo conectar a DWHouse, no se realizaron inserciones.")


def Cargar_Alertas():
    with open('/opt/airflow/processed_data/eco_bici_data_transformed.json', 'r') as f:
        registros = json.load(f)

    # Convertir los datos en un DataFrame
    monitor_estaciones_bici = pd.DataFrame(registros)

    # Datos para la conexión a DWHouse
    db_user = USER
    db_password = PASS
    db_host = 'host.docker.internal'
    db_port = '5148'
    db_name = 'monitor_estaciones'

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
        (combined_df['num_bikes_disabled'] > max_bicis_disable / 100 * combined_df['capacity'])]

    # Agregar la columna 'Alerta_Enviada' con el valor False
    # filtered_df['alerta_enviada'] = False

    # Seleccionar solo las columnas necesarias para la tabla de alertas
    alertas_df = filtered_df[['station_id', 'name', 'address', 'capacity', 'fecha',
                              'hora', 'num_bikes_disabled', 'num_docks_available']]

    # Insertar los registros que cumplen las condiciones en la tabla de alertas
    alertas_df.to_sql('alertas', engine, if_exists='append', index=False)
    print("Registros ALERTAS insertados en DWHouse exitosamente!")


def Enviar_Alerta():
    def enviar_correo(subject, body, to):
        try:
            msg = EmailMessage()
            msg.set_content(body, subtype='html')
            msg['Subject'] = subject
            msg['From'] = MAIL
            msg['To'] = to

            with smtplib.SMTP('vps-3022426-x.dattaweb.com') as server:
                server.login(MAIL, PASSMAIL)
                server.send_message(msg)
            print(f"Correo enviado a {to}")
        except Exception as e:
            print(f"Error al enviar correo: {e}")

    try:
        conn = psycopg2.connect(
            host='host.docker.internal',
            dbname='monitor_estaciones',
            user=USER,
            password=PASS,
            port='5148'
        )
        cursor = conn.cursor()
        print("Conectado a la base de datos")

        cursor.execute("""
            SELECT station_id, name, address, capacity, fecha, hora, num_bikes_disabled, num_docks_available
            FROM alertas
            WHERE Alerta_Enviada IS NOT TRUE
        """)

        alertas = cursor.fetchall()
        alertas_df = pd.DataFrame(alertas, columns=[
            'station_id', 'name', 'address', 'capacity', 'fecha', 'hora', 'num_bikes_disabled', 'num_docks_available'])

        if alertas_df.empty:
            print("No hay alerta para enviar.")
        else:
            print(f"Se encontraron {len(alertas_df)} estaciones en alerta.")

            subject = "Resumen de Alertas de Estaciones"
            body = "<p>Estimados, hemos detectado los siguientes desvíos en el estado de las Estaciones:</p>"
            body += alertas_df.to_html(index=False)

            enviar_correo(subject, body, destinatarios)
            print("Alerta enviada.")

        cursor.execute("""
            UPDATE alertas
            SET Alerta_Enviada = TRUE
            WHERE Alerta_Enviada IS NOT TRUE
        """)

        conn.commit()
        cursor.close()
        conn.close()
        print("Actualización realizada. Conexión a la base de datos cerrada")

    except Exception as e:
        print(f"Error al conectar a la base de datos: {e}")


Bajar_Datos_Estaciones = PythonOperator(
    task_id='Bajar_Datos_Estaciones',
    python_callable=extract_data,
    dag=dag,
)

Procesar_los_Datos = PythonOperator(
    task_id='Procesar_los_Datos',
    python_callable=transform_data,
    dag=dag,
)

Conecta_DWHouse_y_Carga = PythonOperator(
    task_id='Conecta_DWHouse_y_Carga',
    python_callable=load_data,
    dag=dag,
)

Cargar_Alertas = PythonOperator(
    task_id='Cargar_Alertas',
    python_callable=Cargar_Alertas,
    dag=dag,
)

Enviar_Alerta = PythonOperator(
    task_id='Enviar_Alerta',
    python_callable=Enviar_Alerta,
    dag=dag,
)

Bajar_Datos_Estaciones >> Procesar_los_Datos >> Conecta_DWHouse_y_Carga >> Cargar_Alertas >> Enviar_Alerta
