import smtplib
from email.message import EmailMessage
import psycopg2
import pandas as pd

# Función para enviar correos electrónicos


def enviar_correo(subject, body, to):
    try:
        msg = EmailMessage()
        # Definir el cuerpo del correo como HTML
        msg.set_content(body, subtype='html')
        msg['Subject'] = subject  # Asunto del correo
        # Dirección de correo del remitente
        msg['From'] = 'coderbot@gemplast.com.ar'
        msg['To'] = to  # Dirección de correo del destinatario

        # Conectar al servidor SMTP y enviar el correo
        with smtplib.SMTP('vps-3022426-x.dattaweb.com') as server:
            server.login('coderbot@gemplast.com.ar', 'Coder*2024')
            server.send_message(msg)
        print(f"Correo enviado a {to}")
    except Exception as e:
        print(f"Error al enviar correo: {e}")


try:
    # Conexión a la base de datos PostgreSQL
    conn = psycopg2.connect(
        host='localhost',
        dbname="monitor_estaciones",
        user="rds",
        password="rds",
        port='5148'
    )
    cursor = conn.cursor()
    print("Conectado a la base de datos")

    # Consulta para obtener alertas no enviadas
    cursor.execute("""
        SELECT station_id, name, address, capacity, fecha, hora, num_bikes_disabled, num_docks_available 
        FROM alertas 
        WHERE Alerta_Enviada IS NOT TRUE
    """)

    # Obtener los resultados y convertirlos a un DataFrame de pandas
    alertas = cursor.fetchall()
    alertas_df = pd.DataFrame(alertas, columns=[
                              'station_id', 'name', 'address', 'capacity', 'fecha', 'hora', 'num_bikes_disabled', 'num_docks_available'])

    if alertas_df.empty:
        print("No hay alertas no enviadas")
    else:
        print(f"Se encontraron {len(alertas_df)} alertas no enviadas")

        # Si hay alertas no enviadas, enviar un correo con todas las alertas en una tabla
        subject = "Resumen de Alertas de Estaciones"
        # Convertir el DataFrame a una tabla HTML
        body = "<p>Estimados, hemos detectado los siguientes desvíos en el estado de las Estacionesas:</p>"
        body += alertas_df.to_html(index=False)

        enviar_correo(subject, body, 'rafael_del_sol@hotmail.com')

        # Actualizar las alertas como enviadas
        cursor.execute("""
            UPDATE alertas
            SET Alerta_Enviada = TRUE
            WHERE Alerta_Enviada IS NOT TRUE
        """)
        print("Alertas actualizadas a enviadas")

    # Confirmar las transacciones y cerrar la conexión
    conn.commit()
    cursor.close()
    conn.close()
    print("Conexión a la base de datos cerrada")
except Exception as e:
    print(f"Error al conectar a la base de datos: {e}")
