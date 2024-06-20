import requests
import csv

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
    # La solicitud no fue exitosa, imprimir un mensaje y cancelar el programa
    print(f"Error al realizar la solicitud. Código de estado: {
          response.status_code}")
    exit()

# Ruta del archivo de destino
ruta_archivo = r'c:\tempo\estaciones.csv'

# Definir los encabezados que queremos incluir
headers = ["station_id", "name", "address", "capacity"]

# Abrir el archivo en modo escritura
with open(ruta_archivo, mode='w', newline='', encoding='utf-8') as archivo_csv:
    writer = csv.DictWriter(archivo_csv, fieldnames=headers)

    # Escribir los encabezados
    writer.writeheader()

    # Escribir los datos de las estaciones
    for station in data["data"]["stations"]:
        # Convertir cualquier lista o diccionario a cadena
        for header in headers:
            if isinstance(station.get(header, None), (list, dict)):
                station[header] = str(station[header])
        writer.writerow({header: station.get(header, None)
                        for header in headers})

print(f"Los datos de las estaciones han sido guardados en: {ruta_archivo}")
