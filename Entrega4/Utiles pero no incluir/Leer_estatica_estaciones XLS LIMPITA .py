import requests
from openpyxl import Workbook

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
ruta_archivo = r'c:\tempo\estaciones.xlsx'

# Crear un libro de trabajo y una hoja
wb = Workbook()
ws = wb.active
ws.title = "Estaciones"

# Definir los encabezados que queremos incluir
headers = ["station_id", "name", "address", "capacity"]

# Escribir los encabezados en la primera fila
ws.append(headers)

# Escribir los datos de las estaciones
for station in data["data"]["stations"]:
    row = []
    for header in headers:
        value = station.get(header, None)
        if isinstance(value, (list, dict)):
            value = str(value)
        row.append(value)
    ws.append(row)

# Guardar el archivo
wb.save(ruta_archivo)

print(f"Los datos de las estaciones han sido guardados en: {ruta_archivo}")
