import requests
import os
import time

from DatasetsUtils.helper import object_results, process_csv

# Este modulo descarga datasets del catalogo de datos abiertos de gub uy dado un tag de interes
# Los datasets son descargados en formato CSV y luego procesados para truncarlos a 20 filas.

# Obtener todos los tags que contengan cierta palabra de interes
interest = "transparencia"

# Variables de configuración
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
}
identifier_run = f"{interest}_{time.strftime('%Y%m%d%H%M%S')}"

object_results = object_results(interest)

# Niveles del Objeto serializado
#
# Result
## [Results]
### [Resources]
#### Buscar "format" CSV
#### Imprimir "url" e "id"

# Extraer los elementos que tienen el formato "CSV"
csv_resources = [
    resource
    for result in object_results
    for resource in result.resources
    if resource.format == "CSV"
]

# Crear una lista de cadenas con la URL y el ID
output_list = [f"url: {resource.url}, id: {resource.id}" for resource in csv_resources]

# Filtrar las que tienen misma ID
output_list = list(set(output_list))

# Convertir la lista a una cadena
output_str = "\n".join(output_list)

print(
    f"Un total de",
    len(output_list),
    "CSVs unicos fueron encontrados con la palabra",
    interest,
)
print("\n")

# Para descargar los datasets recuperados
download_folder = (
    f"PipelineDatasets/DownloadedDatasets/{interest}_{time.strftime('%Y%m%d%H%M%S')}"
)
os.makedirs(download_folder, exist_ok=True)

for resource in csv_resources:
    file_name = os.path.join(download_folder, f"file_{resource.id}.csv")
    file_response = requests.get(resource.url)

    if file_response.status_code == 200:
        with open(file_name, "wb") as file:
            file.write(file_response.content)
        print(f"Downloaded {resource.url} to {file_name}")
    else:
        print(f"Failed to download {resource.url}")


# Procesamiento de CSVs para truncarlos y uniformizar su encoding
# Ruta al directorio con los CSVs
output_directory = (
    f"PipelineDatasets/DatasetsCollection/{interest}_{time.strftime('%Y%m%d%H%M%S')}"
)
os.makedirs(output_directory, exist_ok=True)

# Variables para las métricas
total_columns_original = 0
total_rows_original = 0
total_cells_original = 0

total_columns_processed = 0
total_rows_processed = 0
total_cells_processed = 0

# Procesamiento de los archivos en el directorio de descargas
print("Procesando archivos...")

# Iteramos sobre los archivos en el directorio de descarga
for filename in os.listdir(download_folder):
    if filename.endswith(".csv"):
        file_path = os.path.join(download_folder, filename)
        output_path = os.path.join(output_directory, filename)

        # Procesar el archivo CSV y obtener métricas del archivo original
        columns_original, rows_original = process_csv(file_path, output_path)

        # Si el archivo fue procesado exitosamente
        if columns_original is not None and rows_original is not None:
            total_columns_original += columns_original
            total_rows_original += rows_original
            total_cells_original += columns_original * rows_original

            # Ahora leemos el archivo procesado para obtener las métricas truncadas
            columns_processed = columns_original
            rows_processed = rows_original if rows_original <= 20 else 20

            total_columns_processed += columns_processed
            total_rows_processed += rows_processed
            total_cells_processed += columns_processed * rows_processed

print("Procesamiento completo.\n")

# Mostrar las métricas de los archivos originales
print("Métricas de los archivos descargados:")
print(
    "Cantidad total de archivos descargados:",
    len([f for f in os.listdir(download_folder) if f.endswith(".csv")]),
)
print("Cantidad total de columnas originales:", total_columns_original)
print("Cantidad total de filas originales:", total_rows_original)
print("Cantidad total de celdas originales:", total_cells_original)

print("\n")

# Mostrar las métricas de los archivos procesados
print("Métricas de los archivos procesados:")
print(
    "Cantidad total de archivos procesados:",
    len([f for f in os.listdir(output_directory) if f.endswith(".csv")]),
)
print("Cantidad total de columnas procesadas:", total_columns_processed)
print("Cantidad total de filas procesadas:", total_rows_processed)
print("Cantidad total de celdas procesadas:", total_cells_processed)
