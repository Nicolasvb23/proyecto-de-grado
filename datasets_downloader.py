import urllib.request
import json
from types import SimpleNamespace
import requests
import os
import pandas as pd
import time
import chardet

endpoint_prefix = "https://catalogodatos.gub.uy/es/api/3/action/"

def do_get_request(url):
  with urllib.request.urlopen(url) as response:
    data = response.read().decode('utf-8')
    return json.loads(data)
  
# Obtenemos todas las paginas de resultados
def do_get_request_all_pages(url, row_size=10):
  response = do_get_request(url)
  object_response = json.loads(json.dumps(response), object_hook=lambda d: SimpleNamespace(**d))
  total_results = object_response.result.count
  total_pages = total_results // row_size + 1
  print(f"Total de resultados: {total_results}, total de paginas: {total_pages}, en la URL: {url}")
  
  results = object_response.result.results
  
  for page in range(2, total_pages+1):
    response = do_get_request(url + f"&rows={row_size}&start={row_size*(page-1)}")
    object_response = json.loads(json.dumps(response), object_hook=lambda d: SimpleNamespace(**d))
    results += object_response.result.results
    
  return results

def build_url(suffix):
  return endpoint_prefix + suffix

def pretty_print_json(loaded_json):
  print(json.dumps(loaded_json, indent=2, ensure_ascii=False))
 
def sanitize(tag):
  return tag.replace(' ', '+')

# Obtener todos los tags que contengan cierta palabra de interes
interest = 'transparencia'
identifier_run = f"{interest}_{time.strftime('%Y%m%d%H%M%S')}"

tags_endpoint_suffix = 'tag_list'
url_tags = build_url(tags_endpoint_suffix)
response_tags = do_get_request(url_tags)

all_tags = response_tags['result']

# Filtrar por tags que contengan la palabra de interes sin ser case sensitive
filtered_tags = [tag for tag in all_tags if interest.lower() in tag.lower()]

# Obtener todos los datasets que contengan los tags filtrados
object_results = []
for tag in filtered_tags:
    endpoint_suffix = f'package_search?fq=tags:{sanitize(tag)}'
    url = build_url(endpoint_suffix)
    
    object_results += do_get_request_all_pages(url)

# Niveles del Objeto serializado
#
# Result
## [Results]
### [Resources]
#### Buscar "format" CSV
#### Imprimir "url" e "id"

# Extraer los elementos que tienen el formato "CSV"
csv_resources = [
  resource for result in object_results for resource in result.resources if resource.format == 'CSV'
]

# Crear una lista de cadenas con la URL y el ID
output_list = [f"url: {resource.url}, id: {resource.id}" for resource in csv_resources]

# Filtrar las que tienen misma ID
output_list = list(set(output_list))

# Convertir la lista a una cadena
output_str = '\n'.join(output_list)

print(f"Un total de", len(output_list), "CSVs unicos fueron encontrados con la palabra", interest)
print("\n")

# Para descargar los datasets recuperados
download_folder = f"DownloadedDatasets/{interest}_{time.strftime('%Y%m%d%H%M%S')}"
os.makedirs(download_folder, exist_ok=True)

for resource in csv_resources:
   file_name = os.path.join(download_folder, f"file_{resource.id}.csv")
   file_response = requests.get(resource.url)

   if file_response.status_code == 200:
      with open(file_name, 'wb') as file:
        file.write(file_response.content)
      print(f"Downloaded {resource.url} to {file_name}")
   else:
      print(f"Failed to download {resource.url}")


# Procesamiento de CSVs para truncarlos y uniformizar su encoding
# Ruta al directorio con los CSVs
output_directory = f"DatasetsCollection/{interest}_{time.strftime('%Y%m%d%H%M%S')}"
os.makedirs(output_directory, exist_ok=True)

# Función para detectar encoding
def detect_encoding(file_path, default_encoding="utf-8"):
    try:
        # Intentamos leer los primeros bytes del archivo para detectar el encoding
        with open(file_path, 'rb') as f:
            result = chardet.detect(f.read())
        return result['encoding'] if result['encoding'] else default_encoding
    except Exception as e:
        return default_encoding

# Función para procesar un archivo CSV
def process_csv(file_path, output_path, max_rows=20):
    # Inicializamos la variable de encoding
    encoding = 'utf-8'
    # Intentamos leer el archivo con utf-8
    try:
        df = pd.read_csv(file_path, sep=None, engine='python', quotechar='"', encoding='utf-8')
    except Exception:
        # Si falla, detectamos el encoding
        encoding = detect_encoding(file_path)
        try:
            df = pd.read_csv(file_path, sep=None, engine='python', quotechar='"', encoding=encoding)
        except Exception as e:
            print(f"Error leyendo el archivo {file_path}: {e}")
            return None, None
    
    # Truncar las filas a un máximo de 20
    # Tomar 20 filas random
    df_truncated = df.sample(n=min(max_rows, len(df)), random_state=1)

    # Guardar el archivo procesado
    df_truncated.to_csv(output_path, index=False, encoding='utf-8')
    
    print(f"Archivo procesado y truncado: {output_path}")
    # Retornar las métricas del archivo original
    return len(df.columns), len(df)

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
print("Cantidad total de archivos descargados:", len([f for f in os.listdir(download_folder) if f.endswith(".csv")]))
print("Cantidad total de columnas originales:", total_columns_original)
print("Cantidad total de filas originales:", total_rows_original)
print("Cantidad total de celdas originales:", total_cells_original)

print("\n")

# Mostrar las métricas de los archivos procesados
print("Métricas de los archivos procesados:")
print("Cantidad total de archivos procesados:", len([f for f in os.listdir(output_directory) if f.endswith(".csv")]))
print("Cantidad total de columnas procesadas:", total_columns_processed)
print("Cantidad total de filas procesadas:", total_rows_processed)
print("Cantidad total de celdas procesadas:", total_cells_processed)
