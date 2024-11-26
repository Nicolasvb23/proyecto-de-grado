import json
import requests
import os

from DatasetsUtils.helper import safe_get, object_results, object_results

# Este modulo descarga datasets del catalogo de datos abiertos de gub uy dado un tag de interes
# Se descarga de los packages los recursos que tengan formato CSV y JSON, siendo estos la tabla
# de datos y la metadata respectivamente. Luego se procesan los archivos CSV para truncarlos a 20 filas.
# Además se extrae información adicional, como la descripción larga de los datasets, la descripción corta
# el nombre del dataset, el nombre de la organización publicadora y la fecha de publicación.

# Variables de configuración
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'}

interest_word = 'transparencia'
object_results = object_results(interest_word)

# Niveles del Objeto serializado
#
# Result
## [Results]
### Buscar "title"  (nombre del dataset)
### Buscar "notes" (descripción larga)
### Buscar "organization" -> "title" (nombre de la organización publicadora)
### Buscar "metadata_created" (fecha de publicación)
###
### [Resources]
#### Buscar "format" CSV
#### Buscar "name"
#### Buscar "description"
#### Imprimir "url" e "id"
####
#### Buscar "format" JSON
#### Buscar "name"
#### Buscar "description"
#### Imprimir "url" e "id"
####
#### Buscar y descargar todos los archivos que tengan metadata_keywords en el nombre o descripción

# Extraer los elementos que tienen el formato "CSV" y "JSON"
# Mapa con la key como id del recurso y el valor un hash con:
# - title
# - notes
# - organization
# - metadata_created
# - table_resource: {id, name, description, url}
# - metadata_resource: {id, name, description, url}

metadata_keywords = ['metadata', 'metadatos', 'descripción de los datos', 'datos descriptivos', 'descripción']
extension_white_list = ["csv", "json", "txt", "xml"]

def potential_metadata_resource(resource):
  for keyword in metadata_keywords:
    if keyword in safe_get(resource, ['name'], '').lower():
      return True
    if keyword in safe_get(resource, ['description'], '').lower():
      return True
  return False

resources = {}

for result in object_results:
  table_resources = {}
  metadata_resources = {}
  potential_metadata_resources = {}
  for resource in safe_get(result, ['resources'], []):
    if safe_get(resource, ['format']) == 'JSON':
      if potential_metadata_resource(resource):
        id = safe_get(resource, ['id'])
        metadata_resources[id] = {
                'name': safe_get(resource, ['name']),
                'description': safe_get(resource, ['description']),
                'url': safe_get(resource, ['url']),
                'size': safe_get(resource, ['size'])
              }
    elif potential_metadata_resource(resource) and safe_get(resource, ['format'], "Sin formato").lower() in extension_white_list:
      id = safe_get(resource, ['id'])
      potential_metadata_resources[id] = {
                'name': safe_get(resource, ['name']),
                'description': safe_get(resource, ['description']),
                'url': safe_get(resource, ['url']),
                'format': safe_get(resource, ['format']),
                'size': safe_get(resource, ['size'])
              }
    elif safe_get(resource, ['format']) == 'CSV':
      id = safe_get(resource, ['id'])
      table_resources[id] = {
                'name': safe_get(resource, ['name']),
                'description': safe_get(resource, ['description']),
                'url': safe_get(resource, ['url']),
                'format': safe_get(resource, ['format']),
                'size': safe_get(resource, ['size'])
              }

  resources[safe_get(result, ['id'])] = {
    'title': safe_get(result, ['title']),
    'notes': safe_get(result, ['notes']),
    'organization': safe_get(result, ['organization', 'title']),
    'metadata_created': safe_get(result, ['metadata_created']),
    'table_resources': table_resources,
    'metadata_resources': metadata_resources,
    'potential_metadata_resources': potential_metadata_resources
  }

print(f"Un total de", len(resources.keys()), "recursos unicos fueron encontrados")
print("\n")

# Para descargar los datasets recuperados
download_folder = f"DownloadedDatasets/{interest_word}"
os.makedirs(download_folder, exist_ok=True)

# Iterar sobre el mapa de recursos y descargar los archivos
for resource_id, resource_info in resources.items():
  package_folder = os.path.join(download_folder, resource_id)
  os.makedirs(package_folder, exist_ok=True)
  
  print("iterating over resource", resource_info['table_resources'])
  for table_resource_id, table_resource in resource_info['table_resources'].items():
    csv_file_name = os.path.join(package_folder, f"table_{table_resource_id}.csv")
  
    # Descargar el archivo CSV si no existe en el directorio, siempre y cuando ya no haya mas de 5
    # "table_.." en el directorio
    if os.path.exists(csv_file_name):
      print(f"File {csv_file_name} already exists. Skipping download.")
      continue
    
    # Limite de tablas relacionadas a un dataset
    if len([f for f in os.listdir(package_folder) if f.startswith("table_")]) >= 5:
      print(f"Skipping download of {table_resource_id}. Enough table files in the directory.")
      continue
    
    # Limites de tamaño de archivo
    if table_resource['size'] and table_resource['size'] > 420000000:
      print(f"Skipping download of {table_resource_id}. File size exceeds 420MB.")
      continue
    
    if table_resource['url']:
      csv_url = table_resource['url']
      csv_response = requests.get(csv_url, headers=headers)
      
      if csv_response.status_code == 200:
        with open(csv_file_name, 'wb') as file:
          file.write(csv_response.content)
        print(f"Downloaded {csv_url} to {csv_file_name}")
      else:
        print(f"Failed to download {csv_url}")
        print(f"Error: {csv_response.status_code}")
        print(f"Content: {csv_response.content}")
  
  for metadata_info_id, metadata_info in resource_info['metadata_resources'].items():
    json_file_name = os.path.join(package_folder, f"metadata_{metadata_info_id}.json")
    # Descargar el archivo JSON si no existe en el directorio
    if os.path.exists(json_file_name):
      print(f"File {json_file_name} already exists. Skipping download.")
      continue
    
    # Limite de tamaño de archivo
    if metadata_info['size'] and metadata_info['size'] > 42000000:
      print(f"Skipping download of {metadata_info_id}. File size exceeds 42MB.")
      continue

    if metadata_info['url']:
      json_url = metadata_info['url']
      json_response = requests.get(json_url, headers=headers)
      
      if json_response.status_code == 200:
        with open(json_file_name, 'wb') as file:
          file.write(json_response.content)
        print(f"Downloaded {json_url} to {json_file_name}")
      else:
        print(f"Failed to download {json_url}")
        print(f"Error: {json_response.status_code}")
        print(f"Content: {json_response.content}")
        
  for potential_metadata_info_id, potential_metadata_info in resource_info['potential_metadata_resources'].items():
    file_name = os.path.join(package_folder, f"potential_metadata_{potential_metadata_info_id}.{potential_metadata_info['format'].lower()}")
    # Descargar el archivo JSON si no existe en el directorio
    if os.path.exists(file_name):
      print(f"File {file_name} already exists. Skipping download.")
      continue
    
    # Limite de tamaño de archivo
    if potential_metadata_info['size'] and potential_metadata_info['size'] > 42000000:
      print(f"Skipping download of {potential_metadata_info_id}. File size exceeds 42MB.")
      continue

    if potential_metadata_info['url']:
      url = potential_metadata_info['url']
      response = requests.get(url, headers=headers)
      
      if response.status_code == 200:
        with open(file_name, 'wb') as file:
          file.write(response.content)
        print(f"Downloaded {url} to {file_name}")
      else:
        print(f"Failed to download {url}")
        print(f"Error: {response.status_code}")
        print(f"Content: {response.content}")
    
  # Guardar la información adicional en un archivo JSON
  additional_info_file = os.path.join(package_folder, f"additional_info.json")
  with open(additional_info_file, 'w', encoding='utf-8') as file:
    json.dump(resource_info, file, ensure_ascii=False, indent=2)
  print(f"Saved additional info to {additional_info_file}")
