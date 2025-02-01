import json
import requests
import os

from DatasetsUtils.helper import safe_get, object_results

# Este modulo descarga datasets del catalogo de datos abiertos de gub uy dado un tag de interes
# Se descarga de los packages los recursos que tengan formato CSV y JSON, siendo estos la tabla
# de datos y la metadata respectivamente. Luego se procesan los archivos CSV para truncarlos a 20 filas.
# Además se extrae información adicional, como la descripción larga de los datasets, la descripción corta
# el nombre del dataset, el nombre de la organización publicadora y la fecha de publicación.

# Variables de configuración
class FullDataDownloader:
  def __init__(self, interest_words):
    self.output_directory = f"PipelineDatasets/DownloadedDatasets"
    self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'}
    self.interest_words = interest_words
    self.metadata_keywords = ['metadata', 'metadatos', 'descripción de los datos', 'datos descriptivos', 'descripción']
    self.extension_white_list = ["csv", "json", "txt", "xml"]
    self.object_results = object_results(interest_words)
    self.resources = {}

  def potential_metadata_resource(self, resource):
    for keyword in self.metadata_keywords:
      if keyword in safe_get(resource, ['name'], '').lower():
        return True
      if keyword in safe_get(resource, ['description'], '').lower():
        return True
    return False

  def extract_resources(self):
    for result in self.object_results:
      table_resources = {}
      metadata_resources = {}
      potential_metadata_resources = {}

      for resource in safe_get(result, ['resources'], []):
        if self.potential_metadata_resource(resource) and safe_get(resource, ['format'], "Sin formato").lower() in self.extension_white_list:
          id = safe_get(resource, ['id'])
          potential_metadata_resources[id] = {
            'name': safe_get(resource, ['name']),
            'description': safe_get(resource, ['description']),
            'url': safe_get(resource, ['url']),
            'format': safe_get(resource, ['format']),
            'size': safe_get(resource, ['size']),
            'created': safe_get(resource, ['created'])
          }
        elif safe_get(resource, ['format']) == 'CSV':
          id = safe_get(resource, ['id'])
          table_resources[id] = {
            'name': safe_get(resource, ['name']),
            'description': safe_get(resource, ['description']),
            'url': safe_get(resource, ['url']),
            'format': safe_get(resource, ['format']),
            'size': safe_get(resource, ['size']),
            'created': safe_get(resource, ['created'])
          }

      self.resources[safe_get(result, ['id'])] = {
        'title': safe_get(result, ['title']),
        'notes': safe_get(result, ['notes']),
        'organization': safe_get(result, ['organization', 'title']),
        'metadata_created': safe_get(result, ['metadata_created']),
        'table_resources': table_resources,
        'metadata_resources': metadata_resources,
        'potential_metadata_resources': potential_metadata_resources
      }

  def download_resources(self):
    self.extract_resources()
    # Para descargar los datasets recuperados
    os.makedirs(self.output_directory, exist_ok=True)

    # Variables para las métricas
    total_tables_processed = 0
    total_tables_error = 0
    total_metadata_processed = 0
    total_metadata_error = 0
    # Iterar sobre el mapa de recursos y descargar los archivos
    for resource_id, resource_info in self.resources.items():
      package_folder = os.path.join(self.output_directory, resource_id)
      os.makedirs(package_folder, exist_ok=True)


      print("iterating over resource", resource_info['table_resources'])
      for table_resource_id, table_resource in resource_info['table_resources'].copy().items():
        csv_file_name = os.path.join(package_folder, f"table_{table_resource_id}.csv")

        
        # Descargar el archivo CSV si no existe en el directorio, siempre y cuando ya no haya mas de 5
        # "table_.." en el directorio
        if os.path.exists(csv_file_name):
          print(f"File {csv_file_name} already exists. Skipping download.")
          continue
        
        # Limite de tablas relacionadas a un dataset
        if len([f for f in os.listdir(package_folder) if f.startswith("table_")]) >= 5:
          print(f"Skipping download of {table_resource_id}. Enough table files in the directory.")
          resource_info['table_resources'].pop(table_resource_id)
          continue
        
        # Limites de tamaño de archivo
        if table_resource['size'] and table_resource['size'] > 420000000:
          print(f"Skipping download of {table_resource_id}. File size exceeds 420MB.")
          resource_info['table_resources'].pop(table_resource_id)
          continue

        if table_resource['url']:
          csv_url = table_resource['url']
          csv_response = requests.get(csv_url, headers=self.headers)
          total_tables_processed += 1
          if csv_response.status_code == 200:
            with open(csv_file_name, 'wb') as file:
              file.write(csv_response.content)
            print(f"Downloaded {csv_url} to {csv_file_name}")
          else:
            total_tables_error += 1
            print(f"Failed to download {csv_url}")
            print(f"Error: {csv_response.status_code}")
            print(f"Content: {csv_response.content}")
  
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
          response = requests.get(url, headers=self.headers)
          total_metadata_processed += 1
          if response.status_code == 200:
            with open(file_name, 'wb') as file:
              file.write(response.content)
            print(f"Downloaded {url} to {file_name}")
          else:
            total_metadata_error += 1
            print(f"Failed to download {url}")
            print(f"Error: {response.status_code}")
            print(f"Content: {response.content}")

      # Guardar la información adicional en un archivo JSON
      additional_info_file = os.path.join(package_folder, "additional_info.json")
      with open(additional_info_file, 'w', encoding='utf-8') as file:
        json.dump(resource_info, file, ensure_ascii=False, indent=2)
      print(f"Saved additional info to {additional_info_file}")

    print("Métricas de archivos con error al descargar:")
    print("Cantidad total de tablas procesadas:", total_tables_processed)
    print("Cantidad total de tablas descartados por error al descargar:", total_tables_error)
    print("Cantidad total de metadata procesadas:", total_metadata_processed)
    print("Cantidad total de metadata descartadas por error al descargar:", total_metadata_error)
