import urllib.request
import json
import requests
import os

endpoint_prefix = "https://catalogodatos.gub.uy/es/api/3/action/"

def do_get_request(url):
  with urllib.request.urlopen(url) as response:
    data = response.read().decode('utf-8')
    return json.loads(data)

def build_url(suffix):
  return endpoint_prefix + suffix

def pretty_print_json(loaded_json):
  print(json.dumps(do_get_request(url), indent=2, ensure_ascii=False))
  
import json
from types import SimpleNamespace

# Serializa Json a Objecto para acceder a propiedades con "."
class JsonObject:
    def __init__(self, dict):
        self.__dict__ = dict

# Podemos incluso hacer queries pidiendo solo packages que tengan ciertos tags asociados,
# en este caso buscamos los relacionados a "Organigramas"
endpoint_suffix = 'package_search?fq=tags:Organigrama'
url = build_url(endpoint_suffix)

pretty_print_json(do_get_request(url))

response = do_get_request(url)

# Serializar JSON a Objeto
obj = json.loads(json.dumps(response), object_hook=lambda d: SimpleNamespace(**d))

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
    for result in obj.result.results
    for resource in result.resources
    if resource.format == 'CSV'
]

# Crear una lista de cadenas con la URL y el ID
output_list = [f"url: {resource.url}, id: {resource.id}" for resource in csv_resources]

# Convertir la lista a una cadena
output_str = '\n'.join(output_list)

print(f"Un total de", obj.result.count, "recursos")
print(output_str)

# Para descargar los datasets recuperados
download_folder = "Downloaded/Datasets"
os.makedirs(download_folder, exist_ok=True)

for resource in csv_resources:
   file_name = os.path.join(download_folder, f"file_{resource.id}.csv")
   file_response = requests.get(resource.url)

   if file_response.status_code == 200:
      with open(file_name, 'wb') as file:
        file.write(file_response.content)
      print(f"Downloaded {url} to {file_name}")
   else:
      print(f"Failed to download {url}")
