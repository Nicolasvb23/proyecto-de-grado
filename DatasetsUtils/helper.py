import urllib.request
import json
from types import SimpleNamespace
import pandas as pd
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

# Deja el tag en un formato que puede ser usado en la URL
def sanitize(tag):
  return tag.replace(' ', '+')

def object_results(interest_word):
  tags_endpoint_suffix = 'tag_list'
  url_tags = build_url(tags_endpoint_suffix)
  response_tags = do_get_request(url_tags)

  all_tags = response_tags['result']

  # Filtrar por tags que contengan la palabra de interes sin ser case sensitive
  filtered_tags = [tag for tag in all_tags if interest_word.lower() in tag.lower()]

  # Obtener todos los datasets que contengan los tags filtrados
  object_results = []
  for tag in filtered_tags:
      endpoint_suffix = f'package_search?fq=tags:{sanitize(tag)}'
      url = build_url(endpoint_suffix)
      
      object_results += do_get_request_all_pages(url)
      
  return object_results

# Obtener todos los recursos
def get_all_resources():
    endpoint_suffix = 'package_search?'
    url = build_url(endpoint_suffix)
    
    object_results = do_get_request_all_pages(url)
    
    return object_results

# Funcion para acceder a atributos de un objeto o diccionario de forma segura
def safe_get(data, keys, default=None):
    """
    Safely access nested dictionary or object attributes.
    
    Parameters:
        data: dict or object
            The dictionary or object to navigate.
        keys: list of str
            Ordered keys or attribute names to access.
        default: any
            The default value to return if any key or attribute is missing.
            
    Returns:
        The value if found, otherwise the default.
    """
    current = data
    for key in keys:
        try:
            if isinstance(current, dict):
                current = current.get(key, default)
            else:
                current = getattr(current, key, default)
        except (AttributeError, TypeError):
            return default
        if current is None:
            return default
    return current

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
