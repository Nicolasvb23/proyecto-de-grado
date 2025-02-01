import urllib.request
import urllib.parse
import urllib.request
import json
from types import SimpleNamespace
import pandas as pd
import chardet
import os
import csv
import os
import csv

endpoint_prefix = "https://catalogodatos.gub.uy/es/api/3/action/"

def do_get_request(url):
    # Codifica la URL para manejar caracteres especiales correctamente
    encoded_url = urllib.parse.quote(url, safe="/:?&=+")  # Los caracteres seguros no se codifican
    with urllib.request.urlopen(encoded_url) as response:
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

def object_results(interest_words):
    tags_endpoint_suffix = 'tag_list'
    url_tags = build_url(tags_endpoint_suffix)
    response_tags = do_get_request(url_tags)

    all_tags = response_tags['result']
    filtered_tags = [tag for tag in all_tags if any(equal_words(word, tag) for word in interest_words)]

    object_results = []
    for tag in filtered_tags:
        print(f"Buscando objetos con tag: {tag}")
        endpoint_suffix = f'package_search?fq=tags:"{sanitize(tag)}"'
        url = build_url(endpoint_suffix)
        object_results += do_get_request_all_pages(url)
    
    return object_results

def equal_words(word, tag):
    '''Saca los tildes de las palabras y las pasa a minuscula'''
    word = word.lower().replace('á', 'a').replace('é', 'e').replace('í', 'i').replace('ó', 'o').replace('ú', 'u')
    tag = tag.lower().replace('á', 'a').replace('é', 'e').replace('í', 'i').replace('ó', 'o').replace('ú', 'u')
    return word in tag

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
    
    # Eliminar columnas "Unnamed"
    df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
    
    # Truncar las filas a un máximo de 20
    # Tomar 20 filas random
    df_truncated = df.sample(n=min(max_rows, len(df)), random_state=1)

    # Guardar el archivo procesado
    df_truncated.to_csv(output_path, index=False, encoding='utf-8')
    
    print(f"Archivo procesado y truncado: {output_path}")
    # Retornar las métricas del archivo original
    return len(df.columns), len(df)

def write_file(output_path, content, file_format, encoding):
    """Guarda contenido en el archivo correspondiente según el formato."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    if file_format == "json":
        with open(output_path, "w", encoding=encoding) as f_out:
            json.dump(content, f_out, indent=2, ensure_ascii=False)
    elif file_format == "csv":
        with open(output_path, "w", encoding=encoding, newline="") as f_out:
            csv_writer = csv.writer(f_out)
            csv_writer.writerows(content)
    elif file_format == "txt":
        with open(output_path, "w", encoding=encoding) as f_out:
            f_out.write(content)

def read_file(file_path, file_format):
    """Lee un archivo según el formato correspondiente."""
    encoding = detect_encoding(file_path)
    with open(file_path, "r", encoding=encoding) as file:
        if file_format == "json":
            return json.load(file)
        elif file_format == "csv":
            csv_reader = csv.reader(file)
            return list(csv_reader)
        elif file_format == "txt":

            return file.read()

def load_additional_info(directory):
    """Loads the additional_info.json file from the directory."""
    filepath = os.path.join(directory, "additional_info.json")
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"additional_info.json not found in {directory}")
    return read_file(filepath, "json")

def find_directory_with_table(root_directory, table_name):
    """
    Busca el directorio que contiene un archivo específico.
    """
    for dirpath, dirnames, filenames in os.walk(root_directory):
        if table_name in filenames:
            return os.path.basename(dirpath)
    return None

