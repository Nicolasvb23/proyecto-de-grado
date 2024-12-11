# Modulo para procesar los archivos de metadata descargados de CKAN.
# Dados los dos tipos de metadata que descargamos
#     - metadata_...json: Contiene la metadata de los datasets en formato JSON
#     - potential_metadata_...ext: Contiene la metadata de los datasets en un formato
#                                  que no es JSON, como CSVs o XMLs.
# En ambos casos el formato puede ser engañoso, por ejemplo un .json que en realidad
# contiene un CSV, o un .csv que contiene un JSON. Por lo que se necesita un proceso
# de reconocimiento de los archivos para poder procesarlos correctamente.
# En caso de no poder reconocer el tipo, se manejará como un .txt.
import os
import json
import csv
import chardet
from DatasetsUtils.helper import detect_encoding, write_file

interest_word = "transparencia"
# Rutas de directorios
download_folder = f"DownloadedDatasets/{interest_word}"
output_directory = f"DatasetsCollection/{interest_word}"
os.makedirs(output_directory, exist_ok=True)

def recognize_and_process_potential_metadata(file_path):
    """Reconoce el tipo de archivo potencial y lo procesa adecuadamente."""
    encoding = detect_encoding(file_path)
    with open(file_path, "r", encoding=encoding) as file:
        try:
            metadata = json.load(file)
            return "json", metadata, encoding
        except json.JSONDecodeError:
            try:
                file.seek(0)  # Reiniciar puntero de lectura
                csv_reader = csv.reader(file)
                rows = list(csv_reader)
                return "csv", rows, encoding
            except Exception:
                file.seek(0)  # Reiniciar puntero de lectura
                return "txt", file.read(), encoding

def process_directory(root, dir_name):
    """Procesa todos los archivos dentro de un directorio específico."""
    dir_path = os.path.join(root, dir_name)
    # Directorio de salida, con el id del package como directorio
    output_dir = os.path.join(output_directory, dir_name)
    additional_info_path = os.path.join(output_dir, "additional_info.json")
    
    # Cargar additional_info.json
    if not os.path.exists(additional_info_path):
        print(f"No se encontró additional_info.json en {dir_name}. Saltando.")
        return
    
    with open(additional_info_path, "r", encoding="utf-8") as f:
        additional_info = json.load(f)

    for filename in os.listdir(dir_path):
        if "metadata" not in filename:
            continue

        print (f"   Procesando {filename}...")
        file_path = os.path.join(dir_path, filename)
        extension, content, encoding = recognize_and_process_potential_metadata(file_path)
        output_path = os.path.join(output_dir, filename)

        if filename.startswith("metadata_") and filename.endswith(".json"):
            # Extraer el id del archivo
            file_id = filename.replace("metadata_", "").replace(".json", "")
            if extension == "json":
                # Se mantiene el formato original
                write_file(output_path, content, "json", encoding)
            elif extension == "csv":
                # El formato reconocido es CSV, se guarda en formato CSV
                file_output = output_path.replace(".json", ".csv")
                write_file(file_output, content, "csv", encoding)
                additional_info["metadata_resources"][file_id]["format"] = "csv"
            else:
                # No se reconocio el formato, se guarda como txt
                file_path = output_path.replace(".json", ".txt")
                write_file(f"potential_{file_path}", content, "txt", encoding)
                # Mover de metadata_resources a potential_metadata_resources
                additional_info["potential_metadata_resources"][file_id] = additional_info["metadata_resources"].pop(file_id, None)
                additional_info["potential_metadata_resources"][file_id]["format"] = "txt"
                
                # Remover metadata de la lista de metadata_resources, ya que no es consistente en principio
                additional_info["metadata_resources"].pop(file_id, None)
        elif filename.startswith("potential_metadata_"):
            # Extraer el id del archivo
            file_id = filename.replace("potential_metadata_", "").split(".")[0]
            if extension == "json":
                file_output = os.path.join(output_dir, f"metadata_{file_id}.json")
                write_file(file_output, content, "json", encoding)
                # Mover de potential_metadata_resources a metadata_resources
                additional_info["metadata_resources"][file_id] = additional_info["potential_metadata_resources"].pop(file_id, None)
                additional_info["metadata_resources"][file_id]["format"] = "json"
                
                # Remover metadata de la lista de potential_metadata_resources, ya que es consistente. Moverlo a metadata_resources
                additional_info["potential_metadata_resources"].pop(file_id, None)
            elif extension == "csv":
                file_output = os.path.join(output_dir, f"metadata_{file_id}.csv")
                write_file(file_output, content, "csv", encoding)
                additional_info["metadata_resources"][file_id] = additional_info["potential_metadata_resources"].pop(file_id, None)
                additional_info["metadata_resources"][file_id]["format"] = "csv"
                
                # Remover metadata de la lista de potential_metadata_resources, ya que es consistente. Moverlo a metadata_resources
                additional_info["potential_metadata_resources"].pop(file_id, None)
            else:
                file_output = os.path.join(output_dir, f"potential_metadata_resources{file_id}.txt")
                write_file(file_output, content, "txt", encoding)
                additional_info["potential_metadata_resources"][file_id]["format"] = "txt"

    # Guardar additional_info actualizado
    additional_info_output_path = os.path.join(output_dir, "additional_info.json")
    write_file(additional_info_output_path, additional_info, "json", "utf-8")

# Procesar archivos
print("Procesando archivos...")

for root, dirs, _ in os.walk(download_folder):
    for dir_name in dirs:
        print(f"# Procesando {dir_name}...")
        process_directory(root, dir_name)

print("Procesamiento completado.")
