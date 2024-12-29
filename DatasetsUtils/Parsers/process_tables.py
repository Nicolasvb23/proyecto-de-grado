import os
import chardet
import pandas as pd
import json
from DatasetsUtils.helper import process_csv

interest_word = "transparencia"
download_folder = f"PipelineDatasets/DownloadedDatasets/{interest_word}"

# Procesamiento de CSVs para truncarlos y uniformizar su encoding
# Ruta al directorio con los CSVs
output_directory = f"PipelineDatasets/DatasetsCollection/{interest_word}"
os.makedirs(output_directory, exist_ok=True)

# Variables para las métricas
total_columns_original = 0
total_rows_original = 0
total_cells_original = 0

total_columns_processed = 0
total_rows_processed = 0
total_cells_processed = 0

total_datasets_processed = 0
total_datasets_encoding_error = 0

# Procesamiento de los archivos en el directorio de descargas
print("Procesando archivos...")

for root, dirs, files in os.walk(download_folder):
  # Ruta relativa y directorio de salida
  relative_path = os.path.relpath(root, download_folder)
  output_dir = os.path.join(output_directory, relative_path)
  if os.path.exists(os.path.join(output_dir, "additional_info.json")):
    additional_info_path = os.path.join(output_dir, "additional_info.json")
  else:
    additional_info_path = os.path.join(root, "additional_info.json")

  if not os.path.exists(additional_info_path):
      print(f"No se encontró additional_info.json en {root}. Saltando.")
      continue

  # Cargar el archivo de información adicional
  with open(additional_info_path, "r", encoding="utf-8") as f:
      additional_info = json.load(f)
  
  os.makedirs(output_dir, exist_ok=True)

  for filename in files:
    if filename.startswith("table_") and filename.endswith(".csv"):
      file_path = os.path.join(root, filename)
      output_path = os.path.join(output_directory, relative_path, filename)
      
      if os.path.exists(output_path):
        print("File already exists. Skipping process.")
        continue
      
      # Crear directorio de salida si no existe
      os.makedirs(os.path.dirname(output_path), exist_ok=True)
      
      # Procesar el archivo CSV y obtener métricas del archivo original
      columns_original, rows_original = process_csv(file_path, output_path)
      total_datasets_processed += 1
      if not columns_original and not rows_original:
         total_datasets_encoding_error += 1
         file_id = filename.replace("table_", "").replace(".csv", "")
         additional_info["table_resources"].pop(file_id, None)

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

  # Guardar additional_info.json en el directorio de salida
  additional_info_output_path = os.path.join(output_dir, "additional_info.json")
  os.makedirs(os.path.dirname(additional_info_output_path), exist_ok=True)
  with open(additional_info_output_path, "w", encoding="utf-8") as f_out:
    json.dump(additional_info, f_out, indent=2, ensure_ascii=False)            
print("Procesamiento completo.\n")

# Mostrar las métricas de los archivos originales
print("Métricas de los archivos descargados:")
print("Cantidad total de archivos descargados:", len([f for root, dirs, files in os.walk(download_folder) for f in files if f.endswith(".csv")]))
print("Cantidad total de columnas originales:", total_columns_original)
print("Cantidad total de filas originales:", total_rows_original)
print("Cantidad total de celdas originales:", total_cells_original)

print("\n")

# Mostrar las métricas de los archivos procesados
print("Métricas de los archivos procesados:")
print("Cantidad total de archivos procesados:", len([f for root, dirs, files in os.walk(output_directory) for f in files if f.endswith(".csv")]))
print("Cantidad total de columnas procesadas:", total_columns_processed)
print("Cantidad total de filas procesadas:", total_rows_processed)
print("Cantidad total de celdas procesadas:", total_cells_processed)

print("\n")

#Mostrar las métricas de cantidad de error de encoding
print("Métricas de datasets con error de encoding:")
print("Cantidad total de tablas procesadas:", total_datasets_processed)
print("Cantidad total de datasets descartados por error de encoding:", total_datasets_encoding_error)
