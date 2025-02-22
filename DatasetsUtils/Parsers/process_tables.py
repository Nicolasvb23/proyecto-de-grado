import os
import pandas as pd
import json
from DatasetsUtils.helper import process_csv

class TableProcessor:
  """
  Clase para procesar archivos CSV descargados.
  """
  def __init__(self):
    self.download_folder = f"PipelineDatasets/DownloadedDatasets"
    self.output_directory = f"PipelineDatasets/DatasetsCollection"
    os.makedirs(self.output_directory, exist_ok=True)

    # Variables para las métricas
    self.total_columns_original = 0
    self.total_rows_original = 0
    self.total_cells_original = 0

    self.total_columns_processed = 0
    self.total_rows_processed = 0
    self.total_cells_processed = 0

    self.total_datasets_processed = 0
    self.total_datasets_encoding_error = 0
    self.total_datasets_extension_error = 0
    self.total_datasets_size_error = 0

  def process_directory(self):
    """Procesa los archivos CSV en el directorio de descargas."""
    print("Procesando archivos...")

    for root, dirs, files in os.walk(self.download_folder):
      relative_path = os.path.relpath(root, self.download_folder)
      output_dir = os.path.join(self.output_directory, relative_path)

      if os.path.exists(os.path.join(output_dir, "additional_info.json")):
        additional_info_path = os.path.join(output_dir, "additional_info.json")
      else:
        additional_info_path = os.path.join(root, "additional_info.json")

      if not os.path.exists(additional_info_path):
        print(f"No se encontró additional_info.json en {root}. Saltando.")
        continue

      with open(additional_info_path, "r", encoding="utf-8") as f:
        additional_info = json.load(f)

      os.makedirs(output_dir, exist_ok=True)

      for filename in files:
        if filename.startswith("table_") and filename.endswith(".csv"):
          file_path = os.path.join(root, filename)
          output_path = os.path.join(self.output_directory, relative_path, filename)

          if os.path.exists(output_path):
            print("File already exists. Skipping process.")
            continue

          # Check that the file does not surpass 42MB
          if os.path.getsize(file_path) > 42 * 1024 * 1024:
            print(f"File {file_path} is too large. Skipping process.")
            self.total_datasets_size_error += 1
            continue

          os.makedirs(os.path.dirname(output_path), exist_ok=True)

          try:
            columns_original, rows_original = process_csv(file_path, output_path)
          except pd.errors.ParserError as e:
            print(f"Error leyendo extension del archivo {file_path}: {e}")
            self.total_datasets_extension_error += 1
            file_id = filename.replace("table_", "").replace(".csv", "")
            additional_info["table_resources"].pop(file_id, None)
            continue
          except Exception as e:
            print(f"Error leyendo encoding del archivo {file_path}: {e}")
            self.total_datasets_encoding_error += 1
            file_id = filename.replace("table_", "").replace(".csv", "")
            additional_info["table_resources"].pop(file_id, None)
            continue
          self.total_datasets_processed += 1

          self.total_columns_original += columns_original
          self.total_rows_original += rows_original
          self.total_cells_original += columns_original * rows_original

          columns_processed = columns_original
          rows_processed = rows_original if rows_original <= 50 else 50

          self.total_columns_processed += columns_processed
          self.total_rows_processed += rows_processed
          self.total_cells_processed += columns_processed * rows_processed

      additional_info_output_path = os.path.join(output_dir, "additional_info.json")
      os.makedirs(os.path.dirname(additional_info_output_path), exist_ok=True)
      with open(additional_info_output_path, "w", encoding="utf-8") as f_out:
        json.dump(additional_info, f_out, indent=2, ensure_ascii=False)

    print("Procesamiento completo.\n")
    """Muestra las métricas de los archivos procesados y originales."""
    print("Métricas de los archivos descargados:")
    print("Cantidad total de archivos descargados:", len([f for root, dirs, files in os.walk(self.download_folder) for f in files if f.endswith(".csv")]))
    print("Cantidad total de columnas originales:", self.total_columns_original)
    print("Cantidad total de filas originales:", self.total_rows_original)
    print("Cantidad total de celdas originales:", self.total_cells_original)

    print("\n")

    print("Métricas de los archivos procesados:")
    print("Cantidad total de archivos procesados:", len([f for root, dirs, files in os.walk(self.output_directory) for f in files if f.endswith(".csv")]))
    print("Cantidad total de columnas procesadas:", self.total_columns_processed)
    print("Cantidad total de filas procesadas:", self.total_rows_processed)
    print("Cantidad total de celdas procesadas:", self.total_cells_processed)

    print("\n")

    print("Métricas de datasets con error de encoding:")
    print("Cantidad total de tablas procesadas:", self.total_datasets_processed)
    print("Cantidad total de datasets descartados por error de encoding:", self.total_datasets_encoding_error)
    print("Cantidad total de datasets descartados por error de extensión:", self.total_datasets_extension_error)
    print("Cantidad total de datasets descartados por tamaño:", self.total_datasets_size_error)

  