""" 
Modulo para seleccionar de los archivos de datos y metadatos descargados de CKAN, solo uno por package
Se elige el dataset, y el archivo de metadata según el nombre de columnas de la tabla (en metadata y table).
Si no se encuentran coincidencias en nombres de columnas se toman los primeros de ambos.
Se guardan hasta 3 archivos potential_metadata 
"""
import os
import pandas as pd
from DatasetsUtils.helper import detect_encoding, write_file, read_file

class DatasetSelector:
    """
    Clase para seleccionar archivos de datos y metadatos descargados de CKAN.
    """
    def __init__(self, interest_word):
        self.interest_word = interest_word
        self.download_folder = f"PipelineDatasets/DatasetsCollection/{interest_word}"
        self.output_directory = f"PipelineDatasets/SelectedDatasets/{interest_word}"
        os.makedirs(self.output_directory, exist_ok=True)

    def process_directory(self, root, dir_name):
        """Procesa todos los archivos dentro de un directorio específico."""
        dir_path = os.path.join(root, dir_name)
        output_dir = os.path.join(self.output_directory, dir_name)

        # Cargar additional_info.json
        additional_info_path = os.path.join(dir_path, "additional_info.json")
        additional_info = read_file(additional_info_path, "json")

        if additional_info['table_resources'] == {}:
            print(f"El directorio {dir_name} no contiene archivos de tablas. Omitiéndolo")
            return

        os.makedirs(output_dir, exist_ok=True)

        metadata_selected = None
        table_selected = None
        atributos_metadata = []
        atributos_tables = []
        first_metadata = None
        first_table = None
        potential_metadata_saved = 0

        for filename in os.listdir(dir_path):
            file_path = os.path.join(dir_path, filename)
            output_path = os.path.join(output_dir, filename)

            # Procesar archivo de metadata
            if filename.startswith("metadata_"):
                if filename.endswith(".json"):
                    file_id = filename.replace("metadata_", "").replace(".json", "")

                    if metadata_selected:
                        # Saltear y remover metadata de la lista de metadata_resources
                        additional_info["metadata_resources"].pop(file_id, None)
                    else:
                        content = read_file(file_path, "json")

                        # Obtengo los atributos
                        if isinstance(content, list): 
                            atributos_metadata = [
                                (atributo.get("nombreAtributo") or atributo.get("nombreDeAtributo") or "").strip().lower()
                                for atributo in content if isinstance(atributo, dict)
                            ]
                        elif isinstance(content, dict):
                            atributos = content.get("atributos", [])
                            atributos_metadata = [
                                (atributo.get("nombreAtributo") or atributo.get("nombreDeAtributo") or "").strip().lower()
                                for atributo in atributos
                            ]
                        else:
                            continue

                        if (not table_selected and not metadata_selected) or (table_selected and set(atributos_metadata) == set(atributos_tables)):
                            # Si es el primer archivo encontrado o si no es el primero pero los atributos matchean, lo selecciono
                            write_file(output_path, content, "json", detect_encoding(file_path))
                            metadata_selected = filename
                        else:
                            # Si es el primero encontrado lo guardo por si no hay matcheo de columnas, sino lo descarto
                            if not first_metadata:
                                first_metadata = filename
                            else:
                                additional_info["metadata_resources"].pop(file_id, None)
                else:
                    file_id = filename.replace("metadata_", "").replace(".csv", "")

                    if metadata_selected:
                        # Saltear y remover metadata de la lista de metadata_resources
                        additional_info["metadata_resources"].pop(file_id, None)
                    else:
                        # Si es el primero encontrado lo guardo por si no hay matcheo de columnas, sino lo descarto
                        if not first_metadata:
                            first_metadata = filename
                        else:
                            additional_info["metadata_resources"].pop(file_id, None)

            # Procesar archivo de tabla
            elif filename.startswith("table_"):
                file_id = filename.replace("table_", "").replace(".csv", "")

                if table_selected:
                    # Saltear y remover tabla de la lista de table_resources
                    additional_info["table_resources"].pop(file_id, None)
                else:
                    df = pd.read_csv(file_path, sep=None, engine='python', quotechar='"', encoding='utf-8')

                    # Obtengo los atributos
                    atributos_tables = [col.strip().lower() for col in df.columns]

                    if (not metadata_selected and not table_selected) or (metadata_selected and set(atributos_tables) == set(atributos_metadata)):
                        # Si es el primer archivo encontrado o si no es el primero pero los atributos matchean, lo selecciono
                        df.to_csv(output_path, index=False)
                        table_selected = filename
                    else:
                        # Si es el primero encontrado lo guardo por si no hay matcheo de columnas, sino lo descarto
                        if not first_table:
                            first_table = filename
                        else:
                            additional_info["table_resources"].pop(file_id, None)

            # Procesar archivo de potential metadata            
            elif filename.startswith("potential_metadata_"):
                file_id = filename.replace("potential_metadata_", "").replace(".txt", "")

                if potential_metadata_saved < 4:
                    # Guardo hasta 3 potential_metadata_resources
                    content = read_file(file_path, "txt")
                    write_file(output_path, content, "txt", detect_encoding(file_path))
                    potential_metadata_saved += 1
                else:
                    additional_info["potential_metadata_resources"].pop(file_id, None)

        # Si no se encontró un mapeo se guarda la primera encontrada
        if first_metadata:
            if not metadata_selected:
                # Guardo el primer archivo encontrado
                file_path = os.path.join(dir_path, first_metadata)
                output_path = os.path.join(output_dir, first_metadata)
                content = read_file(file_path, file_path.split('.')[-1])
                write_file(output_path, content, file_path.split('.')[-1], detect_encoding(file_path))
            else:
                # Descarto el primero encontrado
                if metadata_selected != first_metadata:
                    additional_info["metadata_resources"].pop(first_metadata.replace("metadata_", "").replace(".json", ""), None)
        if first_table:
            if not table_selected:
                file_path = os.path.join(dir_path, first_table)
                output_path = os.path.join(output_dir, first_table)

                df = pd.read_csv(file_path, sep=None, engine='python', quotechar='"', encoding='utf-8')
                df.to_csv(output_path, index=False)
            else:
                if table_selected != first_table:
                    additional_info["table_resources"].pop(first_table.replace("table_", "").replace(".csv", ""), None)

        # Guardar additional_info actualizado
        additional_info_output_path = os.path.join(output_dir, "additional_info.json")
        write_file(additional_info_output_path, additional_info, "json", "utf-8")

    def process_all(self):
        """Procesa todos los directorios en el folder de descarga."""
        for root, dirs, _ in os.walk(self.download_folder):
            for dir_name in dirs:
                print(f"# Procesando {dir_name}...")
                self.process_directory(root, dir_name)

        print("Procesamiento completado.")
        