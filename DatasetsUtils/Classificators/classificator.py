""" 
Se clasifican los packages entre los que: 
1) Tienen metadata
2) Tienen notes
3) Tienen ambos
4) No tienen ningunos
"""
import os
from DatasetsUtils.helper import read_file

interest_word = "transparencia"
# Rutas de directorios
download_folder = f"SelectedDatasets/{interest_word}"

def classificate_files(download_folder):
    files_with_metadata = []
    files_with_notes = []
    files_with_both = []
    files_with_nothing = []
    for root, dirs, _ in os.walk(download_folder):
        for dir_name in dirs:
            """Procesa todos los archivos dentro de un directorio específico."""
            dir_path = os.path.join(root, dir_name)

            # Cargar additional_info.json
            additional_info_path = os.path.join(dir_path, "additional_info.json")
            additional_info = read_file(additional_info_path, "json")
            if additional_info.get("notes"):
                if additional_info.get("metadata_resources"):
                    files_with_both.append(dir_name)
                else:
                    files_with_notes.append(dir_name)
            else:
                if additional_info.get("metadata_resources"):
                    files_with_metadata.append(dir_name)
                else:
                    files_with_nothing.append(dir_name)
    return files_with_metadata, files_with_notes, files_with_both, files_with_nothing


files_with_metadata, files_with_notes, files_with_both, files_with_nothing = classificate_files(download_folder)


print("Procesamiento completado.")
print("Files with metadata:")
print(files_with_metadata)
print("Files with notes:")
print(files_with_notes)
print("Files with both:")
print(files_with_both)
print("Files with nothing:")
print(files_with_nothing)