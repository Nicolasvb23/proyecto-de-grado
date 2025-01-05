'''
  Clase que recibe un directorio en SelectedDatasets, y para cada las carpetas de packages que hayan en el directorio:
      - Remueve las "notes" (descripcion) de 1/3 de los packages
      - Remueve el archivo de metadata de 1/3 de los packages
      - Remueve ambos de 1/3 de los packages
      
  En el directorio original se guardan los packages modificados, y se crea un directorio groundTruth con la informacion original
  de los packages modificados.
'''

import os
import shutil
from DatasetsUtils.helper import write_file, read_file

class DatasetEvaluator:
    def __init__(self, directory):
        self.directory = directory

    def prepare(self):
        # Crear un directorio al mismo nivel que directory para guardar la informacion original
        original_directory = os.path.join("PipelineDatasets", "groundTruth")

        if not os.path.exists(original_directory):
            os.makedirs(original_directory)

        # Copiar el contenido de directory a original_directory
        self.copy_directory(self.directory, original_directory)
        
        # Dividir los packages en 3 grupos
        packages = os.listdir(self.directory)
        third = len(packages) // 3
        
        # Remover la descripcion de 1/3 de los packages
        for package in packages[:third]:
            print("Removing notes from", package)
            additional_info = self.load_additional_info(os.path.join(self.directory, package))
            
            additional_info["notes"] = ""
            write_file(os.path.join(self.directory, package, "additional_info.json"), additional_info, "json", "utf-8")
        
        # Remover el archivo de metadata de 1/3 de los packages
        for package in packages[third:2*third]:
            print("Removing metadata from", package)
            additional_info = self.load_additional_info(os.path.join(self.directory, package))
            metadata_files = list(additional_info['metadata_resources'].keys())
            metadata_file = metadata_files[0] if len(metadata_files) > 0 else None
            if not metadata_file:
                continue
            os.remove(os.path.join(self.directory, package, f"metadata_{metadata_file}.json"))
            additional_info['metadata_resources'] = {}
            write_file(os.path.join(self.directory, package, "additional_info.json"), additional_info, "json", "utf-8")
            
        # Remover ambos de 1/3 de los packages
        for package in packages[2*third:]:
            print("Removing notes and metadata from", package)
            additional_info = self.load_additional_info(os.path.join(self.directory, package))
            metadata_files = list(additional_info['metadata_resources'].keys())
            metadata_file = metadata_files[0] if len(metadata_files) > 0 else None
            if not metadata_file:
               additional_info["notes"] = ""
               write_file(os.path.join(self.directory, package, "additional_info.json"), additional_info, "json", "utf-8")
               continue
            os.remove(os.path.join(self.directory, package, f"metadata_{metadata_file}.json"))
            additional_info['metadata_resources'] = {}
            additional_info["notes"] = ""
            write_file(os.path.join(self.directory, package, "additional_info.json"), additional_info, "json", "utf-8")
               
    
    # TODO: Duplicate code, refactor.
    def copy_directory(self, src, dest):
        if os.path.exists(dest):
            shutil.rmtree(dest)
        shutil.copytree(src, dest)

    def load_additional_info(self, directory):
        """Loads the additional_info.json file from the directory."""
        filepath = os.path.join(directory, "additional_info.json")
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"additional_info.json not found in {directory}")
        return read_file(filepath, "json")
