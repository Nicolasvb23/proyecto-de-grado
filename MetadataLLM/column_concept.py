import pandas as pd
import numpy as np
import torch
from torch.amp import autocast

class ColumnConceptGenerator:
    def __init__(self, model, tokenizer, device="cuda"):
        """
        Inicializa el generador de conceptos para columnas.

        Args:
            model_name (str): Nombre del modelo preentrenado.
            device (str): Dispositivo para ejecutar el modelo ("cpu" o "cuda").
        """
        self.device = device
        self.tokenizer = tokenizer
        self.model = model

    def generate_column_concept_prompt(self, table, table_id, metadata, additional_info, column_name, few_shots_column_concept):
        """
        Genera un prompt para consultar el concepto asociado a una columna específica.

        Args:
            table (list of lists): Datos de la tabla como lista de filas (incluye encabezado).
            metadata (list of dicts): Metadata de la tabla.
            context (str): Contexto general de los datos.
            column_metadata (dict): Metadata específica de la columna.

        Returns:
            str: Prompt generado.
        """
        # Pandas dataframe
        header = table.columns.tolist()
        column_index = header.index(column_name)

        random_indexes = np.random.choice(len(table) - 1, min(5, len(table) - 1), replace=False) + 1
        column_sample_rows = table.iloc[random_indexes, column_index].tolist()

        prompt = f"""Eres un asistente experto en datos...
### Ejemplos
{few_shots_column_concept}

### Ahora genera un concepto para la siguiente columna de la tabla:
Nombre Columna: {column_name}
Ejemplos de valores: {", ".join(map(str, column_sample_rows))}

Nombre Tabla: {additional_info['title']}
Nombre Recurso: {additional_info['table_resources'][table_id]['name']}
Contexto: {additional_info['notes']}
Metadata de la Tabla: {metadata}
Algunas filas de la Tabla: {table.sample(min(5, len(table) - 1))}

### Concepto sugerido:
"""
        return prompt

    def generate_concept(self, table, table_id, metadata, additional_info, column_name, few_shots_column_concept):
        """
        Args:
            table (list of lists): Datos de la tabla como lista de filas (incluye encabezado).
            metadata (list of dicts): Metadata de la tabla.
            context (str): Contexto general de los datos.
            column_metadata (dict): Metadata específica de la columna.

        Returns:
            str: Concepto generado.
        """
        prompt = self.generate_column_concept_prompt(table, table_id, metadata, additional_info, column_name, few_shots_column_concept)
        inputs = self.tokenizer(prompt, return_tensors="pt").to(self.device)
        
        with torch.no_grad():
            with autocast(self.device):
                outputs = self.model.generate(
                    **inputs,
                    max_new_tokens=20,
                    temperature=0.7,
                    top_p=0.8,
                    repetition_penalty=1.1,
                    eos_token_id=self.tokenizer.eos_token_id
                )

        answer = self.tokenizer.decode(outputs[0], skip_special_tokens=True)
        result = answer.split("Concepto sugerido:")[-1].strip().split("\n")[0].strip()
        return result
