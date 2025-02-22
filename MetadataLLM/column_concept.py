from MetadataLLM.abstract import ModelManager
import numpy as np
import torch
from torch.amp import autocast
import re
import json

class ColumnConceptGenerator(ModelManager):
    def __init__(self, device="cuda"):
        """
        Inicializa el generador de conceptos para columnas.

        Args:
            device (str): Dispositivo para ejecutar el modelo ("cpu" o "cuda").
        """
        self.device = device

    def generate_simple_prompt(self, table, column_name, few_shots_column_concept):
        """
        Genera un prompt basado solo en los datos de la tabla y el nombre de la columna.

        Args:
            table_csv (str): Ruta del archivo CSV.
            column_name (str): Nombre de la columna para la cual generar un concepto.

        Returns:
            str: Prompt generado.
        """
        # Convertir la columna en una lista de valores, eliminando NaN
        print("starting generate simple prompt")
        column_values = table[column_name].dropna().astype(str).tolist()
        print("COLUMN VALUES", column_values)
        # Unir todos los valores en una cadena separada por comas (evitar muy larga)
        column_data = ", ".join(column_values)

        prompt = f"""You are a data expert assistant specialized in categorizing data columns using Wikidata concepts.
Your task is to analyze the given column and suggest the most relevant Wikidata label.
- Return **only** the Wikidata concept **label** (e.g., "smartphone model").
- Do **not** explain the reasoning.
- The response should be **only one concept** from Wikidata.

### Examples:
{few_shots_column_concept}

### Now, analyze the following column to generate a Wikidata concept:
Column Name: {column_name}
Column Values: {column_data}

### Wikidata Concept:
"""
        return prompt

    def generate_column_concept_prompt(self, table, table_id, metadata, additional_info, column_name, few_shots_column_concept):
        """
        Genera un prompt para consultar el concepto asociado a una columna específica.

        Args:
            table (DataFrame): Datos de la tabla como un DataFrame de pandas.
            metadata (list of dicts): Metadata de la tabla.
            additional_info (dict): Información adicional de la tabla.
            column_name (str): Nombre de la columna a analizar.
            few_shots_column_concept (str): Ejemplos previos de clasificación de columnas.

        Returns:
            str: Prompt generado.
        """
        # Obtener los valores de la columna
        header = table.columns.tolist()
        column_index = header.index(column_name.strip())
        column_values = table.iloc[:, column_index].dropna().unique().tolist()  # Eliminar duplicados y NaN

        metadata_json = json.dumps(metadata, indent=4, ensure_ascii=False)

        # Formato de lista con viñetas
        column_values_formatted = "\n".join([f"- {str(value)}" for value in column_values])

        prompt = f"""Eres un asistente experto en datos especializado en categorizar columnas de datos utilizando conceptos de Wikidata.
Tu tarea es analizar **todos los valores de la columna** y sugerir **un único concepto de Wikidata** que los represente de la mejor manera posible.
- Considera todos los valores proporcionados, no solo un subconjunto de ellos.
- Devuelve **únicamente** la etiqueta del concepto de Wikidata (por ejemplo, "medical treatment").
- **No** expliques el razonamiento.
- La respuesta debe ser un solo concepto de Wikidata.

### Ejemplos
{few_shots_column_concept}

### Ahora, analiza la siguiente columna y tabla para generar un concepto de Wikidata:
Nombre de la Tabla: {additional_info['title']}
Nombre Columna: {column_name}
Valores:
{column_values_formatted}

### Concepto sugerido:
"""
        return prompt


    def generate_concept(self, table, table_id, metadata, additional_info, column_name, few_shots_column_concept,simple_mode=False):
        """
        Args:
            table (list of lists): Datos de la tabla como lista de filas (incluye encabezado).
            metadata (list of dicts): Metadata de la tabla.
            context (str): Contexto general de los datos.
            column_metadata (dict): Metadata específica de la columna.

        Returns:
            str: Concepto generado.
        """
        if simple_mode:
            prompt = self.generate_simple_prompt(table, column_name, few_shots_column_concept)
        else:
            prompt = self.generate_column_concept_prompt(table, table_id, metadata, additional_info, column_name, few_shots_column_concept)
        
        print("PROMPT", prompt)
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
        if simple_mode:
            # Depuración: ver la salida antes de procesarla
            print("RAW OUTPUT:", repr(answer))

            # Encontrar todas las ocurrencias de "Wikidata Concept:" y tomar la última
            matches = re.findall(r"Wikidata Concept:\s*(.+)", answer, re.IGNORECASE)

            if matches:
                result = matches[-1].strip()  # Toma la última coincidencia
            else:
                result = "Unknown Concept"  # Manejo de error si no se encuentra el concepto

            print("EXTRACTED RESULT:", result)
        else:
            result = answer.split("Concepto sugerido:")[-1].strip().split("\n")[0].strip()
            print("EXTRACTED RESULT:", result)
        return result

    def logger_tag(self):
        "Column Concept Generator"
