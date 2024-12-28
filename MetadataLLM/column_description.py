import os
import json
import torch
from torch.amp import autocast

# Descripcion de una columna
class ColumnDescriptionGenerator:
    def __init__(self, model, tokenizer, device="cuda"):
        self.model = model
        self.tokenizer = tokenizer
        self.device = device

    def column_description_few_shots_to_prompt(self, few_shots_data):
        """Generates the few-shot examples for the prompt."""
        few_shots = ""
        for i, few_shot in enumerate(few_shots_data):
            few_shots += f'''### Ejemplo {i + 1}:
Nombre Tabla: {few_shot['nombre_tabla']}
Nombre Recurso: {few_shot['nombre_recurso']}
Contexto: {few_shot['contexto']}
Tabla:
{few_shot['tabla']}
Columna de interes: {few_shot['columna_de_interes']}
Descripcion de salida para la columna de interes:
{few_shot['descripcion_salida'].strip()}

'''
        return few_shots.strip()

    def column_description_prompt(self, table, table_id, column_name, additional_info, few_shots_prompt_data):
        """Creates the prompt to generate the column description."""
        prompt = f'''Eres un asistente que ayuda en la desambiguación de tablas. Toda la información pertenece al catálogo de datos abierto de Uruguay.

### Instrucciones
- Solo se debe generar como output descripciones detalladas y específicas de la columna de interes.
- No uses frases genéricas como "No hay datos relevantes". Omitir en la respuesta todo lo que no sea una descripción de la columna de interes.
- Solo responder con la descripción, ser lo más objetivo posible.

{self.column_description_few_shots_to_prompt(few_shots_prompt_data)}

### Ahora genera una descripción para la siguiente columna:
Nombre Tabla: {additional_info['title']}
Nombre Recurso: {additional_info['table_resources'][table_id]['name']}
Contexto: {additional_info['notes']}
Tabla:
{table}
Columna de interes: {column_name}
'''
        prompt += '''
Descripcion de salida:
'''
        return prompt

    def generate_column_description(self, table, table_id, column_name, additional_info, few_shots_prompt_data):
        """Generates a description for a given table using the few-shot prompt."""
        # Tokenize the prompt
        inputs = self.tokenizer(self.column_description_prompt(table, table_id, column_name, additional_info, few_shots_prompt_data), return_tensors="pt").to(self.device)

        with torch.no_grad():
            with autocast(self.device):
                outputs = self.model.generate(
                    **inputs, max_new_tokens=65, temperature=0.65, top_p=0.8, repetition_penalty=1.2, eos_token_id=self.tokenizer.eos_token_id
                )

        # Decode and return the result
        answer = self.tokenizer.decode(outputs[0], skip_special_tokens=True)
        result = answer.split("Descripcion de salida:")[-1].strip()
        return result
