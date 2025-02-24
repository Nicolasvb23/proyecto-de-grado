from MetadataLLM.abstract import ModelManager
import torch
from torch.amp import autocast

class TableDescriptionWithMetadataGenerator(ModelManager):
    def __init__(self, device="cuda"):
        self.device = device

    def table_description_with_metadata_few_shots_to_prompt(self, few_shots_data):
        """Generates the few-shot examples with metadata for the prompt."""
        few_shots = ""
        for i, few_shot in enumerate(few_shots_data):
            metadata_files = "\n".join(few_shot['metadata_files'])
            few_shots += f'''### Ejemplo {i + 1}:
Nombre Tabla: {few_shot['nombre_tabla']}
Nombre Recurso: {few_shot['nombre_recurso']}
Tabla:
{few_shot['tabla']}
Archivos de metadatos:
{metadata_files}
Descripcion de salida:
{few_shot['descripcion_salida'].strip()}

'''
        return few_shots.strip()

    def description_with_metadata_prompt(self, table, table_id, additional_info, metadata, few_shots_prompt_data):
        """Creates the prompt to generate the table description with metadata."""
        prompt = f'''Eres un asistente que ayuda en la desambiguación de tablas. Toda la información pertenece al catálogo de datos abierto de Uruguay.

### Instrucciones
- Solo se debe generar como output descripciones detalladas y específicas.
- No uses frases genéricas como "No hay datos relevantes". Omitir en la respuesta todo lo que no sea una descripción.
- Solo responder con la descripción, ser lo más objetivo posible.
- La descripcion no debe exceder las 200 palabras. No finalizar de forma abrupta para no exceder el límite de palabras.

{self.table_description_with_metadata_few_shots_to_prompt(few_shots_prompt_data)}

### Ahora genera una descripción para la siguiente tabla:
Nombre Tabla: {additional_info['title']}
Nombre Recurso: {additional_info['table_resources'][table_id]['name']}
Tabla:
{table.sample(min(len(table) - 1, 20))}

Archivos de metadatos:
{metadata}
'''
        prompt += '''
Descripcion de salida:
'''
        return prompt

    def generate_description_with_metadata(self, table, table_id, metadata, additional_info, few_shots_prompt_with_metadata):
        # Tokenize the prompt
        inputs = self.tokenizer(self.description_with_metadata_prompt(table, table_id, additional_info, metadata, few_shots_prompt_with_metadata), return_tensors="pt").to(self.device)

        with torch.no_grad():
            with autocast(self.device):
                outputs = self.model.generate(
                    **inputs, max_new_tokens=300, temperature=self.temperature, top_p=self.top_p, repetition_penalty=self.repetition_penalty, eos_token_id=self.tokenizer.eos_token_id
                )

        # Decode and return the result
        answer = self.tokenizer.decode(outputs[0], skip_special_tokens=True)
        result = answer.split("Descripcion de salida:")[-1].strip()
        return result

    def logger_tag(self):
        "Table Description with Metadata Generator"
