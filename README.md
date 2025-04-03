# Requerimientos

Para poder ejecutar la herramienta localmente se debe contar con:

- **Python 3.10.11 o posterior.**
- **Manejador de dependencias pip 25.0.**
- **Entorno de ejecución de Jupyter Notebook.** Se recomienda utilizar [Visual Studio Code](https://code.visualstudio.com/) junto con la extensión de [Jupyter](https://marketplace.visualstudio.com/items?itemName=ms-toolsai.jupyter) provista por Microsoft.
- **Unidad de procesamiento gráfica (GPU) que soporte CUDA.** Este requerimiento es necesario en caso de que se quiera utilizar el LLM para generación de metadatos, RAG o como *fallback* de TableMiner+. Ver más información en [CUDA Toolkit](https://developer.nvidia.com/cuda-toolkit).

---

# Estructura de directorios

El punto de entrada principal es el *notebook* `DataDiscovery.ipynb`, donde se llevan a cabo todas las etapas del procesamiento de los *datasets* hasta la salida final.

A continuación, se listan las carpetas dedicadas a cada uno de los distintos módulos:

- **`Aurum`**: Este directorio contiene la implementación de Aurum.
- **`D3L`**: Contiene la implementación de D3L.
- **`DatasetsUtils`**: Incluye los *scripts* de descarga a través de *tags* (usando la API de CKAN), así como las tareas de preprocesamiento y extracción de métricas de los datos.
- **`MetadataLLM`**: Contiene todos los *prompts* que se realizan al LLM, tanto los de la generación de metadatos como el *fallback* de TableMiner+.
- **`TableMiner`**: En esta carpeta se encuentra la implementación de TableMiner+.

---

# Instalación de dependencias

Como primer paso, se deben instalar todas las dependencias del proyecto. Estas se encuentran especificadas (junto con su versión) en el archivo `requirements.txt`:

```bash
pip install -r requirements.txt
```

# Ejecución de la herramienta

Una vez instaladas las dependencias, se puede comenzar a ejecutar el *notebook* principal `DataDiscovery.ipynb`. A continuación se describen las etapas de ejecución:

1. **Descarga, procesamiento y extracción de métricas de *datasets* del catálogo**  
   - Se puede especificar el conjunto de palabras de interés para la descarga modificando la variable `interest_words` en el *notebook*.
   - Al finalizar esta etapa, se crea un directorio llamado `PipelineDatasets`, con tres subdirectorios:
     - `DownloadedDatasets`: contiene los *datasets* recién descargados sin preprocesamiento.
     - `DatasetsCollection`: contiene los *datasets* luego del preprocesamiento.
     - `SelectedDatasets`: contiene los *datasets* que se seleccionan automáticamente.
   - Además, se crea el directorio `Datasets` que contiene únicamente los archivos CSV de los *datasets* seleccionados.

2. **Búsqueda de conjuntos de datos (D3L)**  
   - Una vez generados los índices, se guardan en un archivo *pickle* dentro del directorio `Results`.
   - Si se cierra la sesión del *notebook* y se desea volver a ejecutar las celdas, en caso de que existan índices LSH guardados en `Results`, estos se cargarán para evitar generar nuevos índices.
   - Si se desea comenzar desde cero, es recomendable borrar o renombrar el directorio `Results`.

3. **Navegación de datos (Aurum)**  
   - Como resultado, se genera una visualización de un grafo simple que muestra las relaciones entre los *datasets* identificadas por D3L.

4. **Carga del LLM**  
   - Se omite esta etapa si no se cuenta con los requisitos de hardware para cargar el modelo de lenguaje localmente.
   - Para acceder a los modelos gratuitos que provee [HuggingFace](https://huggingface.co/), se necesita un *token* de acceso, que se puede generar teniendo una cuenta en el sitio web.

5. **Anotación de datos (TableMiner+)**  
   - Como salida, se genera el archivo `annotationDict.json` en la carpeta `Results`. Este archivo contiene las anotaciones semánticas obtenidas de la base de conocimiento, así como los tipos de datos de las columnas.

6. **Generación de metadatos con el LLM**  
   - Se generan dos nuevos directorios dentro de `PipelineDatasets`:
     - `EnrichedDatasets`: contiene los metadatos generados.
     - `FinalDatasets`: contiene los conjuntos de datos originales con los metadatos generados ya incorporados.

7. **Mecanismo RAG**  
   - Requiere del LLM para funcionar. En caso de disponer de los recursos necesarios, basta con cambiar la *query* para realizar la consulta deseada al mecanismo de *Retrieval Augmented Generation*.
