import pandas as pd
import TableMiner.LearningPhase.Learning as learn
from TableMiner.SearchOntology import SearchOntology
from TableMiner.Utils import stabilized, def_bow
from TableMiner.SCDection.TableAnnotation import TableColumnAnnotation as TA
from MetadataLLM.column_concept import ColumnConceptGenerator
from DatasetsUtils.helper import load_additional_info, find_directory_with_table
import torch
import os
import json

DEVICE = 'cuda' if torch.cuda.is_available() else 'cpu'
column_concepts_generator = ColumnConceptGenerator(DEVICE)

# Few shots. TODO: Agregar más, y mejores.
few_shots_column_concept = '''
#### Ejemplo 1:
Nombre Columna: Zona
Ejemplos de valores: LITORAL, SUR, ESTE, OESTE

Nombre Tabla: ventas_gas_natural
Nombre Recursos: Ventas Gas Natural - Volúmenes por zona geográfica
Contexto: Esta tabla contiene datos de ventas de gas natural por mes, año, zona geográfica, transporte firme, transporte interrumpible y gas consumido
Metadata de la Tabla: {
    "atributos": [
        {
            "descripcion": "Mes",
            "informacionAdicional": "",
            "tipoDeDato": "Integer",
            "recursoRelacionado": "",
            "nombreDeAtributo": "Mes"
        },
        {
            "descripcion": "Año",
            "informacionAdicional": "",
            "tipoDeDato": "Integer",
            "recursoRelacionado": "",
            "nombreDeAtributo": "Año"
        },
        {
            "descripcion": "Zona",
            "informacionAdicional": "",
            "tipoDeDato": "String",
            "recursoRelacionado": "",
            "nombreDeAtributo": "Zona"
        },
        {
            "descripcion": "TransporteFirme",
            "informacionAdicional": "",
            "tipoDeDato": "Integer",
            "recursoRelacionado": "",
            "nombreDeAtributo": "TransporteFirme"
        },
        {
            "descripcion": "TransporteInterrumpible",
            "informacionAdicional": "",
            "tipoDeDato": "Integer",
            "recursoRelacionado": "",
            "nombreDeAtributo": "TransporteInterrumpible"
        },
        {
            "descripcion": "GasConsumido",
            "informacionAdicional": "",
            "tipoDeDato": "Integer",
            "recursoRelacionado": "",
            "nombreDeAtributo": "GasConsumido"
        }
}
Algunas filas de la tabla:
Mes,Año,Zona,TransporteFirme,TransporteInterrumpible,GasConsumido
"1";"2019";"LITORAL";"1753825";"0";"267638"
"1";"2019";"SUR";"9913738";"113289";"2341025"
"2";"2019";"LITORAL";"1584100";"0";"177916"
"2";"2019";"SUR";"8954344";"101339";"2408347"
"3";"2019";"LITORAL";"1753825";"0";"311369"

### Concepto sugerido:
Zona Geográfica
'''

class TableLearning:
    def __init__(self, tableName, table: pd.DataFrame, KB="Wikidata", NE_column: dict = None):
        self._table = table
        self._tableName = tableName
        self._annotation_classes = {}
        self._NE_Column = {}
        self._domain_representation = {}
        self.kb = KB
        self._onto = SearchOntology(kb=KB)
        self.update_NE_Column(NE_Column=NE_column)

    def get_annotation_class(self):
        return self._annotation_classes

    def update_NE_Column(self, NE_Column: dict = None):
        if NE_Column is None:
            self._NE_Column = TA(self._table).subcol_Tjs()
        else:
            self._NE_Column = NE_Column

    def update_annotation_class(self, column_index, new_learning: learn):
        self._annotation_classes[column_index] = new_learning

    def get_table(self):
        return self._table
    
    def get_tableName(self):
        return self._tableName

    def get_NE_Column(self):
        return self._NE_Column

    def table_learning(self):
        """
        Learning phase of Table Miner+
        """

        # Show the columns that are going to be annotated
        for column_index in self._NE_Column.keys():
            print(self._table.columns[column_index])

        for column_index in self._NE_Column.keys():
            print("Started learning phase")
            print("Column index: ", column_index)
            print("Column name: ", self._table.columns[column_index])
            learning = learn.Learning(self._table, kb=self.kb)
            ne_column = self._table.columns[column_index]
            print("Preliminary Column Classification")
            learning.preliminaryColumnClassification(ne_column)
            print("Preliminary Cell Disambiguation")
            learning.preliminaryCellDisambiguation()
            self._annotation_classes[column_index] = learning

    # Este metodo es para obtener el bag of words de las definiciones de las entidades ganadoras
    # buscando en la KB.
    def domain_bow(self):
        """
        return the bows of table's domain set
        """
        winning_entities_definitions = set()
        for column_index, learning in self._annotation_classes.items():
            for entity, ids in learning.get_winning_entitiesId().items():
                for entity_id in ids:
                    definition = self._onto.defnition_sentences(entity_id)
                    if definition is not None:
                        winning_entities_definitions.add(definition)
        bow_domain = def_bow(list(winning_entities_definitions))
        return bow_domain


def updatePhase(currentLearnings: TableLearning):
    """
    Update phase of tableMiner+
    """
    table = currentLearnings.get_table()
    print("Starting update")
    previousLearnings = None
    i = 0
    while table_stablized(currentLearnings, previousLearnings) is False:
        previousLearnings = currentLearnings
        bow_domain = currentLearnings.domain_bow()
        for column_index in currentLearnings.get_annotation_class().keys():
            learning = currentLearnings.get_annotation_class()[column_index]
            concepts = learning.get_concepts()
            for concept in concepts:
                print("UPDATE CONCEPTS SCORES FOR", concept)
                learning.update_conceptScores(concept, table.columns[column_index], bow_domain)
            learning.preliminaryCellDisambiguation()
            currentLearnings.update_annotation_class(column_index, learning)
        i += 1


def fallBack(currentLearnings: TableLearning):
    """
    Mecanismo de Fallback de TableMiner.
    Si no se encuentra un concepto ganador para la columna, se usa sugerencia de LLM
    para buscar un concepto y catalogar la columna desde ese concepto.
    """
    datasets_directory = "PipelineDatasets/SelectedDatasets"
    interest_word = "transparencia"

    table = currentLearnings.get_table()
    tableName = currentLearnings.get_tableName()
    print("Starting fallback for table ", tableName)

    for column_index in currentLearnings.get_annotation_class().keys():
        print("index", column_index)
        learning = currentLearnings.get_annotation_class()[column_index]
        concepts = list(learning.get_winning_concepts())
        print("concepts", concepts)
        if len(concepts) == 0:
            # Llamada al LLM con los datos de la columna que no pudo clasificar y el contexto
            
            package_directory = find_directory_with_table(datasets_directory, tableName + ".csv")
            directory = os.path.join(datasets_directory, interest_word, package_directory)

            column_name = column_name = table.columns[column_index]

            additional_info = load_additional_info(directory)
            table_resources = additional_info.get("table_resources", {})
            
            if len(table_resources) == 0:
                print(f"No resources found")
                exit()
                
            # Tomar la primera key de table_resources (es la única porque elegimos solo una tabla)
            table_id = list(table_resources.keys())[0]
            table = pd.read_csv(os.path.join(directory, f"table_{table_id}.csv"))

            # Metadata
            metadata_resources = additional_info.get("metadata_resources", {})

            if len(metadata_resources) == 0:
                print(f"No metadata resources found")
            else:
                metadata_id = list(metadata_resources.keys())[0]
                with open(os.path.join(directory, f"metadata_{metadata_id}.json"), "r", encoding="utf-8") as file:
                    metadata = json.load(file)

            llm_concept = column_concepts_generator.generate_concept(table, table_id, metadata, additional_info, column_name, few_shots_column_concept)
            print("LLM CONCEPT", llm_concept)

            learning.findConceptsFromLLMPrediction(llm_concept)
            currentLearnings.update_annotation_class(column_index, learning)

def table_stablized(currentLearnings, previousLearnings=None):
    if previousLearnings is None:
        return False
    else:
        stablizedTrigger = True
        for column_index in currentLearnings.get_NE_Column().keys():
            currentLearning_index = currentLearnings.get_annotation_class()[column_index]
            previousLearning_index = previousLearnings.get_annotation_class()[column_index]
            winning_entities = currentLearning_index.get_Entities()
            previous_entities = previousLearning_index.get_Entities()
            concepts = currentLearning_index.get_winning_concepts()
            previous_concepts = previousLearning_index.get_winning_concepts()
            if stabilized(winning_entities, previous_entities) is True and stabilized(concepts,
                                                                                      previous_concepts) is True:
                stablizedTrigger = True
            else:
                stablizedTrigger = False
                print("Unstabilized! Re-running ...")
        return stablizedTrigger
