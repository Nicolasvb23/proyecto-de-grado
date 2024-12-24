from Utils import mkdir
# Import and initialize modules
from d3l.indexing.similarity_indexes import NameIndex, FormatIndex, ValueIndex, EmbeddingIndex, DistributionIndex
from d3l.input_output.dataloaders import CSVDataLoader
from d3l.querying.query_engine import QueryEngine
from d3l.utils.functions import pickle_python_object, unpickle_python_object
import os
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt

#Imprimir lindo el resultado de d3l
def convert_to_dataframe(similar_columns_results):
    data = []
    for column_id, scores in similar_columns_results:
        row = {"id": column_id}
        for i, score in enumerate(scores):
            row[f"score_{i+1}"] = score
        data.append(row)

    # Convertir a DataFrame
    return pd.DataFrame(data)


#Retorna el nombre de la columna dentro del arreglo dado
def SC(searched_table :str,subject_columns: list ) -> str:
#Subject_columns tiene todas las subject columns
    matching_entry = next(
        (entry for entry in subject_columns if entry.split(".")[0] == searched_table),
        None  # Si no hay coincidencias
    )

    print(matching_entry)
    if matching_entry:
        print(f"La columna {matching_entry.split(".")[1]} es la subject column de la tabla {searched_table}.")
        return matching_entry
    else:
        print(f"No tiene Subject column la tabla {searched_table}.")
        return None
    


