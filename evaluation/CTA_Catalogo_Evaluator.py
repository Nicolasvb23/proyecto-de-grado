import pandas as pd
import os
from pathlib import Path

def filter_gt_for_datasets(gt: pd.DataFrame, datasets_dir: str) -> pd.DataFrame:
    """
    Dado un DataFrame de ground truth (gt) y un directorio que contiene
    archivos de tablas (datasets_dir), retorna solo las filas del GT
    cuyos 'tab_id' coinciden con los nombres de archivo (sin extensión)
    existentes en datasets_dir.

    Args:
        gt (pd.DataFrame): Ground truth con una columna 'tab_id'.
        datasets_dir (str): Ruta al directorio donde están las tablas en formato CSV.

    Returns:
        pd.DataFrame: SubDataFrame de `gt` filtrado solo con los 'tab_id' que se encuentran en datasets_dir.
    """
    # 1) Recorre todos los archivos en datasets_dir y extrae los nombres (sin extensión)
    #    para guardarlos en un conjunto (set) de tab_ids válidos.
    valid_tab_ids = set()
    for filename in os.listdir(datasets_dir):
        # Verificamos que sea un archivo .csv
        if filename.endswith(".csv"):
            # Quitamos la extensión para obtener, por ejemplo, 'tabla123' de 'tabla123.csv'
            table_id = Path(filename).stem
            valid_tab_ids.add(table_id)

    # 2) Filtramos el DataFrame gt para que solo incluya filas cuyo tab_id
    #    esté dentro del set valid_tab_ids.
    gt_filtered = gt[gt['tab_id'].isin(valid_tab_ids)].copy()
    return gt_filtered

class CTA_Evaluator:
  def __init__(self, answer_file_path, round=1):
    self.answer_file_path = answer_file_path
    self.round = round

  def _evaluate(self, client_payload, _context={}):
    """
    Evaluates the submission file against the ground truth.

    Args:
    - client_payload (dict): Contains the path to the submission file and other metadata.

    Returns:
    - dict: Evaluation scores (F1-score, Precision, Recall).
    """
    submission_file_path = client_payload["submission_file_path"]

    # Cargar el archivo de respuestas
    cols, col_type = set(), {}
    gt = pd.read_csv(self.answer_file_path, delimiter=',', names=['tab_id', 'col_id', 'type'],
                     dtype={'tab_id': str, 'col_id': str, 'type': str}, keep_default_na=False)
    
    gt_filtrado = filter_gt_for_datasets(gt, 'Datasets')
    for _, row in gt.iterrows():
        col = f"{row['tab_id']} {row['col_id']}"
        col_type[col] = row['type']
        cols.add(col)

    # Leer el archivo de la sumisión
    annotated_cols = set()
    total_score = 0
    well_classificated = 0
    wrong_classificated = 0
    wrong_classification = 0
    wrong_type = 0

    sub = pd.read_csv(submission_file_path, delimiter=',', names=['tab_id', 'col_id', 'annotation'],
                      dtype={'tab_id': str, 'col_id': str, 'annotation': str}, keep_default_na=False)

    for _, row in sub.iterrows():
        col = f"{row['tab_id']} {row['col_id']}"

        if col in annotated_cols:
            raise Exception(f"Duplicate columns found in the submission file: {col}")

        annotated_cols.add(col)

        annotation_list = row['annotation'].split() if row['annotation'] else []
        
        print("________________________________________________________________________")
        print("Col:", col)
        print("Annotations:", annotation_list)

        # Normalizar URIs de Wikidata si es necesario
        annotation_list = [
            ann if ann.startswith('http://www.wikidata.org/entity/')
            else f'http://www.wikidata.org/entity/{ann}'
            for ann in annotation_list
        ]

        if col in cols:
            max_score = 0  # Mejor puntuación de las anotaciones
            for annotation in annotation_list:
                for gt_type in col_type[col].split():
                    print("Real Annotation:", gt_type)

                    # Obtener los ancestros y descendientes, manejando errores si no existen
                    if annotation.lower() == gt_type.lower():
                        print("EXACT MATCH ✅")
                        score = 1.0
                    else:
                        print("WRONG ❌")
                        score = 0

                    max_score = max(max_score, score)

            if max_score == 1:
                well_classificated += 1
            else:
                wrong_classificated += 1

            total_score += max_score
        else:
            wrong_type += 1
            print("WRONG TYPE")

    # Resultados de la evaluación
    num_annotated = len(annotated_cols)
    num_gt_cols = len(gt_filtrado)
    num_real_annotated = num_annotated - wrong_type

    print(f"Total score: {total_score}")
    print(f"Num gt cols: {num_gt_cols}")
    print(f"Total de columnas anotadas: {num_annotated}")
    print(f"Total de columnas clasificadas exactas: {well_classificated}")
    print(f"Total de columnas clasificadas incorrectamente: {wrong_classificated}")
    print(f"Total de columnas clasificadas erroneamente: {wrong_classification}")   
    print(f"Total de columnas clasificadas erroneamente como named_entity: {wrong_type}")  

    real_precision = total_score / num_real_annotated if num_real_annotated > 0 else 0.0
    precision = total_score / num_annotated if num_annotated > 0 else 0.0
    recall = total_score / num_gt_cols if num_gt_cols > 0 else 0.0
    f1 = (2 * precision * recall) / (precision + recall) if (precision + recall) > 0 else 0.0

    print(f'📊 F1: {f1:.3f}, Precision: {precision:.3f}, Recall: {recall:.3f}')
    print("Real precision:", real_precision)

    # Devolver los resultados en un diccionario
    return {
        "score": f1,
        "score_secondary": precision
    }
