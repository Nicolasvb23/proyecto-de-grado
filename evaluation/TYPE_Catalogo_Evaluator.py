import os
import pandas as pd

class TypeEvaluator:
    """
    Evalúa si los tipos de dato predichos en el submission coinciden con
    el ground truth, calculando Precision, Recall y F1.
    """
    def __init__(self, gt_file_path):
        # Cargamos el ground truth (GT)
        # Aquí asumimos que la primera fila del CSV NO es cabecera, 
        # por lo que usamos 'names=[...]'.
        # Ajusta según tu caso (si tu CSV sí tiene header, usa header=0 y quita `names=[...]`).
        self.gt = pd.read_csv(gt_file_path, 
                              delimiter=',', 
                              names=['table_name', 'col_idx', 'type'],
                              dtype=str, 
                              keep_default_na=False)
        
        # Creamos un dict para buscar rápidamente el tipo de cada columna
        # clave = "table_name col_idx", valor = "tipo"
        self.col_type_map = {}
        for _, row in self.gt.iterrows():
            col_key = f"{row['table_name']} {row['col_idx']}"
            self.col_type_map[col_key] = row['type']
            
        # Guardamos el set de columnas totales en el GT
        self.gt_cols = set(self.col_type_map.keys())
        print(self.gt_cols)

    def evaluate(self, submission_path):
        """
        Evalúa un submission (CSV) que tenga la misma estructura:
        'table_name, col_idx, type'.
        Retorna un dict con f1, precision y recall.
        """
        # Leemos la predicción
        sub = pd.read_csv(submission_path,
                          delimiter=',',
                          names=['table_name', 'col_idx', 'type'],
                          dtype=str,
                          keep_default_na=False)
        annotated_cols = set()
        well_classified = 0
        wrong_classified = 0
        correct_named_entity = 0
        
        # Recorremos cada fila de la predicción
        for _, row in sub.iterrows():
            col_key = f"{row['table_name']} {row['col_idx']}"
            # Verificamos que no aparezca más de una vez la misma columna
            if col_key in annotated_cols:
                raise Exception(f"Duplicado en submission: {col_key}")
            annotated_cols.add(col_key)
            
            # Si la columna está en el ground truth
            print("Column:", row['table_name'], row['col_idx'])

            if col_key in self.gt_cols:
                gt_type = self.col_type_map[col_key]    # tipo esperado
                pred_type = row['type']                 # tipo predicho
                print("Pred type:",pred_type)
                print("GT type:",gt_type)
                if pred_type.strip() == gt_type.strip():
                    if(pred_type.strip() == 'named_entity'):
                        print("EXACT NAMED_ENTITY")
                        correct_named_entity += 1
                    print("EXACT CLASSIFICATION ✅")
                    well_classified += 1
                else:
                    print("WRONG CLASSIFICATION❌")
                    wrong_classified += 1
                print("_______________________")
            else:
                print("??")
        # Calculamos las métricas
        # - num_annotated = cantidad de columnas que aparecen en la predicción
        # - num_gt_cols = cantidad de columnas en el ground truth
        num_annotated = len(annotated_cols)
        num_gt_cols = len(self.gt_cols)
        
        # total_score = well_classified (las que coinciden exactamente)
        total_score = well_classified
        
        precision = total_score / num_annotated if num_annotated else 0
        recall = total_score / num_gt_cols if num_gt_cols else 0
        f1 = (2 * precision * recall) / (precision + recall) if (precision + recall) else 0
        
        # Mensajes por consola (opcional)
        print("==========================================")
        print("CANT", num_annotated)
        print(f"Bien clasificadas: {well_classified}")
        print(f"Mal clasificadas: {wrong_classified}")
        print(f"Named_entity correctas: {correct_named_entity}")
        print(f"Precision = {precision:.3f}")
        
        return {
            "f1": f1,
            "precision": precision,
            "recall": recall
        }
