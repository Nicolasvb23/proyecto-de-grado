import math
from math import sqrt
import pandas as pd
from TableMiner.Utils import I_inf
import TableMiner.LearningPhase.SamplingRanking as Ranking
from TableMiner.Utils import bow, keys_with_max_value, dice_coefficient
import TableMiner.SearchOntology as So
import time
from unidecode import unidecode

class Learning:
    _shared_mapping_id_label = {}

    def __init__(self, dataframe: pd.DataFrame, kb="Wikidata"):
        self._dataframe = dataframe
        self._column = None
        self._winning_entities_dict = {}
        self._rowEntities = {}
        self._conceptScores = {}
        self._onto = So.SearchOntology(kb=kb)
        self._winningConcepts = {}
        self._mapping_id_label = Learning._shared_mapping_id_label

    def get_winning_concepts(self):
        return keys_with_max_value(self._conceptScores)

    def get_concepts(self):
        return self._conceptScores

    def get_mapping_id_label(self):
        return self._mapping_id_label

    def get_column(self):
        return self._column

    def get_Entities(self):
        return [
            entity for entities in self._rowEntities.values() for entity in entities
        ]

    def get_cell_annotation(self):
        return pd.Series(
            list(self._rowEntities.values()), index=self._rowEntities.keys()
        )

    def get_winning_entitiesId(self):
        winning_dict = {}
        for index, entities in self._rowEntities.items():
            for entity in entities:
                winning_dict[entity] = self._winning_entities_dict[entity]["id"]
        return winning_dict

    def update_conceptScores(self, concept, column_name, domain):
        concept_new_score = self.conceptScore(concept, column_name, domain)
        self._conceptScores[concept] = concept_new_score

    def get_concepts(self):
        return list(self._conceptScores.keys())

    def __sampleRank__(self, column_name):
        self._dataframe = Ranking.reorder_dataframe_rows(self._dataframe, column_name)

    def get_dataframe(self):
        return self._dataframe

    def get_column_with_name(self, column_name):
        if column_name not in self._dataframe.columns:
            raise ValueError("column not exists!")
        else:
            self.__sampleRank__(column_name)
            self._column = self._dataframe[column_name]
            return self._dataframe[column_name]

    """
    The following code is for obtaining In-table context, this includes:
        1. Column header 
        2. row context
        3. column context
    """

    def get_column_content(self, current_row_index=None, current_column_name=None):
        """
        find the column content of cell xi,j
        :param current_row_index: xi,j current row index
        :param current_column_name: xi,j current column name
        :return: column_content
        """
        if current_row_index is not None:
            column_content = self._dataframe.loc[
                self._dataframe.index != current_row_index, current_column_name
            ].tolist()
        else:
            column_content = self._dataframe[current_column_name].tolist()
        return " ".join([str(element) for element in column_content])

    def get_row_content(self, current_row_index, current_column_name):
        """
        find the column content of cell xi,j
        :param current_row_index: xi,j current row index
        :param current_column_name:  xi,j current column name
        :return: row_content
        """
        row_content = self._dataframe.loc[
            current_row_index, self._dataframe.columns != current_column_name
        ].tolist()
        return " ".join([str(element) for element in row_content])

    @staticmethod
    def coverage(entity_text, cell_context):
        """
        Calculate the coverage score between the bag-of-words of an entity and a context.
        :param entity_text: The text representing the entity.
        :param cell_context: The text representing the context.
        :return: The coverage score.
        """
        bow_entity_text = bow(entity_text)
        bow_context_text = bow(cell_context)
        # Calculate the intersection of the two bags-of-words
        intersection = set(bow_entity_text) & set(bow_context_text)

        # Calculate the sum of frequencies in the context for the intersection words
        sum_freq_intersection = sum(bow_context_text[word] for word in intersection)

        total_context_words = sum(bow_context_text.values())
        if total_context_words == 0:
            return 0.0

        return sum_freq_intersection / total_context_words

    @staticmethod
    def ec(entity, contexts, overlap, context_weights=None):
        """
        Calculate the entity context score for a candidate entity.

        :param overlap: The overlap function to use (either dice or coverage).
        :param entity: The text related to a candidate entity.
        :param contexts: A list of context texts.
        :param context_weights: A dictionary of weights for each context text, if available.
        :return: The entity context score.
        """
        # If no specific weights are provided, assume equal weight for all contexts
        if context_weights is None:
            context_weights = {context: 1 for context in contexts}

        # Initialize the entity context score
        entity_context_score = 0

        # Iterate over each context
        for context in contexts:
            # Calculate the overlap using the provided function
            overlap_score = overlap(entity, context)

            # Retrieve the weight for this context
            weight = context_weights.get(context, 1)  # Default to 1 if not specified

            # Add the weighted overlap to the entity context score
            entity_context_score += overlap_score * weight

        return entity_context_score

    @staticmethod
    def en(entity, cell_text):
        """
        Calculate the en score using the provided bag-of-words sets.

        Args:
        - entity: entity e.
        - cell_text: table cell content T.

        Returns:
        - float: The calculated en score.
        """
        # Calculate the intersection of the two sets
        bowset_e = bow(entity)
        bowset_T = bow(cell_text)
        intersection = set(bowset_e) & set(bowset_T)
        # Calculate the sum of frequencies in the context for the intersection words
        sum_freq_intersection = sum(bowset_e[word] for word in intersection)
        denominator = sum(bowset_e.values()) + sum(bowset_T.values())
        if denominator == 0:
            en_score = 0  # O cualquier valor por defecto que consideres apropiado
        else:
            en_score = sqrt(2 * sum_freq_intersection / denominator)
        return en_score

    @staticmethod
    def calculate_cf(en_score, ec_score, cell_text):
        """
        Calculate the overall confidence score for an entity.

        :param en_score: The entity name score.
        :param ec_score: The entity context score.
        :param cell_text: T_i,j.
        :return: The overall confidence score.
        """
        # Calculate the number of tokens in bow(T_i,j)
        num_tokens = sum(bow(cell_text).values())
        # Calculate the confidence score cf(e_i,j)
        cf_score = en_score + (ec_score / sqrt(num_tokens))
        return cf_score

    def cellWinningEntity(self, cell, index, column: pd.Series):
        print("Cell winning entity for cell: ", cell)
        winning_concepts_list = list(self.get_winning_concepts())
        print("Winning concepts list: ", winning_concepts_list)

        winningEntity = None
        column_name = column.name
        entity_score = {}
        if index in self._rowEntities.keys():
            # Se entra aca en el PreliminaryCellDisambiguation, y es donde se da el incremento de entities
            print("Entity already exists, augmenting entities")
            (candidate_entities, _) = self._onto.find_candidate_entities(cell)
            print("Candidate entities: ", candidate_entities)
            entities = []
            for entity in candidate_entities:
                concepts_entity = self.candidateConceptGeneration(entity)
                if not set(winning_concepts_list).isdisjoint(set(concepts_entity)):
                    entities.append(entity)

        else:
            # Se entra aca en el ColdStartDisambiguation del PreliminaryColumnClassifiaction, ya que aun no hay ninguna entity para la fila.
            print("Entity does not exist, cold start disambiguation")
            (entities, _) = self._onto.find_candidate_entities(cell)
            print("Candidate entities: ", entities)
        for entity in entities:
            # print("For entity: ", entity)
            # print("Finding entity triples")
            triples = self._onto.find_entity_triple_objects(entity)
            # print("Triples found: ", triples)

            # El find triple retorna las triplas con las property en base a la entity encontrada.
            # TODO: Ver si podemos usarlo mejor (relacionar columnas con properties de las entities)
            if len(triples) > 0:
                print("Triples found for entity: ", entity)
                entity_score[entity] = {}
                rowContent = self.get_row_content(index, column_name)
                columnContent = self.get_column_content(index, column_name)
                ec = self.ec(
                    entity, [column_name, rowContent, columnContent], self.coverage
                )
                en = self.en(entity, cell)
                cf = self.calculate_cf(en, ec, cell)
                entity_score[entity]["score"] = cf
                entity_id = self._onto.get_entity_id(entity)
                entity_score[entity]["id"] = entity_id
                if entity not in self._winning_entities_dict:
                    self._winning_entities_dict[entity] = {
                        "id": entity_score[entity]["id"],
                        "score": entity_score[entity]["score"],
                        "concept": [],
                    }
                if index in self._rowEntities:
                    if entity not in self._rowEntities[index]:
                        self._rowEntities[index].append(entity)
                else:
                    self._rowEntities[index] = [entity]
        print("ENTITY SCORE", entity_score)
        if len(entity_score) > 0:
            winningEntity = max(entity_score, key=lambda k: entity_score[k]["score"])

        return entities

    def candidateConceptGeneration(self, entity):
        # Placeholder: Replace with actual lookup
        concepts_entity = []
        if entity is None:
            return []
        else:
            (concepts_entity, mapping) = self._onto.findConcepts(entity)
            actual_mapping = self.get_mapping_id_label()
            print("mapping actual: ", actual_mapping)
            for label in mapping.keys():
                if label in actual_mapping:
                    actual_mapping[label].extend(mapping[label])
                    actual_mapping[label] = list(set(actual_mapping[label]))
                else:
                    actual_mapping[label] = list(set(mapping[label]))
            self._mapping_id_label = actual_mapping
            print(self._mapping_id_label)

            # Dejo este comment por si es algo útil en el futuro
            """
            if entity in self._winning_entities_dict.keys():
                entity_ids = self._winning_entities_dict[entity]['id']
            else:
                entity_ids = self._onto.get_entity_id(entity)
            for eid in entity_ids:
                concepts = self._onto.findConcepts(eid)
                for concept in concepts:
                    if concept not in concepts_entity:
                        concepts_entity.append(concept)
            """

            print("Candidate concepts for entity: ", entity, " Are: ", concepts_entity)
            if entity in self._winning_entities_dict.keys():
                print("Updating winning entity with concepts")
                self._winning_entities_dict[entity]["concept"] = concepts_entity
        return concepts_entity

    def conceptInstanceScore(self, concept):
        score = 0
        for index, entities in self._rowEntities.items():
            for winning_entity in entities:
                if winning_entity is not None:
                    property_eni = self._winning_entities_dict[winning_entity]
                    concept_row = property_eni["concept"]
                    if concept_row:
                        if concept in concept_row:
                            score += property_eni["score"]
        score = score / len(self._rowEntities)
        return score

    def conceptContextScore(self, concept, column_name):
        concept_context = concept
        uris = self._onto.concept_uris(concept)
        column_content = self.get_column_content(current_column_name=column_name)
        if uris:
            concept_context = concept_context + " " + uris[0]
        ec = self.ec(concept_context, [column_name, column_content], dice_coefficient)
        return ec

    @staticmethod
    def domainConceptScore(concept, domain):
        return sqrt(dice_coefficient(concept, domain))

    def conceptScore(self, concept, column_name, domain=None):
        ce_cj = self.conceptInstanceScore(concept)
        cc_cj = self.conceptContextScore(concept, column_name)
        dc_cj = 0 if domain is None else self.domainConceptScore(concept, domain)
        return ce_cj + cc_cj + dc_cj

    def coldStartDisambiguation(self, cell, index):
        print("Starting cold start disambiguation for cell: ", cell)
        concept_pairs = {}
        if isinstance(cell, float):
            if math.isnan(cell):
                return concept_pairs
        winning_entity = self.cellWinningEntity(cell, index, self._column)
        print("Winning entity: ", winning_entity)

        concepts_found_for_entities = True
        for entity in winning_entity:
            if entity in self._winning_entities_dict:
                if not self._winning_entities_dict[entity]['concept']:
                    concepts_found_for_entities = False
                    break

        # El segundo condicional de self._conceptScores es el que hace que no se entre a este if
        # en el preliminaryColumnClassification, ya que el indice de la fila ya existe en self._rowEntities.

        # El tercer condicional es para cuando entra por primera vez a una celda en la ronda de preliminaryDisambiguation
        # en este caso tiene que buscar conceptos ya que no existen para la entidad
        if (
            index in self._rowEntities.keys()
            and self._conceptScores
            and concepts_found_for_entities
        ):
            # Si es a partir de la segunda vuelta, ya tengo entities generadas entonces solo retorno los conceptos
            print("Round of disambiguation, entity and conceptScores already exists")
            return concept_pairs
        else:
            # Se entra aca en el ColdStartDisambiguation del PreliminaryColumnClassifiaction, ya que aun no hay ninguna entity para la fila.
            print("First round of disambiguation, candidate concepts generation")
            for entity in winning_entity:
                if (
                    entity in self._winning_entities_dict
                    and not self._winning_entities_dict[entity]["concept"]
                ):
                    concepts_entity = self.candidateConceptGeneration(entity)
                    print("The candidate concepts are: ", concepts_entity)
                    print("Calculating the concept scores")
                    if concepts_entity:
                        for cj in concepts_entity:
                            cf_cj = self.conceptScore(cj, self._column.name)
                            concept_pairs[cj] = cf_cj
                    print("Concept pairs concluded: ", concept_pairs)
            return concept_pairs

    def retrieveConceptsFromLLMPrediction(self, llm_concept):
        concept_pairs = {}
        (candidate_entities, mapping) = self._onto.find_llm_concept(llm_concept)
        actual_mapping = self.get_mapping_id_label()
        for label in mapping.keys():
            if label in actual_mapping:
                actual_mapping[label].extend(mapping[label])
                actual_mapping[label] = list(set(actual_mapping[label]))
            else:
                actual_mapping[label] = list(set(mapping[label]))
        self._mapping_id_label = actual_mapping

        print("The concepts are: ", candidate_entities)
        if candidate_entities:
            for cj in candidate_entities:
                # Score = 1 (podríamos calcular otro en base al contenido de la columna?)
                concept_pairs[cj] = 1
        return concept_pairs

    @staticmethod
    def updateCandidateConcepts(current_pairs, concept_pairs):
        for concept, score in concept_pairs.items():
            current_pairs[concept] = score
        return current_pairs

    def preliminaryColumnClassification(self, column_name):
        column = self.get_column_with_name(column_name)
        conceptScores = {}
        print("Starting I_inf for column: ", column_name)
        self._conceptScores = I_inf(
            column,
            conceptScores,
            self.coldStartDisambiguation,
            self.updateCandidateConcepts,
        )
        self._winningConcepts = keys_with_max_value(self._conceptScores)
        print("Winning concepts: ", self._winningConcepts)

    def preliminaryCellDisambiguation(self):
        start_time = time.perf_counter()
        for index, data_item in enumerate(self._column):
            concept_pairs = self.coldStartDisambiguation(data_item, index)
            self._conceptScores = self.updateCandidateConcepts(
                self._conceptScores, concept_pairs
            )
        end_time = time.perf_counter()
        print(f"PreliminaryCellDisambiguation time: {end_time - start_time} sec \n")

        return pd.Series(self._rowEntities.values(), index=self._rowEntities.keys())

    def findConceptsFromLLMPrediction(self, llm_concept):
        start_time = time.perf_counter()
        concept_pairs = self.retrieveConceptsFromLLMPrediction(llm_concept)
        self._conceptScores = self.updateCandidateConcepts(
            self._conceptScores, concept_pairs
        )
        end_time = time.perf_counter()
        print(f"Fallback mechanism time: {end_time - start_time} sec \n")
        return pd.Series(self._rowEntities.values(), index=self._rowEntities.keys())

    def findConceptsFromLLM(self, simple_mode, column_concepts_generator, table, column_name, few_shots_column_concept, table_id=None, metadata=None, additional_info=None):
        concept_pairs = {}
        i = 0
        while not concept_pairs and i < 3:
            if simple_mode: 
                llm_concept = column_concepts_generator.generate_concept(
                            table=table, 
                            column_name=column_name, 
                            few_shots_column_concept=few_shots_column_concept,
                            simple_mode=simple_mode,
                            table_id=None,
                            metadata=None,
                            additional_info=None
                        )
            else:
                llm_concept = column_concepts_generator.generate_concept(
                            table=table, 
                            table_id=table_id, 
                            metadata=metadata, 
                            additional_info=additional_info, 
                            column_name=column_name, 
                            few_shots_column_concept=few_shots_column_concept
                        )
            print("LLMCONCEPT.", llm_concept)
            start_time = time.perf_counter()
            concept_pairs = self.retrieveConceptsFromLLMPrediction(llm_concept)
            i += 1
        llm_concept_cleaned = unidecode(llm_concept)

        print("LLM CONCEPT (original):", llm_concept)
        print("LLM CONCEPT (cleaned):", llm_concept_cleaned)    
        self._conceptScores = self.updateCandidateConcepts(self._conceptScores, concept_pairs)
        end_time = time.perf_counter()
        print(f"Fallback mechanism time: {end_time - start_time} sec \n")
        return pd.Series(self._rowEntities.values(), index=self._rowEntities.keys())
