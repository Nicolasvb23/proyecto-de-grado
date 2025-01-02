from TableMiner.SearchOntology import SearchWikidata
from d3l.utils.functions import pickle_python_object, unpickle_python_object
import os
import json

class OntologyRequestHandler:
    def __init__(self, dict_path, dict_name):
        self.dict_path = dict_path
        self.dict_name = dict_name
        self.target_file = os.path.join(self.dict_path, self.dict_name)

    @staticmethod
    def pretty_print_json(loaded_json):
        """Imprime en formato bonito un JSON."""
        print(json.dumps(loaded_json, indent=2, ensure_ascii=False))

    @staticmethod
    def merge_dicts(dict1, dict2):
        """Mergea dos diccionarios. Los valores de dict1 sobrescriben a los de dict2 en caso de colisión."""
        return {**dict2, **dict1}

    def load_ontology_requests(self):
        """Carga las solicitudes guardadas en un archivo pickle al diccionario de solicitudes de SearchWikidata."""
        if not os.path.isfile(self.target_file):
            return {}

        request_cache = unpickle_python_object(self.target_file)

        SearchWikidata.searches_dictionary = self.merge_dicts(request_cache.get('searches', {}), SearchWikidata.searches_dictionary)
        SearchWikidata.retrieve_entity_triples_dictionary = self.merge_dicts(request_cache.get('retrieve_entity_triples', {}), SearchWikidata.retrieve_entity_triples_dictionary)
        SearchWikidata.retrieve_concepts_dictionary = self.merge_dicts(request_cache.get('retrieve_concepts', {}), SearchWikidata.retrieve_concepts_dictionary)
        SearchWikidata.retrieve_concept_uri_dictionary = self.merge_dicts(request_cache.get('get_concept_uri', {}), SearchWikidata.retrieve_concept_uri_dictionary)
        SearchWikidata.retrieve_definitional_sentence_dictionary = self.merge_dicts(request_cache.get('get_definitional_sentence', {}), SearchWikidata.retrieve_definitional_sentence_dictionary)

        return request_cache

    def store_ontology_requests(self):
        """Guarda las solicitudes cacheadas de la ontología en un archivo pickle."""
        if not os.path.exists(self.target_file):
            saved_requests = {
                'searches': {},
                'retrieve_entity_triples': {},
                'retrieve_concepts': {},
                'get_concept_uri': {},
                'get_definitional_sentence': {}
            }
        else:
            saved_requests = self.load_ontology_requests()

        request_caching = {
            'searches': self.merge_dicts(SearchWikidata.searches_dictionary, saved_requests.get('searches', {})),
            'retrieve_entity_triples': self.merge_dicts(SearchWikidata.retrieve_entity_triples_dictionary, saved_requests.get('retrieve_entity_triples', {})),
            'retrieve_concepts': self.merge_dicts(SearchWikidata.retrieve_concepts_dictionary, saved_requests.get('retrieve_concepts', {})),
            'get_concept_uri': self.merge_dicts(SearchWikidata.retrieve_concept_uri_dictionary, saved_requests.get('get_concept_uri', {})),
            'get_definitional_sentence': self.merge_dicts(SearchWikidata.retrieve_definitional_sentence_dictionary, saved_requests.get('get_definitional_sentence', {}))
        }

        pickle_python_object(request_caching, self.target_file)

    def display_network_calls(self):
        """Imprime estadísticas sobre las solicitudes de red de SearchWikidata."""
        print("Network Calls")
        print("Amount of searches", SearchWikidata.amount_of_search)
        print("Amount of unique searches", len(SearchWikidata.unique_searches), "\n", SearchWikidata.unique_searches, "\n")

        print("Amount of retrieve entity triples", SearchWikidata.amount_of_retrieve_entity_triples)
        print("Amount of unique entity triples", len(SearchWikidata.unique_retrieve_entity_triples), "\n", SearchWikidata.unique_retrieve_entity_triples, "\n")

        print("Amount of retrieve concepts", SearchWikidata.amount_of_retrieve_concepts)
        print("Amount of unique concepts", len(SearchWikidata.unique_retrieve_concepts), "\n", SearchWikidata.unique_retrieve_concepts, "\n")

        print("Amount of concept uri", SearchWikidata.amount_of_get_concept_uri)
        print("Amount of unique concept uri", len(SearchWikidata.unique_get_concept_uri), "\n", SearchWikidata.unique_get_concept_uri, "\n")

        print("Amount of definitional sentences", SearchWikidata.amount_of_get_definitional_sentence)
        print("Amount of unique definitional sentences", len(SearchWikidata.unique_get_definitional_sentence), "\n", SearchWikidata.unique_get_definitional_sentence, "\n")

