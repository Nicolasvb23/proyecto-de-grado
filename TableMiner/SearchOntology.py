import time
import requests
from TableMiner.Utils import nltk_tokenize, tokenize_with_number
from SPARQLWrapper import SPARQLWrapper, JSON


class SearchOntology:
    def __init__(self, kb="Wikidata"):
        self._kb = kb
        self._candidates = []
        if self._kb == "Wikidata":
            self._ontology = SearchWikidata
        else:
            self._ontology = SearchDBPedia

    def get_candidates(self):
        return self._candidates

    def get_entity_id(self, entity_name):
        print("Looking the ID for Entity name", entity_name)
        print("Candidates", self._candidates)
        entities = [i["label"] for i in self._candidates]
        if entity_name not in entities:
            print(entity_name, entities, "Entity not found")
            return []
        else:
            if self._kb != "Wikidata":
                entity_ids = [
                    i["uri"] for i in self._candidates if i["label"] == entity_name
                ]
            else:
                entity_ids = [
                    i["id"] for i in self._candidates if i["label"] == entity_name
                ]
            print("Entity ID found", entity_ids)
            return entity_ids

    def find_candidate_entities(self, cell_content):
        """
        Filters candidate entities based on overlap with cell content.

        Args:
        - cell_content (str): The text content of the cell.
        - candidate_entities (list of str): A list of candidate entity names.

        Returns:
        - list of str: A filtered list of candidate entity names that overlap with cell content.
        """

        # Convert cell content and candidate names to lower case for case-insensitive matching
        id_label_mapping = {}
        lower_content = cell_content.lower()
        cell_content_token = tokenize_with_number(lower_content).split(
            " "
        )  # nltk_tokenize(lower_content)
        # print(cell_content, cell_content_token)
        print("Searching for entities in ontology")
        entities = self._ontology.search(cell_content)  # cell_content
        print("Entities found", entities)
        self._candidates = []
        for candidate in entities or []:
            entity = candidate["label"]
            candidate_token = tokenize_with_number(entity.lower()).split(" ")
            # Check if there's an overlap between the cell content and candidate name
            if set(candidate_token).intersection(cell_content_token):
                # filtered_candidates.append(candidate)
                self._candidates.append(candidate)
        entities = [i["label"] for i in self._candidates]
        for i in self._candidates:
            if i["label"] in id_label_mapping:
                id_label_mapping[i["label"]].append(i["id"])
            else:
                id_label_mapping[i["label"]] = [i["id"]]

        print("Entities found", entities)
        return (list(set(entities)), id_label_mapping)

    def find_llm_concept(self, cell_content):
        """
        Filters candidate entities based on overlap with cell content.

        Args:
        - cell_content (str): The text content of the cell.
        - candidate_entities (list of str): A list of candidate entity names.

        Returns:
        - list of str: A filtered list of candidate entity names that overlap with cell content.
        """

        # Convert cell content and candidate names to lower case for case-insensitive matching
        id_label_mapping = {}
        # print(cell_content, cell_content_token)
        print("Searching for entities in ontology")
        entities = self._ontology.search(cell_content, language="es") #cell_content

        print("Entities found", entities)
        self._candidates = []
        for candidate in entities or []:
            self._candidates.append(candidate)
        entities = [i["label"] for i in self._candidates]
        for i in self._candidates:
            if i["label"] in id_label_mapping:
                id_label_mapping[i["label"]].append(i["id"])
            else:
                id_label_mapping[i["label"]] = [i["id"]]

        print("Entities found", entities)
        return (list(set(entities)), id_label_mapping)

    def find_entity_triple_objects(self, entity_name):
        entity_ids = self.get_entity_id(entity_name)

        candidate_triples = []
        for entity_id in entity_ids:
            triples = self._ontology.retrieve_entity_triples(entity_id)
            if triples is not None:
                for triple in triples:
                    candidate_triples.append(triple["value"])

        return " ".join(candidate_triples)

    def findConcepts(self, cell_content):
        entity_ids = self.get_entity_id(cell_content)
        concepts_all = []
        all_mapping = {}
        for entity_id in entity_ids:
            print("Looking for concepts of entity", entity_id)
            print("Using ontology", self._ontology)
            (concepts, mapping) = self._ontology.retrieve_concepts(entity_id)
            print("mapping", mapping)
            for label in mapping.keys():
                if label in all_mapping:
                    all_mapping[label].extend(mapping[label])
                    all_mapping[label] = list(set(all_mapping[label]))
                else:
                    all_mapping[label] = list(set(mapping[label]))

            print("Concepts found", concepts)
            print("Concepts all", concepts_all)
            if concepts:
                for concept in concepts:
                    if concept not in concepts_all:
                        concepts_all.append(concept)
        print("Concepts found", concepts_all)
        return (concepts_all, all_mapping)

    def concept_uris(self, cell_content):
        return self._ontology.get_concept_uri(cell_content)

    def defnition_sentences(self, cell_uri):
        return self._ontology.get_definitional_sentence(cell_uri)


class SearchWikidata:
    # Count of network calls to the ontology
    amount_of_search = 0
    unique_searches = set()
    searches_dictionary = {}

    amount_of_retrieve_entity_triples = 0
    unique_retrieve_entity_triples = set()
    retrieve_entity_triples_dictionary = {}

    amount_of_retrieve_concepts = 0
    unique_retrieve_concepts = set()
    retrieve_concepts_dictionary = {}

    amount_of_get_concept_uri = 0
    unique_get_concept_uri = set()
    retrieve_concept_uri_dictionary = {}

    amount_of_get_definitional_sentence = 0
    unique_get_definitional_sentence = set()
    retrieve_definitional_sentence_dictionary = {}

    @staticmethod
    def search(cell_content, limit=6, language="es"):
        """
        Search for candidate entities in Wikidata based on the cell text.

        Args:
        - cell_text (str): The text content of the table cell to search for.

        Returns:
        - list: A list of candidate entities with their Wikidata IDs and labels.
        """
        print("Incrementing amount of search", SearchWikidata.amount_of_search)
        SearchWikidata.amount_of_search += 1
        if cell_content in SearchWikidata.searches_dictionary:
            print("Found in dictionary")
            return SearchWikidata.searches_dictionary[cell_content]
        else:
            SearchWikidata.unique_searches.add(cell_content)
            print("Searching for entities in Wikidata, cell content:", cell_content)
            # URL for the Wikidata SPARQL endpoint
            SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"

            # SPARQL query to search for entities with a matching label
            query = f"""
            SELECT ?item ?itemLabel WHERE {{
            SERVICE wikibase:mwapi {{
                bd:serviceParam wikibase:endpoint "www.wikidata.org";
                                wikibase:api "EntitySearch";
                                mwapi:search "{cell_content}";
                                mwapi:language "{language}".
                ?item wikibase:apiOutputItem mwapi:item.
            }}
            SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],{language}". }}
            }}
            LIMIT {limit}
            """

            headers = {
                "User-Agent": "Wikidata Search Python script",
                "Accept": "application/sparql-results+json",
            }

            retries = 0
            while retries < 3:
                try:
                    print("Starting request")
                    start_time = time.time()
                    response = requests.get(
                        SPARQL_ENDPOINT,
                        params={"query": query},
                        headers=headers,
                        timeout=2,
                    )

                    if response.status_code == 429:
                        print(
                            f"Received 429 Too Many Requests. Retrying after backoff..."
                        )
                        time.sleep(retries)  # Exponential backoff
                        retries += 1
                        continue

                    response.raise_for_status()  # Raise an exception for HTTP error codes other than 429

                    data = response.json()
                    print("Request done")
                    print("time taken", time.time() - start_time)
                    print("Response status code", response.status_code)
                    print("Response", data)

                    # Extract the candidate entities
                    candidates = [
                        {
                            "id": binding["item"]["value"].split("/")[-1],
                            "label": binding["itemLabel"]["value"],
                        }
                        for binding in data["results"]["bindings"]
                    ]
                    SearchWikidata.searches_dictionary[cell_content] = candidates
                    return candidates

                except requests.exceptions.HTTPError as err:
                    print(f"HTTP error occurred: {err}")
                    return None
                except Exception as err:
                    print(f"An error occurred: {err}")
                    return None

            print("Max retries reached. Failed to retrieve data.")
            return None

    @staticmethod
    def retrieve_entity_triples(entity_id):
        # print("Incrementing amount of retrieve entity triples", SearchWikidata.amount_of_retrieve_entity_triples)
        SearchWikidata.amount_of_retrieve_entity_triples += 1
        if entity_id in SearchWikidata.retrieve_entity_triples_dictionary:
            # print("Found in dictionary")
            return SearchWikidata.retrieve_entity_triples_dictionary[entity_id]
        else:
            SearchWikidata.unique_retrieve_entity_triples.add(entity_id)
            print("Looking for triples of entity on SPARQL: ID-", entity_id)
            sparql_query = f"""
            SELECT ?property ?propertyLabel ?value ?valueLabel WHERE {{
            BIND(wd:{entity_id} AS ?entity)
            ?entity ?p ?statement .
            ?statement ?ps ?value .
            ?property wikibase:claim ?p.
            ?property wikibase:statementProperty ?ps.
            SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],es". }}
            }}
            """

            retries = 0
            while retries < 3:
                try:
                    # print("SPARQL Query", sparql_query)
                    # print("Starting request")
                    start_time = time.time()
                    url = "https://query.wikidata.org/sparql"
                    response = requests.get(
                        url, params={"query": sparql_query, "format": "json"}
                    )
                    # print("Request done")
                    # print("Time taken", time.time() - start_time)
                    # print("Response status code", response.status_code)

                    if response.status_code == 429:
                        # print(f"Rate limit exceeded. Retrying in {retries} seconds...")
                        time.sleep(retries)
                        retries += 1
                        continue

                    response.raise_for_status()
                    results = response.json()["results"]["bindings"]
                    triples = [
                        {
                            "property": result["propertyLabel"]["value"],
                            "value": result.get("valueLabel", {}).get(
                                "value", result["value"]["value"]
                            ),
                        }
                        for result in results
                    ]
                    SearchWikidata.retrieve_entity_triples_dictionary[entity_id] = (
                        triples
                    )
                    return triples

                except requests.exceptions.RequestException as err:
                    # print(f"An error occurred: {err}")
                    retries += 1
                    time.sleep(retries)

            # print(f"Failed to retrieve data after {3} attempts.")
            return None

    @staticmethod
    def retrieve_concepts(entity_id):
        # wd:%s wdt:P31/wdt:P279* ?concept .
        # wd:%s wdt:P31/wdt:P279?/wdt:P279? ?concept .
        print(
            "Incrementing amount of retrieve concepts",
            SearchWikidata.amount_of_retrieve_concepts,
        )
        SearchWikidata.amount_of_retrieve_concepts += 1
        if entity_id in SearchWikidata.retrieve_concepts_dictionary:
            print("Found in dictionary")
            return (SearchWikidata.retrieve_concepts_dictionary[entity_id], {})
        else:
            id_label_mapping = {}
            SearchWikidata.unique_retrieve_concepts.add(entity_id)
            print("Looking for concepts of entity on SPARQL: ID-", entity_id)
            sparql_query = (
                """
            SELECT ?concept ?conceptLabel WHERE {
            wd:%s wdt:P31/wdt:P279? ?concept .
            SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],es". }
            }
            """
                % entity_id
            )
            url = "https://query.wikidata.org/sparql"
            retries = 0
            while retries < 3:
                try:
                    print("SPARQL Query", sparql_query)
                    print("Starting request")
                    start_time = time.time()
                    response = requests.get(
                        url, params={"query": sparql_query, "format": "json"}
                    )
                    print("Request done")
                    print("Time taken", time.time() - start_time)
                    print("Response status code", response.status_code)

                    if response.status_code == 429:
                        print(f"Rate limit exceeded. Retrying in {retries} seconds...")
                        time.sleep(retries)
                        retries += 1
                        continue

                    response.raise_for_status()
                    results = response.json()["results"]["bindings"]
                    concepts = set()
                    for result in results:
                        concepts.add(result["conceptLabel"]["value"])
                        if result["conceptLabel"]["value"] in id_label_mapping:
                            id_label_mapping[result["conceptLabel"]["value"]].append(
                                result["concept"]["value"]
                            )
                        else:
                            id_label_mapping[result["conceptLabel"]["value"]] = [
                                result["concept"]["value"]
                            ]
                    SearchWikidata.retrieve_concepts_dictionary[entity_id] = concepts
                    return (concepts, id_label_mapping)

                except requests.exceptions.RequestException as err:
                    print(f"An error occurred: {err}")
                    retries += 1
                    time.sleep(retries)

            print(f"Failed to retrieve concepts after {3} attempts.")
            return (set(), {})

    @staticmethod
    def get_concept_uri(concept_label):
        """
        Get the URI of a concept in Wikidata by its label using P279 (subclass of).

        Args:
        - concept_label (str): The label of the concept to search for.

        Returns:
        - list: A list of URIs for the concept found in Wikidata.
        """
        # Endpoint URL for the Wikidata SPARQL service
        print(
            "Incrementing amount of get concept uri",
            SearchWikidata.amount_of_get_concept_uri,
        )
        SearchWikidata.amount_of_get_concept_uri += 1
        if concept_label in SearchWikidata.retrieve_concept_uri_dictionary:
            print("Found in dictionary")
            return SearchWikidata.retrieve_concept_uri_dictionary[concept_label]
        else:
            SearchWikidata.unique_get_concept_uri.add(concept_label)

            SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"

            # SPARQL query to find concepts with the specified label
            sparql_query = f"""
            SELECT ?concept WHERE {{
            ?concept wdt:P279* wd:{concept_label}.
            SERVICE wikibase:label {{ bd:serviceParam wikibase:language "es". }}
            }}
            """

            headers = {
                "User-Agent": "Wikidata SPARQL Python script",
                "Accept": "application/sparql-results+json",
            }

            try:
                # Perform the HTTP request to the SPARQL endpoint
                response = requests.get(
                    SPARQL_ENDPOINT, params={"query": sparql_query}, headers=headers
                )
                response.raise_for_status()  # Raise an exception for HTTP error codes

                # Parse the response to JSON
                data = response.json()

                # Extract the URIs for the concept
                uris = [
                    binding["concept"]["value"]
                    for binding in data["results"]["bindings"]
                ]
                SearchWikidata.retrieve_concept_uri_dictionary[concept_label] = uris
                return uris
            except:
                return []

    @staticmethod
    def get_definitional_sentence(wikidata_id):
        # Define the SPARQL query
        print(
            "Incrementing amount of get definitional sentence",
            SearchWikidata.amount_of_get_definitional_sentence,
        )
        SearchWikidata.amount_of_get_definitional_sentence += 1
        if wikidata_id in SearchWikidata.retrieve_definitional_sentence_dictionary:
            print("Found in dictionary")
            return SearchWikidata.retrieve_definitional_sentence_dictionary[wikidata_id]
        else:
            SearchWikidata.unique_get_definitional_sentence.add(wikidata_id)
            query = (
                """
            SELECT ?entityDescription WHERE {
                wd:""" + wikidata_id + """ schema:description ?entityDescription.
                FILTER(LANG(?entityDescription) = "es")

            }
            """
            )

            url = "https://query.wikidata.org/sparql"
            headers = {
                "User-Agent": "Wikidata SPARQL Python script",
                "Accept": "application/sparql-results+json",
            }

            try:
                response = requests.get(
                    url, headers=headers, params={"query": query, "format": "json"}
                )
                response.raise_for_status()  # This will raise an exception for HTTP errors
                data = response.json()

                results = data.get("results", {}).get("bindings", [])
                if results:
                    SearchWikidata.retrieve_definitional_sentence_dictionary[
                        wikidata_id
                    ] = results[0]["entityDescription"]["value"]
                    description = results[0]["entityDescription"]["value"]
                    return description
                else:
                    return "No description found."
            except requests.exceptions.RequestException as e:
                return f"An error occurred: {e}"

    # Example usage:


# wikidata_id = 'Q183259'  # Wikidata ID for Earth
# print(SearchWikidata.get_definitional_sentence(wikidata_id))


class SearchDBPedia:
    # Count of network calls to the ontology
    amount_of_search = 0
    unique_searches = set()
    searches_dictionary = {}

    amount_of_retrieve_entity_triples = 0
    unique_retrieve_entity_triples = set()
    retrieve_entity_triples_dictionary = {}

    amount_of_retrieve_concepts = 0
    unique_retrieve_concepts = set()
    retrieve_concepts_dictionary = {}

    amount_of_get_concept_uri = 0
    unique_get_concept_uri = set()
    retrieve_concept_uri_dictionary = {}

    amount_of_get_definitional_sentence = 0
    unique_get_definitional_sentence = set()
    retrieve_definitional_sentence_dictionary = {}

    @staticmethod
    def search(cell_content, limit=8):
        """
        Search for entities in DBpedia related to the given text.

        :param cell_content: The text to search for.
        :param limit: Maximum number of results to return.
        :return: A list of matching DBpedia resources.
        """
        print("Incrementing amount of search", SearchDBPedia.amount_of_search)
        SearchDBPedia.amount_of_search += 1
        if cell_content in SearchDBPedia.searches_dictionary:
            print("Found in dictionary")
            return SearchDBPedia.searches_dictionary[cell_content]
        else:
            SearchDBPedia.unique_searches.add(cell_content)

            sparql = SPARQLWrapper("http://dbpedia.org/sparql")
            cell_content_escaped = cell_content.replace("'", r" ")
            query = """
            SELECT DISTINCT ?resource ?label WHERE {
            ?resource_dummy rdfs:label ?label_dummy.
            ?label_dummy bif:contains "'%s'".
            ?resource_dummy dbo:wikiPageRedirects ?resource.
            ?resource rdfs:label ?label.
            FILTER (lang(?label) = 'en')
            } LIMIT %d
            """ % (
                cell_content_escaped,
                limit,
            )  # Simple escaping, more sophisticated escaping may be needed
            try:
                print("Querying for entities in DBpedia, cell content:", cell_content)
                print("SPARQL Query", query)
                sparql.setQuery(query)
                sparql.setReturnFormat(JSON)
                sparql.setTimeout(1)
                results = sparql.query().convert()
                print("Results", results)

                entities = []
                for result in results["results"]["bindings"]:
                    entities.append(
                        {
                            "uri": result["resource"]["value"],
                            "label": result["label"]["value"],
                        }
                    )
                SearchDBPedia.searches_dictionary[cell_content] = entities
                return entities
            except Exception as e:
                print(f"An error occurred: {e}")
                return []

    @staticmethod
    def retrieve_entity_triples(entity_uri, limit=2):
        """
        Retrieve the triples for a given entity URI from DBpedia.

        :param entity_uri: The URI of the entity in DBpedia.
        :return: A list of triples (subject, predicate, object) associated with the entity.
        """

        def simplify_uri(uri):
            """
            Simplifies a URI to its last component which is usually the most meaningful part.

            :param uri: The full URI as a string.
            :return: The simplified version of the URI.
            """
            # Split the URI by '#' and '/' and return the last part
            return uri.split("#")[-1].split("/")[-1]

        def format_triple(predicate, obj):
            """
            Formats the predicate and object of a triple to a more readable form.

            :param predicate: The predicate URI of the triple.
            :param obj: The object URI or literal of the triple.
            :return: A tuple of simplified predicate and object.
            """
            simplified_predicate = simplify_uri(predicate)
            simplified_object = simplify_uri(obj) if obj.startswith("http") else obj
            return simplified_predicate, simplified_object

        print(
            "Incrementing amount of retrieve entity triples",
            SearchDBPedia.amount_of_retrieve_entity_triples,
        )
        SearchDBPedia.amount_of_retrieve_entity_triples += 1
        if entity_uri in SearchDBPedia.retrieve_entity_triples_dictionary:
            print("Found in dictionary")
            return SearchDBPedia.retrieve_entity_triples_dictionary[entity_uri]
        else:
            SearchDBPedia.unique_retrieve_entity_triples.add(entity_uri)

            sparql = SPARQLWrapper("http://dbpedia.org/sparql")
            query = """
            SELECT ?predicate ?object WHERE {
            <%s> ?predicate ?object.
            } LIMIT %d
            """ % (entity_uri, limit)
            try:
                print(
                    "Querying for triples of entity in DBpedia, entity URI:", entity_uri
                )
                print("SPARQL Query", query)
                sparql.setQuery(query)
                sparql.setReturnFormat(JSON)
                sparql.setTimeout(2)
                results = sparql.query().convert()
                print("Results", results)

                triples = []

                for result in results["results"]["bindings"]:
                    property_per, value_per = format_triple(
                        result["predicate"]["value"], result["object"]["value"]
                    )
                    triples.append({"property": property_per, "value": value_per})
                SearchDBPedia.retrieve_entity_triples_dictionary[entity_uri] = triples
                return triples
            except Exception as e:
                print(f"An error occurred: {e}")
                return []

    @staticmethod
    def retrieve_concepts(uri, limit=5):
        """
        Retrieve concepts associated with a given DBpedia entity URI.

        :param uri: The URI of the DBpedia entity.
        :param limit: Maximum number of concepts to return.
        :return: A list of concepts associated with the entity.
        """
        print(
            "Incrementing amount of retrieve concepts",
            SearchDBPedia.amount_of_retrieve_concepts,
        )
        SearchDBPedia.amount_of_retrieve_concepts += 1
        if uri in SearchDBPedia.retrieve_concepts_dictionary:
            print("Found in dictionary")
            return (SearchDBPedia.retrieve_concepts_dictionary[uri], {})
        else:
            SearchDBPedia.unique_retrieve_concepts.add(uri)

            sparql = SPARQLWrapper("http://dbpedia.org/sparql")
            query = """
            SELECT ?type ?typeLabel ?broader ?broaderLabel WHERE {
            {
            <%s> rdf:type ?type . OPTIONAL { ?type rdfs:label ?typeLabel . FILTER (lang(?typeLabel) = 'en') }
            }
            UNION
            {
            <%s> skos:broader ?broader . OPTIONAL { ?broader rdfs:label ?broaderLabel . FILTER (lang(?broaderLabel) = 'en') } }
            } LIMIT %d
            """ % (uri, uri, limit)

            try:
                print("Querying for concepts of entity in DBpedia, entity URI:", uri)
                print("SPARQL Query", query)
                sparql.setQuery(query)
                sparql.setReturnFormat(JSON)
                sparql.setTimeout(1)
                results = sparql.query().convert()
                print("Results", results)

                concepts = []
                for result in results["results"]["bindings"]:
                    if "typeLabel" in result:
                        type_uri = result["typeLabel"]["value"]
                        concepts.append(
                            type_uri.split("/")[-1].split("#")[-1]
                        )  # Get the last part of the URL or fragment
                    if "broaderLabel" in result:
                        broader_uri = result["broaderLabel"]["value"]
                        concepts.append(
                            broader_uri.split("/")[-1].split("#")[-1]
                        )  # Same as above
                SearchDBPedia.retrieve_concepts_dictionary[uri] = concepts
                return (concepts, {})
            except Exception as e:
                print(f"An error occurred: {e}")
                return ([], {})

    @staticmethod
    def get_concept_uri(concept_name):
        """
        Fetches the URI for a concept in DBpedia using the concept name.

        :param concept_name: The name of the concept (e.g., "Python (programming language)").
        :return: The URI of the concept in DBpedia if found, None otherwise.
        """
        print(
            "Incrementing amount of get concept uri",
            SearchDBPedia.amount_of_get_concept_uri,
        )
        SearchDBPedia.amount_of_get_concept_uri += 1
        if concept_name in SearchDBPedia.retrieve_concept_uri_dictionary:
            print("Found in dictionary")
            return SearchDBPedia.retrieve_concept_uri_dictionary[concept_name]
        else:
            SearchDBPedia.unique_get_concept_uri.add(concept_name)

            sparql = SPARQLWrapper("http://dbpedia.org/sparql")
            query = f"""
            SELECT ?concept WHERE {{
                ?concept rdfs:label "{concept_name}"@en.
            }}
            LIMIT 1
            """

            print("Querying for concept URI in DBpedia, concept name:", concept_name)
            print("SPARQL Query", query)
            sparql.setQuery(query)
            sparql.setReturnFormat(JSON)
            sparql.setTimeout(1)

            try:
                results = sparql.query().convert()
                print("Results", results)
                uris = []
                for result in results["results"]["bindings"]:
                    uris.append(result["concept"]["value"])
                SearchDBPedia.retrieve_concept_uri_dictionary[concept_name] = uris
                return uris
            except Exception as e:
                print(f"An error occurred: {e}")
                return []

    @staticmethod
    def get_definitional_sentence(entity_uri, language="en"):
        """
        Fetches the definitional sentence (abstract) of a specified entity from DBpedia based on its URI.

        Parameters:
        - entity_uri: The URI of the entity in DBpedia.
        - language: The language of the abstract (default is English, 'en').

        Returns:
        - The abstract (definitional sentence) of the entity in the specified language, or None if not found.
        """
        print(
            "Incrementing amount of get definitional sentence",
            SearchDBPedia.amount_of_get_definitional_sentence,
        )
        SearchDBPedia.amount_of_get_definitional_sentence += 1
        if entity_uri in SearchDBPedia.retrieve_definitional_sentence_dictionary:
            print("Found in dictionary")
            return SearchDBPedia.retrieve_definitional_sentence_dictionary[entity_uri]
        else:
            SearchDBPedia.unique_get_definitional_sentence.add(entity_uri)

            sparql = SPARQLWrapper("http://dbpedia.org/sparql")
            query = f"""
            SELECT ?abstract WHERE {{
            <{entity_uri}> dbo:abstract ?abstract .
            FILTER (lang(?abstract) = '{language}')
            }}
            LIMIT 1
            """
            try:
                sparql.setQuery(query)
                sparql.setReturnFormat(JSON)
                sparql.setTimeout(1)
                results = sparql.query().convert()
                if results["results"]["bindings"]:
                    SearchDBPedia.retrieve_definitional_sentence_dictionary[
                        entity_uri
                    ] = results["results"]["bindings"][0]["abstract"]["value"]
                    return results["results"]["bindings"][0]["abstract"]["value"]
                else:
                    SearchDBPedia.retrieve_definitional_sentence_dictionary[
                        entity_uri
                    ] = None
                    return None
            except:
                return None
