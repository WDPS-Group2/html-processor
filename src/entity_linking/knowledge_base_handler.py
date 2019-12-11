import requests

QUERY = """
select distinct ?abstract where {
    ?s <http://www.w3.org/2002/07/owl#sameAs> <http://rdf.freebase.com/ns/%s> .
    ?s <http://www.w3.org/2002/07/owl#sameAs> ?o .
    ?o <http://dbpedia.org/ontology/abstract> ?abstract .
}"""

TRIDENT_SPARQL_ENDPOINT = "/sparql"


def query_trident_for_abstract_content(trident_host, trident_port, freebase_id):
    key = freebase_id[1:].replace("/", ".")
    q = QUERY % key
    url = "http://{}:{}{}".format(trident_host, trident_port, TRIDENT_SPARQL_ENDPOINT)

    try:
        response = requests.post(url, data={'print': True, 'query': q})
        response = response.json() if response else None
    except Exception as e:
        print("Exception when querying Trident: ", e)
        return None

    if not response:
        print("Could not parse response for query: %s", q)
        return None

    for binding in response.get('results', {}).get('bindings', []):
        abstract = binding.get('abstract', {}).get('value')
        if abstract[-3:-1] == "en":
            return abstract


if __name__ == '__main__':
    host = "localhost"
    port = "9200"
    freebase_id = "test"

    query_response = query_trident_for_abstract_content(host, port, freebase_id)
    print(query_response)




