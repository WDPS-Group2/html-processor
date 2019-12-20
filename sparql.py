import requests

QUERY = """
    select distinct ?abstract where {
    ?s <http://www.w3.org/2002/07/owl#sameAs> <http://rdf.freebase.com/ns/%s> .
    ?s <http://www.w3.org/2002/07/owl#sameAs> ?o .
    ?o <http://dbpedia.org/ontology/abstract> ?abstract .
}"""


def sparql(domain, query):
    url = 'http://%s/sparql' % domain
    print("Querying trident with sparkql: %s" % query)
    response = requests.post(url, data={'print': True, 'query': query})
    if response:
        try:
            response = response.json()
            print("Got response:", response)
            for binding in response.get('results', {}).get('bindings', []):
                abstract = binding.get('abstract', {}).get('value')

                if abstract[-3:-1] == "en":
                    return abstract
        except Exception as e:
            print(e)
            raise e
    else:
        print("Could not get response from Trident", response.status_code, response.content)


def query_abstract(domain, freebaseId):
    key = freebaseId[1:].replace("/", ".")
    q = QUERY % key
    try:
        abstract = sparql(domain, q)
        return abstract
    except Exception as e:
        print(e)
        return None
