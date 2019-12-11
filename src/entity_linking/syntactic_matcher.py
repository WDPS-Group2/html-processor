import requests

# TODO: Extract to environment variables
EL_HOST = "10.141.0.51"
EL_PORT = "9200"
EL_ENDPOINT = "/freebase/label/_search"
EL_MAX_SIZE = 20


class FreebaseItem:
    def __init__(self, freebase_id, freebase_label, freebase_score):
        self.freebase_id = freebase_id
        self.freebase_label = freebase_label
        self.freebase_score = freebase_score


def extract_matches_from_es_response(entity, es_response):
    if not es_response:
        print("Could not obtain any response from elasticsearch for entity: %s" % entity)
        return {}

    try:
        es_response_json = es_response.json()
    except Exception as e:
        print("Could not parse json elasticsearch response", e)
        return {}

    matches = []
    for hit in es_response_json.get('hits', {}).get('hits', []):
        freebase_label = hit.get('_source', {}).get('label')
        freebase_id = hit.get('_source', {}).get('resource')
        freebase_score = hit.get('_score')

        matches.append(FreebaseItem(freebase_id, freebase_label, freebase_score))

    return matches


def query_elasticsearch_for_candidate_entities(entity):
    url = "http://{}:{}{}".format(EL_HOST, EL_PORT, EL_ENDPOINT)

    try:
        response = requests.get(url, params={'q': entity, 'size': EL_MAX_SIZE})
        return extract_matches_from_es_response(entity, response)
    except Exception as e:
        print(e)
        return {}


if __name__ == '__main__':
    matches = query_elasticsearch_for_candidate_entities("batman")
    print(matches)
