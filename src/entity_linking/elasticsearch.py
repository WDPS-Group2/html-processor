import requests


def search(domain, query):
    url = 'http://%s/freebase/label/_search' % domain
    try:
        response = requests.get(url, params={'q': query, 'size':10})
    except Exception as e:
        return {}
    id_labels = {}

    if response:
        response = response.json()
        for hit in response.get('hits', {}).get('hits', []):
            freebase_label = hit.get('_source', {}).get('label')
            freebase_id = hit.get('_source', {}).get('resource')
            id_labels.setdefault(freebase_id, set()).add( freebase_label )
    return id_labels

