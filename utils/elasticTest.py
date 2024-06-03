from elasticsearch import Elasticsearch

try:
    es = Elasticsearch("http://localhost:9200")
    # es.indices.create(index='people', ignore=400) Passing transport options in the API method is deprecated. Use 'Elasticsearch.options()' instead.
    # es.options(timeout=30).indices.create(index='people', ignore=400)
    print(es)
except:
    print("Elasticsearch not available.")