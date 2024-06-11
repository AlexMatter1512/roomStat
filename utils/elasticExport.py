# script to export data from elasticsearch to csv
from elasticsearch import Elasticsearch
import json

INDICES = ["macs_raw", "envdata"]
# create an Elasticsearch client
try:
    es = Elasticsearch("http://localhost:9200")
    print(es)
except:
    print("Elasticsearch not available.")
    exit(1)

def export_data(index):
    headers = []
    # search for all documents in the index
    res = es.search(index=index, body={"query": {"match_all": {}}}, size=1000)
    print("Got %d Hits:" % res['hits']['total']['value'])
    # print the documents
    with open(index+".csv", 'w') as f:
        for hit in res['hits']['hits']:
            jsonString = str(hit["_source"]).replace("'", "\"")
            jsonLine = json.loads(jsonString)
            # if headers are not set, set them
            if headers == []:
                headers = list(jsonLine.keys())
                # convert headers to csv string
                headerString = (','.join(headers))
                print(headerString)
                f.write(headerString+'\n')
            # convert jsonLine to csv string
            csvLine = (','.join([str(jsonLine[header]) for header in headers]))
            print(csvLine)
            f.write(csvLine+'\n')

if __name__ == '__main__':
    for index in INDICES:
        export_data(index)