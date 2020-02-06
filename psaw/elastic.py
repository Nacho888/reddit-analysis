import requests

from elasticsearch import Elasticsearch, helpers, ElasticsearchException


def index_data(data, index_name, doc_type):
    try:
        res = requests.get('http://localhost:9200')
        print('\n')
        print(res.content)
        es = Elasticsearch([{'host': 'localhost', 'port': '9200'}])

        # # Create index (if not exists)
        # if not es.indices.exists(index=index_name):
        #     es.indices.create(index=index_name, ignore=[400, 404])

        # Load data
        try:
            print('\nIndexing using Elasticsearch - helpers.bulk()')
            resp = helpers.bulk(es, data, index=index_name, doc_type=doc_type)
            print('helpers.bulk() - OK - RESPONSE:', resp)
        except ElasticsearchException as err:
            print('helpers.bulk() - ERROR:', err)
            quit()
    except ConnectionRefusedError as err:
        print("You have to open ElasticSearch client!")
        pass
