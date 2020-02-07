import uuid
from elasticsearch import Elasticsearch, helpers, ElasticsearchException


def setup_for_index(data, _index, _type):
    """ Function to add the necessary fields to index with ElasticSearch

        Parameters:
            data -- list
                list of documents
            _index -- string
                the index name
            _type -- string
                the document type

        Returns:
            yield -- generator/iterator
                the documents
    """

    for doc in data:
        yield {
            "_index": _index,
            "_id": uuid.uuid4(),
            "_type": _type,
            "_source": doc
        }


def index_data(data, _index, _type):
    """ Function to index the data with ElasticSearch

        Parameters:
            data -- list
                list of documents
            _index -- string
                the index name
            _type -- string
                the document type
    """

    try:
        es = Elasticsearch([{'host': 'localhost', 'port': '9200'}])

        # Create index (if not exists)
        if not es.indices.exists(index=_index):
            es.indices.create(index=_index, ignore=[400, 404])

        # Load data
        try:
            # TODO: move prints to log
            # print('\nIndexing using Elasticsearch - helpers.bulk()')
            resp = helpers.bulk(es, setup_for_index(data, _index, _type), index=_index, doc_type=_type)
            # print('helpers.bulk() - OK - RESPONSE:', resp)
        except ElasticsearchException as err:
            # print('helpers.bulk() - ERROR:', err)
            quit()
    except ConnectionRefusedError as err:
        print("You have to open ElasticSearch client!")
        pass
