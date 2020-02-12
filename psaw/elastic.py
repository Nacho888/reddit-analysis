import uuid
import logging
#####
import logging_factory
#####
from elasticsearch import Elasticsearch, helpers, ElasticsearchException
#####
logger = logging_factory.get_module_logger("elastic", logging.DEBUG)
logger_err = logging_factory.get_module_logger("elastic", logging.ERROR)


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


def index_data(host, port, data, _index, _type):
    """ Function to index the data with ElasticSearch

        Parameters:
            host -- string
                host to connect
            port -- string
                port to connect
            data -- list
                list of documents
            _index -- string
                the index name
            _type -- string
                the document type
    """

    try:
        logger.debug("Trying to establish connection with ElasticSearch: host '{}' - port '{}'".format(host, port))
        es = Elasticsearch([{"host": host, "port": port}])

        # Create index (if not exists)
        if not es.indices.exists(index=_index):
            es.indices.create(index=_index, ignore=[400, 404])

        # Load data
        try:
            logger.debug('Trying to index with ElasticSearch - helpers.bulk()')
            resp = helpers.bulk(es, setup_for_index(data, _index, _type), index=_index, doc_type=_type)
            logger.debug('helpers.bulk() - OK - RESPONSE:', resp)
        except ElasticsearchException as err:
            logger_err.error('helpers.bulk() - ERROR:', err)
            quit()
    except ElasticsearchException:
        logger_err.error("ElasticSearch client problem (check if open)")
        quit()
