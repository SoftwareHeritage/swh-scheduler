# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


"""Elastic Search backend

"""
from copy import deepcopy

from swh.core import utils
from elasticsearch import Elasticsearch
from elasticsearch import helpers


DEFAULT_CONFIG = {
    'elastic_search': {
        'storage_nodes': {'host': 'localhost', 'port': 9200},
        'index_name_prefix': 'swh-tasks',
        'client_options': {
            'sniff_on_start': False,
            'sniff_on_connection_fail': True,
            'http_compress': False,
        },
    },
}


class SWHElasticSearchClient:
    def __init__(self, **config):
        self.config = deepcopy(DEFAULT_CONFIG)
        self.config.update(config)
        es_conf = self.config['elastic_search']
        options = es_conf.get('client_options', {})
        self.storage = Elasticsearch(
            # nodes to use by default
            es_conf['storage_nodes'],
            # auto detect cluster's status
            sniff_on_start=options['sniff_on_start'],
            sniff_on_connection_fail=options['sniff_on_connection_fail'],
            sniffer_timeout=60,
            # compression or not
            http_compress=options['http_compress'])
        self.index_name_prefix = es_conf['index_name_prefix']
        # document's index type (cf. ../../data/elastic-template.json)
        self.doc_type = 'task'

    def compute_index_name(self, year, month):
        """Given a year, month, compute the index's name.

        """
        return '%s-%s-%s' % (
            self.index_name_prefix, year, '%02d' % month)

    def index(self, data):
        """Index given data to elasticsearch.

        The field 'ended' in data is used to compute the index to
        index data to.

        """
        date = data['ended']
        index_name = self.compute_index_name(date.year, date.month)
        return self.storage.index(index=index_name,
                                  doc_type=self.doc_type,
                                  body=data)

    def mget(self, index_name, doc_ids, chunk_size=500,
             source=True, log=None):
        """Retrieve document's full content according to their ids as per
           source's setup.

           The `source` permits to retrieve only what's of interest to
           us, e.g:
           - source=True ; gives back the original indexed data
           - source=False ; returns without the original _source field
           - source=['task_id'] ; returns only task_id in the _source field

        Args:
            index_name (str): Name of the concerned index.
            doc_ids (generator): Generator of ids to retrieve
            chunk_size (int): Number of documents chunk to send for retrieval
            source (bool/[str]): Source of information to return

        Yields:
            document indexed as per source's setup

        """
        if isinstance(source, list):
            source = {'_source': ','.join(source)}
        else:
            source = {'_source': str(source).lower()}

        for ids in utils.grouper(doc_ids, n=1000):
            res = self.storage.mget(body={'ids': list(ids)},
                                    index=index_name,
                                    doc_type=self.doc_type,
                                    params=source)
            if not res:
                if log:
                    log.error('Error during retrieval of data, skipping!')
                continue

            for doc in res['docs']:
                found = doc.get('found')
                if not found:
                    msg = 'Doc id %s not found, not indexed yet' % doc['_id']
                    if log:
                        log.warning(msg)
                    continue
                yield doc['_source']

    def _streaming_bulk(self, index_name, doc_stream, chunk_size=500,
                        log=None):
        """Bulk index data and returns the successful indexed data's
           identifier.

        Args:
            index_name (str): Name of the concerned index.
            doc_stream (generator): Generator of documents to index
            chunk_size (int): Number of documents chunk to send for indexation

        Yields:
            document id indexed

        """
        actions = ({'_index': index_name,
                    '_op_type': 'index',
                    '_type': self.doc_type,
                    '_source': data} for data in doc_stream)
        for ok, result in helpers.streaming_bulk(client=self.storage,
                                                 actions=actions,
                                                 chunk_size=chunk_size,
                                                 raise_on_error=False,
                                                 raise_on_exception=False):
            if not ok:
                if log:
                    log.error('Error during %s indexation. Skipping.' % result)
                continue
            yield result['index']['_id']

    def streaming_bulk(self, index_name, doc_stream, chunk_size=500,
                       source=True, log=None):
        """Bulk index data and returns the successful indexed data as per
           source's setup.

           the `source` permits to retrieve only what's of interest to
           us, e.g:

           - source=True ; gives back the original indexed data
           - source=False ; returns without the original _source field
           - source=['task_id'] ; returns only task_id in the _source field

        Args:
            index_name (str): Name of the concerned index.
            doc_stream (generator): Document generator to index
            chunk_size (int): Number of documents chunk to send
            source (bool, [str]): the information to return

        """

        indexed_ids = self._streaming_bulk(
            index_name, doc_stream, chunk_size=chunk_size, log=log)
        yield from self.mget(index_name, indexed_ids, chunk_size=chunk_size,
                             source=source, log=log)
