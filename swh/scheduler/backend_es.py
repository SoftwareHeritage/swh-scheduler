# Copyright (C) 2018-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Elastic Search backend

"""

import logging

from copy import deepcopy

from swh.core import utils
from elasticsearch import Elasticsearch
from elasticsearch import helpers

logger = logging.getLogger(__name__)


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


class ElasticSearchBackend:
    """ElasticSearch backend to index tasks

    """
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

    def mget(self, index_name, doc_ids, chunk_size=500,
             source=True):
        """Retrieve document's full content according to their ids as per
           source's setup.

           The `source` allows to retrieve only what's interesting, e.g:
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
                logger.error('Error during retrieval of data, skipping!')
                continue

            for doc in res['docs']:
                found = doc.get('found')
                if not found:
                    msg = 'Doc id %s not found, not indexed yet' % doc['_id']
                    logger.warning(msg)
                    continue
                yield doc['_source']

    def _streaming_bulk(self, index_name, doc_stream, chunk_size=500):
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
                logger.error('Error during %s indexation. Skipping.', result)
                continue
            yield result['index']['_id']

    def is_index_opened(self, index_name: str) -> bool:
        """Determine if an index is opened or not

        """
        try:
            self.storage.indices.stats(index_name)
            return True
        except Exception:
            # fails when indice is closed (no other api call found)
            return False

    def streaming_bulk(self, index_name, doc_stream, chunk_size=500,
                       source=True):
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
        to_close = False
        # index must exist
        if not self.storage.indices.exists(index_name):
            # server is setup-ed correctly (mappings, settings are
            # automatically set, cf. /data/README.md)
            self.storage.indices.create(index_name)
            # Close that new index (to avoid too much opened indices)
            to_close = True
        # index must be opened
        if not self.is_index_opened(index_name):
            to_close = True
            self.storage.indices.open(index_name)

        try:
            indexed_ids = self._streaming_bulk(
                index_name, doc_stream, chunk_size=chunk_size)
            yield from self.mget(
                index_name, indexed_ids, chunk_size=chunk_size, source=source)
        finally:
            # closing it to stay in the same state as prior to the call
            if to_close:
                self.storage.indices.close(index_name)
