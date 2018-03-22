# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


"""Elastic Search backend

"""


from swh.core.config import SWHConfig
from elasticsearch import Elasticsearch
# from elasticsearch.helpers import streaming_bulk


class SWHElasticSearchClient(SWHConfig):
    DEFAULT_BASE_FILENAME = 'backend/elastic'

    DEFAULT_CONFIG = {
        'storage_nodes': ('[dict]', [{'host': 'localhost', 'port': 9200}]),
        'index_name_prefix': ('str', 'swh-tasks'),
        'client_options': ('dict', {
            'sniff': True,
            'http_compress': False,
        })
    }

    def __init__(self, **config):
        if config:
            self.config = config
        else:
            self.config = self.parse_config_file()

        options = self.config['client_options']
        sniff = options['sniff']
        self.storage = Elasticsearch(
            # nodes to use by default
            self.config['storage_nodes'],
            # auto detect cluster's status
            sniff_on_start=sniff, sniff_on_connection_fail=sniff,
            sniffer_timeout=60,
            # compression or not
            http_compress=options['http_compress'])
        self.index_name_prefix = self.config['index_name_prefix']

    def _index_name(self, year, month):
        return '%s-%s-%s' % (
            self.index_name_prefix, year, '%02d' % month)

    def index(self, data):
        """Index given data to elasticsearch.

        The field 'ended' in data is used to compute the index to
        index data to.

        """
        date = data['ended']
        index_name = self._index_name(date.year, date.month)
        return self.storage.index(index=index_name,
                                  doc_type='task', body=data)

    # def bulk(self, data_stream):
    #     yield from streaming_bulk(self, actions=data_stream)

    def list(self, year, month):
        """List the current index's content.

        """
        index_name = self._index_name(year, month)
        return self.storage.search(index=index_name,
                                   body={"query": {"match_all": {}}})
