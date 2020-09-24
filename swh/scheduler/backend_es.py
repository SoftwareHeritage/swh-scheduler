# Copyright (C) 2018-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Elastic Search backend

"""

from copy import deepcopy
import datetime  # noqa
import logging
from typing import Any, Dict

from elasticsearch import helpers

from swh.core import utils

logger = logging.getLogger(__name__)


DEFAULT_CONFIG = {
    "elasticsearch": {
        "cls": "local",
        "args": {
            "index_name_prefix": "swh-tasks",
            "storage_nodes": ["localhost:9200"],
            "client_options": {
                "sniff_on_start": False,
                "sniff_on_connection_fail": True,
                "http_compress": False,
                "sniffer_timeout": 60,
            },
        },
    }
}


def get_elasticsearch(cls: str, args: Dict[str, Any] = {}):
    """Instantiate an elastic search instance

    """
    if cls == "local":
        from elasticsearch import Elasticsearch
    elif cls == "memory":
        from .elasticsearch_memory import (  # type: ignore  # noqa
            MemoryElasticsearch as Elasticsearch,
        )
    else:
        raise ValueError("Unknown elasticsearch class `%s`" % cls)

    return Elasticsearch(**args)


class ElasticSearchBackend:
    """ElasticSearch backend to index tasks

    This uses an elasticsearch client to actually discuss with the
    elasticsearch instance.

    """

    def __init__(self, **config):
        self.config = deepcopy(DEFAULT_CONFIG)
        self.config.update(config)
        es_conf = self.config["elasticsearch"]
        args = deepcopy(es_conf["args"])
        self.index_name_prefix = args.pop("index_name_prefix")
        self.storage = get_elasticsearch(
            cls=es_conf["cls"],
            args={
                "hosts": args.get("storage_nodes", []),
                **args.get("client_options", {}),
            },
        )
        # document's index type (cf. /data/elastic-template.json)
        self.doc_type = "task"

    def initialize(self):
        self.storage.indices.put_mapping(
            index=f"{self.index_name_prefix}-*",
            doc_type=self.doc_type,
            # to allow type definition below
            include_type_name=True,
            # to allow install mapping even if no index yet
            allow_no_indices=True,
            body={
                "properties": {
                    "task_id": {"type": "double"},
                    "task_policy": {"type": "text"},
                    "task_status": {"type": "text"},
                    "task_run_id": {"type": "double"},
                    "arguments": {
                        "type": "object",
                        "properties": {
                            "args": {"type": "nested", "dynamic": False},
                            "kwargs": {"type": "text"},
                        },
                    },
                    "type": {"type": "text"},
                    "backend_id": {"type": "text"},
                    "metadata": {"type": "object", "enabled": False},
                    "scheduled": {
                        "type": "date",
                        "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||strict_date_optional_time||epoch_millis",  # noqa
                    },
                    "started": {
                        "type": "date",
                        "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||strict_date_optional_time||epoch_millis",  # noqa
                    },
                    "ended": {
                        "type": "date",
                        "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||strict_date_optional_time||epoch_millis",  # noqa
                    },
                }
            },
        )

        self.storage.indices.put_settings(
            index=f"{self.index_name_prefix}-*",
            allow_no_indices=True,
            body={
                "index": {
                    "codec": "best_compression",
                    "refresh_interval": "1s",
                    "number_of_shards": 1,
                }
            },
        )

    def create(self, index_name) -> None:
        """Create and initialize index_name with mapping for all indices
           matching `swh-tasks-` pattern

        """
        assert index_name.startswith(self.index_name_prefix)
        self.storage.indices.create(index_name)

    def compute_index_name(self, year, month):
        """Given a year, month, compute the index's name.

        """
        return "%s-%s-%s" % (self.index_name_prefix, year, "%02d" % month)

    def mget(self, index_name, doc_ids, chunk_size=500, source=True):
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
            source = {"_source": ",".join(source)}
        else:
            source = {"_source": str(source).lower()}

        for ids in utils.grouper(doc_ids, n=1000):
            res = self.storage.mget(
                body={"ids": list(ids)},
                index=index_name,
                doc_type=self.doc_type,
                params=source,
            )
            if not res:
                logger.error("Error during retrieval of data, skipping!")
                continue

            for doc in res["docs"]:
                found = doc.get("found")
                if not found:
                    msg = "Doc id %s not found, not indexed yet" % doc["_id"]
                    logger.warning(msg)
                    continue
                yield doc["_source"]

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
        actions = (
            {
                "_index": index_name,
                "_op_type": "index",
                "_type": self.doc_type,
                "_source": data,
            }
            for data in doc_stream
        )
        for ok, result in helpers.streaming_bulk(
            client=self.storage,
            actions=actions,
            chunk_size=chunk_size,
            raise_on_error=False,
            raise_on_exception=False,
        ):
            if not ok:
                logger.error("Error during %s indexation. Skipping.", result)
                continue
            yield result["index"]["_id"]

    def is_index_opened(self, index_name: str) -> bool:
        """Determine if an index is opened or not

        """
        try:
            self.storage.indices.stats(index_name)
            return True
        except Exception:
            # fails when indice is closed (no other api call found)
            return False

    def streaming_bulk(self, index_name, doc_stream, chunk_size=500, source=True):
        """Bulk index data and returns the successful indexed data as per
           source's setup.

           the `source` permits to retrieve only what's of interest to
           us, e.g:

           - source=True ; gives back the original indexed data
           - source=False ; returns without the original _source field
           - source=['task_id'] ; returns only task_id in the _source field

           Note that:
           - if the index is closed, it will be opened
           - if the index does not exist, it will be created and opened

           This keeps the index opened for performance reasons.

        Args:
            index_name (str): Name of the concerned index.
            doc_stream (generator): Document generator to index
            chunk_size (int): Number of documents chunk to send
            source (bool, [str]): the information to return

        """
        # index must exist
        if not self.storage.indices.exists(index_name):
            self.create(index_name)
        # index must be opened
        if not self.is_index_opened(index_name):
            self.storage.indices.open(index_name)

        indexed_ids = self._streaming_bulk(
            index_name, doc_stream, chunk_size=chunk_size
        )
        yield from self.mget(
            index_name, indexed_ids, chunk_size=chunk_size, source=source
        )
