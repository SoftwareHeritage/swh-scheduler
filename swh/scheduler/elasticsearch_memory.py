# Copyright (C) 2018-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Memory Elastic Search backend

"""

from ast import literal_eval
import datetime  # noqa serialization purposes
import hashlib
import logging
from typing import Optional

import psycopg2  # noqa serialization purposes

logger = logging.getLogger(__name__)


class BasicSerializer:
    """For memory elastic search implementation (not for production)

    """

    def __init__(self, *args, **kwargs):
        pass

    def dumps(self, *args, **kwargs):
        return str(*args)


class BasicTransport:
    """For memory elastic search implementation, (not for production)

    """

    def __init__(self, *args, **kwargs):
        self.serializer = BasicSerializer()


class MemoryElasticsearch:
    """Memory Elasticsearch instance (for test purposes)

    Partial implementation oriented towards index storage (and not search)

    For now, its sole client is the scheduler for task archival purposes.

    """

    def __init__(self, *args, **kwargs):
        self.index = {}
        self.mapping = {}
        self.settings = {}
        self.indices = self  # HACK
        self.main_mapping_key: Optional[str] = None
        self.main_settings_key: Optional[str] = None
        self.transport = BasicTransport()

    def create(self, index, **kwargs):
        logger.debug(f"create index {index}")
        logger.debug(f"indices: {self.index}")
        logger.debug(f"mapping: {self.mapping}")
        logger.debug(f"settings: {self.settings}")
        self.index[index] = {
            "status": "opened",
            "data": {},
            "mapping": self.get_mapping(self.main_mapping_key),
            "settings": self.get_settings(self.main_settings_key),
        }
        logger.debug(f"index {index} created")

    def close(self, index, **kwargs):
        """Close index"""
        idx = self.index.get(index)
        if idx:
            idx["status"] = "closed"

    def open(self, index, **kwargs):
        """Open index"""
        idx = self.index.get(index)
        if idx:
            idx["status"] = "opened"

    def bulk(self, body, **kwargs):
        """Bulk insert document in index"""
        assert isinstance(body, str)
        all_data = body.split("\n")
        if all_data[-1] == "":
            all_data = all_data[:-1]  # drop the empty line if any
        ids = []
        # data is sent as tuple (index, data-to-index)
        for i in range(0, len(all_data), 2):
            # The first entry is about the index to use
            # not about a data to index
            # find the index
            index_data = literal_eval(all_data[i])
            idx_name = index_data["index"]["_index"]
            # associated data to index
            data = all_data[i + 1]
            _id = hashlib.sha1(data.encode("utf-8")).hexdigest()
            parsed_data = eval(data)  # for datetime
            self.index[idx_name]["data"][_id] = parsed_data
            ids.append(_id)

        # everything is indexed fine
        return {"items": [{"index": {"status": 200, "_id": _id,}} for _id in ids]}

    def mget(self, *args, body, index, **kwargs):
        """Bulk indexed documents retrieval"""
        idx = self.index[index]
        docs = []
        idx_docs = idx["data"]
        for _id in body["ids"]:
            doc = idx_docs.get(_id)
            if doc:
                d = {
                    "found": True,
                    "_source": doc,
                }
                docs.append(d)
        return {"docs": docs}

    def stats(self, index, **kwargs):
        idx = self.index[index]  # will raise if it does not exist
        if not idx or idx["status"] == "closed":
            raise ValueError("Closed index")  # simulate issue if index closed

    def exists(self, index, **kwargs):
        return self.index.get(index) is not None

    def put_mapping(self, index, body, **kwargs):
        self.mapping[index] = body
        self.main_mapping_key = index

    def get_mapping(self, index, **kwargs):
        return self.mapping.get(index) or self.index.get(index, {}).get("mapping", {})

    def put_settings(self, index, body, **kwargs):
        self.settings[index] = body
        self.main_settings_key = index

    def get_settings(self, index, **kwargs):
        return self.settings.get(index) or self.index.get(index, {}).get("settings", {})
