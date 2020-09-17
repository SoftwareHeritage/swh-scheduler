# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import hashlib
import logging
import random
from typing import Any, Dict

import pytest

from swh.scheduler.elasticsearch_memory import BasicSerializer, BasicTransport

from ..common import TEMPLATES, tasks_from_template

logger = logging.getLogger(__name__)


def test_serializer():
    s = BasicSerializer()
    assert s

    data = {"something": [1, 2, 3], "cool": {"1": "2"}}
    actual_data = s.dumps(data)

    assert isinstance(actual_data, str)
    assert actual_data == str(data)


def test_basic_transport():
    b = BasicTransport()
    assert b

    assert isinstance(b.serializer, BasicSerializer)


def test_index_manipulation(swh_elasticsearch_memory):
    index_name = "swh-tasks-xxxx"
    indices = swh_elasticsearch_memory.index

    assert not swh_elasticsearch_memory.exists(index_name)
    assert index_name not in indices

    # so stat raises
    with pytest.raises(Exception):
        swh_elasticsearch_memory.stats(index_name)

    # we create the index
    swh_elasticsearch_memory.create(index_name)

    # now the index exists
    assert swh_elasticsearch_memory.exists(index_name)
    assert index_name in indices
    # it's opened
    assert indices[index_name]["status"] == "opened"

    # so stats is happy
    swh_elasticsearch_memory.stats(index_name)

    # open the index, nothing changes
    swh_elasticsearch_memory.open(index_name)
    assert indices[index_name]["status"] == "opened"

    # close the index
    swh_elasticsearch_memory.close(index_name)

    assert indices[index_name]["status"] == "closed"

    # reopen the index (fun times)
    swh_elasticsearch_memory.open(index_name)
    assert indices[index_name]["status"] == "opened"


def test_bulk_and_mget(swh_elasticsearch_memory):
    # initialize tasks
    template_git = TEMPLATES["git"]
    next_run_start = datetime.datetime.utcnow() - datetime.timedelta(days=1)

    tasks = tasks_from_template(template_git, next_run_start, 100)

    def compute_id(stask):
        return hashlib.sha1(stask.encode("utf-8")).hexdigest()

    body = []
    ids_to_task = {}
    for task in tasks:
        date = task["next_run"]
        index_name = f"swh-tasks-{date.year}-{date.month}"
        idx = {"index": {"_index": index_name}}
        sidx = swh_elasticsearch_memory.transport.serializer.dumps(idx)
        body.append(sidx)

        stask = swh_elasticsearch_memory.transport.serializer.dumps(task)
        body.append(stask)

        _id = compute_id(stask)
        ids_to_task[_id] = task
        logger.debug(f"_id: {_id}, task: {task}")

    # store

    # create the index first
    swh_elasticsearch_memory.create(index_name)

    # then bulk insert new data
    result = swh_elasticsearch_memory.bulk("\n".join(body))

    # no guarantee in the order
    assert result
    actual_items = result["items"]
    assert len(actual_items) == len(ids_to_task)

    def get_id(data: Dict[str, Any]) -> str:
        return data["index"]["_id"]

    actual_items = sorted(actual_items, key=get_id)

    expected_items = {
        "items": [{"index": {"status": 200, "_id": _id}} for _id in list(ids_to_task)]
    }

    expected_items = sorted(expected_items["items"], key=get_id)
    assert actual_items == expected_items

    # retrieve

    nb_docs = 10
    ids = list(ids_to_task)
    random_ids = []
    # add some inexistent ids
    for i in range(16):
        noisy_id = f"{i}" * 40
        random_ids.append(noisy_id)
    random_ids.extend(random.sample(ids, nb_docs))  # add relevant ids
    for i in range(16, 32):
        noisy_id = f"{i}" * 40
        random_ids.append(noisy_id)

    result = swh_elasticsearch_memory.mget(index=index_name, body={"ids": random_ids})
    assert result["docs"]
    assert len(result["docs"]) == nb_docs, "no random and inexistent id found"
    for doc in result["docs"]:
        assert doc["found"]

        actual_task = doc["_source"]
        _id = compute_id(str(actual_task))
        expected_task = ids_to_task[_id]
        assert actual_task == expected_task
