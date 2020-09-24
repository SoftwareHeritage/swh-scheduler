# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime

import elasticsearch
import pytest

from swh.scheduler.backend_es import get_elasticsearch

from ..common import TEMPLATES, tasks_from_template


def test_get_elasticsearch():
    with pytest.raises(ValueError, match="Unknown elasticsearch class"):
        get_elasticsearch("unknown")

    es = get_elasticsearch("memory")
    assert es
    from swh.scheduler.elasticsearch_memory import MemoryElasticsearch

    assert isinstance(es, MemoryElasticsearch)

    es = get_elasticsearch("local")
    assert es
    assert isinstance(es, elasticsearch.Elasticsearch)


def test_backend_setup_basic(swh_elasticsearch_backend):
    """Elastic search instance should allow to create/close/check index

    """
    index_name = "swh-tasks-2010-01"
    try:
        swh_elasticsearch_backend.storage.indices.get_mapping(index_name)
    except (elasticsearch.exceptions.NotFoundError, KeyError):
        pass

    assert not swh_elasticsearch_backend.storage.indices.exists(index_name)
    swh_elasticsearch_backend.create(index_name)
    assert swh_elasticsearch_backend.storage.indices.exists(index_name)
    assert swh_elasticsearch_backend.is_index_opened(index_name)

    # index exists with a mapping
    mapping = swh_elasticsearch_backend.storage.indices.get_mapping(index_name)
    assert mapping != {}


def test_backend_setup_index(swh_elasticsearch_backend):
    """Elastic search instance should allow to bulk index

    """
    template_git = TEMPLATES["git"]
    next_run_date = datetime.datetime.utcnow() - datetime.timedelta(days=1)
    tasks = tasks_from_template(template_git, next_run_date, 1)
    index_name = swh_elasticsearch_backend.compute_index_name(
        next_run_date.year, next_run_date.month
    )
    assert not swh_elasticsearch_backend.storage.indices.exists(index_name)

    tasks = list(swh_elasticsearch_backend.streaming_bulk(index_name, tasks))
    assert len(tasks) > 0

    for output_task in tasks:
        assert output_task is not None
        assert output_task["type"] == template_git["type"]
        assert output_task["arguments"] is not None
        next_run = output_task["next_run"]
        if isinstance(next_run, str):  # real elasticsearch
            assert next_run == next_run_date.isoformat()
        else:  # memory implem. does not really index
            assert next_run == next_run_date

    assert swh_elasticsearch_backend.storage.indices.exists(index_name)
    assert swh_elasticsearch_backend.is_index_opened(index_name)
    mapping = swh_elasticsearch_backend.storage.indices.get_mapping(index_name)
    assert mapping != {}
