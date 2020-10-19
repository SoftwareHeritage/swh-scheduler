# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest
import yaml

from swh.scheduler import get_scheduler


@pytest.fixture
def swh_sched_config(swh_scheduler_config):
    return {
        "scheduler": {"cls": "local", **swh_scheduler_config,},
        "elasticsearch": {
            "cls": "memory",
            "args": {"index_name_prefix": "swh-tasks",},
        },
    }


@pytest.fixture
def swh_sched_config_file(swh_sched_config, monkeypatch, tmp_path):
    conffile = str(tmp_path / "elastic.yml")
    with open(conffile, "w") as f:
        f.write(yaml.dump(swh_sched_config))
    monkeypatch.setenv("SWH_CONFIG_FILENAME", conffile)
    return conffile


@pytest.fixture
def swh_sched(swh_sched_config):
    return get_scheduler(**swh_sched_config["scheduler"])


@pytest.fixture
def swh_elasticsearch_backend(swh_sched_config):
    from swh.scheduler.backend_es import ElasticSearchBackend

    backend = ElasticSearchBackend(**swh_sched_config)
    backend.initialize()
    return backend


@pytest.fixture
def swh_elasticsearch_memory(swh_elasticsearch_backend):
    return swh_elasticsearch_backend.storage
