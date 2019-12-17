# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import yaml

import pytest

from swh.scheduler import get_scheduler


@pytest.fixture
def swh_scheduler_conf(swh_scheduler_config):
    return {
        'scheduler': {
            'cls': 'local',
            'args': swh_scheduler_config,
        },
        'elasticsearch': {
            'cls': 'memory',
            'args': {
                'index_name_prefix': 'swh-tasks',
            },
        },
    }


@pytest.fixture
def swh_scheduler_conf_file(swh_scheduler_conf, monkeypatch, tmp_path):
    conffile = str(tmp_path / 'elastic.yml')
    with open(conffile, 'w') as f:
        f.write(yaml.dump(swh_scheduler_conf))
    monkeypatch.setenv('SWH_CONFIG_FILENAME', conffile)
    return conffile


@pytest.fixture
def swh_scheduler(swh_scheduler_conf):
    return get_scheduler(**swh_scheduler_conf['scheduler'])


@pytest.fixture
def swh_elasticsearch(swh_scheduler_conf):
    from swh.scheduler.backend_es import ElasticSearchBackend
    backend = ElasticSearchBackend(**swh_scheduler_conf)
    backend.initialize()
    return backend


@pytest.fixture
def swh_memory_elasticsearch(swh_elasticsearch):
    return swh_elasticsearch.storage
