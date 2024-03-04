# Copyright (C) 2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Dict

import pytest
import yaml

from swh.scheduler import DEFAULT_CONFIG
from swh.scheduler.cli.config import read_config


@pytest.fixture
def local_sched_config(swh_scheduler_config):
    """Expose the local scheduler configuration"""
    return {"scheduler": {"cls": "local", **swh_scheduler_config}}


@pytest.fixture
def local_sched_configfile(local_sched_config, tmp_path):
    """Write in temporary location the local scheduler configuration"""
    configfile = tmp_path / "config.yml"
    configfile.write_text(yaml.dump(local_sched_config))
    return configfile.as_posix()


def test_read_config_from_file(local_sched_configfile, local_sched_config):
    """Loading configuration from file should be ok."""

    actual_config = read_config(local_sched_configfile)

    assert actual_config is not None
    assert isinstance(actual_config, Dict)

    assert actual_config == local_sched_config


def test_read_config_from_environment(
    local_sched_configfile, local_sched_config, monkeypatch
):
    """Loading configuration from environment variable should be ok."""

    with monkeypatch.context() as m:
        m.setenv("SWH_CONFIG_FILENAME", local_sched_configfile)
        from os import environ

        assert environ["SWH_CONFIG_FILENAME"] == str(local_sched_configfile)
        actual_config = read_config()

    assert actual_config is not None
    assert isinstance(actual_config, Dict)

    assert actual_config == local_sched_config


def test_read_config_default(local_sched_configfile):
    """Loading configuration from default configuration should be ok."""

    actual_config = read_config()

    assert actual_config is not None
    assert isinstance(actual_config, Dict)

    assert actual_config["scheduler"] == DEFAULT_CONFIG["scheduler"][1]
