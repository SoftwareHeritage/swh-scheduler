# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
from typing import Dict, List

from click.testing import CliRunner, Result
from confluent_kafka import Producer
import pytest
import yaml

from swh.journal.serializers import value_to_kafka
from swh.scheduler import get_scheduler
from swh.scheduler.cli import cli
from swh.scheduler.tests.test_journal_client import VISIT_STATUSES_1


@pytest.fixture
def swh_scheduler_cfg(postgresql_scheduler, kafka_server):
    """Journal client configuration ready"""
    return {
        "scheduler": {"cls": "local", "db": postgresql_scheduler.dsn,},
        "journal": {
            "brokers": [kafka_server],
            "group_id": "test-consume-visit-status",
        },
    }


def _write_configuration_path(config: Dict, tmp_path: str) -> str:
    config_path = os.path.join(str(tmp_path), "scheduler.yml")
    with open(config_path, "w") as f:
        f.write(yaml.dump(config))
    return config_path


@pytest.fixture
def swh_scheduler_cfg_path(swh_scheduler_cfg, tmp_path):
    """Write scheduler configuration in temporary path and returns such path"""
    return _write_configuration_path(swh_scheduler_cfg, tmp_path)


def invoke(args: List[str], config_path: str, catch_exceptions: bool = False) -> Result:
    """Invoke swh scheduler journal subcommands

    """
    runner = CliRunner()
    result = runner.invoke(cli, ["-C" + config_path] + args)
    if not catch_exceptions and result.exception:
        print(result.output)
        raise result.exception
    return result


def test_cli_journal_client_origin_visit_status_misconfiguration_no_scheduler(
    swh_scheduler_cfg, tmp_path
):
    config = swh_scheduler_cfg.copy()
    config["scheduler"] = {"cls": "foo"}
    config_path = _write_configuration_path(config, tmp_path)
    with pytest.raises(ValueError, match="must be instantiated"):
        invoke(
            ["journal-client", "--stop-after-objects", "1",], config_path,
        )


def test_cli_journal_client_origin_visit_status_misconfiguration_missing_journal_conf(
    swh_scheduler_cfg, tmp_path
):
    config = swh_scheduler_cfg.copy()
    config.pop("journal", None)
    config_path = _write_configuration_path(config, tmp_path)

    with pytest.raises(ValueError, match="Missing 'journal'"):
        invoke(
            ["journal-client", "--stop-after-objects", "1",], config_path,
        )


def test_cli_journal_client_origin_visit_status(
    swh_scheduler_cfg, swh_scheduler_cfg_path,
):
    kafka_server = swh_scheduler_cfg["journal"]["brokers"][0]
    swh_scheduler = get_scheduler(**swh_scheduler_cfg["scheduler"])
    producer = Producer(
        {
            "bootstrap.servers": kafka_server,
            "client.id": "test visit-stats producer",
            "acks": "all",
        }
    )
    visit_status = VISIT_STATUSES_1[0]

    value = value_to_kafka(visit_status)
    topic = "swh.journal.objects.origin_visit_status"
    producer.produce(topic=topic, key=b"bogus-origin", value=value)
    producer.flush()

    result = invoke(
        ["journal-client", "--stop-after-objects", "1",], swh_scheduler_cfg_path,
    )

    # Check the output
    expected_output = "Processed 1 message(s).\nDone.\n"
    assert result.exit_code == 0, result.output
    assert result.output == expected_output

    actual_visit_stats = swh_scheduler.origin_visit_stats_get(
        [(visit_status["origin"], visit_status["type"])]
    )

    assert actual_visit_stats
    assert len(actual_visit_stats) == 1
