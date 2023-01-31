# Copyright (C) 2021-2023 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

import pytest
import yaml

from swh.scheduler.celery_backend.config import (
    MAX_NUM_TASKS,
    app,
    get_available_slots,
    route_for_task,
    setup_log_handler,
)


@pytest.mark.parametrize("name", ["swh.something", "swh.anything"])
def test_route_for_task_routing(name):
    assert route_for_task(name, [], {}, {}) == {"queue": name}


@pytest.mark.parametrize("name", [None, "foobar"])
def test_route_for_task_no_routing(name):
    assert route_for_task(name, [], {}, {}) is None


def test_get_available_slots_no_max_length():
    actual_num = get_available_slots(app, "anything", None)
    assert actual_num == MAX_NUM_TASKS


def test_get_available_slots_issue_when_reading_queue(mocker):
    mock = mocker.patch("swh.scheduler.celery_backend.config.get_queue_length")
    mock.side_effect = ValueError

    actual_num = get_available_slots(app, "anything", max_length=10)
    assert actual_num == MAX_NUM_TASKS
    assert mock.called


def test_get_available_slots_no_queue_length(mocker):
    mock = mocker.patch("swh.scheduler.celery_backend.config.get_queue_length")
    mock.return_value = None
    actual_num = get_available_slots(app, "anything", max_length=100)
    assert actual_num == MAX_NUM_TASKS
    assert mock.called


def test_get_available_slots_no_more_slots(mocker):
    mock = mocker.patch("swh.scheduler.celery_backend.config.get_queue_length")
    max_length = 100
    queue_length = 9000
    mock.return_value = queue_length
    actual_num = get_available_slots(app, "anything", max_length)
    assert actual_num == 0
    assert mock.called


def test_get_available_slots(mocker):
    mock = mocker.patch("swh.scheduler.celery_backend.config.get_queue_length")
    max_length = 100
    queue_length = 90
    mock.return_value = queue_length
    actual_num = get_available_slots(app, "anything", max_length)
    assert actual_num == max_length - queue_length
    assert mock.called


def test_setup_log_handler(capsys):
    log_level = logging.DEBUG

    setup_log_handler(loglevel=log_level)

    loggers = [
        ("celery", logging.INFO),
        ("amqp", log_level),
        ("urllib3", logging.WARNING),
        ("azure.core.pipeline.policies.http_logging_policy", logging.WARNING),
        ("swh", log_level),
        ("celery.task", log_level),
    ]

    for module, log_level in loggers:
        logger = logging.getLogger(module)
        assert logger.getEffectiveLevel() == log_level

    # exceptions are caught and displayed by the setup_log_handler function
    # as celery eats tracebacks in signal handler, check no traceback was
    # displayed then
    assert "Traceback" not in capsys.readouterr().err


def test_setup_log_handler_with_env_configuration(capsys, monkeypatch, datadir):
    logging_config_path = datadir / "logging-config.yaml"
    with monkeypatch.context() as m:
        m.setenv("SWH_LOG_CONFIG", logging_config_path)
        from os import environ

        assert environ["SWH_LOG_CONFIG"] == str(logging_config_path)
        setup_log_handler()

    # exceptions are caught and displayed by the setup_log_handler function
    # as celery eats tracebacks in signal handler, check no traceback was
    # displayed then
    assert "Traceback" not in capsys.readouterr().err

    # Reading the actual config and checking it's actually applied
    with open(logging_config_path, "r") as f:
        config_d = yaml.safe_load(f.read())

    for module, config in config_d["loggers"].items():
        logger = logging.getLogger(module)

        assert logger.getEffectiveLevel() == logging.getLevelName(config["level"])
