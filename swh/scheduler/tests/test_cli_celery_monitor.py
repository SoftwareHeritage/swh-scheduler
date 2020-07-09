# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

from click.testing import CliRunner
import pytest

from swh.scheduler.cli import cli


def invoke(*args, catch_exceptions=False):
    result = CliRunner(mix_stderr=False).invoke(
        cli, ["celery-monitor", *args], catch_exceptions=catch_exceptions,
    )

    return result


def test_celery_monitor():
    """Check that celery-monitor returns its help text"""

    result = invoke()

    assert "Commands:" in result.stdout
    assert "Options:" in result.stdout


def test_celery_monitor_ping(
    caplog, swh_scheduler_celery_app, swh_scheduler_celery_worker
):
    caplog.set_level(logging.INFO, "swh.scheduler.cli.celery_monitor")

    result = invoke("--pattern", swh_scheduler_celery_worker.hostname, "ping-workers")

    assert result.exit_code == 0

    assert len(caplog.records) == 1

    (record,) = caplog.records

    assert record.levelname == "INFO"
    assert f"response from {swh_scheduler_celery_worker.hostname}" in record.message


@pytest.mark.parametrize(
    "filter_args,filter_message,exit_code",
    [
        ((), "Matching all workers", 0),
        (
            ("--pattern", "celery@*.test-host"),
            "Using glob pattern celery@*.test-host",
            1,
        ),
        (
            ("--pattern", "celery@test-type.test-host"),
            "Using destinations celery@test-type.test-host",
            1,
        ),
        (
            ("--pattern", "celery@test-type.test-host,celery@test-type2.test-host"),
            (
                "Using destinations "
                "celery@test-type.test-host, celery@test-type2.test-host"
            ),
            1,
        ),
    ],
)
def test_celery_monitor_ping_filter(
    caplog,
    swh_scheduler_celery_app,
    swh_scheduler_celery_worker,
    filter_args,
    filter_message,
    exit_code,
):
    caplog.set_level(logging.DEBUG, "swh.scheduler.cli.celery_monitor")

    result = invoke("--timeout", "1.5", *filter_args, "ping-workers")

    assert result.exit_code == exit_code, result.stdout

    got_no_response_message = False
    got_filter_message = False

    for record in caplog.records:
        # Check the proper filter has been generated
        if record.levelname == "DEBUG":
            if filter_message in record.message:
                got_filter_message = True
        # Check that no worker responded
        if record.levelname == "INFO":
            if "No response in" in record.message:
                got_no_response_message = True

    assert got_filter_message

    if filter_args:
        assert got_no_response_message


def test_celery_monitor_list_running(
    caplog, swh_scheduler_celery_app, swh_scheduler_celery_worker
):
    caplog.set_level(logging.DEBUG, "swh.scheduler.cli.celery_monitor")

    result = invoke("--pattern", swh_scheduler_celery_worker.hostname, "list-running")

    assert result.exit_code == 0, result.stdout

    for record in caplog.records:
        if record.levelname != "INFO":
            continue
        assert (
            f"{swh_scheduler_celery_worker.hostname}: no active tasks" in record.message
        )


@pytest.mark.parametrize("format", ["csv", "pretty"])
def test_celery_monitor_list_running_format(
    caplog, swh_scheduler_celery_app, swh_scheduler_celery_worker, format
):
    caplog.set_level(logging.DEBUG, "swh.scheduler.cli.celery_monitor")

    result = invoke(
        "--pattern",
        swh_scheduler_celery_worker.hostname,
        "list-running",
        "--format",
        format,
    )

    assert result.exit_code == 0, result.stdout

    for record in caplog.records:
        if record.levelname != "INFO":
            continue
        assert (
            f"{swh_scheduler_celery_worker.hostname}: no active tasks" in record.message
        )

    if format == "csv":
        lines = result.stdout.splitlines()
        assert lines == ["worker,name,args,kwargs,duration,worker_pid"]
