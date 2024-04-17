# Copyright (C) 2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import traceback

from click.testing import CliRunner
import pytest
import yaml

from swh.scheduler import get_scheduler
from swh.scheduler.cli import cli

FAKE_MODULE_ENTRY_POINTS = {
    "lister.foo=swh.scheduler.tests.fixtures.lister.foo:register",
    "loader.bar=swh.scheduler.tests.fixtures.loader.bar:register",
}


@pytest.fixture
def cli_runner():
    return CliRunner()


@pytest.fixture(autouse=True)
def mock_plugin_worker_descriptions(mocker):
    """Register fake lister and loader for testing celery tasks registration."""
    from pkg_resources import Distribution, EntryPoint

    d = Distribution()
    entry_points = [
        EntryPoint.parse(entry, dist=d) for entry in FAKE_MODULE_ENTRY_POINTS
    ]
    mocker.patch(
        "swh.scheduler.cli.task_type._plugin_worker_descriptions",
    ).return_value = {entry_point.name: entry_point for entry_point in entry_points}


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


def test_register_unknown_task_type(
    cli_runner,
    local_sched_configfile,
):
    """Trying to register an unknown task type should return an error."""

    command = [
        "--config-file",
        local_sched_configfile,
        "task-type",
        "register",
        "-p",
        "lister.baz",
    ]

    result = cli_runner.invoke(cli, command)

    assert result.exit_code != 0
    assert (
        "That provided plugin is unknown: lister.baz.\n"
        "Available ones are: lister.foo, loader.bar."
    ) in result.output


def test_register_task_types_all(
    cli_runner,
    local_sched_config,
    local_sched_configfile,
):
    """Registering all task types"""

    for command in [
        ["--config-file", local_sched_configfile, "task-type", "register"],
        ["--config-file", local_sched_configfile, "task-type", "register", "-p", "all"],
        [
            "--config-file",
            local_sched_configfile,
            "task-type",
            "register",
            "-p",
            "lister.foo",
            "-p",
            "loader.bar",
        ],
    ]:
        result = cli_runner.invoke(cli, command)

        assert result.exit_code == 0, traceback.print_exception(*result.exc_info)

        scheduler = get_scheduler(**local_sched_config["scheduler"])

        all_task_type_names = ["list-foo-full", "load-bar"]
        for task_type_name in all_task_type_names:
            task_type = scheduler.get_task_type(task_type_name)
            assert task_type
            assert task_type.type == task_type_name
            assert task_type.backoff_factor == 1.0


def test_register_task_types_filter(
    cli_runner,
    local_sched_config,
    local_sched_configfile,
):
    """Filtering on one worker should only register its associated task type"""
    result = cli_runner.invoke(
        cli,
        [
            "--config-file",
            local_sched_configfile,
            "task-type",
            "register",
            "--plugins",
            "lister.foo",
        ],
    )

    assert result.exit_code == 0, traceback.print_exception(*result.exc_info)

    scheduler = get_scheduler(**local_sched_config["scheduler"])

    task_type_name = "list-foo-full"
    task_type = scheduler.get_task_type(task_type_name)
    assert task_type
    assert task_type.type == task_type_name
    assert task_type.backoff_factor == 1.0


@pytest.mark.parametrize("cli_command", ["list", "register", "add"])
def test_cli_task_type_raise(cli_runner, cli_command):
    """Without a proper configuration, the cli raises"""
    result = cli_runner.invoke(cli, ["task-type", cli_command])
    assert "Scheduler class" in result.output
    assert result.exit_code != 0
