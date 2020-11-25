# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import traceback

from click.testing import CliRunner
import pkg_resources
import pytest
import yaml

from swh.scheduler import get_scheduler
from swh.scheduler.cli import cli

FAKE_MODULE_ENTRY_POINTS = {
    "lister.gnu=swh.lister.gnu:register",
    "lister.pypi=swh.lister.pypi:register",
}


@pytest.fixture
def cli_runner():
    return CliRunner()


@pytest.fixture
def mock_pkg_resources(monkeypatch):
    """Monkey patch swh.scheduler's mock_pkg_resources.iter_entry_point call

    """

    def fake_iter_entry_points(*args, **kwargs):
        """Substitute fake function to return a fixed set of entrypoints

        """
        from pkg_resources import Distribution, EntryPoint

        d = Distribution()
        return [EntryPoint.parse(entry, dist=d) for entry in FAKE_MODULE_ENTRY_POINTS]

    original_method = pkg_resources.iter_entry_points
    monkeypatch.setattr(pkg_resources, "iter_entry_points", fake_iter_entry_points)

    yield

    # reset monkeypatch: is that needed?
    monkeypatch.setattr(pkg_resources, "iter_entry_points", original_method)


@pytest.fixture
def local_sched_config(swh_scheduler_config):
    """Expose the local scheduler configuration

    """
    return {"scheduler": {"cls": "local", **swh_scheduler_config}}


@pytest.fixture
def local_sched_configfile(local_sched_config, tmp_path):
    """Write in temporary location the local scheduler configuration

    """
    configfile = tmp_path / "config.yml"
    configfile.write_text(yaml.dump(local_sched_config))
    return configfile.as_posix()


def test_register_ttypes_all(
    cli_runner, mock_pkg_resources, local_sched_config, local_sched_configfile
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
            "lister.gnu",
            "-p",
            "lister.pypi",
        ],
    ]:
        result = cli_runner.invoke(cli, command)

        assert result.exit_code == 0, traceback.print_exception(*result.exc_info)

        scheduler = get_scheduler(**local_sched_config["scheduler"])
        all_tasks = [
            "list-gnu-full",
            "list-pypi",
        ]
        for task in all_tasks:
            task_type_desc = scheduler.get_task_type(task)
            assert task_type_desc
            assert task_type_desc["type"] == task
            assert task_type_desc["backoff_factor"] == 1


def test_register_ttypes_filter(
    mock_pkg_resources, cli_runner, local_sched_config, local_sched_configfile
):
    """Filtering on one worker should only register its associated task type

    """
    result = cli_runner.invoke(
        cli,
        [
            "--config-file",
            local_sched_configfile,
            "task-type",
            "register",
            "--plugins",
            "lister.gnu",
        ],
    )

    assert result.exit_code == 0, traceback.print_exception(*result.exc_info)

    scheduler = get_scheduler(**local_sched_config["scheduler"])
    all_tasks = [
        "list-gnu-full",
    ]
    for task in all_tasks:
        task_type_desc = scheduler.get_task_type(task)
        assert task_type_desc
        assert task_type_desc["type"] == task
        assert task_type_desc["backoff_factor"] == 1


@pytest.mark.parametrize("cli_command", ["list", "register", "add"])
def test_cli_task_type_raise(cli_runner, cli_command):
    """Without a proper configuration, the cli raises"""
    with pytest.raises(ValueError, match="Scheduler class"):
        cli_runner.invoke(cli, ["task-type", cli_command], catch_exceptions=False)
