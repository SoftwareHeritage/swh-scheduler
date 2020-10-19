# Copyright (C) 2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import inspect

import pytest

from swh.scheduler import get_scheduler
from swh.scheduler.api.client import RemoteScheduler
from swh.scheduler.backend import SchedulerBackend
from swh.scheduler.interface import SchedulerInterface

SERVER_IMPLEMENTATIONS = [
    ("remote", RemoteScheduler, {"url": "localhost"}),
    ("local", SchedulerBackend, {"db": "something"}),
]


@pytest.fixture
def mock_psycopg2(mocker):
    mocker.patch("swh.scheduler.backend.psycopg2.pool")


def test_init_get_scheduler_failure():
    with pytest.raises(ValueError, match="Unknown Scheduler class"):
        get_scheduler("unknown-scheduler-storage")


@pytest.mark.parametrize("class_name,expected_class,kwargs", SERVER_IMPLEMENTATIONS)
def test_init_get_scheduler(class_name, expected_class, kwargs, mock_psycopg2):
    concrete_scheduler = get_scheduler(class_name, **kwargs)
    assert isinstance(concrete_scheduler, expected_class)
    assert isinstance(concrete_scheduler, SchedulerInterface)


@pytest.mark.parametrize("class_name,expected_class,kwargs", SERVER_IMPLEMENTATIONS)
def test_init_get_scheduler_deprecation_warning(
    class_name, expected_class, kwargs, mock_psycopg2
):
    with pytest.warns(DeprecationWarning):
        concrete_scheduler = get_scheduler(class_name, args=kwargs)
    assert isinstance(concrete_scheduler, expected_class)


def test_types(swh_scheduler) -> None:
    """Checks all methods of SchedulerInterface are implemented by this
    backend, and that they have the same signature."""
    # Create an instance of the protocol (which cannot be instantiated
    # directly, so this creates a subclass, then instantiates it)
    interface = type("_", (SchedulerInterface,), {})()

    missing_methods = []

    for meth_name in dir(interface):
        if meth_name.startswith("_"):
            continue
        interface_meth = getattr(interface, meth_name)
        try:
            concrete_meth = getattr(swh_scheduler, meth_name)
        except AttributeError:
            missing_methods.append(meth_name)
            continue

        expected_signature = inspect.signature(interface_meth)
        actual_signature = inspect.signature(concrete_meth)

        assert expected_signature == actual_signature, meth_name

    assert missing_methods == []

    # If all the assertions above succeed, then this one should too.
    # But there's no harm in double-checking.
    # And we could replace the assertions above by this one, but unlike
    # the assertions above, it doesn't explain what is missing.
    assert isinstance(swh_scheduler, SchedulerInterface)
