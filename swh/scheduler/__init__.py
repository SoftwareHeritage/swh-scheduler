# Copyright (C) 2018-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict
import warnings

DEFAULT_CONFIG_RAW = {
    "scheduler": {
        "cls": "postgresql",
        "db": "dbname=softwareheritage-scheduler-dev",
    }
}


DEFAULT_CONFIG = {
    "scheduler": ("dict", DEFAULT_CONFIG_RAW["scheduler"]),
}

# current configuration. To be set by the config loading mechanism
CONFIG: Dict[str, Any] = {}


if TYPE_CHECKING:
    from swh.scheduler.interface import SchedulerInterface


def get_scheduler(cls: str, **kwargs) -> "SchedulerInterface":
    """
    Get a scheduler object of class `cls` with arguments `**kwargs`.

    Args:
        cls: scheduler's class, either 'local' or 'remote'
        kwargs: arguments to pass to the class' constructor

    Returns:
        an instance of swh.scheduler, either local or remote:

        local: swh.scheduler.backend.SchedulerBackend
        remote: swh.scheduler.api.client.RemoteScheduler

    Raises:
        ValueError if passed an unknown storage class.

    """
    from swh.core.config import get_swh_backend_module

    if "args" in kwargs:
        warnings.warn(
            'Explicit "args" key is deprecated, use keys directly instead.',
            DeprecationWarning,
        )
        kwargs = kwargs["args"]

    if cls == "local":
        warnings.warn(
            'The "local" storage class is deprecated, use "postgresql" instead.',
            DeprecationWarning,
        )
        cls = "postgresql"

    _, BackendClass = get_swh_backend_module("scheduler", cls)
    assert BackendClass is not None
    return BackendClass(**kwargs)
