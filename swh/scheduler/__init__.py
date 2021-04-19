# Copyright (C) 2018-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any, Dict
import warnings

DEFAULT_CONFIG = {
    "scheduler": (
        "dict",
        {"cls": "local", "db": "dbname=softwareheritage-scheduler-dev",},
    )
}
# current configuration. To be set by the config loading mechanism
CONFIG = {}  # type: Dict[str, Any]


if TYPE_CHECKING:
    from swh.scheduler.interface import SchedulerInterface


BACKEND_TYPES: Dict[str, str] = {
    "local": ".backend.SchedulerBackend",
    "remote": ".api.client.RemoteScheduler",
}


def get_scheduler(cls: str, **kwargs) -> SchedulerInterface:
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

    if "args" in kwargs:
        warnings.warn(
            'Explicit "args" key is deprecated, use keys directly instead.',
            DeprecationWarning,
        )
        kwargs = kwargs["args"]

    class_path = BACKEND_TYPES.get(cls)
    if class_path is None:
        raise ValueError(
            f"Unknown Scheduler class `{cls}`. "
            f"Supported: {', '.join(BACKEND_TYPES)}"
        )

    (module_path, class_name) = class_path.rsplit(".", 1)
    module = import_module(module_path, package=__package__)
    BackendClass = getattr(module, class_name)
    return BackendClass(**kwargs)
