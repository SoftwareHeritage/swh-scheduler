# Copyright (C) 2017-2024 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Defines the <swh scheduler origin send-origins-from-file-to-celery> cli utility
functions. This uses a list of origins read from the standard input or file, massage
them into scheduler tasks to send directly to celery to a queue (according to a task
type specified).

The list of origins has been extracted by other means (e.g. sentry extract, combination
of various shell scripts, ...). Then, a human operator provides the list to the cli so
it's consumed by standard swh queues (understand scheduler configured backend).

"""

from __future__ import annotations

from functools import partial
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional

    from swh.scheduler.interface import SchedulerInterface
    from swh.scheduler.model import TaskType


def get_scheduler_task_type(
    scheduler: SchedulerInterface, task_type_name: str
) -> TaskType:
    """Retrieve a TaskType instance for a task type name from the scheduler.

    Args:
        scheduler: Scheduler instance to lookup data from
        task_type_name: The task type name to lookup

    Raises:
        ValueError when task_type_name or its fallback are not found.

    Returns:
        Information about the task type

    """
    origin_task_type_name = task_type_name
    # Lookup standard scheduler task (e.g. load-git, load-hg, ...)
    scheduler_info = scheduler.get_task_type(task_type_name)
    while True:
        if scheduler_info:
            return scheduler_info
        # Lookup task type derivative (e.g. load-git-large, load-hg-bitbucket, ...)
        new_task_type_name = task_type_name.rsplit("-", 1)[0]
        if new_task_type_name == task_type_name:
            error_msg = f"Could not find scheduler <{origin_task_type_name}> task type"
            raise ValueError(error_msg)
        task_type_name = new_task_type_name
        scheduler_info = scheduler.get_task_type(task_type_name)


def lines_to_task_args(
    lines: Iterable[str],
    columns: List[str] = ["url"],
    postprocess: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
    **kwargs,
) -> Iterator[Dict[str, Any]]:
    """Iterate over the lines and convert them into celery tasks ready to be sent.

    Args:
        lines: Line read from a file or stdin
        columns: structure of the lines to be read (usually only the url column)
        postprocess: An optional callable to enrich the task with
        **kwargs: extra static arguments to enrich the task with

    Yield:
        task ready to be sent to celery

    """
    for line in lines:
        values = line.strip().split()
        ret = dict(zip(columns, values))
        ret.update(kwargs)
        if postprocess:
            ret = postprocess(ret)
        yield {"args": [], "kwargs": ret}


_to_mercurial_tasks = partial(
    lines_to_task_args, columns=["origin_url", "archive_path"]
)

_to_bitbucket_mercurial_tasks = partial(
    lines_to_task_args, columns=["url", "directory", "visit_date"]
)


def _to_svn_tasks(lines: Iterable[str], type: str = "svn", **kwargs) -> Iterator[Dict]:
    """Generates proper task argument for the loader-svn worker.

    Yields:
        svn task

    """
    if type == "svn":
        yield from lines_to_task_args(lines=lines, columns=["url"], **kwargs)
    else:
        yield from lines_to_task_args(
            lines=lines,
            columns=["origin_url", "archive_path"],
            **kwargs,
        )


def _update_git_task_kwargs(kwargs: Dict[str, Any]) -> Dict[str, Any]:
    """Create simple git task kwargs from an url."""

    from urllib.parse import urlparse

    ret = kwargs.copy()

    parsed_url = urlparse(ret["url"])
    if parsed_url.netloc == "github.com":
        ret.update(  # extra information for the loader-metadata
            {
                "lister_name": "github",
                "lister_instance_name": "github",
            }
        )

    return ret


# Generates task argument for loader-git for usual origins
_to_git_normal_tasks = partial(
    lines_to_task_args,
    columns=["url"],
    postprocess=_update_git_task_kwargs,
)


# Generates task argument for loader-git for origins whose packfile is very large
_to_git_large_tasks = partial(
    _to_git_normal_tasks,
    pack_size_bytes=34359738368,
    verify_certs=False,
)

# Default task arguments generator for previously scheduled kind of origins
# It lifts the other callables into a simple dict to ease
TASK_ARGS_GENERATOR_CALLABLES: Dict[str, Callable] = {
    "load-svn": _to_svn_tasks,
    "load-hg-from-archive-mercurial": partial(
        _to_mercurial_tasks,
        visit_date="Tue, 3 May 2016 17:16:32 +0200",
    ),
    "load-hg-bitbucket": _to_bitbucket_mercurial_tasks,
    "load-git-normal": _to_git_normal_tasks,
    "load-git-large": _to_git_large_tasks,
}
