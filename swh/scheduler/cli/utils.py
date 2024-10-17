# Copyright (C) 2019-2024 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

# WARNING: do not import unnecessary things here to keep cli startup time under
# control

from __future__ import annotations

from datetime import datetime, timedelta
import re
from typing import TYPE_CHECKING, Iterable

import click

from swh.scheduler.model import Task

if TYPE_CHECKING:
    from typing import Any, Dict, List, Optional, Tuple

    from swh.scheduler.interface import SchedulerInterface
    from swh.scheduler.model import TaskPolicy, TaskPriority, TaskRun


TASK_BATCH_SIZE = 1000  # Number of tasks per query to the scheduler


TIME_INTERVAL_REGEXP = re.compile(
    r"""
    # optional group for days
    (?:
        (?P<days>\d+\.?|\d*\.\d+) # floating point number, e.g. "1", "2.", "3.4" or ".5".
        \x20*
        (?:day|days|d)
    )?
    \x20? # optional space
    # optional group for hours
    (?:
        (?P<hours>\d+\.?|\d*\.\d+)
        \x20*
        (?:h|hr|hrs|hour|hours)
    )?
    \x20? # optional space
    # optional group for minutes
    (?:
        (?P<minutes>\d+\.?|\d*\.\d+)
        \x20*
        (?:m|min|mins|minute|minutes)
    )?
    \x20? # optional space
    # optional group for seconds
    (?:
        (?P<seconds>\d+\.?|\d*\.\d+)
        \x20*
        (?:s|sec|second|seconds)
    )?
    """,
    re.VERBOSE,
)


def parse_time_interval(time_str: str) -> timedelta:
    """Parse a basic time interval e.g. '1 day' or '2 hours' into a timedelta object.

    Args:
        time_str: A string representing a basic time interval in days or hours.

    Returns:
        An equivalent representation of the string as a datetime.timedelta object.

    Raises:
        ValueError if the time interval could not be parsed.

    """
    parts = TIME_INTERVAL_REGEXP.fullmatch(time_str)
    if not parts:
        raise ValueError(f"{time_str!r} could not be parsed as a time interval")
    time_params = {
        name: float(param) for name, param in parts.groupdict().items() if param
    }
    if not time_params:
        # The regexp lets a bare space go through
        raise ValueError(f"{time_str!r} could not be parsed as a time interval")
    return timedelta(**time_params)


def schedule_origin_batches(scheduler, task_type, origins, origin_batch_size, kwargs):
    from itertools import islice

    from swh.scheduler.utils import create_task

    nb_origins = 0
    nb_tasks = 0

    while True:
        task_batch = []
        for _ in range(TASK_BATCH_SIZE):
            # Group origins
            origin_batch = []
            for origin in islice(origins, origin_batch_size):
                origin_batch.append(origin)
            nb_origins += len(origin_batch)
            if not origin_batch:
                break

            # Create a task for these origins
            args = [origin_batch]
            task_dict = create_task(task_type, "oneshot", *args, **kwargs)
            task_batch.append(task_dict)

        # Schedule a batch of tasks
        if not task_batch:
            break
        nb_tasks += len(task_batch)
        if scheduler:
            scheduler.create_tasks(task_batch)
        click.echo("Scheduled %d tasks (%d origins)." % (nb_tasks, nb_origins))

    # Print final status.
    if nb_tasks:
        click.echo("Done.")
    else:
        click.echo("Nothing to do (no origin metadata matched the criteria).")


def parse_argument(option):
    import yaml

    if option == "":
        # yaml.safe_load("") returns None
        return ""

    try:
        return yaml.safe_load(option)
    except Exception:
        raise click.ClickException("Invalid argument: {}".format(option))


def parse_options(options: List[str]) -> Tuple[List[str], Dict]:
    """Parses options from a CLI as YAML and turns it into Python
    args and kwargs.

    >>> parse_options([])
    ([], {})
    >>> parse_options(['foo', 'bar'])
    (['foo', 'bar'], {})
    >>> parse_options(['[foo, bar]'])
    ([['foo', 'bar']], {})
    >>> parse_options(['"foo"', '"bar"'])
    (['foo', 'bar'], {})
    >>> parse_options(['foo="bar"'])
    ([], {'foo': 'bar'})
    >>> parse_options(['"foo"', 'bar="baz"'])
    (['foo'], {'bar': 'baz'})
    >>> parse_options(['42', 'bar=False'])
    ([42], {'bar': False})
    >>> parse_options(['42', 'bar=false'])
    ([42], {'bar': False})
    >>> parse_options(['foo', ''])
    (['foo', ''], {})
    >>> parse_options(['foo', 'bar='])
    (['foo'], {'bar': ''})
    >>> parse_options(['foo', 'null'])
    (['foo', None], {})
    >>> parse_options(['foo', 'bar=null'])
    (['foo'], {'bar': None})
    >>> parse_options(['42', '"foo'])
    Traceback (most recent call last):
      ...
    click.exceptions.ClickException: Invalid argument: "foo
    """
    kw_pairs = [x.split("=", 1) for x in options if "=" in x]
    args = [parse_argument(x) for x in options if "=" not in x]
    kw = {k: parse_argument(v) for (k, v) in kw_pairs}
    return (args, kw)


def pretty_print_list(list: List[Any], indent: int = 0):
    """Pretty-print a list"""
    return "".join("%s%r\n" % (" " * indent, item) for item in list)


def pretty_print_dict(dict: Dict[str, Any], indent: int = 0):
    """Pretty-print a list"""
    return "".join(
        "%s%s: %r\n" % (" " * indent, click.style(key, bold=True), value)
        for key, value in sorted(dict.items())
    )


def format_dict(d):
    """Recursively format date objects in the dict passed as argument"""
    import datetime

    ret = {}
    for k, v in d.items():
        if isinstance(v, (datetime.date, datetime.datetime)):
            v = v.isoformat()
        elif isinstance(v, dict):
            v = format_dict(v)
        ret[k] = v
    return ret


def pretty_print_run(run: TaskRun, indent: int = 4):
    fmt = (
        "{indent}{backend_id} [{status}]\n"
        "{indent}  scheduled: {scheduled} [{started}:{ended}]"
    )
    return fmt.format(indent=" " * indent, **format_dict(run.to_dict()))


def pretty_print_task(task: Task, full: bool = False):
    """Pretty-print a task

    If ``full`` is :const:`True`, also print the status and priority fields.

    >>> import datetime
    >>> from swh.scheduler.model import Task, TaskArguments
    >>> task = Task(
    ...     id=1234,
    ...     arguments=TaskArguments(
    ...         args=["foo", "bar", True],
    ...         kwargs={"key": "value", "key2": 42},
    ...     ),
    ...     current_interval=datetime.timedelta(hours=1),
    ...     next_run=datetime.datetime(2019, 2, 21, 13, 52, 35, 407818),
    ...     policy="oneshot",
    ...     priority=None,
    ...     status="next_run_not_scheduled",
    ...     type="test_task",
    ... )
    >>> print(click.unstyle(pretty_print_task(task)))
    Task 1234
      Next run: ... (2019-02-21T13:52:35.407818)
      Interval: 1:00:00
      Type: test_task
      Policy: oneshot
      Args:
        'foo'
        'bar'
        True
      Keyword args:
        key: 'value'
        key2: 42
    <BLANKLINE>
    >>> print(click.unstyle(pretty_print_task(task, full=True)))
    Task 1234
      Next run: ... (2019-02-21T13:52:35.407818)
      Interval: 1:00:00
      Type: test_task
      Policy: oneshot
      Status: next_run_not_scheduled
      Priority:\x20
      Args:
        'foo'
        'bar'
        True
      Keyword args:
        key: 'value'
        key2: 42
    <BLANKLINE>
    """
    import humanize

    next_run = task.next_run
    lines = [
        "%s %s\n" % (click.style("Task", bold=True), task.id),
        click.style("  Next run: ", bold=True),
        "%s (%s)" % (humanize.naturaldate(next_run), next_run.isoformat()),
        "\n",
        click.style("  Interval: ", bold=True),
        str(task.current_interval),
        "\n",
        click.style("  Type: ", bold=True),
        task.type or "",
        "\n",
        click.style("  Policy: ", bold=True),
        task.policy or "",
        "\n",
    ]
    if full:
        lines += [
            click.style("  Status: ", bold=True),
            task.status or "",
            "\n",
            click.style("  Priority: ", bold=True),
            task.priority or "",
            "\n",
        ]
    lines += [
        click.style("  Args:\n", bold=True),
        pretty_print_list(task.arguments.args, indent=4),
        click.style("  Keyword args:\n", bold=True),
        pretty_print_dict(task.arguments.kwargs, indent=4),
    ]

    return "".join(lines)


def task_add(
    scheduler: SchedulerInterface,
    task_type_name: str,
    args: List[str],
    kw: Dict,
    policy: TaskPolicy,
    priority: Optional[TaskPriority] = None,
    next_run: Optional[datetime] = None,
):
    """Add a task task_type_name in the scheduler."""
    from swh.scheduler.model import TaskArguments
    from swh.scheduler.utils import utcnow

    task = Task(
        type=task_type_name,
        policy=policy,
        priority=priority,
        arguments=TaskArguments(
            args=args,
            kwargs=kw,
        ),
        next_run=next_run or utcnow(),
    )
    created = scheduler.create_tasks([task])

    output = [f"Created {len(created)} tasks\n"]
    for task in created:
        output.append(pretty_print_task(task))

    click.echo("\n".join(output))


def lister_task_type(lister_name: str, lister_type: Optional[str] = None) -> str:
    """Compute expected scheduler task type from the lister name and its optional
    listing type (full, incremental).

    """
    prefix = f"list-{lister_name}"
    return f"{prefix}-{lister_type}" if lister_type else prefix


def check_listed_origins(
    scheduler: SchedulerInterface,
    lister_name: str,
    instance_name: str,
    limit: int = 100000,
):
    listed_origins_lister = scheduler.get_lister(
        name=lister_name, instance_name=instance_name
    )

    if listed_origins_lister is None:
        exit(
            f"Forge {instance_name} ({lister_name}) isn't registered \
in the scheduler database."
        )
    else:
        lister_id = listed_origins_lister.id

    listed_origins = scheduler.get_listed_origins(
        lister_id=lister_id,
        enabled=None,
        limit=limit,
    ).results

    if len(listed_origins) == 0:
        exit(
            f"Forge {instance_name} ({lister_name}) has {len(listed_origins)} \
listed origin in the scheduler database."
        )

    return listed_origins


def count_ingested_origins(
    scheduler: SchedulerInterface,
    ids: Iterable[Tuple[str, str]],
    instance_name: str,
    with_listing: Optional[bool] = False,
) -> Tuple[Dict[str, int], List]:
    """Count number of ingested origins grouped by status."""

    ingested_origins = scheduler.origin_visit_stats_get(ids=ids)
    status_counters = {
        "failed": 0,
        "None": 0,
        "not_found": 0,
        "successful": 0,
        "total": len(ingested_origins),
    }
    if with_listing:
        ingested_origins_table = []

    if status_counters["total"] == 0:
        exit(
            f"Forge {instance_name} has {len(ingested_origins)} \
scheduled ingest in the scheduler database."
        )

    for ingested_origin in ingested_origins:
        if ingested_origin.last_visit_status is not None:
            if with_listing:
                ingested_origins_table.append(
                    [
                        ingested_origin.url,
                        ingested_origin.last_visit_status.value,
                        str(ingested_origin.last_visit),
                    ]
                )
            counter_key = ingested_origin.last_visit_status.value
        else:
            counter_key = None
        status_counters[str(counter_key)] += 1

    return status_counters, ingested_origins_table if with_listing else []
