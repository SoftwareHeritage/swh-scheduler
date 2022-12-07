# Copyright (C) 2019-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

# WARNING: do not import unnecessary things here to keep cli startup time under
# control

from __future__ import annotations

from typing import TYPE_CHECKING

import click

if TYPE_CHECKING:
    from typing import Dict, List, Optional, Tuple

    from swh.scheduler.interface import SchedulerInterface


TASK_BATCH_SIZE = 1000  # Number of tasks per query to the scheduler


def schedule_origin_batches(scheduler, task_type, origins, origin_batch_size, kwargs):
    from itertools import islice

    from swh.scheduler.utils import create_task_dict

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
            task_dict = create_task_dict(task_type, "oneshot", *args, **kwargs)
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


def get_task_type(scheduler: SchedulerInterface, visit_type: str) -> Optional[Dict]:
    "Given a visit type, return its associated task type."
    return scheduler.get_task_type(f"load-{visit_type}")


def send_to_celery(
    scheduler: SchedulerInterface,
    visit_type_to_queue: Dict[str, str],
    enabled: bool = True,
    lister_name: Optional[str] = None,
    lister_instance_name: Optional[str] = None,
    policy: str = "oldest_scheduled_first",
    tablesample: Optional[float] = None,
):
    """Utility function to read tasks from the scheduler and send those directly to
    celery.

    Args:
        visit_type_to_queue: Optional mapping of visit/loader type (e.g git, svn, ...)
          to queue to send task to.
        enabled: Determine whether we want to list enabled or disabled origins. As
          default, we want reasonably enabled origins. For some edge case, we might
          want the others.
        lister_name: Determine the list of origins listed from the lister with name
        lister_instance_name: Determine the list of origins listed from the lister
          with instance name
        policy: the scheduling policy used to select which visits to schedule
        tablesample: the percentage of the table on which we run the query
          (None: no sampling)

    """

    from kombu.utils.uuid import uuid

    from swh.scheduler.celery_backend.config import app, get_available_slots

    from ..utils import create_origin_task_dicts

    for visit_type_name, queue_name in visit_type_to_queue.items():
        task_type = get_task_type(scheduler, visit_type_name)
        assert task_type is not None
        task_name = task_type["backend_name"]
        num_tasks = get_available_slots(app, queue_name, task_type["max_queue_length"])

        click.echo(f"{num_tasks} slots available in celery queue")

        origins = scheduler.grab_next_visits(
            visit_type_name,
            num_tasks,
            policy=policy,
            tablesample=tablesample,
            enabled=enabled,
            lister_name=lister_name,
            lister_instance_name=lister_instance_name,
        )

        click.echo(f"{len(origins)} visits to send to celery")
        for task_dict in create_origin_task_dicts(origins, scheduler):
            app.send_task(
                task_name,
                task_id=uuid(),
                args=task_dict["arguments"]["args"],
                kwargs=task_dict["arguments"]["kwargs"],
                queue=queue_name,
            )


def pretty_print_list(list, indent=0):
    """Pretty-print a list"""
    return "".join("%s%r\n" % (" " * indent, item) for item in list)


def pretty_print_dict(dict, indent=0):
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


def pretty_print_run(run, indent=4):
    fmt = (
        "{indent}{backend_id} [{status}]\n"
        "{indent}  scheduled: {scheduled} [{started}:{ended}]"
    )
    return fmt.format(indent=" " * indent, **format_dict(run))


def pretty_print_task(task, full=False):
    """Pretty-print a task

    If 'full' is True, also print the status and priority fields.

    >>> import datetime
    >>> task = {
    ...     'id': 1234,
    ...     'arguments': {
    ...         'args': ['foo', 'bar', True],
    ...         'kwargs': {'key': 'value', 'key2': 42},
    ...     },
    ...     'current_interval': datetime.timedelta(hours=1),
    ...     'next_run': datetime.datetime(2019, 2, 21, 13, 52, 35, 407818),
    ...     'policy': 'oneshot',
    ...     'priority': None,
    ...     'status': 'next_run_not_scheduled',
    ...     'type': 'test_task',
    ... }
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

    next_run = task["next_run"]
    lines = [
        "%s %s\n" % (click.style("Task", bold=True), task["id"]),
        click.style("  Next run: ", bold=True),
        "%s (%s)" % (humanize.naturaldate(next_run), next_run.isoformat()),
        "\n",
        click.style("  Interval: ", bold=True),
        str(task["current_interval"]),
        "\n",
        click.style("  Type: ", bold=True),
        task["type"] or "",
        "\n",
        click.style("  Policy: ", bold=True),
        task["policy"] or "",
        "\n",
    ]
    if full:
        lines += [
            click.style("  Status: ", bold=True),
            task["status"] or "",
            "\n",
            click.style("  Priority: ", bold=True),
            task["priority"] or "",
            "\n",
        ]
    lines += [
        click.style("  Args:\n", bold=True),
        pretty_print_list(task["arguments"]["args"], indent=4),
        click.style("  Keyword args:\n", bold=True),
        pretty_print_dict(task["arguments"]["kwargs"], indent=4),
    ]

    return "".join(lines)


def task_add(
    scheduler: SchedulerInterface,
    task_type_name: str,
    args: List[str],
    kw: Dict,
    policy: str,
    priority: Optional[str] = None,
    next_run: Optional[str] = None,
):
    """Add a task task_type_name in the scheduler."""
    from swh.scheduler.utils import utcnow

    task = {
        "type": task_type_name,
        "policy": policy,
        "priority": priority,
        "arguments": {
            "args": args,
            "kwargs": kw,
        },
        "next_run": next_run or utcnow(),
    }
    created = scheduler.create_tasks([task])

    output = [f"Created {len(created)} tasks\n"]
    for task in created:
        output.append(pretty_print_task(task))

    click.echo("\n".join(output))


def lister_task_type(lister_name: str, lister_type: str) -> str:
    return f"list-{lister_name}-{lister_type}"
