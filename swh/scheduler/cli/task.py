# Copyright (C) 2016-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from __future__ import annotations

# WARNING: do not import unnecessary things here to keep cli startup time under
# control
import locale
from typing import TYPE_CHECKING, Iterator, List, Optional

import click

from . import cli

if TYPE_CHECKING:
    import datetime

    # importing swh.storage.interface triggers the load of 300+ modules, so...
    import swh.model.model
    from swh.storage.interface import StorageInterface


locale.setlocale(locale.LC_ALL, "")
CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])
DATETIME = click.DateTime()


@cli.group("task")
@click.pass_context
def task(ctx):
    """Manipulate tasks."""
    pass


@task.command("schedule")
@click.option(
    "--columns",
    "-c",
    multiple=True,
    default=["type", "args", "kwargs", "next_run"],
    type=click.Choice(["type", "args", "kwargs", "policy", "next_run"]),
    help="columns present in the CSV file",
)
@click.option("--delimiter", "-d", default=",")
@click.argument("file", type=click.File(encoding="utf-8"))
@click.pass_context
def schedule_tasks(ctx, columns, delimiter, file):
    """Schedule tasks from a CSV input file.

    The following columns are expected, and can be set through the -c option:

     - type: the type of the task to be scheduled (mandatory)

     - args: the arguments passed to the task (JSON list, defaults to an empty
       list)

     - kwargs: the keyword arguments passed to the task (JSON object, defaults
       to an empty dict)

     - next_run: the date at which the task should run (datetime, defaults to
       now)

    The CSV can be read either from a named file, or from stdin (use - as
    filename).

    Use sample:

    cat scheduling-task.txt | \
        python3 -m swh.scheduler.cli \
            --database 'service=swh-scheduler-dev' \
            task schedule \
                --columns type --columns kwargs --columns policy \
                --delimiter ';' -

    """
    import csv
    import json

    from swh.scheduler.utils import utcnow

    from .utils import pretty_print_task

    tasks = []
    now = utcnow()
    scheduler = ctx.obj["scheduler"]
    if not scheduler:
        raise ValueError("Scheduler class (local/remote) must be instantiated")

    reader = csv.reader(file, delimiter=delimiter)
    for line in reader:
        task = dict(zip(columns, line))
        args = json.loads(task.pop("args", "[]"))
        kwargs = json.loads(task.pop("kwargs", "{}"))
        task["arguments"] = {
            "args": args,
            "kwargs": kwargs,
        }
        task["next_run"] = task.get("next_run", now)
        tasks.append(task)

    created = scheduler.create_tasks(tasks)

    output = [
        "Created %d tasks\n" % len(created),
    ]
    for task in created:
        output.append(pretty_print_task(task))

    click.echo_via_pager("\n".join(output))


@task.command("add")
@click.argument("task_type_name", nargs=1, required=True)
@click.argument("options", nargs=-1)
@click.option(
    "--policy", "-p", default="recurring", type=click.Choice(["recurring", "oneshot"])
)
@click.option(
    "--priority", "-P", default=None, type=click.Choice(["low", "normal", "high"])
)
@click.option("--next-run", "-n", default=None)
@click.pass_context
def schedule_task(ctx, task_type_name, options, policy, priority, next_run):
    """Schedule one task from arguments.

    The first argument is the name of the task type. Flag options (policy, priority) are
    task configuration. Further options are positional and keyword argument(s) of the
    task, in YAML format. Keyword args are of the form key=value.

    Usage sample:

    swh-scheduler --database 'service=swh-scheduler' \
        task add list-pypi

    swh-scheduler --database 'service=swh-scheduler' \
        task add list-debian-distribution --policy=oneshot distribution=stretch

    Note: if the priority is not given, the task won't have the priority set,
    which is considered as the lowest priority level.

    """
    from .utils import parse_options, task_add

    scheduler = ctx.obj["scheduler"]
    if not scheduler:
        raise ValueError("Scheduler class (local/remote) must be instantiated")

    task_type = scheduler.get_task_type(task_type_name)
    if not task_type:
        raise ValueError(f"Unknown task name {task_type_name}.")

    (args, kw) = parse_options(options)
    task_add(
        scheduler,
        task_type_name=task_type_name,
        policy=policy,
        priority=priority,
        next_run=next_run,
        args=args,
        kw=kw,
    )


def iter_origins(  # use string annotations to prevent some pkg loading
    storage: StorageInterface,
    page_token: Optional[str] = None,
) -> Iterator[swh.model.model.Origin]:
    """Iterate over origins in the storage. Optionally starting from page_token.

    This logs regularly an info message during pagination with the page_token. This, in
    order to feed it back to the cli if the process interrupted.

    Yields
        origin model objects from the storage

    """
    while True:
        page_result = storage.origin_list(page_token=page_token)
        page_token = page_result.next_page_token
        yield from page_result.results
        if not page_token:
            break
        click.echo(f"page_token: {page_token}\n")


@task.command("schedule_origins")
@click.argument("type", nargs=1, required=True)
@click.argument("options", nargs=-1)
@click.option(
    "--batch-size",
    "-b",
    "origin_batch_size",
    default=10,
    show_default=True,
    type=int,
    help="Number of origins per task",
)
@click.option(
    "--page-token",
    default=0,
    show_default=True,
    type=str,
    help="Only schedule tasks for origins whose ID is greater",
)
@click.option(
    "--limit",
    default=None,
    type=int,
    help="Limit the tasks scheduling up to this number of tasks",
)
@click.option("--storage-url", "-g", help="URL of the (graph) storage API")
@click.option(
    "--dry-run/--no-dry-run",
    is_flag=True,
    default=False,
    help="List only what would be scheduled.",
)
@click.pass_context
def schedule_origin_metadata_index(
    ctx, type, options, storage_url, origin_batch_size, page_token, limit, dry_run
):
    """Schedules tasks for origins that are already known.

    The first argument is the name of the task type, further ones are
    keyword argument(s) of the task in the form key=value, where value is
    in YAML format.

    Usage sample:

    swh-scheduler --database 'service=swh-scheduler' \
        task schedule_origins index-origin-metadata
    """
    from itertools import islice

    from swh.storage import get_storage

    from .utils import parse_options, schedule_origin_batches

    scheduler = ctx.obj["scheduler"]
    storage = get_storage("remote", url=storage_url)
    if dry_run:
        scheduler = None

    (args, kw) = parse_options(options)
    if args:
        raise click.ClickException("Only keywords arguments are allowed.")

    origins = iter_origins(storage, page_token=page_token)
    if limit:
        origins = islice(origins, limit)

    origin_urls = (origin.url for origin in origins)
    schedule_origin_batches(scheduler, type, origin_urls, origin_batch_size, kw)


@task.command("list-pending")
@click.argument("task-types", required=True, nargs=-1)
@click.option(
    "--limit",
    "-l",
    "num_tasks",
    required=False,
    type=click.INT,
    help="The maximum number of tasks to fetch",
)
@click.option(
    "--before",
    "-b",
    required=False,
    type=DATETIME,
    help="List all jobs supposed to run before the given date",
)
@click.pass_context
def list_pending_tasks(ctx, task_types, num_tasks, before):
    """List tasks with no priority that are going to be run.

    You can override the number of tasks to fetch with the --limit flag.

    """
    from .utils import pretty_print_task

    scheduler = ctx.obj["scheduler"]
    if not scheduler:
        raise ValueError("Scheduler class (local/remote) must be instantiated")

    output = []
    for task_type in task_types:
        pending = scheduler.peek_ready_tasks(
            task_type,
            timestamp=before,
            num_tasks=num_tasks,
        )
        output.append("Found %d %s tasks\n" % (len(pending), task_type))

        for task in pending:
            output.append(pretty_print_task(task))

    click.echo("\n".join(output))


@task.command("list")
@click.option(
    "--task-id",
    "-i",
    default=None,
    multiple=True,
    metavar="ID",
    help="List only tasks whose id is ID.",
)
@click.option(
    "--task-type",
    "-t",
    default=None,
    multiple=True,
    metavar="TYPE",
    help="List only tasks of type TYPE",
)
@click.option(
    "--limit",
    "-l",
    required=False,
    type=click.INT,
    help="The maximum number of tasks to fetch.",
)
@click.option(
    "--status",
    "-s",
    multiple=True,
    metavar="STATUS",
    type=click.Choice(
        ("next_run_not_scheduled", "next_run_scheduled", "completed", "disabled")
    ),
    default=None,
    help="List tasks whose status is STATUS.",
)
@click.option(
    "--policy",
    "-p",
    default=None,
    type=click.Choice(["recurring", "oneshot"]),
    help="List tasks whose policy is POLICY.",
)
@click.option(
    "--priority",
    "-P",
    default=None,
    multiple=True,
    type=click.Choice(["all", "low", "normal", "high"]),
    help="List tasks whose priority is PRIORITY.",
)
@click.option(
    "--before",
    "-b",
    required=False,
    type=DATETIME,
    metavar="DATETIME",
    help="Limit to tasks supposed to run before the given date.",
)
@click.option(
    "--after",
    "-a",
    required=False,
    type=DATETIME,
    metavar="DATETIME",
    help="Limit to tasks supposed to run after the given date.",
)
@click.option(
    "--list-runs",
    "-r",
    is_flag=True,
    default=False,
    help="Also list past executions of each task.",
)
@click.pass_context
def list_tasks(
    ctx, task_id, task_type, limit, status, policy, priority, before, after, list_runs
):
    """List tasks."""
    from operator import itemgetter

    from .utils import pretty_print_run, pretty_print_task

    scheduler = ctx.obj["scheduler"]
    if not scheduler:
        raise ValueError("Scheduler class (local/remote) must be instantiated")

    if not task_type:
        task_type = [x["type"] for x in scheduler.get_task_types()]

    # if task_id is not given, default value for status is
    #  'next_run_not_scheduled'
    # if task_id is given, default status is 'all'
    if task_id is None and status is None:
        status = ["next_run_not_scheduled"]
    if status and "all" in status:
        status = None

    if priority and "all" in priority:
        priority = None

    output = []
    tasks = scheduler.search_tasks(
        task_id=task_id,
        task_type=task_type,
        status=status,
        priority=priority,
        policy=policy,
        before=before,
        after=after,
        limit=limit,
    )
    if list_runs:
        runs = {t["id"]: [] for t in tasks}
        for r in scheduler.get_task_runs([task["id"] for task in tasks]):
            runs[r["task"]].append(r)
    else:
        runs = {}

    output.append("Found %d tasks\n" % (len(tasks)))
    for task in sorted(tasks, key=itemgetter("id")):
        output.append(pretty_print_task(task, full=True))
        if runs.get(task["id"]):
            output.append(click.style("  Executions:", bold=True))
            for run in sorted(runs[task["id"]], key=itemgetter("id")):
                output.append(pretty_print_run(run, indent=4))

    click.echo("\n".join(output))


@task.command("respawn")
@click.argument("task-ids", required=True, nargs=-1)
@click.option(
    "--next-run",
    "-n",
    required=False,
    type=DATETIME,
    metavar="DATETIME",
    default=None,
    help="Re spawn the selected tasks at this date",
)
@click.pass_context
def respawn_tasks(ctx, task_ids: List[str], next_run: datetime.datetime):
    """Respawn tasks.

    Respawn tasks given by their ids (see the 'task list' command to
    find task ids) at the given date (immediately by default).

    Eg.

       swh-scheduler task respawn 1 3 12
    """
    from swh.scheduler.utils import utcnow

    scheduler = ctx.obj["scheduler"]
    if not scheduler:
        raise ValueError("Scheduler class (local/remote) must be instantiated")
    if next_run is None:
        next_run = utcnow()
    output = []

    task_ids_int = [int(id_) for id_ in task_ids]

    scheduler.set_status_tasks(
        task_ids_int, status="next_run_not_scheduled", next_run=next_run
    )
    output.append("Respawn tasks %s\n" % (task_ids_int,))

    click.echo("\n".join(output))
