# Copyright (C) 2021-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from __future__ import annotations

from typing import TYPE_CHECKING

import click

from . import cli
from ..utils import create_origin_tasks

if TYPE_CHECKING:
    from typing import Dict, Iterable, List, Optional
    from uuid import UUID

    from ..interface import SchedulerInterface, TaskPolicy
    from ..model import ListedOrigin


@cli.group("origin")
@click.pass_context
def origin(ctx):
    """Manipulate listed origins."""
    if not ctx.obj["scheduler"]:
        ctx.fail("Scheduler class (local/remote) must be instantiated")


def format_origins(
    ctx: click.Context,
    origins: List[ListedOrigin],
    fields: Optional[List[str]] = None,
    with_header: bool = True,
) -> Iterable[str]:
    """Format a list of origins as CSV.

    Arguments:
       origins: list of origins to output
       fields: optional list of fields to output (defaults to all fields)
       with_header: if True, output a CSV header.
    """
    import csv
    from io import StringIO

    import attr

    from ..model import ListedOrigin

    expected_fields = [field.name for field in attr.fields(ListedOrigin)]
    if not fields:
        fields = expected_fields

    unknown_fields = set(fields) - set(expected_fields)
    if unknown_fields:
        ctx.fail("Unknown ListedOrigin field(s): %s" % ", ".join(unknown_fields))

    output = StringIO()
    writer = csv.writer(output)

    def csv_row(data):
        """Return a single CSV-formatted row. We clear the output buffer after we're
        done to keep it reasonably sized."""
        writer.writerow(data)
        output.seek(0)
        ret = output.read().rstrip()
        output.seek(0)
        output.truncate()
        return ret

    if with_header:
        yield csv_row(fields)

    for origin in origins:
        yield csv_row(str(getattr(origin, field)) for field in fields)


@origin.command("grab-next")
@click.option(
    "--policy", "-p", default="oldest_scheduled_first", help="Scheduling policy"
)
@click.option(
    "--fields", "-f", default=None, help="Listed origin fields to print on output"
)
@click.option(
    "--with-header/--without-header",
    is_flag=True,
    default=True,
    help="Print the CSV header?",
)
@click.argument("type", type=str)
@click.argument("count", type=int)
@click.pass_context
def grab_next(
    ctx,
    policy: TaskPolicy,
    fields: Optional[str],
    with_header: bool,
    type: str,
    count: int,
):
    """Grab the next COUNT origins to visit using the TYPE loader from the
    listed origins table."""

    if fields:
        parsed_fields: Optional[List[str]] = fields.split(",")
    else:
        parsed_fields = None

    scheduler = ctx.obj["scheduler"]

    origins = scheduler.grab_next_visits(type, count, policy=policy)
    for line in format_origins(
        ctx, origins, fields=parsed_fields, with_header=with_header
    ):
        click.echo(line)


@origin.command("schedule-next")
@click.option(
    "--policy", "-p", default="oldest_scheduled_first", help="Scheduling policy"
)
@click.argument("type", type=str)
@click.argument("count", type=int)
@click.pass_context
def schedule_next(ctx, policy: TaskPolicy, type: str, count: int):
    """Send the next COUNT origin visits of the TYPE loader to the scheduler as
    one-shot tasks."""
    from ..utils import utcnow
    from .utils import pretty_print_task

    scheduler = ctx.obj["scheduler"]

    origins = scheduler.grab_next_visits(type, count, policy=policy)

    created = scheduler.create_tasks(
        [
            task.evolve(
                policy="oneshot",
                next_run=utcnow(),
                retries_left=1,
            )
            for task in create_origin_tasks(origins, scheduler)
        ]
    )

    output = ["Created %d tasks\n" % len(created)]
    for task in created:
        output.append(pretty_print_task(task))

    click.echo_via_pager("\n".join(output))


@origin.command("send-origins-from-scheduler-to-celery")
@click.option(
    "--policy", "-p", default="oldest_scheduled_first", help="Scheduling policy"
)
@click.option(
    "--queue",
    "-q",
    help="Target celery queue",
    type=str,
)
@click.option(
    "--tablesample",
    help="Table sampling percentage",
    type=float,
)
@click.option(
    "--only-enabled/--only-disabled",
    "enabled",
    is_flag=True,
    default=True,
    help="""Determine whether we want to scheduled enabled or disabled origins. As default, we
            want to reasonably deal with enabled origins. For some edge case though, we
            might want the disabled ones.""",
)
@click.option(
    "--lister-name",
    default=None,
    help="Limit origins to those listed from lister with provided name",
)
@click.option(
    "--lister-instance-name",
    default=None,
    help="Limit origins to those listed from lister with instance name",
)
@click.option(
    "--absolute-cooldown",
    "absolute_cooldown_str",
    default="12 hours",
    help="Minimal interval between two visits of the same origin",
)
@click.option(
    "--scheduled-cooldown",
    "scheduled_cooldown_str",
    default="7 days",
    help="Minimal interval to wait before scheduling the same origins again",
)
@click.option(
    "--not-found-cooldown",
    "not_found_cooldown_str",
    default="14 days",
    help="The minimal interval to wait before rescheduling not_found origins",
)
@click.option(
    "--failed-cooldown",
    "failed_cooldown_str",
    default="31 days",
    help="Minimal interval to wait before rescheduling failed origins",
)
@click.argument("visit_type_name", type=str)
@click.pass_context
def send_from_scheduler_to_celery_cli(
    ctx,
    policy: str,
    queue: Optional[str],
    tablesample: Optional[float],
    visit_type_name: str,
    enabled: bool,
    lister_name: Optional[str] = None,
    lister_instance_name: Optional[str] = None,
    absolute_cooldown_str: Optional[str] = None,
    scheduled_cooldown_str: Optional[str] = None,
    failed_cooldown_str: Optional[str] = None,
    not_found_cooldown_str: Optional[str] = None,
):
    """Send next origin visits of VISIT_TYPE_NAME to celery, filling the queue."""

    from swh.scheduler.celery_backend.utils import get_loader_task_type, send_to_celery

    from .utils import parse_time_interval

    absolute_cooldown = (
        parse_time_interval(absolute_cooldown_str) if absolute_cooldown_str else None
    )
    scheduled_cooldown = (
        parse_time_interval(scheduled_cooldown_str) if scheduled_cooldown_str else None
    )
    failed_cooldown = (
        parse_time_interval(failed_cooldown_str) if failed_cooldown_str else None
    )
    not_found_cooldown = (
        parse_time_interval(not_found_cooldown_str) if not_found_cooldown_str else None
    )

    scheduler = ctx.obj["scheduler"]

    task_type = get_loader_task_type(scheduler, visit_type_name)
    if not task_type:
        ctx.fail(f"Unknown task type {task_type}.")
        assert False  # for mypy

    queue_name = queue or task_type.backend_name

    send_to_celery(
        scheduler,
        visit_type_to_queue={visit_type_name: queue_name},
        policy=policy,
        tablesample=tablesample,
        enabled=enabled,
        lister_name=lister_name,
        lister_instance_name=lister_instance_name,
        absolute_cooldown=absolute_cooldown,
        scheduled_cooldown=scheduled_cooldown,
        failed_cooldown=failed_cooldown,
        not_found_cooldown=not_found_cooldown,
    )


@origin.command("update-metrics")
@click.option("--lister", default=None, help="Only update metrics for this lister")
@click.option(
    "--instance", default=None, help="Only update metrics for this lister instance"
)
@click.pass_context
def update_metrics(ctx, lister: Optional[str], instance: Optional[str]):
    """Update the scheduler metrics on listed origins.

    Examples:
       swh scheduler origin update-metrics
       swh scheduler origin update-metrics --lister github
       swh scheduler origin update-metrics --lister phabricator --instance llvm
    """
    import json

    scheduler: SchedulerInterface = ctx.obj["scheduler"]

    lister_id: Optional[UUID] = None
    if lister is not None:
        lister_instance = scheduler.get_lister(name=lister, instance_name=instance)
        if not lister_instance:
            click.echo(f"Lister not found: {lister} instance={instance}")
            ctx.exit(2)
            assert False  # for mypy

        lister_id = lister_instance.id

    def dictify_metrics(d):
        return {k: str(v) for (k, v) in d.to_dict().items()}

    ret = scheduler.update_metrics(lister_id=lister_id)
    click.echo(json.dumps(list(map(dictify_metrics, ret)), indent=4, sort_keys=True))


@origin.command("check-listed-origins")
@click.option(
    "--list",
    "-l",
    is_flag=True,
    help="Display listed origins (disabled by default).",
    required=False,
)
@click.argument("lister_name", nargs=1, required=True)
@click.argument("instance_name", nargs=1, required=True)
@click.pass_context
def check_listed_origins_cli(ctx, list, lister_name, instance_name):
    """
    Check listed origins registered in the scheduler database.
    """

    from tabulate import tabulate

    from .utils import check_listed_origins

    scheduler = ctx.obj["scheduler"]
    listed_origins = check_listed_origins(
        scheduler=scheduler,
        lister_name=lister_name,
        instance_name=instance_name,
    )
    listed_origins_table = [
        (origin.url, str(origin.last_seen), str(origin.last_update))
        for origin in listed_origins
    ]
    headers = ["url", "last_seen", "last_update"]

    if list:
        print(tabulate(listed_origins_table, headers))
    print(
        f"\nForge {instance_name} ({lister_name}) has {len(listed_origins)} \
listed origins in the scheduler database."
    )


def _print_status_summary(
    lister_name: str, instance_name: str, status_counters: Dict, watch: bool = False
) -> None:
    """Print a status of the ingestion and a small success rate summary."""
    if watch:
        suffix_str = " ingestion is still in progress..."
    else:
        suffix_str = f" ({lister_name}) has {status_counters['total']} \
scheduled ingests in the scheduler."

    print(f"\nForge {instance_name}{suffix_str}")
    for status, counter in status_counters.items():
        print(f"{status:<12}: {counter}")

    success_rate = status_counters["successful"] / status_counters["total"] * 100
    print(f"{'success rate':<12}: {success_rate:.2f}%")


@origin.command("check-ingested-origins")
@click.option(
    "--list",
    "-l",
    "with_listing",
    is_flag=True,
    help="Display listed origins (disabled by default).",
    required=False,
)
@click.option(
    "--watch",
    "-w",
    is_flag=True,
    required=False,
    help="Watch periodically for ingestion status.",
)
@click.option(
    "--watch-period",
    required=False,
    default="30 minutes",
    help="Watch period ingestion.",
)
@click.argument("lister_name", nargs=1, required=True)
@click.argument("instance_name", nargs=1, required=True)
@click.pass_context
def check_ingested_origins_cli(
    ctx, with_listing, watch, watch_period, lister_name, instance_name
):
    """
    Check the origins marked as ingested in the scheduler database.
    """

    from time import sleep

    from .utils import check_listed_origins, count_ingested_origins, parse_time_interval

    scheduler = ctx.obj["scheduler"]
    listed_origins = check_listed_origins(
        scheduler=scheduler,
        lister_name=lister_name,
        instance_name=instance_name,
    )
    ids = [(origin.url, origin.visit_type) for origin in listed_origins]
    status_counters, _ = count_ingested_origins(
        scheduler=scheduler,
        ids=ids,
        instance_name=instance_name,
    )

    if watch:
        watch_period_seconds = parse_time_interval(watch_period).total_seconds()
        while status_counters["None"] != 0:
            status_counters, _ = count_ingested_origins(
                scheduler=scheduler,
                ids=ids,
                instance_name=instance_name,
            )
            if status_counters["None"] > 0:
                _print_status_summary(
                    lister_name, instance_name, status_counters, watch
                )

            sleep(watch_period_seconds)

    status_counters, ingested_origins_table = count_ingested_origins(
        scheduler=scheduler,
        ids=ids,
        instance_name=instance_name,
        with_listing=with_listing,
    )

    if with_listing:
        from tabulate import tabulate

        headers = ("url", "last_visit_status", "last_visit")
        print(tabulate(ingested_origins_table, headers))

    _print_status_summary(lister_name, instance_name, status_counters)


@origin.command("send-origins-from-file-to-celery")
@click.option(
    "--queue-name-prefix",
    help=(
        "Prefix to add to the default queue name (if needed). Usually needed to treat "
        "special origins (e.g. large repositories, fill-in-the-hole datasets, ... ) "
        "in another dedicated queue."
    ),
)
@click.option(
    "--threshold",
    help="Threshold override for the queue.",
    type=click.INT,
    default=1000,
)
@click.option(
    "--limit",
    help="Number of origins to send. Usually to limit to a small number for debug purposes.",
    type=click.INT,
    default=None,
)
@click.option(
    "--waiting-period",
    help="Waiting time between checks",
    type=click.INT,
    default=10,
)
@click.option("--dry-run", is_flag=True, help="Print only messages to send to celery")
@click.option("--debug", is_flag=True, default=False, help="Print extra messages")
# Scheduler task type (e.g. load-git, load-svn, ...)
@click.argument(
    "task_type",
    nargs=1,
    required=True,
)
# Dataset file of origins to schedule, use '-' when piping to the cli.
@click.argument(
    "file_input",
    type=click.File("r"),
    required=False,
    default="-",
)
# Extra options passed directly to the task
@click.argument(
    "options",
    nargs=-1,
    required=False,
)
@click.pass_context
def send_origins_from_file_to_celery(
    ctx,
    queue_name_prefix,
    threshold,
    waiting_period,
    dry_run,
    task_type,
    options,
    debug,
    limit,
    file_input,
):
    """Send origins directly from file/stdin to celery, filling the queue according to
    its standard configuration (and some optional adjustments).

    Arguments:

        TASK_TYPE: Scheduler task type (e.g. load-git, load-svn, ...)

        INPUT: Dataset file of origins to schedule, use '-' when piping to the cli.

        OPTIONS: Extra options (in the key=value form, e.g. base_git_url=<foo>) passed
                 directly to the task to be scheduled

    """
    from functools import partial
    from time import sleep

    from swh.scheduler.celery_backend.config import app, get_available_slots
    from swh.scheduler.cli.utils import parse_options

    from .origin_utils import (
        TASK_ARGS_GENERATOR_CALLABLES,
        get_scheduler_task_type,
        lines_to_task_args,
    )

    scheduler = ctx.obj["scheduler"]

    try:
        task_type_info = get_scheduler_task_type(scheduler, task_type)
    except ValueError as e:
        ctx.fail(e)

    if debug:
        click.echo(f"Scheduler task information: {task_type_info}")

    celery_task_name = task_type_info.backend_name
    if queue_name_prefix:
        queue_name = f"{queue_name_prefix}:{celery_task_name}"
    else:
        queue_name = celery_task_name

    if debug:
        click.echo(f"Destination queue: {queue_name}")

    if not threshold:
        threshold = task_type_info.max_queue_length

    # Compute the callable function to generate the tasks to schedule. Tasks are read
    # out of the file_input.
    task_fn = partial(
        TASK_ARGS_GENERATOR_CALLABLES.get(
            task_type, partial(lines_to_task_args, columns=["url"])
        ),
        lines=file_input,
    )

    (extra_args, extra_kwargs) = parse_options(options)

    if debug:
        print(f"Task extra_args: {extra_args}")
        print(f"task Extra_kwargs: {extra_kwargs}")

    limit_reached = False
    while True:
        throttled = False
        remains_data = False
        pending_tasks = []

        if limit_reached:
            break

        # we can send new tasks, compute how many we can send
        nb_tasks_to_send = get_available_slots(app, queue_name, threshold)

        if debug:
            click.echo(f"Nb tasks to send : {nb_tasks_to_send}")

        if nb_tasks_to_send > 0:
            count = 0
            for _task in task_fn(**extra_kwargs):
                pending_tasks.append(_task)
                count += 1

                if debug:
                    click.echo(f"Task: {_task}")

                if limit is not None and count >= limit:
                    click.echo(f"Limit {limit} messages specified reached, stopping.")
                    limit_reached = True
                    break

                if count >= nb_tasks_to_send:
                    throttled = True
                    remains_data = True
                    break

            if not pending_tasks:
                # check for some more data on stdin
                if not remains_data:
                    # if no more data, we break to exit
                    break

            from kombu.utils.uuid import uuid

            for _task_args in pending_tasks:
                send_task_kwargs = dict(
                    name=celery_task_name,
                    task_id=uuid(),
                    args=tuple(_task_args["args"]) + tuple(extra_args),
                    kwargs=_task_args["kwargs"],
                    queue=queue_name,
                )
                if dry_run:
                    click.echo(
                        f"** DRY-RUN ** Call app.send_task with: {send_task_kwargs}"
                    )
                else:
                    app.send_task(**send_task_kwargs)
                    click.echo(send_task_kwargs)

        else:
            throttled = True

        if throttled:
            sleep(waiting_period)
