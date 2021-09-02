# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from __future__ import annotations

from typing import TYPE_CHECKING, Iterable, List, Optional

import click

from . import cli

if TYPE_CHECKING:
    from uuid import UUID

    from ..interface import SchedulerInterface
    from ..model import ListedOrigin


@cli.group("origin")
@click.pass_context
def origin(ctx):
    """Manipulate listed origins."""
    if not ctx.obj["scheduler"]:
        raise ValueError("Scheduler class (local/remote) must be instantiated")


def format_origins(
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
        raise ValueError(
            "Unknown ListedOrigin field(s): %s" % ", ".join(unknown_fields)
        )

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
    ctx, policy: str, fields: Optional[str], with_header: bool, type: str, count: int
):
    """Grab the next COUNT origins to visit using the TYPE loader from the
    listed origins table."""

    if fields:
        parsed_fields: Optional[List[str]] = fields.split(",")
    else:
        parsed_fields = None

    scheduler = ctx.obj["scheduler"]

    origins = scheduler.grab_next_visits(type, count, policy=policy)
    for line in format_origins(origins, fields=parsed_fields, with_header=with_header):
        click.echo(line)


@origin.command("schedule-next")
@click.option(
    "--policy", "-p", default="oldest_scheduled_first", help="Scheduling policy"
)
@click.argument("type", type=str)
@click.argument("count", type=int)
@click.pass_context
def schedule_next(ctx, policy: str, type: str, count: int):
    """Send the next COUNT origin visits of the TYPE loader to the scheduler as
    one-shot tasks."""
    from ..utils import utcnow
    from .task import pretty_print_task

    scheduler = ctx.obj["scheduler"]

    origins = scheduler.grab_next_visits(type, count, policy=policy)

    created = scheduler.create_tasks(
        [
            {
                **origin.as_task_dict(),
                "policy": "oneshot",
                "next_run": utcnow(),
                "retries_left": 1,
            }
            for origin in origins
        ]
    )

    output = ["Created %d tasks\n" % len(created)]
    for task in created:
        output.append(pretty_print_task(task))

    click.echo_via_pager("\n".join(output))


@origin.command("send-to-celery")
@click.option(
    "--policy", "-p", default="oldest_scheduled_first", help="Scheduling policy"
)
@click.option(
    "--queue", "-q", help="Target celery queue", type=str,
)
@click.option(
    "--tablesample", help="Table sampling percentage", type=float,
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
    "--lister-uuid",
    default=None,
    help="Limit origins to those listed from such lister",
)
@click.argument("type", type=str)
@click.pass_context
def send_to_celery(
    ctx,
    policy: str,
    queue: Optional[str],
    tablesample: Optional[float],
    type: str,
    enabled: bool,
    lister_uuid: Optional[str] = None,
):
    """Send the next origin visits of the TYPE loader to celery, filling the queue."""
    from kombu.utils.uuid import uuid

    from swh.scheduler.celery_backend.config import app, get_available_slots

    scheduler = ctx.obj["scheduler"]

    task_type = scheduler.get_task_type(f"load-{type}")

    task_name = task_type["backend_name"]
    queue_name = queue or task_name

    num_tasks = get_available_slots(app, queue_name, task_type["max_queue_length"])

    click.echo(f"{num_tasks} slots available in celery queue")
    origins = scheduler.grab_next_visits(
        type,
        num_tasks,
        policy=policy,
        tablesample=tablesample,
        enabled=enabled,
        lister_uuid=lister_uuid,
    )

    click.echo(f"{len(origins)} visits to send to celery")
    for origin in origins:
        task_dict = origin.as_task_dict()
        app.send_task(
            task_name,
            task_id=uuid(),
            args=task_dict["arguments"]["args"],
            kwargs=task_dict["arguments"]["kwargs"],
            queue=queue_name,
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

    import attr

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
        return {k: str(v) for (k, v) in attr.asdict(d).items()}

    ret = scheduler.update_metrics(lister_id=lister_id)
    click.echo(json.dumps(list(map(dictify_metrics, ret)), indent=4, sort_keys=True))
