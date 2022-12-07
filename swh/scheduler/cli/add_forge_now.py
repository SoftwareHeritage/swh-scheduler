# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

# WARNING: do not import unnecessary things here to keep cli startup time under
# control

from __future__ import annotations

from typing import TYPE_CHECKING

import click

from . import cli

if TYPE_CHECKING:
    from typing import Dict, List, Optional


@cli.group("add-forge-now")
@click.option(
    "-p",
    "--preset",
    "preset",
    default="production",
    type=click.Choice(["production", "staging"]),
    help='Determine preset to use, "production" by default.',
)
@click.pass_context
def add_forge_now(ctx, preset):
    """Manipulate add-forge-now requests."""
    if not ctx.obj["scheduler"]:
        raise ValueError("Scheduler class (local/remote) must be instantiated")

    ctx.obj["preset"] = preset


@add_forge_now.command("register-lister")
@click.argument("lister_name", nargs=1, required=True)
@click.argument("options", nargs=-1)
@click.pass_context
def register_lister_cli(
    ctx,
    lister_name,
    options,
):
    """Register the lister tasks in the scheduler.

    The specifics of what tasks are registered depends on the add-forge-now --preset
    option:
    - staging preset: a single oneshot full listing task is scheduled. This "full"
      listing is limited to 3 pages and 10 origins per page. The origins are recorded as
      disabled (to avoid their recurrent loading).
    - production preset: a recurrent full and incremental (if the loader has such a
      task) listing task are scheduled. The first run of the full lister is scheduled
      immediately, and the first run of the incremental lister is delayed by a day.

    """
    from .utils import lister_task_type, parse_options, task_add

    scheduler = ctx.obj["scheduler"]
    preset = ctx.obj["preset"]

    # Map the associated task types for the lister
    task_type_names: Dict[str, str] = {
        listing_type: lister_task_type(lister_name, listing_type)
        for listing_type in ["full", "incremental"]
    }

    task_types: Dict[str, Dict] = {}
    for listing_type, task_type_name in task_type_names.items():
        task_type = scheduler.get_task_type(task_type_name)
        if task_type:
            task_types[listing_type] = task_type

    if not task_types:
        raise ValueError(f"Unknown lister type {lister_name}.")

    (args, kw) = parse_options(options)

    # Recurring policy on production
    if preset == "production":
        policy = "recurring"
    else:  # staging, "full" but limited listing as a oneshot
        policy = "oneshot"
        kw.update({"max_pages": 3, "max_origins_per_page": 10, "enable_origins": False})
        # We want a "full" listing in production if both incremental and full exists
        if "full" in task_types:
            task_types.pop("incremental", None)

    from datetime import timedelta

    from swh.scheduler.utils import utcnow

    for listing_type, task_type in task_types.items():
        now = utcnow()
        next_run = now if listing_type == "full" else now + timedelta(days=1)
        task_add(
            scheduler,
            task_type_name=task_type["type"],
            args=args,
            kw=kw,
            policy=policy,
            next_run=next_run,
        )


@add_forge_now.command("schedule-first-visits")
@click.option(
    "--type-name",
    "-t",
    "visit_type_names",
    help="Visit/loader type (can be provided multiple times)",
    type=str,
    multiple=True,
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
@click.pass_context
def schedule_first_visits_cli(
    ctx,
    visit_type_names: List[str],
    lister_name: Optional[str] = None,
    lister_instance_name: Optional[str] = None,
):
    """Send next origin visits of VISIT_TYPE_NAME(S) loader to celery, filling the
    associated add_forge_now queue(s).

    """
    from .utils import get_task_type, send_to_celery

    scheduler = ctx.obj["scheduler"]
    preset = ctx.obj["preset"]

    visit_type_to_queue: Dict[str, str] = {}
    unknown_task_types = []
    for visit_type_name in visit_type_names:
        task_type = get_task_type(scheduler, visit_type_name)
        if not task_type:
            unknown_task_types.append(visit_type_name)
            continue
        queue_name = task_type["backend_name"]
        visit_type_to_queue[visit_type_name] = f"add_forge_now:{queue_name}"

    if unknown_task_types:
        raise ValueError(f"Unknown task types {','.join(unknown_task_types)}.")

    send_to_celery(
        scheduler,
        visit_type_to_queue=visit_type_to_queue,
        enabled=preset == "production",
        lister_name=lister_name,
        lister_instance_name=lister_instance_name,
    )
