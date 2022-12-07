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
@click.pass_context
def add_forge_now(ctx):
    """Manipulate listed origins."""
    if not ctx.obj["scheduler"]:
        raise ValueError("Scheduler class (local/remote) must be instantiated")


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
    "--production/--staging",
    "enabled",
    is_flag=True,
    default=True,
    help="""Determine whether we want to scheduled enabled origins (on production) or
            disabled ones (on staging).""",
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
    enabled: bool,
    lister_name: Optional[str] = None,
    lister_instance_name: Optional[str] = None,
):
    """Send next origin visits of VISIT_TYPE_NAME(S) loader to celery, filling the
    associated add_forge_now queue(s).

    """
    from .utils import get_task_type, send_to_celery

    scheduler = ctx.obj["scheduler"]

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
        enabled=enabled,
        lister_name=lister_name,
        lister_instance_name=lister_instance_name,
    )
