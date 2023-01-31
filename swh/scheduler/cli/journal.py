# Copyright (C) 2021-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


import logging

import click

from . import cli as cli_scheduler_group

logger = logging.getLogger(__name__)


@cli_scheduler_group.command("journal-client")
@click.pass_context
@click.option(
    "--stop-after-objects",
    "-m",
    default=None,
    type=int,
    help="Maximum number of objects to replay. Default is to run forever.",
)
def visit_stats_journal_client(ctx, stop_after_objects):
    """Keep the the origin visits stats table up to date from a swh kafka journal"""
    from functools import partial

    from swh.journal.client import get_journal_client
    from swh.scheduler.journal_client import process_journal_objects

    if not ctx.obj["scheduler"]:
        message = "Scheduler class (local/remote) must be instantiated."
        any_exception = ctx.obj.get("scheduler_exc")
        if any_exception:
            extra_message = f"Scheduler problems: {any_exception}"
            message = "\n".join([message, extra_message])
            logger.exception(any_exception)
        raise ValueError(message)

    scheduler = ctx.obj["scheduler"]
    config = ctx.obj["config"]

    if "journal" not in config:
        raise ValueError("Missing 'journal' configuration key")

    journal_cfg = config["journal"]
    journal_cfg["stop_after_objects"] = stop_after_objects or journal_cfg.get(
        "stop_after_objects"
    )

    client = get_journal_client(
        cls="kafka",
        object_types=["origin_visit_status"],
        prefix="swh.journal.objects",
        **journal_cfg,
    )
    worker_fn = partial(
        process_journal_objects,
        scheduler=scheduler,
    )
    nb_messages = 0
    try:
        nb_messages = client.process(worker_fn)
        print(f"Processed {nb_messages} message(s).")
    except KeyboardInterrupt:
        ctx.exit(0)
    else:
        print("Done.")
    finally:
        client.close()
