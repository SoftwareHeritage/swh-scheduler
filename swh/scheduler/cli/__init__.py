# Copyright (C) 2016-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

# WARNING: do not import unnecessary things here to keep cli startup time under
# control
import logging

import click

from swh.core.cli import CONTEXT_SETTINGS, AliasedGroup
from swh.core.cli import swh as swh_cli_group

# If you're looking for subcommand imports, they are further down this file to
# avoid a circular import!


@swh_cli_group.group(
    name="scheduler", context_settings=CONTEXT_SETTINGS, cls=AliasedGroup
)
@click.option(
    "--config-file",
    "-C",
    default=None,
    type=click.Path(
        exists=True,
        dir_okay=False,
    ),
    help=(
        "Configuration file. This has a higher priority than SWH_CONFIG_FILENAME "
        "environment variable if set. "
    ),
)
@click.option(
    "--database",
    "-d",
    default=None,
    help="Scheduling database DSN (imply cls is 'postgresql')",
)
@click.option(
    "--url", "-u", default=None, help="Scheduler's url access (imply cls is 'remote')"
)
@click.option(
    "--no-stdout", is_flag=True, default=False, help="Do NOT output logs on the console"
)
@click.pass_context
def cli(ctx, config_file, database, url, no_stdout):
    """Software Heritage Scheduler tools.

    Use a local scheduler instance by default (plugged to the
    main scheduler db).

    Expected configuration:

    * :ref:`cli-config-scheduler`
    """
    try:
        from psycopg import OperationalError
    except ImportError:

        class OperationalError(Exception):
            pass

    from swh.scheduler import get_scheduler
    from swh.scheduler.cli.config import read_config

    ctx.ensure_object(dict)

    logger = logging.getLogger(__name__)
    scheduler = None

    # Read configuration out of config_file or SWH_CONFIG_FILENAME environment variable
    conf = read_config(config_file)

    if "scheduler" not in conf:
        ctx.fail("missing 'scheduler' configuration")

    if database:
        conf["scheduler"]["cls"] = "postgresql"
        conf["scheduler"]["db"] = database
    elif url:
        conf["scheduler"]["cls"] = "remote"
        conf["scheduler"]["url"] = url
    sched_conf = conf["scheduler"]
    try:
        logger.debug("Instantiating scheduler with %s", sched_conf)
        scheduler = get_scheduler(**sched_conf)

    except (ValueError, OperationalError) as e:
        # Propagate scheduler instantiation exception context to subcommands, and let
        # them report properly the issue
        ctx.obj["scheduler_exc"] = e

    ctx.obj["scheduler"] = scheduler
    ctx.obj["config"] = conf


from . import (  # noqa
    add_forge_now,
    admin,
    celery_monitor,
    journal,
    origin,
    simulator,
    task,
    task_type,
)


def main():
    import click.core

    click.core.DEPRECATED_HELP_NOTICE = """

DEPRECATED! Please use the command 'swh scheduler'."""
    cli.deprecated = True
    return cli(auto_envvar_prefix="SWH_SCHEDULER")


if __name__ == "__main__":
    main()
