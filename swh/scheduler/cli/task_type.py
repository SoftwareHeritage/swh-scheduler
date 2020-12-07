# Copyright (C) 2016-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from importlib import import_module
import logging
from typing import Mapping

# WARNING: do not import unnecessary things here to keep cli startup time under
# control
import click
from pkg_resources import iter_entry_points

from . import cli

logger = logging.getLogger(__name__)


DEFAULT_TASK_TYPE = {
    "full": {  # for tasks like 'list_xxx_full()'
        "default_interval": "90 days",
        "min_interval": "90 days",
        "max_interval": "90 days",
        "backoff_factor": 1,
    },
    "*": {  # value if not suffix matches
        "default_interval": "1 day",
        "min_interval": "1 day",
        "max_interval": "1 day",
        "backoff_factor": 1,
    },
}


PLUGIN_WORKER_DESCRIPTIONS = {
    entry_point.name: entry_point for entry_point in iter_entry_points("swh.workers")
}


@cli.group("task-type")
@click.pass_context
def task_type(ctx):
    """Manipulate task types."""
    scheduler = ctx.obj["scheduler"]
    if not scheduler:
        raise ValueError("Scheduler class (local/remote) must be instantiated")


@task_type.command("list")
@click.option("--verbose", "-v", is_flag=True, default=False, help="Verbose mode")
@click.option(
    "--task_type",
    "-t",
    multiple=True,
    default=None,
    help="List task types of given type",
)
@click.option(
    "--task_name",
    "-n",
    multiple=True,
    default=None,
    help="List task types of given backend task name",
)
@click.pass_context
def list_task_types(ctx, verbose, task_type, task_name):
    click.echo("Known task types:")
    if verbose:
        tmpl = (
            click.style("{type}: ", bold=True)
            + """{backend_name}
  {description}
  interval: {default_interval} [{min_interval}, {max_interval}]
  backoff_factor: {backoff_factor}
  max_queue_length: {max_queue_length}
  num_retries: {num_retries}
  retry_delay: {retry_delay}
"""
        )
    else:
        tmpl = "{type}:\n  {description}"
    for tasktype in sorted(
        ctx.obj["scheduler"].get_task_types(), key=lambda x: x["type"]
    ):
        if task_type and tasktype["type"] not in task_type:
            continue
        if task_name and tasktype["backend_name"] not in task_name:
            continue
        click.echo(tmpl.format(**tasktype))


@task_type.command("register")
@click.option(
    "--plugins",
    "-p",
    "plugins",
    multiple=True,
    default=("all",),
    type=click.Choice(["all"] + list(PLUGIN_WORKER_DESCRIPTIONS)),
    help="Registers task-types for provided plugins. " "Defaults to all",
)
@click.pass_context
def register_task_types(ctx, plugins):
    """Register missing task-type entries in the scheduler.

    According to declared tasks in each loaded worker (e.g. lister, loader,
    ...) plugins.

    """
    import celery.app.task

    scheduler = ctx.obj["scheduler"]

    if plugins == ("all",):
        plugins = list(PLUGIN_WORKER_DESCRIPTIONS)

    for plugin in plugins:
        entrypoint = PLUGIN_WORKER_DESCRIPTIONS[plugin]
        logger.info("Loading entrypoint for plugin %s", plugin)
        registry_entry = entrypoint.load()()

        for task_module in registry_entry["task_modules"]:
            mod = import_module(task_module)
            for task_name in (x for x in dir(mod) if not x.startswith("_")):
                logger.debug("Loading task name %s", task_name)
                taskobj = getattr(mod, task_name)
                if isinstance(taskobj, celery.app.task.Task):
                    tt_name = task_name.replace("_", "-")
                    task_cfg = registry_entry.get("task_types", {}).get(tt_name, {})
                    ensure_task_type(task_module, tt_name, taskobj, task_cfg, scheduler)


def ensure_task_type(
    task_module: str, task_type: str, swhtask, task_config: Mapping, scheduler
):
    """Ensure a given task-type (for the task_module) exists in the scheduler.

    Args:
        task_module: task module we are currently checking for task type
            consistency
        task_type: the type of the task to check/insert (correspond to
            the 'type' field in the db)
        swhtask (SWHTask): the SWHTask instance the task-type correspond to
        task_config: a dict with specific/overloaded values for the
            task-type to be created
        scheduler: the scheduler object used to access the scheduler db

    """
    for suffix, defaults in DEFAULT_TASK_TYPE.items():
        if task_type.endswith("-" + suffix):
            task_type_dict = defaults.copy()
            break
    else:
        task_type_dict = DEFAULT_TASK_TYPE["*"].copy()

    task_type_dict["type"] = task_type
    task_type_dict["backend_name"] = swhtask.name
    if swhtask.__doc__:
        task_type_dict["description"] = swhtask.__doc__.splitlines()[0]

    task_type_dict.update(task_config)

    current_task_type = scheduler.get_task_type(task_type)
    if current_task_type:
        # Ensure the existing task_type is consistent in the scheduler
        if current_task_type["backend_name"] != task_type_dict["backend_name"]:
            logger.warning(
                "Existing task type %s for module %s has a "
                "different backend name than current "
                "code version provides (%s vs. %s)",
                task_type,
                task_module,
                current_task_type["backend_name"],
                task_type_dict["backend_name"],
            )
    else:
        logger.info("Create task type %s in scheduler", task_type)
        logger.debug("  %s", task_type_dict)
        scheduler.create_task_type(task_type_dict)


@task_type.command("add")
@click.argument("type", required=True)
@click.argument("task-name", required=True)
@click.argument("description", required=True)
@click.option(
    "--default-interval",
    "-i",
    default="90 days",
    help='Default interval ("90 days" by default)',
)
@click.option(
    "--min-interval",
    default=None,
    help="Minimum interval (default interval if not set)",
)
@click.option(
    "--max-interval",
    "-i",
    default=None,
    help="Maximal interval (default interval if not set)",
)
@click.option("--backoff-factor", "-f", type=float, default=1, help="Backoff factor")
@click.pass_context
def add_task_type(
    ctx,
    type,
    task_name,
    description,
    default_interval,
    min_interval,
    max_interval,
    backoff_factor,
):
    """Create a new task type
    """
    task_type = dict(
        type=type,
        backend_name=task_name,
        description=description,
        default_interval=default_interval,
        min_interval=min_interval,
        max_interval=max_interval,
        backoff_factor=backoff_factor,
        max_queue_length=None,
        num_retries=None,
        retry_delay=None,
    )
    ctx.obj["scheduler"].create_task_type(task_type)
    click.echo("OK")
