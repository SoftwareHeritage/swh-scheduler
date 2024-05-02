# Copyright (C) 2016-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from __future__ import annotations

from datetime import timedelta
from importlib import import_module
import logging
from typing import TYPE_CHECKING, Any, Dict

# WARNING: do not import unnecessary things here to keep cli startup time under
# control
import click
from pkg_resources import iter_entry_points

from swh.scheduler.model import TaskType

if TYPE_CHECKING:
    from swh.scheduler.task import SWHTask

from . import cli

logger = logging.getLogger(__name__)


DEFAULT_TASK_TYPE_PARAMETERS = {
    "full": dict(  # for tasks like 'list_xxx_full()'
        default_interval=timedelta(days=90),
        min_interval=timedelta(days=90),
        max_interval=timedelta(days=90),
        backoff_factor=1.0,
    ),
    "*": dict(  # value if not suffix matches
        default_interval=timedelta(days=1),
        min_interval=timedelta(days=1),
        max_interval=timedelta(days=1),
        backoff_factor=1.0,
    ),
}


def _plugin_worker_descriptions():
    return {
        entry_point.name: entry_point
        for entry_point in iter_entry_points("swh.workers")
    }


@cli.group("task-type")
@click.pass_context
def task_type(ctx):
    """Manipulate task types.

    Expected configuration:

    \b
    * :ref:`cli-config-scheduler`
    """
    scheduler = ctx.obj["scheduler"]
    if not scheduler:
        ctx.fail("Scheduler class (local/remote) must be instantiated")


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
    from operator import attrgetter

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
        ctx.obj["scheduler"].get_task_types(), key=attrgetter("type")
    ):
        if task_type and tasktype.type not in task_type:
            continue
        if task_name and tasktype.backend_name not in task_name:
            continue
        click.echo(tmpl.format(**tasktype.to_dict()))


@task_type.command("register")
@click.option(
    "--plugins",
    "-p",
    "plugins",
    multiple=True,
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

    plugin_worker_descriptions = _plugin_worker_descriptions()
    plugin_names = list(sorted(plugin_worker_descriptions))

    if not plugins or plugins == ("all",):
        plugins = plugin_names
    else:
        unknown_plugins = [plugin for plugin in plugins if plugin not in plugin_names]
        if unknown_plugins:
            if len(unknown_plugins) == 1:
                error_msg = f"That provided plugin is unknown: {unknown_plugins[0]}."
            else:
                error_msg = (
                    f"Those provided plugins are unknown: {', '.join(unknown_plugins)}."
                )
            ctx.fail(f"{error_msg}\nAvailable ones are: {', '.join(plugin_names)}.")

    for plugin in plugins:
        entrypoint = plugin_worker_descriptions[plugin]
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
    task_module: str,
    task_type_name: str,
    swhtask: SWHTask,
    task_config: Dict[str, Any],
    scheduler,
):
    """Ensure a given task-type (for the task_module) exists in the scheduler.

    Args:
        task_module: task module we are currently checking for task type
            consistency
        task_type: the type of the task to check/insert (correspond to
            the 'type' field in the db)
        swhtask: the SWHTask instance the task-type correspond to
        task_config: a dict with specific/overloaded values for the
            task-type to be created
        scheduler: the scheduler object used to access the scheduler db

    """
    task_type_params = dict(
        type=task_type_name,
        backend_name=swhtask.name,
        description=swhtask.__doc__,
    )
    for suffix, defaults in DEFAULT_TASK_TYPE_PARAMETERS.items():
        if task_type_name.endswith("-" + suffix):
            task_type_params.update(defaults)
            break
    else:
        task_type_params.update(DEFAULT_TASK_TYPE_PARAMETERS["*"])

    task_type_params.update(task_config)

    task_type = TaskType(**task_type_params)

    current_task_type = scheduler.get_task_type(task_type_name)
    if current_task_type:
        # Ensure the existing task_type is consistent in the scheduler
        if current_task_type.backend_name != task_type.backend_name:
            logger.warning(
                "Existing task type %s for module %s has a "
                "different backend name than current "
                "code version provides (%s vs. %s)",
                task_type,
                task_module,
                current_task_type.backend_name,
                task_type.backend_name,
            )
    else:
        logger.info("Create task type %s in scheduler", task_type_name)
        logger.debug("  %s", task_type)
        scheduler.create_task_type(task_type)


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
    """Create a new task type"""
    task_type = TaskType(
        type=type,
        backend_name=task_name,
        description=description,
        default_interval=default_interval,
        min_interval=min_interval,
        max_interval=max_interval,
        backoff_factor=backoff_factor,
    )
    ctx.obj["scheduler"].create_task_type(task_type)
    click.echo("OK")
