# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import time

import click

from . import cli


@cli.group("simulator")
def simulator():
    """Scheduler simulator."""
    pass


@simulator.command("fill-test-data")
@click.pass_context
def fill_test_data_command(ctx):
    """Fill the scheduler with test data for simulation purposes."""
    from swh.scheduler.simulator import fill_test_data

    click.echo("Filling test data...")
    start = time.monotonic()
    fill_test_data(ctx.obj["scheduler"])
    runtime = time.monotonic() - start
    click.echo(f"Completed in {runtime:.2f} seconds")


@simulator.command("run")
@click.option(
    "--scheduler",
    "-s",
    type=click.Choice(["task_scheduler", "origin_scheduler"]),
    default="origin_scheduler",
    help="Scheduler to simulate",
)
@click.option(
    "--policy",
    "-p",
    type=click.Choice(["oldest_scheduled_first"]),
    default="oldest_scheduled_first",
    help="Scheduling policy to simulate (only for origin_scheduler)",
)
@click.option("--runtime", "-t", type=float, help="Simulated runtime")
@click.pass_context
def run_command(ctx, scheduler, policy, runtime):
    """Run the scheduler simulator.

    By default, the simulation runs forever. You can cap the simulated runtime
    with the --runtime option, and you can always press Ctrl+C to interrupt the
    running simulation.

    'task_scheduler' is the "classic" task-based scheduler; 'origin_scheduler'
    is the new origin-visit-aware simulator. The latter uses --policy to decide
    which origins to schedule first based on information from listers.
    """
    from swh.scheduler.simulator import run

    policy = policy if scheduler == "origin_scheduler" else None
    run(
        scheduler=ctx.obj["scheduler"],
        scheduler_type=scheduler,
        policy=policy,
        runtime=runtime,
    )
