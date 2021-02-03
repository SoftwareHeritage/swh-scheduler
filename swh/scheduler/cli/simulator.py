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
@click.option(
    "--num-origins",
    "-n",
    type=int,
    default=100000,
    help="Number of listed origins to add",
)
@click.pass_context
def fill_test_data_command(ctx, num_origins):
    """Fill the scheduler with test data for simulation purposes."""
    from swh.scheduler.simulator import fill_test_data

    click.echo(f"Filling {num_origins:,} listed origins data...")
    start = time.monotonic()
    fill_test_data(ctx.obj["scheduler"], num_origins=num_origins)
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
    default="oldest_scheduled_first",
    help="Scheduling policy to simulate (only for origin_scheduler)",
)
@click.option("--runtime", "-t", type=float, help="Simulated runtime")
@click.option(
    "--plots/--no-plots",
    "-P",
    "showplots",
    help="Show results as plots (with plotille)",
)
@click.option(
    "--csv", "-o", "csvfile", type=click.File("w"), help="Export results in a CSV file"
)
@click.pass_context
def run_command(ctx, scheduler, policy, runtime, showplots, csvfile):
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
    report = run(
        scheduler=ctx.obj["scheduler"],
        scheduler_type=scheduler,
        policy=policy,
        runtime=runtime,
    )

    print(report.format(with_plots=showplots))
    if csvfile is not None:
        report.metrics_csv(csvfile)
