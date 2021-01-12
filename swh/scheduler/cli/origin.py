# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from __future__ import annotations

from typing import TYPE_CHECKING, Iterable, List, Optional

import click

from . import cli

if TYPE_CHECKING:
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
@click.argument("count", type=int)
@click.pass_context
def grab_next(ctx, policy: str, fields: Optional[str], with_header: bool, count: int):
    """Grab the next COUNT origins to visit from the listed origins table."""

    if fields:
        parsed_fields: Optional[List[str]] = fields.split(",")
    else:
        parsed_fields = None

    scheduler = ctx.obj["scheduler"]

    origins = scheduler.grab_next_visits(count, policy=policy)
    for line in format_origins(origins, fields=parsed_fields, with_header=with_header):
        click.echo(line)
