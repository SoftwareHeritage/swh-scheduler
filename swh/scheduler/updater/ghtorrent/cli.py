# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import click


@click.command()
@click.option('--verbose/--no-verbose', '-v', default=False,
              help='Verbose mode')
@click.pass_context
def main(ctx, verbose):
    """Consume events from ghtorrent and write them to cache.

    """
    click.echo("Deprecated! Use 'swh-scheduler updater' instead.",
               err=True)
    ctx.exit(1)


if __name__ == '__main__':
    main()
