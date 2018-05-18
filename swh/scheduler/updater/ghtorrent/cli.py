# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import click
import logging

from swh.scheduler.updater.ghtorrent import GHTorrentConsumer


@click.command()
@click.option('--verbose/--no-verbose', '-v', default=False,
              help='Verbose mode')
def main(verbose):
    """Consume events from ghtorrent and write them to cache.

    """
    log = logging.getLogger('swh.scheduler.updater.ghtorrent.cli')
    log.addHandler(logging.StreamHandler())
    _loglevel = logging.DEBUG if verbose else logging.INFO
    log.setLevel(_loglevel)

    GHTorrentConsumer().run()


if __name__ == '__main__':
    main()
