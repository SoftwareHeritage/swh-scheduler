# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import click

from swh.scheduler.updater.ghtorrent import GHTorrentConsumer


@click.command()
def main():
    """Consume events from ghtorrent and write them to cache.

    """
    GHTorrentConsumer().run()


if __name__ == '__main__':
    main()
