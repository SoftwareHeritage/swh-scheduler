# Copyright (C) 2023 the Software Heritage developers
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from celery import shared_task


@shared_task(name=__name__ + ".FooListerTask")
def list_foo_full(**lister_args):
    """List foo"""
    pass


@shared_task(name=__name__ + ".ping")
def _ping():
    return "OK"
