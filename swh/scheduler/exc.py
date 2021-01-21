# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

__all__ = [
    "SchedulerException",
    "StaleData",
    "UnknownPolicy",
]


class SchedulerException(Exception):
    pass


class StaleData(SchedulerException):
    pass


class UnknownPolicy(SchedulerException):
    pass
